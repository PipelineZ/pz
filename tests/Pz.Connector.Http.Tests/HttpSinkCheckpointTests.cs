using System.Text.Json.Nodes;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

/// <summary>Merge = keyed per-row PUT/PATCH (idempotent), and the checkpoint
/// contract — acknowledgment counts only 2xx-confirmed rows, TryGetAcknowledgedRows dedups,
/// TryResumeFrom folds the prefix into commit totals.</summary>
public sealed class HttpSinkCheckpointTests : IAsyncLifetime
{
    private StubHttpServer _server = null!;

    public Task InitializeAsync()
    {
        _server = new StubHttpServer();
        return Task.CompletedTask;
    }

    public async Task DisposeAsync() => await _server.DisposeAsync();

    private static readonly Schema TestSchema = new(
    [
        new Field("id", Int64Type.Default, nullable: false),
        new Field("name", StringType.Default, nullable: false),
    ], null);

    private ConnectorConfig Config() => new(new Dictionary<string, object?> { ["base_url"] = _server.BaseUrl.ToString() });

    private static RecordBatch Rows(long startId, int count)
    {
        var builder = new ArrowBatchBuilder(TestSchema);
        for (var i = 0; i < count; i++)
        {
            builder.AppendRow([startId + i, $"row-{startId + i}"]);
        }

        return builder.Flush()!;
    }

    private async Task<ISinkWriteSession> OpenSessionAsync(OutputSpec spec)
    {
        ISinkConnector connector = new HttpConnector();
        var sink = await connector.OpenAsync(Config(), CancellationToken.None);
        return await sink.BeginWriteAsync(spec, TestSchema, CancellationToken.None);
    }

    [Fact]
    public void Capabilities_declare_merge_and_checkpointable_writes()
    {
        var capabilities = new HttpConnector().Capabilities;
        Assert.True(capabilities.HasFlag(ConnectorCapabilities.Merge));
        Assert.True(capabilities.HasFlag(ConnectorCapabilities.CheckpointableWrites));
        Assert.False(capabilities.HasFlag(ConnectorCapabilities.ReplaceWrites));
    }

    [Fact]
    public async Task Merge_puts_each_row_to_its_keyed_path()
    {
        _server.MapPrefix("/items/", _ => new StubResponse(200, "{}"));
        var spec = new OutputSpec("api", "out", "merge", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "/items/{key}" }) { Keys = ["id"] };
        await using var session = await OpenSessionAsync(spec);

        using (var batch = Rows(7, 2))
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        var result = await session.CommitAsync(CancellationToken.None);

        Assert.Equal(2, result.RowsWritten);
        var puts = _server.Requests.Where(r => r.Method == "PUT").ToArray();
        Assert.Equal(["/items/7", "/items/8"], puts.Select(p => p.Url.AbsolutePath).ToArray());
        Assert.Equal(7, JsonNode.Parse(puts[0].Body)!["id"]!.GetValue<long>()); // full row object body
    }

    [Fact]
    public async Task Multi_key_merge_is_refused()
    {
        var spec = new OutputSpec("api", "out", "merge", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "/items/{key}" }) { Keys = ["id", "name"] };
        ISinkConnector connector = new HttpConnector();
        var sink = await connector.OpenAsync(Config(), CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, TestSchema, CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("exactly one key", ex.Message);
    }

    [Fact]
    public async Task Acknowledgment_counts_only_confirmed_requests()
    {
        var served = 0;
        _server.Map("/ingest", _ => ++served <= 2 ? new StubResponse(200, "{}") : new StubResponse(503, "{}"));
        var spec = new OutputSpec("api", "out", "append", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "/ingest", ["rows_per_request"] = 3 });
        await using var session = await OpenSessionAsync(spec);
        var checkpointing = (ICheckpointingSinkSession)session;

        using var batch = Rows(0, 9);
        await Assert.ThrowsAsync<PzConnectorException>(
            async () => await session.WriteBatchAsync(batch, CancellationToken.None));

        Assert.True(checkpointing.TryGetAcknowledgedRows(out var acknowledged));
        Assert.Equal(6, acknowledged);                                    // 2 confirmed requests × 3 rows
        Assert.False(checkpointing.TryGetAcknowledgedRows(out _));        // dedup: unchanged since last true
    }

    [Fact]
    public async Task Resume_prefix_counts_toward_commit_total()
    {
        _server.Map("/ingest", _ => new StubResponse(200, "{}"));
        var spec = new OutputSpec("api", "out", "append", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "/ingest", ["rows_per_request"] = 5 });
        await using var session = await OpenSessionAsync(spec);
        var checkpointing = (ICheckpointingSinkSession)session;

        Assert.True(checkpointing.TryResumeFrom(40));
        using (var batch = Rows(40, 10))
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        var result = await session.CommitAsync(CancellationToken.None);
        Assert.Equal(50, result.RowsWritten); // 40 resumed + 10 delivered
        Assert.True(checkpointing.TryGetAcknowledgedRows(out var acknowledged));
        Assert.Equal(50, acknowledged);
    }
}
