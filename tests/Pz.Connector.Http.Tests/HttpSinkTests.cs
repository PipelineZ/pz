using System.Text.Json.Nodes;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

/// <summary>Append writes: chunked JSON-array POSTs, ndjson variant, ack-on-2xx
/// accounting, transient/permanent classification, and the commit/abort/no-op contracts.
/// StubHttpServer captures bodies for exact request assertions.</summary>
public sealed class HttpSinkTests : IAsyncLifetime
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

    private static OutputSpec AppendOutput(Dictionary<string, object?>? extra = null)
    {
        var options = new Dictionary<string, object?> { ["path"] = "/ingest" };
        foreach (var (k, v) in extra ?? []) options[k] = v;
        return new OutputSpec("api", "out", "append", "fail_on_change", options);
    }

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
    public async Task Append_chunks_rows_into_json_array_posts()
    {
        _server.Map("/ingest", _ => new StubResponse(200, "{}"));
        await using var session = await OpenSessionAsync(AppendOutput(new() { ["rows_per_request"] = 3 }));

        using (var batch = Rows(0, 8))
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        var result = await session.CommitAsync(CancellationToken.None);

        Assert.Equal(8, result.RowsWritten);
        Assert.Equal(3, result.BatchesWritten); // 3 requests: 3+3+2 rows
        var posts = _server.Requests.Where(r => r.Method == "POST").ToArray();
        Assert.Equal(3, posts.Length);
        Assert.Equal([3, 3, 2], posts.Select(p => ((JsonArray)JsonNode.Parse(p.Body)!).Count).ToArray());
        var first = (JsonArray)JsonNode.Parse(posts[0].Body)!;
        Assert.Equal(0, (long)first[0]!["id"]!.GetValue<long>());
        Assert.Equal("row-0", first[0]!["name"]!.GetValue<string>());
    }

    [Fact]
    public async Task Ndjson_body_format_sends_newline_delimited_rows()
    {
        _server.Map("/ingest", _ => new StubResponse(200, "{}"));
        await using var session = await OpenSessionAsync(
            AppendOutput(new() { ["body_format"] = "ndjson", ["rows_per_request"] = 10 }));

        using (var batch = Rows(0, 2))
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        await session.CommitAsync(CancellationToken.None);

        var post = Assert.Single(_server.Requests, r => r.Method == "POST");
        var lines = post.Body.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(2, lines.Length);
        Assert.Equal(0, JsonNode.Parse(lines[0])!["id"]!.GetValue<long>());
    }

    [Fact]
    public async Task Transient_status_throws_transient_with_retry_after()
    {
        _server.Map("/ingest", _ => new StubResponse(429, "{}", new Dictionary<string, string> { ["Retry-After"] = "7" }));
        await using var session = await OpenSessionAsync(AppendOutput());

        using var batch = Rows(0, 1);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await session.WriteBatchAsync(batch, CancellationToken.None));

        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(7), ex.RetryAfter);
    }

    [Fact]
    public async Task Permanent_status_throws_non_transient_without_echoing_the_body()
    {
        _server.Map("/ingest", _ => new StubResponse(422, """{"secret":"do-not-echo"}"""));
        await using var session = await OpenSessionAsync(AppendOutput());

        using var batch = Rows(0, 1);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await session.WriteBatchAsync(batch, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("422", ex.Message);
        Assert.DoesNotContain("do-not-echo", ex.Message);
    }

    [Fact]
    public async Task Unknown_output_option_is_refused()
    {
        ISinkConnector connector = new HttpConnector();
        var sink = await connector.OpenAsync(Config(), CancellationToken.None);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await sink.BeginWriteAsync(AppendOutput(new() { ["rows_per_request"] = 0 }), TestSchema, CancellationToken.None));
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public async Task Append_only_options_are_refused_under_merge()
    {
        // Merge's per-row keyed PUT/PATCH path never reads body_format or
        // rows_per_request -- silently accepting them would look like they
        // do something. Both violations must surface in ONE aggregate error (the config parser's
        // report-everything house rule), naming each option and pointing at append.
        ISinkConnector connector = new HttpConnector();
        var sink = await connector.OpenAsync(Config(), CancellationToken.None);
        var spec = new OutputSpec("api", "merge-out", "merge", "fail_on_change",
            new Dictionary<string, object?>
            {
                ["path"] = "/items/{key}",
                ["body_format"] = "ndjson",
                ["rows_per_request"] = 100,
            })
        { Keys = ["id"] };

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await sink.BeginWriteAsync(spec, TestSchema, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("body_format", ex.Message);
        Assert.Contains("rows_per_request", ex.Message);
        Assert.Contains("append", ex.Message);
    }

    [Fact]
    public async Task Sink_declares_none_abort_semantics_and_abort_is_a_no_op()
    {
        _server.Map("/ingest", _ => new StubResponse(200, "{}"));
        ISinkConnector connector = new HttpConnector();
        var sink = await connector.OpenAsync(Config(), CancellationToken.None);
        Assert.Equal(AbortSemantics.None, sink.AbortSemantics);

        await using var session = await sink.BeginWriteAsync(AppendOutput(), TestSchema, CancellationToken.None);
        using (var batch = Rows(0, 2))
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        await session.AbortAsync(CancellationToken.None); // must not throw
        Assert.Single(_server.Requests, r => r.Method == "POST"); // nothing un-POSTed
        await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await session.CommitAsync(CancellationToken.None));
    }
}
