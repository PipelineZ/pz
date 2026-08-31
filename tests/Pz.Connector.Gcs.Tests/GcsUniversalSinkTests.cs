using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Gcs.Tests;

/// <summary>Offline proof of the gcs universal (SDK) write tier over a fake in-memory
/// <see cref="Google.Cloud.Storage.V1.StorageClient"/>: the spool-then-atomic-upload commit protocol
/// (a gcs object becomes visible only when its upload completes, so upload IS the commit — no temp
/// object, no promote), the commit-xor-abort state machine, transient classification, and the
/// partition_by fan-out. Real end-to-end bytes ride the docker fake-gcs-server suite.</summary>
public sealed class GcsUniversalSinkTests
{
    private static ConnectorConfig Adc(string? root = "my-bucket/out") =>
        new(new Dictionary<string, object?> { ["auth"] = "adc", ["root"] = root });

    private static OutputSpec Out(
        string output = "daily", string mode = "replace", string? format = "csv", string? path = null,
        IReadOnlyList<string>? partitionBy = null)
    {
        var options = new Dictionary<string, object?>();
        if (format is not null) options["format"] = format;
        if (path is not null) options["path"] = path;
        if (partitionBy is not null) options["partition_by"] = partitionBy;
        return new OutputSpec("lake", output, mode, "strict", options);
    }

    private static Schema IdNameSchema() => new(
        [
            new Field("id", Int32Type.Default, true),
            new Field("name", StringType.Default, true),
        ], null);

    private static RecordBatch IdNameBatch(params (int? Id, string? Name)[] rows)
    {
        var ids = new Int32Array.Builder();
        var names = new StringArray.Builder();
        foreach (var (id, name) in rows)
        {
            if (id is null) ids.AppendNull(); else ids.Append(id.Value);
            if (name is null) names.AppendNull(); else names.Append(name);
        }

        return new RecordBatch(IdNameSchema(), [ids.Build(), names.Build()], rows.Length);
    }

    private static (GcsSink Sink, FakeStorageClient Client) SinkWithFake(string? root = "my-bucket/out")
    {
        var client = new FakeStorageClient();
        return (new GcsSink(Adc(root), () => client), client);
    }

    [Fact]
    public async Task Csv_commit_uploads_the_codec_bytes_to_the_final_object()
    {
        var (sink, client) = SinkWithFake();
        await using var session = await sink.BeginWriteAsync(Out(), IdNameSchema(), CancellationToken.None);
        await session.WriteBatchAsync(IdNameBatch((1, "a"), (2, null)), CancellationToken.None);
        var result = await session.CommitAsync(CancellationToken.None);

        Assert.Equal(2, result.RowsWritten);
        Assert.Equal(1, result.BatchesWritten);
        var upload = Assert.Single(client.Uploads);
        Assert.Equal("my-bucket", upload.Bucket);
        Assert.Equal("out/daily.csv", upload.Name);
        Assert.Equal("id,name\n1,a\n2,\n", Encoding.UTF8.GetString(upload.Content));
    }

    [Fact]
    public async Task Json_commit_uploads_ndjson()
    {
        var (sink, client) = SinkWithFake();
        await using var session = await sink.BeginWriteAsync(Out(format: "json"), IdNameSchema(), CancellationToken.None);
        await session.WriteBatchAsync(IdNameBatch((7, "x")), CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);

        var upload = Assert.Single(client.Uploads);
        Assert.Equal("out/daily.json", upload.Name);
        Assert.Equal("{\"id\":7,\"name\":\"x\"}\n", Encoding.UTF8.GetString(upload.Content));
    }

    [Fact]
    public async Task Parquet_commit_uploads_a_readable_parquet_object()
    {
        var (sink, client) = SinkWithFake();
        await using var session = await sink.BeginWriteAsync(Out(format: "parquet"), IdNameSchema(), CancellationToken.None);
        await session.WriteBatchAsync(IdNameBatch((1, "a")), CancellationToken.None);
        await session.WriteBatchAsync(IdNameBatch((2, "b")), CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);

        var upload = Assert.Single(client.Uploads);
        Assert.Equal("out/daily.parquet", upload.Name);
        await using var reader = await Parquet.ParquetReader.CreateAsync(new MemoryStream(upload.Content));
        Assert.Equal(2, reader.RowGroupCount);
        Assert.Equal(["id", "name"], reader.Schema.DataFields.Select(f => f.Name).ToArray());
        using var rowGroup = reader.OpenRowGroupReader(0);
        Assert.Equal(1, rowGroup.RowCount);
    }

    [Fact]
    public async Task Append_mode_uploads_a_guid_suffixed_object()
    {
        var (sink, client) = SinkWithFake();
        await using var session = await sink.BeginWriteAsync(Out(mode: "append"), IdNameSchema(), CancellationToken.None);
        await session.WriteBatchAsync(IdNameBatch((1, "a")), CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);

        var upload = Assert.Single(client.Uploads);
        Assert.StartsWith("out/daily-", upload.Name, StringComparison.Ordinal);
        Assert.EndsWith(".csv", upload.Name, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Abort_never_uploads()
    {
        var (sink, client) = SinkWithFake();
        await using var session = await sink.BeginWriteAsync(Out(), IdNameSchema(), CancellationToken.None);
        await session.WriteBatchAsync(IdNameBatch((1, "a")), CancellationToken.None);
        await session.AbortAsync(CancellationToken.None);

        Assert.Empty(client.Uploads);
    }

    [Fact]
    public async Task Dispose_without_commit_never_uploads()
    {
        var (sink, client) = SinkWithFake();
        var session = await sink.BeginWriteAsync(Out(), IdNameSchema(), CancellationToken.None);
        await session.WriteBatchAsync(IdNameBatch((1, "a")), CancellationToken.None);
        await session.DisposeAsync();

        Assert.Empty(client.Uploads);
    }

    [Fact]
    public async Task Committed_session_rejects_further_writes_commits_and_aborts()
    {
        var (sink, _) = SinkWithFake();
        await using var session = await sink.BeginWriteAsync(Out(), IdNameSchema(), CancellationToken.None);
        await session.WriteBatchAsync(IdNameBatch((1, "a")), CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await session.WriteBatchAsync(IdNameBatch((2, "b")), CancellationToken.None));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await session.CommitAsync(CancellationToken.None));
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await session.AbortAsync(CancellationToken.None));
    }

    [Fact]
    public async Task Transient_upload_failure_is_classified_transient()
    {
        var (sink, client) = SinkWithFake();
        client.ThrowOnUpload = new IOException("connection reset");
        await using var session = await sink.BeginWriteAsync(Out(), IdNameSchema(), CancellationToken.None);
        await session.WriteBatchAsync(IdNameBatch((1, "a")), CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await session.CommitAsync(CancellationToken.None));
        Assert.True(ex.IsTransient);
        Assert.Contains("daily.csv", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Gate_routes_the_upload_op()
    {
        var (sink, client) = SinkWithFake();
        var gate = new CountingGate();
        ((IOperationGateAware)sink).UseOperationGate(gate);
        await using var session = await sink.BeginWriteAsync(Out(), IdNameSchema(), CancellationToken.None);
        await session.WriteBatchAsync(IdNameBatch((1, "a")), CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);

        Assert.Equal(["gcs.upload"], gate.Ops);
        Assert.Single(client.Uploads);
    }

    [Fact]
    public async Task Partitioned_write_fans_out_per_rendered_folder()
    {
        var schema = new Schema(
            [
                new Field("id", Int32Type.Default, true),
                new Field("ts", new TimestampType(TimeUnit.Microsecond, "+00:00"), true),
            ], null);
        var ids = new Int32Array.Builder().Append(1).Append(2).Append(3);
        var ts = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "+00:00"))
            .Append(new DateTimeOffset(2026, 7, 11, 5, 0, 0, TimeSpan.Zero))
            .Append(new DateTimeOffset(2026, 7, 12, 6, 0, 0, TimeSpan.Zero))
            .Append(new DateTimeOffset(2026, 7, 11, 7, 0, 0, TimeSpan.Zero));
        var batch = new RecordBatch(schema, [ids.Build(), ts.Build()], 3);

        var (sink, client) = SinkWithFake();
        await using var session = await sink.BeginWriteAsync(
            Out(format: "csv", path: "d={yyyy}-{MM}-{dd}", partitionBy: ["ts"]), schema, CancellationToken.None);
        await session.WriteBatchAsync(batch, CancellationToken.None);
        var result = await session.CommitAsync(CancellationToken.None);

        Assert.Equal(3, result.RowsWritten);
        Assert.Equal(2, client.Uploads.Count);
        var names = client.Uploads.Select(u => u.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.Equal("out/d=2026-07-11/daily.csv", names[0]);
        Assert.Equal("out/d=2026-07-12/daily.csv", names[1]);
    }

    [Fact]
    public async Task Partition_column_missing_or_untyped_is_a_named_permanent_error()
    {
        var (sink, _) = SinkWithFake();
        var missing = await Assert.ThrowsAsync<PzConnectorException>(async () => await sink.BeginWriteAsync(
            Out(path: "d={yyyy}", partitionBy: ["nope"]), IdNameSchema(), CancellationToken.None));
        Assert.Contains("'nope'", missing.Message, StringComparison.Ordinal);

        var untyped = await Assert.ThrowsAsync<PzConnectorException>(async () => await sink.BeginWriteAsync(
            Out(path: "d={yyyy}", partitionBy: ["name"]), IdNameSchema(), CancellationToken.None));
        Assert.Contains("'name'", untyped.Message, StringComparison.Ordinal);
        Assert.Contains("timestamp", untyped.Message, StringComparison.Ordinal);
    }

    private sealed class CountingGate : IOperationGate
    {
        public List<string> Ops { get; } = [];

        public Task<T> ExecuteAsync<T>(string opLabel, bool idempotent, Func<CancellationToken, Task<T>> op, CancellationToken ct)
        {
            Ops.Add(opLabel);
            return op(ct);
        }

        public void ReportBudget(int remaining, DateTimeOffset resetAt)
        {
        }
    }
}
