using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Gcs.Tests;

/// <summary>fake-gcs-server Testcontainers e2e for the UNIVERSAL tier (docker+network gated -- see
/// <see cref="FakeGcsFixture"/>): drives <see cref="GcsSink"/>'s SDK write sessions against a real
/// GCS JSON api implementation over real HTTP -- proving the spool-then-atomic-upload protocol,
/// resumable-upload mechanics, and abort semantics beyond what the in-memory FakeStorageClient can.
/// The client rides the sink's factory seam because the production auth path (OAuth token minting
/// against Google's token endpoint) has no offline equivalent; everything downstream of the client
/// is the production code.</summary>
[Collection("fake-gcs")]
public sealed class GcsUniversalSinkEndToEndTests(FakeGcsFixture fixture)
{
    private static ConnectorConfig Adc() => new(new Dictionary<string, object?>
    {
        ["auth"] = "adc",
        ["root"] = $"{FakeGcsFixture.Bucket}/out",
    });

    private static OutputSpec Out(string output, string format, string? path = null,
        IReadOnlyList<string>? partitionBy = null)
    {
        var options = new Dictionary<string, object?> { ["format"] = format };
        if (path is not null) options["path"] = path;
        if (partitionBy is not null) options["partition_by"] = partitionBy;
        return new OutputSpec("lake", output, "replace", "strict", options);
    }

    private static Schema IdNameSchema() => new(
        [
            new Field("id", Int32Type.Default, true),
            new Field("name", StringType.Default, true),
        ], null);

    private static RecordBatch IdNameBatch(params (int Id, string Name)[] rows)
    {
        var ids = new Int32Array.Builder();
        var names = new StringArray.Builder();
        foreach (var (id, name) in rows)
        {
            ids.Append(id);
            names.Append(name);
        }

        return new RecordBatch(IdNameSchema(), [ids.Build(), names.Build()], rows.Length);
    }

    private async Task<string> DownloadTextAsync(string objectName)
    {
        var client = fixture.CreateClient();
        using var buffer = new MemoryStream();
        await client.DownloadObjectAsync(FakeGcsFixture.Bucket, objectName, buffer);
        return Encoding.UTF8.GetString(buffer.ToArray());
    }

    [SkippableFact]
    public async Task Csv_session_lands_the_object_over_real_http()
    {
        var sink = new GcsSink(Adc(), fixture.CreateClient);
        await using var session = await sink.BeginWriteAsync(Out("customers", "csv"), IdNameSchema(), CancellationToken.None);
        await session.WriteBatchAsync(IdNameBatch((1, "alice"), (2, "bob")), CancellationToken.None);
        var result = await session.CommitAsync(CancellationToken.None);

        Assert.Equal(2, result.RowsWritten);
        Assert.Equal("id,name\n1,alice\n2,bob\n", await DownloadTextAsync("out/customers.csv"));
    }

    [SkippableFact]
    public async Task Parquet_session_lands_a_readable_object()
    {
        var sink = new GcsSink(Adc(), fixture.CreateClient);
        await using var session = await sink.BeginWriteAsync(Out("events", "parquet"), IdNameSchema(), CancellationToken.None);
        await session.WriteBatchAsync(IdNameBatch((7, "x")), CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);

        var client = fixture.CreateClient();
        using var buffer = new MemoryStream();
        await client.DownloadObjectAsync(FakeGcsFixture.Bucket, "out/events.parquet", buffer);
        buffer.Position = 0;
        await using var reader = await Parquet.ParquetReader.CreateAsync(buffer, leaveStreamOpen: true);
        Assert.Equal(1, reader.RowGroupCount);
        using var rowGroup = reader.OpenRowGroupReader(0);
        Assert.Equal(1, rowGroup.RowCount);
    }

    [SkippableFact]
    public async Task Aborted_session_leaves_no_object_behind()
    {
        var sink = new GcsSink(Adc(), fixture.CreateClient);
        await using var session = await sink.BeginWriteAsync(Out("aborted", "csv"), IdNameSchema(), CancellationToken.None);
        await session.WriteBatchAsync(IdNameBatch((1, "a")), CancellationToken.None);
        await session.AbortAsync(CancellationToken.None);

        var client = fixture.CreateClient();
        await Assert.ThrowsAsync<Google.GoogleApiException>(async () =>
            await client.GetObjectAsync(FakeGcsFixture.Bucket, "out/aborted.csv"));
    }

    [SkippableFact]
    public async Task Partitioned_write_lands_one_object_per_folder()
    {
        var tsType = new TimestampType(TimeUnit.Microsecond, "+00:00");
        var schema = new Schema(
            [new Field("id", Int32Type.Default, true), new Field("ts", tsType, true)], null);
        var ids = new Int32Array.Builder().Append(1).Append(2);
        var ts = new TimestampArray.Builder(tsType)
            .Append(new DateTimeOffset(2026, 7, 11, 5, 0, 0, TimeSpan.Zero))
            .Append(new DateTimeOffset(2026, 7, 12, 6, 0, 0, TimeSpan.Zero));
        var batch = new RecordBatch(schema, [ids.Build(), ts.Build()], 2);

        var sink = new GcsSink(Adc(), fixture.CreateClient);
        await using var session = await sink.BeginWriteAsync(
            Out("parts", "csv", path: "d={yyyy}-{MM}-{dd}", partitionBy: ["ts"]), schema, CancellationToken.None);
        await session.WriteBatchAsync(batch, CancellationToken.None);
        var result = await session.CommitAsync(CancellationToken.None);

        Assert.Equal(2, result.RowsWritten);
        Assert.StartsWith("id,ts\n1,", await DownloadTextAsync("out/d=2026-07-11/parts.csv"), StringComparison.Ordinal);
        Assert.StartsWith("id,ts\n2,", await DownloadTextAsync("out/d=2026-07-12/parts.csv"), StringComparison.Ordinal);
    }
}
