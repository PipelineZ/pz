using Apache.Arrow;
using Apache.Arrow.Types;
using Parquet;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.LocalFiles.Tests;

/// <summary>Pins the microsecond-precision timestamp round trip through the LocalFiles managed
/// parquet write path (<see cref="ParquetSinkWriteSession"/>): a value with a sub-millisecond
/// fractional-second component must come back unchanged, since DuckDB's own timestamps are also
/// microsecond-precision and nothing in the write path may round or truncate one.</summary>
public sealed class ParquetTimestampPrecisionTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-localfiles-tests", Guid.NewGuid().ToString("N"));

    public ParquetTimestampPrecisionTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task Sub_millisecond_timestamp_survives_the_managed_parquet_write_path()
    {
        var schema = new Schema([new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: true)], null);
        // .123456 -- six sub-second digits, exactly a whole number of microseconds so the value
        // is representable losslessly at Micros precision with nothing left over to round away.
        var stamp = new DateTimeOffset(2026, 3, 4, 5, 6, 7, TimeSpan.Zero).AddTicks(1_234_560);
        var builder = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        builder.Append(stamp);
        using var batch = new RecordBatch(schema, [builder.Build()], 1);

        var connector = new LocalFilesConnector();
        await using var sink = await ((ISinkConnector)connector).OpenAsync(
            new ConnectorConfig(new Dictionary<string, object?> { ["base_dir"] = _dir }), CancellationToken.None);
        var spec = new OutputSpec("lake", "ts_precision", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["format"] = "parquet" });

        var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None);
        await using (session)
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
            await session.CommitAsync(CancellationToken.None);
        }

        var path = Path.Combine(_dir, "ts_precision", "ts_precision.parquet");
        await using var reader = await ParquetReader.CreateAsync(path);
        var field = reader.Schema.DataFields.Single(f => f.Name == "ts");
        using var rowGroup = reader.OpenRowGroupReader(0);
        var values = new DateTime?[1];
        await rowGroup.ReadAsync<DateTime>(field, values);

        Assert.Equal(stamp.UtcDateTime, values[0]!.Value);
        Assert.Equal(123_456, (values[0]!.Value.Ticks % TimeSpan.TicksPerSecond) / 10);
    }
}
