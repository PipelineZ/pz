using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.LocalFiles.Tests;

/// <summary>Byte-exact characterization of the universal (Arrow) write tier's text formats — csv and
/// NDJSON — over the whole v0 type matrix plus the quoting/escaping edge cases. These formats are a
/// stability contract (the byte-stable-writer rule): whatever the writers emit here must survive any
/// rewrite of how they emit it, so this suite pins the exact bytes rather than round-tripping values.
/// Drives the real <see cref="LocalFilesConnector"/> sink through its public ISink surface.</summary>
public sealed class UniversalWriteFormatTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-localfiles-tests", Guid.NewGuid().ToString("N"));

    public UniversalWriteFormatTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static readonly Schema AllTypes = new(
    [
        new Field("i32", Int32Type.Default, nullable: true),
        new Field("i64", Int64Type.Default, nullable: true),
        new Field("dbl", DoubleType.Default, nullable: true),
        new Field("dec", new Decimal128Type(38, 9), nullable: true),
        new Field("txt", StringType.Default, nullable: true),
        new Field("flag", BooleanType.Default, nullable: true),
        new Field("d", Date32Type.Default, nullable: true),
        new Field("ts", new TimestampType(TimeUnit.Microsecond, "+00:00"), nullable: true),
    ], null);

    /// <summary>One batch covering: ordinary values, every-column nulls, and the numeric/text extremes
    /// (int/long bounds, non-finite doubles, high-precision decimals, quoting triggers, non-ASCII).</summary>
    private static RecordBatch BuildAllTypesBatch()
    {
        var i32 = new Int32Array.Builder();
        var i64 = new Int64Array.Builder();
        var dbl = new DoubleArray.Builder();
        var dec = new Decimal128Array.Builder(new Decimal128Type(38, 9));
        var txt = new StringArray.Builder();
        var flag = new BooleanArray.Builder();
        var d = new Date32Array.Builder();
        var ts = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "+00:00"));

        void Row(int? a, long? b, double? c, decimal? e, string? f, bool? g, DateOnly? h, DateTimeOffset? i)
        {
            if (a is null) { i32.AppendNull(); } else { i32.Append(a.Value); }
            if (b is null) { i64.AppendNull(); } else { i64.Append(b.Value); }
            if (c is null) { dbl.AppendNull(); } else { dbl.Append(c.Value); }
            if (e is null) { dec.AppendNull(); } else { dec.Append(e.Value); }
            if (f is null) { txt.AppendNull(); } else { txt.Append(f); }
            if (g is null) { flag.AppendNull(); } else { flag.Append(g.Value); }
            if (h is null) { d.AppendNull(); } else { d.Append(h.Value); }
            if (i is null) { ts.AppendNull(); } else { ts.Append(i.Value); }
        }

        var stamp = new DateTimeOffset(2026, 7, 13, 10, 30, 15, TimeSpan.Zero).AddTicks(1234560);
        Row(1, 2L, 3.5d, 4.25m, "plain", true, new DateOnly(2026, 7, 13), stamp);
        Row(null, null, null, null, null, null, null, null);
        Row(int.MinValue, long.MinValue, 0.1d, -1.000000001m, "a,b", false, new DateOnly(1, 1, 1), DateTimeOffset.UnixEpoch);
        Row(int.MaxValue, long.MaxValue, 1e300d, 12345678901234567890.123456789m, "say \"hi\"", true,
            new DateOnly(9999, 12, 31), new DateTimeOffset(9999, 12, 31, 23, 59, 59, TimeSpan.Zero));
        Row(0, 0L, double.NaN, 0m, "line1\nline2", false, new DateOnly(2026, 1, 1), stamp);
        Row(-1, -1L, double.PositiveInfinity, 0.000000001m, "cr\rhere", true, new DateOnly(2026, 2, 28), stamp);
        Row(7, 7L, double.NegativeInfinity, -0.000000001m, "héllo 🌍", false, new DateOnly(2026, 2, 28), stamp);
        Row(8, 8L, -0.0d, 1m, string.Empty, true, new DateOnly(2026, 3, 1), stamp);

        return new RecordBatch(AllTypes,
            [i32.Build(), i64.Build(), dbl.Build(), dec.Build(), txt.Build(), flag.Build(), d.Build(), ts.Build()], 8);
    }

    private async Task<byte[]> WriteAsync(string format, params RecordBatch[] batches)
    {
        var connector = new LocalFilesConnector();
        await using var sink = await ((ISinkConnector)connector).OpenAsync(
            new ConnectorConfig(new Dictionary<string, object?> { ["base_dir"] = _dir }), CancellationToken.None);

        var spec = new OutputSpec("lake", $"out_{format}", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["format"] = format });

        var session = await sink.BeginWriteAsync(spec, AllTypes, CancellationToken.None);
        await using (session)
        {
            foreach (var batch in batches)
            {
                await session.WriteBatchAsync(batch, CancellationToken.None);
            }

            await session.CommitAsync(CancellationToken.None);
        }

        return await File.ReadAllBytesAsync(Path.Combine(_dir, $"out_{format}", $"out_{format}.{format}"));
    }

    [Fact]
    public async Task Csv_bytes_are_exact()
    {
        using var batch = BuildAllTypesBatch();
        var bytes = await WriteAsync("csv", batch);

        const string expected =
            // No BOM: DuckDB's native COPY does not write one, and the planner — not the author —
            // picks the tier, so the two writers must agree byte-for-byte.
            "i32,i64,dbl,dec,txt,flag,d,ts\n" +
            "1,2,3.5,4.250000000,plain,True,2026-07-13T00:00:00.0000000,2026-07-13T10:30:15.1234560+00:00\n" +
            ",,,,,,,\n" +
            "-2147483648,-9223372036854775808,0.1,-1.000000001,\"a,b\",False,0001-01-01T00:00:00.0000000,1970-01-01T00:00:00.0000000+00:00\n" +
            "2147483647,9223372036854775807,1E+300,12345678901234567890.123456789,\"say \"\"hi\"\"\",True,9999-12-31T00:00:00.0000000,9999-12-31T23:59:59.0000000+00:00\n" +
            "0,0,NaN,0.000000000,\"line1\nline2\",False,2026-01-01T00:00:00.0000000,2026-07-13T10:30:15.1234560+00:00\n" +
            "-1,-1,Infinity,0.000000001,\"cr\rhere\",True,2026-02-28T00:00:00.0000000,2026-07-13T10:30:15.1234560+00:00\n" +
            "7,7,-Infinity,-0.000000001,héllo 🌍,False,2026-02-28T00:00:00.0000000,2026-07-13T10:30:15.1234560+00:00\n" +
            "8,8,-0,1.000000000,,True,2026-03-01T00:00:00.0000000,2026-07-13T10:30:15.1234560+00:00\n";

        Assert.Equal(expected, Encoding.UTF8.GetString(bytes));
    }

    [Fact]
    public async Task Ndjson_bytes_are_exact()
    {
        using var batch = BuildAllTypesBatch();
        var bytes = await WriteAsync("json", batch);

        const string expected =
            """{"i32":1,"i64":2,"dbl":3.5,"dec":4.250000000,"txt":"plain","flag":true,"d":"2026-07-13","ts":"2026-07-13T10:30:15.123456Z"}""" + "\n" +
            """{"i32":null,"i64":null,"dbl":null,"dec":null,"txt":null,"flag":null,"d":null,"ts":null}""" + "\n" +
            """{"i32":-2147483648,"i64":-9223372036854775808,"dbl":0.1,"dec":-1.000000001,"txt":"a,b","flag":false,"d":"0001-01-01","ts":"1970-01-01T00:00:00.000000Z"}""" + "\n" +
            """{"i32":2147483647,"i64":9223372036854775807,"dbl":1E+300,"dec":12345678901234567890.123456789,"txt":"say \u0022hi\u0022","flag":true,"d":"9999-12-31","ts":"9999-12-31T23:59:59.000000Z"}""" + "\n" +
            """{"i32":0,"i64":0,"dbl":null,"dec":0.000000000,"txt":"line1\nline2","flag":false,"d":"2026-01-01","ts":"2026-07-13T10:30:15.123456Z"}""" + "\n" +
            """{"i32":-1,"i64":-1,"dbl":null,"dec":0.000000001,"txt":"cr\rhere","flag":true,"d":"2026-02-28","ts":"2026-07-13T10:30:15.123456Z"}""" + "\n" +
            """{"i32":7,"i64":7,"dbl":null,"dec":-0.000000001,"txt":"h\u00E9llo \uD83C\uDF0D","flag":false,"d":"2026-02-28","ts":"2026-07-13T10:30:15.123456Z"}""" + "\n" +
            """{"i32":8,"i64":8,"dbl":-0,"dec":1.000000000,"txt":"","flag":true,"d":"2026-03-01","ts":"2026-07-13T10:30:15.123456Z"}""" + "\n";

        Assert.Equal(expected, Encoding.UTF8.GetString(bytes));
    }

    [Fact]
    public async Task Csv_writes_one_header_across_many_batches()
    {
        using var b0 = BuildAllTypesBatch();
        using var b1 = BuildAllTypesBatch();
        var bytes = await WriteAsync("csv", b0, b1);
        var text = Encoding.UTF8.GetString(bytes);

        // The header is written once, at session open — not per batch.
        Assert.Equal(1, text.Split("i32,i64,dbl").Length - 1);
        Assert.EndsWith("+00:00\n", text, StringComparison.Ordinal);
    }

    /// <summary>A value wider than the csv writer's internal buffer takes a different path through it
    /// (the row cannot be sized into the buffer up front, so it is emitted cell by cell with flushes in
    /// between). Quoting has to survive being split across those flushes — an internal quote can land on
    /// either side of a chunk boundary — so this asserts the whole file, not just its length.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Csv_value_wider_than_the_write_buffer_round_trips(bool withQuoteTriggers)
    {
        // Comfortably over the writer's 256 KiB buffer, and not a multiple of it, so chunk boundaries
        // fall inside the value rather than neatly between cells.
        const int width = (700 * 1024) + 37;
        var payload = withQuoteTriggers
            ? BuildQuoteHeavyPayload(width)
            : new string('x', width);

        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, nullable: true),
            new Field("payload", StringType.Default, nullable: true),
            new Field("tail", Int64Type.Default, nullable: true),
        ], null);

        var id = new Int64Array.Builder();
        var text = new StringArray.Builder();
        var tail = new Int64Array.Builder();
        id.Append(1).Append(2);
        text.Append(payload).AppendNull();
        tail.Append(7).Append(8);
        using var batch = new RecordBatch(schema, [id.Build(), text.Build(), tail.Build()], 2);

        var connector = new LocalFilesConnector();
        await using var sink = await ((ISinkConnector)connector).OpenAsync(
            new ConnectorConfig(new Dictionary<string, object?> { ["base_dir"] = _dir }), CancellationToken.None);
        var spec = new OutputSpec("lake", "wide", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["format"] = "csv" });

        var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None);
        await using (session)
        {
            await session.WriteBatchAsync(batch, CancellationToken.None);
            await session.CommitAsync(CancellationToken.None);
        }

        // Bytes, not ReadAllText: byte-exact means byte-exact, including the (absent) preamble.
        var actual = Encoding.UTF8.GetString(await File.ReadAllBytesAsync(Path.Combine(_dir, "wide", "wide.csv")));
        var quoted = withQuoteTriggers ? "\"" + payload.Replace("\"", "\"\"") + "\"" : payload;
        Assert.Equal("id,payload,tail\n1," + quoted + ",7\n2,,8\n", actual);
    }

    /// <summary>A payload whose quoting triggers are spread far enough apart that some land mid-chunk and
    /// some near a 256 KiB boundary, including two adjacent quotes.</summary>
    private static string BuildQuoteHeavyPayload(int width)
    {
        var chars = new char[width];
        for (var i = 0; i < width; i++)
        {
            chars[i] = (i % 65_536) switch
            {
                0 => '"',
                1 => '"',
                12_345 => ',',
                40_000 => '\n',
                50_000 => '\r',
                _ => (char)('a' + (i % 26)),
            };
        }

        return new string(chars);
    }

    [Fact]
    public async Task Empty_batch_writes_header_only_for_csv_and_nothing_for_ndjson()
    {
        using var empty = new RecordBatch(AllTypes,
        [
            new Int32Array.Builder().Build(), new Int64Array.Builder().Build(), new DoubleArray.Builder().Build(),
            new Decimal128Array.Builder(new Decimal128Type(38, 9)).Build(), new StringArray.Builder().Build(),
            new BooleanArray.Builder().Build(), new Date32Array.Builder().Build(),
            new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "+00:00")).Build(),
        ], 0);

        Assert.Equal("i32,i64,dbl,dec,txt,flag,d,ts\n", Encoding.UTF8.GetString(await WriteAsync("csv", empty)));
        Assert.Empty(await WriteAsync("json", empty));
    }
}
