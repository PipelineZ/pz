using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Xunit;

namespace Pz.Connector.Sftp.Tests;

public class SftpWindowFilterTests
{
    private static Schema OneColumnSchema(string name, IArrowType type) =>
        new(new List<Field> { new(name, type, nullable: true) }, new Dictionary<string, string>());

    private static RecordBatch BuildBatch(Schema schema, IEnumerable<object?[]> rows)
    {
        var builder = new ArrowBatchBuilder(schema);
        foreach (var row in rows)
        {
            builder.AppendRow(row);
        }

        return builder.Flush()!;
    }

    private static DatasetSpec WindowedSpec(string cursor, string? value, string? upper) =>
        new("sftp", "ds", new Dictionary<string, object?>())
        {
            WatermarkCursor = cursor,
            WatermarkValue = value,
            WatermarkUpperBound = upper,
        };

    [Fact]
    public void TimestampCursor_RowAtLowerBound_Excluded_RowAtUpperBound_Included()
    {
        var schema = OneColumnSchema("ts", new TimestampType(TimeUnit.Microsecond, "+00:00"));
        var lo = new DateTimeOffset(2026, 8, 27, 0, 0, 0, TimeSpan.Zero);
        var hi = new DateTimeOffset(2026, 8, 28, 0, 0, 0, TimeSpan.Zero);
        var mid = lo.AddHours(12);
        var batch = BuildBatch(schema,
        [
            [lo],
            [hi],
            [mid],
        ]);

        var spec = WindowedSpec("ts", "2026-08-27T00:00:00", "2026-08-28T00:00:00");
        var filter = new SftpWindowFilter(spec, schema, "timestamp");
        Assert.True(filter.IsActive);

        var result = filter.Filter(batch);
        Assert.NotNull(result);
        Assert.Equal(2, result.Length);
        var tsArray = (TimestampArray)result.Column(0);
        Assert.Equal(hi, tsArray.GetTimestamp(0));
        Assert.Equal(mid, tsArray.GetTimestamp(1));
    }

    [Fact]
    public void IntCursor_FiltersByWindow()
    {
        var schema = OneColumnSchema("n", Int32Type.Default);
        var batch = BuildBatch(schema,
        [
            [5],   // below lo, excluded
            [10],  // at lo, excluded
            [15],  // in window, kept
            [20],  // at hi, kept
            [25],  // above hi, excluded
        ]);

        var spec = WindowedSpec("n", "10", "20");
        var filter = new SftpWindowFilter(spec, schema, "int");

        var result = filter.Filter(batch)!;
        Assert.Equal(2, result.Length);
        var arr = (Int32Array)result.Column(0);
        Assert.Equal(15, arr.GetValue(0));
        Assert.Equal(20, arr.GetValue(1));
    }

    [Fact]
    public void BigintCursor_FiltersByWindow()
    {
        var schema = OneColumnSchema("n", Int64Type.Default);
        var batch = BuildBatch(schema,
        [
            [100L],
            [200L],
            [300L],
        ]);

        var spec = WindowedSpec("n", "100", "200");
        var filter = new SftpWindowFilter(spec, schema, "bigint");

        var result = filter.Filter(batch)!;
        Assert.Equal(1, result.Length);
        Assert.Equal(200L, ((Int64Array)result.Column(0)).GetValue(0));
    }

    [Fact]
    public void DecimalCursor_FiltersByWindow()
    {
        var schema = OneColumnSchema("n", new Decimal128Type(38, 9));
        var batch = BuildBatch(schema,
        [
            [1.5m],
            [10.5m],
            [20.0m],
            [30.5m],
        ]);

        var spec = WindowedSpec("n", "10.5", "20.0");
        var filter = new SftpWindowFilter(spec, schema, "decimal");

        var result = filter.Filter(batch)!;
        Assert.Equal(1, result.Length);
        Assert.Equal(20.0m, ((Decimal128Array)result.Column(0)).GetValue(0));
    }

    [Fact]
    public void DateCursor_FiltersByWindow()
    {
        var schema = OneColumnSchema("d", Date32Type.Default);
        var batch = BuildBatch(schema,
        [
            [new DateOnly(2026, 8, 26)],
            [new DateOnly(2026, 8, 27)],
            [new DateOnly(2026, 8, 28)],
            [new DateOnly(2026, 8, 29)],
        ]);

        var spec = WindowedSpec("d", "2026-08-27", "2026-08-28");
        var filter = new SftpWindowFilter(spec, schema, "date");

        var result = filter.Filter(batch)!;
        Assert.Equal(1, result.Length);
        Assert.Equal(new DateOnly(2026, 8, 28), ((Date32Array)result.Column(0)).GetDateOnly(0));
    }

    [Fact]
    public void NullCursorCell_IsExcluded()
    {
        var schema = OneColumnSchema("n", Int32Type.Default);
        var batch = BuildBatch(schema,
        [
            [15],
            [null],
            [25],
        ]);

        var spec = WindowedSpec("n", "10", "20");
        var filter = new SftpWindowFilter(spec, schema, "int");

        var result = filter.Filter(batch)!;
        Assert.Equal(1, result.Length);
        Assert.Equal(15, ((Int32Array)result.Column(0)).GetValue(0));
    }

    [Fact]
    public void CursorAndUpperSet_ValueNull_ThrowsInvariantGuard()
    {
        var schema = OneColumnSchema("ts", new TimestampType(TimeUnit.Microsecond, "+00:00"));
        var spec = WindowedSpec("ts", value: null, upper: "2026-08-28T00:00:00");

        Assert.Throws<InvalidOperationException>(() => new SftpWindowFilter(spec, schema, "timestamp"));
    }

    [Fact]
    public void NoBounds_FilterIsPassThrough()
    {
        var schema = OneColumnSchema("n", Int32Type.Default);
        var batch = BuildBatch(schema, [[1], [2], [3]]);

        var spec = new DatasetSpec("sftp", "ds", new Dictionary<string, object?>());
        var filter = new SftpWindowFilter(spec, schema, "int");

        Assert.False(filter.IsActive);
        Assert.Same(batch, filter.Filter(batch));
    }

    [Fact]
    public void AllRowsKept_ReturnsInputBatchUnchanged()
    {
        var schema = OneColumnSchema("n", Int32Type.Default);
        var batch = BuildBatch(schema, [[15], [16], [17]]);

        var spec = WindowedSpec("n", "10", "20");
        var filter = new SftpWindowFilter(spec, schema, "int");

        Assert.Same(batch, filter.Filter(batch));
    }

    [Fact]
    public void AllRowsDropped_ReturnsNull()
    {
        var schema = OneColumnSchema("n", Int32Type.Default);
        var batch = BuildBatch(schema, [[1], [2], [3]]);

        var spec = WindowedSpec("n", "10", "20");
        var filter = new SftpWindowFilter(spec, schema, "int");

        Assert.Null(filter.Filter(batch));
    }

    [Fact]
    public void CursorColumnMissingFromSchema_WhenActive_ThrowsNamingDatasetAndColumn()
    {
        var schema = OneColumnSchema("other", Int32Type.Default);
        var spec = WindowedSpec("n", "10", "20");

        var ex = Assert.Throws<PzConnectorException>(() => new SftpWindowFilter(spec, schema, "int"));
        Assert.Contains("ds", ex.Message);
        Assert.Contains("'n'", ex.Message);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Filter_DoesNotDisposeInputBatch()
    {
        var schema = OneColumnSchema("n", Int32Type.Default);
        var batch = BuildBatch(schema, [[1], [15]]);

        var spec = WindowedSpec("n", "10", "20");
        var filter = new SftpWindowFilter(spec, schema, "int");

        _ = filter.Filter(batch);

        // Batch must still be usable after Filter -- the filter never takes ownership.
        Assert.Equal(2, batch.Length);
        Assert.Equal(1, ((Int32Array)batch.Column(0)).GetValue(0));
    }
}
