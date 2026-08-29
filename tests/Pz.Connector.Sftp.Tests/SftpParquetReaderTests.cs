using Apache.Arrow;
using Apache.Arrow.Types;
using Parquet;
using Parquet.Schema;
using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.Sftp.Tests;

public class SftpParquetReaderTests
{
    private static readonly DataField IntField = new("int_col", typeof(int), isNullable: true);
    private static readonly DataField LongField = new("long_col", typeof(long), isNullable: true);
    private static readonly DataField DoubleField = new("double_col", typeof(double), isNullable: true);
    private static readonly DataField BoolField = new("bool_col", typeof(bool), isNullable: true);
    private static readonly DataField StringField = new("string_col", typeof(string), isNullable: true);
    private static readonly DataField DateField = new DateTimeDataField("date_col", DateTimeFormat.Date, isNullable: true);
    private static readonly DataField TimestampField = new DateTimeDataField(
        "timestamp_col", DateTimeFormat.DateAndTime, isAdjustedToUTC: true, unit: DateTimeTimeUnit.Micros, isNullable: true);

    private static readonly DataField[] Fields =
    [
        IntField, LongField, DoubleField, BoolField, StringField, DateField, TimestampField,
    ];

    // Row group 1: row 0 all-values, row 1 all-null, row 2 all-values.
    // Row group 2: row 0 all-null, row 1 all-values.
    // Every column carries a null in both positions (row 1 of group 1, row 0 of group 2).
    private static readonly DateTime Date0 = new(2024, 1, 1);
    private static readonly DateTime Date2 = new(2024, 1, 3);
    private static readonly DateTime Date4 = new(2024, 1, 5);
    private static readonly DateTime Ts0 = new(2024, 1, 1, 10, 30, 0, DateTimeKind.Utc);
    private static readonly DateTime Ts2 = new(2024, 1, 3, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Ts4 = new(2024, 1, 5, 23, 59, 59, DateTimeKind.Utc);

    private static async Task<MemoryStream> BuildFixtureAsync()
    {
        var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(Fields), stream, cancellationToken: CancellationToken.None))
        {
            using (var rg1 = writer.CreateRowGroup())
            {
                await WriteBaseColumnsAsync(rg1,
                    ints: [1, null, 3],
                    longs: [10L, null, 30L],
                    doubles: [1.5, null, 3.5],
                    bools: [true, null, false],
                    strings: ["a", null, "c"],
                    dates: [Date0, null, Date2],
                    timestamps: [Ts0, null, Ts2]);
            }

            using (var rg2 = writer.CreateRowGroup())
            {
                await WriteBaseColumnsAsync(rg2,
                    ints: [null, 5],
                    longs: [null, 50L],
                    doubles: [null, 5.5],
                    bools: [null, true],
                    strings: [null, "e"],
                    dates: [null, Date4],
                    timestamps: [null, Ts4]);
            }
        }

        stream.Position = 0;
        return stream;
    }

    private static async Task WriteBaseColumnsAsync(
        ParquetRowGroupWriter rowGroup,
        int?[] ints, long?[] longs, double?[] doubles, bool?[] bools, string?[] strings, DateTime?[] dates, DateTime?[] timestamps)
    {
        await rowGroup.WriteAsync<int>(IntField, ints);
        await rowGroup.WriteAsync<long>(LongField, longs);
        await rowGroup.WriteAsync<double>(DoubleField, doubles);
        await rowGroup.WriteAsync<bool>(BoolField, bools);
        await rowGroup.WriteAsync(StringField, strings);
        await rowGroup.WriteAsync<DateTime>(DateField, dates);
        await rowGroup.WriteAsync<DateTime>(TimestampField, timestamps);
    }

    [Fact]
    public async Task ReadSchemaAsync_MapsToExpectedArrowFields()
    {
        await using var stream = await BuildFixtureAsync();

        var schema = await SftpParquetReader.ReadSchemaAsync(stream, "sftp:fixture.parquet", CancellationToken.None);

        Assert.Equal(7, schema.FieldsList.Count);
        Assert.Equal(ArrowTypeId.Int32, schema.GetFieldByName("int_col").DataType.TypeId);
        Assert.Equal(ArrowTypeId.Int64, schema.GetFieldByName("long_col").DataType.TypeId);
        Assert.Equal(ArrowTypeId.Double, schema.GetFieldByName("double_col").DataType.TypeId);
        Assert.Equal(ArrowTypeId.Boolean, schema.GetFieldByName("bool_col").DataType.TypeId);
        Assert.Equal(ArrowTypeId.String, schema.GetFieldByName("string_col").DataType.TypeId);
        Assert.Equal(ArrowTypeId.Date32, schema.GetFieldByName("date_col").DataType.TypeId);

        var tsType = Assert.IsType<TimestampType>(schema.GetFieldByName("timestamp_col").DataType);
        Assert.Equal(TimeUnit.Microsecond, tsType.Unit);
        Assert.Equal("+00:00", tsType.Timezone);

        foreach (var field in schema.FieldsList)
        {
            Assert.True(field.IsNullable);
        }
    }

    [Fact]
    public async Task ReadAsync_RoundTripsValuesAndNullsAcrossRowGroups()
    {
        await using var stream = await BuildFixtureAsync();

        var batches = new List<RecordBatch>();
        await foreach (var batch in SftpParquetReader.ReadAsync(
            stream, projectedColumns: null, "sftp:fixture.parquet", BatchOptions.Default, CancellationToken.None))
        {
            batches.Add(batch);
        }

        Assert.NotEmpty(batches);
        Assert.Equal(5, batches.Sum(b => b.Length));

        var ints = ConcatValues(batches, 0, (Int32Array a, int i) => a.GetValue(i));
        var longs = ConcatValues(batches, 1, (Int64Array a, int i) => a.GetValue(i));
        var doubles = ConcatValues(batches, 2, (DoubleArray a, int i) => a.GetValue(i));
        var bools = ConcatValues(batches, 3, (BooleanArray a, int i) => a.GetValue(i));
        var strings = ConcatValues(batches, 4, (StringArray a, int i) => a.IsNull(i) ? null : a.GetString(i));
        var dates = ConcatValues(batches, 5, (Date32Array a, int i) => a.GetDateOnly(i));
        var timestamps = ConcatValues(batches, 6, (TimestampArray a, int i) => a.GetTimestamp(i));

        int?[] expectedInts = [1, null, 3, null, 5];
        long?[] expectedLongs = [10L, null, 30L, null, 50L];
        double?[] expectedDoubles = [1.5, null, 3.5, null, 5.5];
        bool?[] expectedBools = [true, null, false, null, true];
        string?[] expectedStrings = ["a", null, "c", null, "e"];
        DateOnly?[] expectedDates =
        [
            DateOnly.FromDateTime(Date0), null, DateOnly.FromDateTime(Date2), null, DateOnly.FromDateTime(Date4),
        ];
        DateTimeOffset?[] expectedTimestamps =
        [
            new DateTimeOffset(Ts0), null, new DateTimeOffset(Ts2), null, new DateTimeOffset(Ts4),
        ];

        Assert.Equal(expectedInts, ints);
        Assert.Equal(expectedLongs, longs);
        Assert.Equal(expectedDoubles, doubles);
        Assert.Equal(expectedBools, bools);
        Assert.Equal(expectedStrings, strings);
        Assert.Equal(expectedDates, dates);
        Assert.Equal(expectedTimestamps, timestamps);
    }

    private static List<TValue?> ConcatValues<TArray, TValue>(
        IEnumerable<RecordBatch> batches, int columnIndex, Func<TArray, int, TValue?> select)
        where TArray : IArrowArray
    {
        var result = new List<TValue?>();
        foreach (var batch in batches)
        {
            var array = (TArray)batch.Column(columnIndex);
            for (var i = 0; i < batch.Length; i++)
            {
                result.Add(select(array, i));
            }
        }

        return result;
    }

    [Fact]
    public async Task ReadAsync_DecimalColumn_ThrowsPermanentErrorNamingColumnAndNextStep()
    {
        DataField[] fields = [IntField, new DecimalDataField("price", precision: 18, scale: 2, isNullable: true)];
        await using var stream = await BuildFixtureWithDecimalAsync(fields);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var _ in SftpParquetReader.ReadAsync(
                stream, projectedColumns: null, "sftp:fixture.parquet", BatchOptions.Default, CancellationToken.None))
            {
            }
        });

        Assert.False(ex.IsTransient);
        Assert.Contains("'price'", ex.Message);
        Assert.Contains("regenerate the file with double/varchar, or convert upstream", ex.Message);
    }

    private static async Task<MemoryStream> BuildFixtureWithDecimalAsync(DataField[] fields)
    {
        var stream = new MemoryStream();
        await using (var writer = await ParquetWriter.CreateAsync(new ParquetSchema(fields), stream, cancellationToken: CancellationToken.None))
        {
            using var rg = writer.CreateRowGroup();
            int?[] ids = [1, 2];
            decimal?[] prices = [1.23m, null];
            await rg.WriteAsync<int>(fields[0], ids);
            await rg.WriteAsync<decimal>(fields[1], prices);
        }

        stream.Position = 0;
        return stream;
    }

    [Fact]
    public async Task ReadAsync_ColumnProjection_ReturnsOnlyHintedColumns()
    {
        await using var stream = await BuildFixtureAsync();

        var batches = new List<RecordBatch>();
        await foreach (var batch in SftpParquetReader.ReadAsync(
            stream, projectedColumns: ["string_col", "int_col"], "sftp:fixture.parquet", BatchOptions.Default, CancellationToken.None))
        {
            batches.Add(batch);
        }

        Assert.NotEmpty(batches);
        var names = batches[0].Schema.FieldsList.Select(f => f.Name).ToArray();
        Assert.Equal(2, names.Length);
        Assert.Contains("string_col", names);
        Assert.Contains("int_col", names);
    }

    [Fact]
    public async Task ReadAsync_OnlyNonexistentColumnsRequested_ThrowsPermanentError()
    {
        await using var stream = await BuildFixtureAsync();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var _ in SftpParquetReader.ReadAsync(
                stream, projectedColumns: ["does_not_exist"], "sftp:fixture.parquet", BatchOptions.Default, CancellationToken.None))
            {
            }
        });

        Assert.False(ex.IsTransient);
        Assert.Contains("none of the requested columns exist", ex.Message);
    }
}
