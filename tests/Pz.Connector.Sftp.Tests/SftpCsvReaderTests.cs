using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Sylvan.Data.Csv;
using Xunit;

namespace Pz.Connector.Sftp.Tests;

public class SftpCsvReaderTests
{
    [Fact]
    public async Task TypedContract_ProducesCorrectArrowTypes()
    {
        var csv = """
id,name,ts
1,Alice,2024-01-01T10:00:00Z
2,Bob,2024-01-02T11:00:00Z
3,Charlie,2024-01-03T12:00:00Z
""";

        var schema = new Schema(new List<Field>
        {
            new("id", Int32Type.Default, nullable: true),
            new("name", StringType.Default, nullable: true),
            new("ts", new TimestampType(TimeUnit.Microsecond, "+00:00"), nullable: true),
        }, new Dictionary<string, string>());

        var typeNames = new[] { "int", "varchar", "timestamp" };
        var ordinals = new[] { 0, 1, 2 };
        var path = "test.csv";
        var options = BatchOptions.Default;
        var rowNumberOffset = 0L;

        var reader = CsvDataReader.Create(new StringReader(csv));
        var batches = new List<RecordBatch>();
        await foreach (var b in CsvArrowReader.ReadAsync(reader, schema, typeNames, ordinals, path, options, rowNumberOffset, CancellationToken.None))
        {
            batches.Add(b);
        }

        Assert.Single(batches);
        var batch = batches[0];
        Assert.Equal(3, batch.Length);
        Assert.Equal(3, batch.ColumnCount);

        // Verify id column
        var idArray = (Int32Array)batch.Column(0);
        Assert.Equal(1, idArray.GetValue(0));
        Assert.Equal(2, idArray.GetValue(1));
        Assert.Equal(3, idArray.GetValue(2));

        // Verify name column
        var nameArray = (StringArray)batch.Column(1);
        Assert.Equal("Alice", nameArray.GetString(0));
        Assert.Equal("Bob", nameArray.GetString(1));
        Assert.Equal("Charlie", nameArray.GetString(2));

        // Verify ts column type and values
        var tsArray = (TimestampArray)batch.Column(2);
        Assert.IsType<TimestampArray>(tsArray);

        // Verify timestamp values (stored as microseconds since Unix epoch)
        var expected1 = new DateTimeOffset(2024, 1, 1, 10, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds() * 1000;
        var expected2 = new DateTimeOffset(2024, 1, 2, 11, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds() * 1000;
        var expected3 = new DateTimeOffset(2024, 1, 3, 12, 0, 0, TimeSpan.Zero).ToUnixTimeMilliseconds() * 1000;

        Assert.Equal(expected1, tsArray.GetValue(0));
        Assert.Equal(expected2, tsArray.GetValue(1));
        Assert.Equal(expected3, tsArray.GetValue(2));
    }

    [Fact]
    public async Task EmptyCell_BecomesNull_AllTypes()
    {
        var csv = """
int_col,bigint_col,double_col,decimal_col,varchar_col,boolean_col,date_col,timestamp_col
1,100,1.5,10.5,text,true,2024-01-01,2024-01-01T10:00:00Z
,,,,,,
3,300,3.5,30.5,text3,false,2024-01-03,2024-01-03T12:00:00Z
""";

        var schema = new Schema(new List<Field>
        {
            new("int_col", Int32Type.Default, nullable: true),
            new("bigint_col", Int64Type.Default, nullable: true),
            new("double_col", DoubleType.Default, nullable: true),
            new("decimal_col", new Decimal128Type(38, 9), nullable: true),
            new("varchar_col", StringType.Default, nullable: true),
            new("boolean_col", BooleanType.Default, nullable: true),
            new("date_col", Date32Type.Default, nullable: true),
            new("timestamp_col", new TimestampType(TimeUnit.Microsecond, "+00:00"), nullable: true),
        }, new Dictionary<string, string>());

        var typeNames = new[] { "int", "bigint", "double", "decimal", "varchar", "boolean", "date", "timestamp" };
        var ordinals = new[] { 0, 1, 2, 3, 4, 5, 6, 7 };
        var path = "test.csv";
        var options = BatchOptions.Default;
        var rowNumberOffset = 0L;

        var reader = CsvDataReader.Create(new StringReader(csv));
        var batches = new List<RecordBatch>();
        await foreach (var b in CsvArrowReader.ReadAsync(reader, schema, typeNames, ordinals, path, options, rowNumberOffset, CancellationToken.None))
        {
            batches.Add(b);
        }

        Assert.Single(batches);
        var batch = batches[0];
        Assert.Equal(3, batch.Length);

        // Verify that row 1 (index 1) has all nulls across all types
        Assert.Null(((Int32Array)batch.Column(0)).GetValue(1));
        Assert.Null(((Int64Array)batch.Column(1)).GetValue(1));
        Assert.Null(((DoubleArray)batch.Column(2)).GetValue(1));
        Assert.Null(((Decimal128Array)batch.Column(3)).GetValue(1));
        Assert.Null(((StringArray)batch.Column(4)).GetString(1));
        Assert.Null(((BooleanArray)batch.Column(5)).GetValue(1));
        Assert.Null(((Date32Array)batch.Column(6)).GetValue(1));
        Assert.Null(((TimestampArray)batch.Column(7)).GetValue(1));
    }

    [Fact]
    public async Task MalformedCell_ThrowsWithFileLineColumnValue()
    {
        var csv = """
qty,name
1,Alice
abc,Bob
""";

        var schema = new Schema(new List<Field>
        {
            new("qty", Int32Type.Default, nullable: true),
            new("name", StringType.Default, nullable: true),
        }, new Dictionary<string, string>());

        var typeNames = new[] { "int", "varchar" };
        var ordinals = new[] { 0, 1 };
        var path = "test.csv";
        var options = BatchOptions.Default;
        var rowNumberOffset = 0L;

        var reader = CsvDataReader.Create(new StringReader(csv));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var b in CsvArrowReader.ReadAsync(reader, schema, typeNames, ordinals, path, options, rowNumberOffset, CancellationToken.None))
            {
                // Consume batches
            }
        });

        // Verify exception message contains file, line, column, and value
        Assert.Contains(path, ex.Message);
        Assert.Contains("line 2", ex.Message);
        Assert.Contains("'qty'", ex.Message);
        Assert.Contains("abc", ex.Message);
    }

    [Fact]
    public async Task OutOfOrderOrdinals_ResolvesCorrectly()
    {
        var csv = """
a,b
1,2
3,4
5,6
""";

        var schema = new Schema(new List<Field>
        {
            new("b", Int32Type.Default, nullable: true),
            new("a", Int32Type.Default, nullable: true),
        }, new Dictionary<string, string>());

        var typeNames = new[] { "int", "int" };
        var ordinals = new[] { 1, 0 };  // Contract b,a against header a,b
        var path = "test.csv";
        var options = BatchOptions.Default;
        var rowNumberOffset = 0L;

        var reader = CsvDataReader.Create(new StringReader(csv));
        var batches = new List<RecordBatch>();
        await foreach (var b in CsvArrowReader.ReadAsync(reader, schema, typeNames, ordinals, path, options, rowNumberOffset, CancellationToken.None))
        {
            batches.Add(b);
        }

        Assert.Single(batches);
        var batch = batches[0];
        Assert.Equal(3, batch.Length);

        // First column (b) should get values from second CSV column
        var bArray = (Int32Array)batch.Column(0);
        Assert.Equal(2, bArray.GetValue(0));
        Assert.Equal(4, bArray.GetValue(1));
        Assert.Equal(6, bArray.GetValue(2));

        // Second column (a) should get values from first CSV column
        var aArray = (Int32Array)batch.Column(1);
        Assert.Equal(1, aArray.GetValue(0));
        Assert.Equal(3, aArray.GetValue(1));
        Assert.Equal(5, aArray.GetValue(2));
    }
}
