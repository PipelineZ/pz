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

        // Verify ts column type
        var tsArray = (TimestampArray)batch.Column(2);
        Assert.IsType<TimestampArray>(tsArray);
    }

    [Fact]
    public async Task EmptyCell_BecomesNull()
    {
        var csv = """
id,name
1,Alice
2,
3,Charlie
""";

        var schema = new Schema(new List<Field>
        {
            new("id", Int32Type.Default, nullable: true),
            new("name", StringType.Default, nullable: true),
        }, new Dictionary<string, string>());

        var typeNames = new[] { "int", "varchar" };
        var ordinals = new[] { 0, 1 };
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

        // Verify name column has null at index 1
        var nameArray = (StringArray)batch.Column(1);
        Assert.Null(nameArray.GetString(1));
    }

    [Fact]
    public async Task MalformedCell_ThrowsWithFileLineColumnValue()
    {
        var csv = """
id,name
1,Alice
abc,Bob
""";

        var schema = new Schema(new List<Field>
        {
            new("id", Int32Type.Default, nullable: true),
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
        Assert.Contains("id", ex.Message);
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
