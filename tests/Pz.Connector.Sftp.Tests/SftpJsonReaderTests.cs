using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.Sftp.Tests;

public class SftpJsonReaderTests
{
    private static readonly Dictionary<string, string> AllTypesColumns = new()
    {
        ["id"] = "int",
        ["big"] = "bigint",
        ["amt"] = "double",
        ["price"] = "decimal",
        ["name"] = "varchar",
        ["active"] = "boolean",
        ["d"] = "date",
        ["ts"] = "timestamp",
    };

    private static Stream ToStream(string text) => new MemoryStream(Encoding.UTF8.GetBytes(text));

    private static async Task<List<RecordBatch>> ReadAllAsync(
        string ndjson, IReadOnlyDictionary<string, string> columns, BatchOptions? options = null, string context = "test")
    {
        var batches = new List<RecordBatch>();
        await foreach (var batch in SftpJsonReader.ReadAsync(
            ToStream(ndjson), columns, context, options ?? BatchOptions.Default, CancellationToken.None))
        {
            batches.Add(batch);
        }

        return batches;
    }

    [Fact]
    public async Task NdjsonLines_ProduceCorrectBatch_PerV0TypeMatrix()
    {
        const string ndjson = """
            {"id":1,"big":123456789012,"amt":1.5,"price":10.75,"name":"Alice","active":true,"d":"2024-01-01","ts":"2024-01-01T10:00:00Z"}
            {"id":2,"big":223456789012,"amt":2.5,"price":20.75,"name":"Bob","active":false,"d":"2024-01-02","ts":"2024-01-02T11:00:00Z"}
            """;

        var batches = await ReadAllAsync(ndjson, AllTypesColumns);

        Assert.Single(batches);
        var batch = batches[0];
        Assert.Equal(2, batch.Length);
        Assert.Equal(8, batch.ColumnCount);

        Assert.Equal(1, ((Int32Array)batch.Column(0)).GetValue(0));
        Assert.Equal(2, ((Int32Array)batch.Column(0)).GetValue(1));
        Assert.Equal(123456789012L, ((Int64Array)batch.Column(1)).GetValue(0));
        Assert.Equal(1.5, ((DoubleArray)batch.Column(2)).GetValue(0));
        Assert.Equal(10.75m, ((Decimal128Array)batch.Column(3)).GetValue(0));
        Assert.Equal("Alice", ((StringArray)batch.Column(4)).GetString(0));
        Assert.True(((BooleanArray)batch.Column(5)).GetValue(0));
        Assert.False(((BooleanArray)batch.Column(5)).GetValue(1));
        Assert.Equal(new DateOnly(2024, 1, 1), ((Date32Array)batch.Column(6)).GetDateOnly(0));
        Assert.Equal(
            new DateTimeOffset(2024, 1, 1, 10, 0, 0, TimeSpan.Zero),
            ((TimestampArray)batch.Column(7)).GetTimestamp(0));
    }

    [Fact]
    public async Task ExtraJsonKeys_AreIgnored()
    {
        var columns = new Dictionary<string, string> { ["id"] = "int" };
        const string ndjson = """{"id":1,"unexpected":"value","another":42}""";

        var batches = await ReadAllAsync(ndjson, columns);

        Assert.Single(batches);
        var batch = batches[0];
        Assert.Equal(1, batch.ColumnCount);
        Assert.Equal(1, ((Int32Array)batch.Column(0)).GetValue(0));
    }

    [Fact]
    public async Task MissingKey_BecomesNull()
    {
        var columns = new Dictionary<string, string> { ["id"] = "int", ["name"] = "varchar" };
        const string ndjson = """{"id":1}""";

        var batches = await ReadAllAsync(ndjson, columns);

        Assert.Single(batches);
        var batch = batches[0];
        Assert.Equal(1, ((Int32Array)batch.Column(0)).GetValue(0));
        Assert.Null(((StringArray)batch.Column(1)).GetString(0));
    }

    [Fact]
    public async Task TypeMismatch_ThrowsPermanentError_NamingColumn_NeverEchoingValue()
    {
        var columns = new Dictionary<string, string> { ["id"] = "int" };
        const string secretLookingValue = "not-an-int-abc123";
        var ndjson = $$"""{"id":"{{secretLookingValue}}"}""";

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await ReadAllAsync(ndjson, columns);
        });

        Assert.False(ex.IsTransient);
        Assert.Contains("'id'", ex.Message);
        Assert.DoesNotContain(secretLookingValue, ex.Message);
    }

    [Fact]
    public async Task BlankLines_AreSkipped()
    {
        var columns = new Dictionary<string, string> { ["id"] = "int" };
        const string ndjson = "{\"id\":1}\n\n{\"id\":2}\n";

        var batches = await ReadAllAsync(ndjson, columns);

        Assert.Single(batches);
        var batch = batches[0];
        Assert.Equal(2, batch.Length);
        Assert.Equal(1, ((Int32Array)batch.Column(0)).GetValue(0));
        Assert.Equal(2, ((Int32Array)batch.Column(0)).GetValue(1));
    }

    [Fact]
    public async Task Batch_SplitsAt_MaxRowsPerBatch()
    {
        var columns = new Dictionary<string, string> { ["id"] = "int" };
        var ndjson = string.Join('\n', Enumerable.Range(1, 5).Select(i => $$"""{"id":{{i}}}"""));
        var options = new BatchOptions(MaxRowsPerBatch: 2);

        var batches = await ReadAllAsync(ndjson, columns, options);

        Assert.Equal(3, batches.Count);
        Assert.Equal(2, batches[0].Length);
        Assert.Equal(2, batches[1].Length);
        Assert.Equal(1, batches[2].Length);

        Assert.Equal(1, ((Int32Array)batches[0].Column(0)).GetValue(0));
        Assert.Equal(2, ((Int32Array)batches[0].Column(0)).GetValue(1));
        Assert.Equal(3, ((Int32Array)batches[1].Column(0)).GetValue(0));
        Assert.Equal(4, ((Int32Array)batches[1].Column(0)).GetValue(1));
        Assert.Equal(5, ((Int32Array)batches[2].Column(0)).GetValue(0));
    }
}
