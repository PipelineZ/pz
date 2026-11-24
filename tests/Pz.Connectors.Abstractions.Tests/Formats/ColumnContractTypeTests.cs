using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Abstractions.Formats;

namespace Pz.Connectors.Abstractions.Tests.Formats;

/// <summary>The <c>columns:</c> contract type grammar: flat names, lists, fixed-size arrays, structs (bare and
/// quoted field names) and maps nest freely and map to DuckDB's spelling; malformed text fails with what and
/// where; and the universal ndjson reader fills a nested contract from real JSON.</summary>
public sealed class ColumnContractTypeTests
{
    [Theory]
    [InlineData("bigint", "BIGINT")]
    [InlineData("varchar[]", "VARCHAR[]")]
    [InlineData("int[3]", "INTEGER[3]")]
    [InlineData("varchar[][]", "VARCHAR[][]")]
    [InlineData("struct(name varchar, tags varchar[])", "STRUCT(\"name\" VARCHAR, \"tags\" VARCHAR[])")]
    [InlineData("struct( \"first name\" varchar , \"say \"\"hi\"\"\" int )", "STRUCT(\"first name\" VARCHAR, \"say \"\"hi\"\"\" INTEGER)")]
    [InlineData("map(varchar, struct(at timestamp, amount decimal)[])", "MAP(VARCHAR, STRUCT(\"at\" TIMESTAMP, \"amount\" DECIMAL(38,9))[])")]
    [InlineData("struct(m map(date, boolean))[2]", "STRUCT(\"m\" MAP(DATE, BOOLEAN))[2]")]
    public void Types_parse_and_map_to_DuckDB(string text, string duckDb)
    {
        Assert.True(ColumnContractType.TryParse(text, out var type, out var error), error);
        Assert.Equal(duckDb, ColumnContractType.ToDuckDbName(type!));
    }

    [Theory]
    [InlineData("varchar2", "unknown type 'varchar2'")]
    [InlineData("struct(a int", "expected ')' at the end")]
    [InlineData("struct(a int, a varchar)", "struct field 'a' appears twice")]
    [InlineData("map(int[], varchar)", "a map key must be a flat type")]
    [InlineData("int[0]", "array size '0' must be a positive whole number")]
    [InlineData("int[] x", "unexpected text at position 7 ('x')")]
    [InlineData("struct(1a int)", "expected a field name at position 8 ('1')")]
    [InlineData("", "expected a type at the end")]
    public void Malformed_types_say_what_and_where(string text, string error)
    {
        Assert.False(ColumnContractType.TryParse(text, out _, out var actual));
        Assert.Equal(error, actual);
    }

    [Fact]
    public async Task The_ndjson_reader_fills_a_nested_contract()
    {
        var contract = new Dictionary<string, string>
        {
            ["id"] = "bigint",
            ["person"] = "struct(name varchar, langs varchar[])",
            ["counts"] = "map(varchar, bigint)",
            ["pair"] = "int[2]",
        };
        const string input =
            "{\"id\":1,\"person\":{\"name\":\"ann\",\"langs\":[\"en\",null],\"extra\":true},\"counts\":{\"x\":2},\"pair\":[1,2]}\n" +
            "{\"id\":2,\"person\":null}\n";

        var batches = new List<RecordBatch>();
        await foreach (var batch in NdjsonCodec.ReadAsync(new MemoryStream(Encoding.UTF8.GetBytes(input)), contract, null,
            BatchOptions.Default, CancellationToken.None))
        {
            batches.Add(batch);
        }

        using var only = Assert.Single(batches);
        Assert.IsType<StructType>(only.Schema.GetFieldByName("person").DataType);
        using var output = new MemoryStream();
        await NdjsonCodec.WriteAsync(only, output, CancellationToken.None);
        Assert.Equal(
            "{\"id\":1,\"person\":{\"name\":\"ann\",\"langs\":[\"en\",null]},\"counts\":{\"x\":2},\"pair\":[1,2]}\n" +
            "{\"id\":2,\"person\":null,\"counts\":null,\"pair\":null}\n",
            Encoding.UTF8.GetString(output.ToArray()));
    }

    [Fact]
    public async Task A_nested_value_of_the_wrong_JSON_kind_names_line_and_column_not_the_value()
    {
        var contract = new Dictionary<string, string> { ["tags"] = "varchar[]" };
        var input = new MemoryStream(Encoding.UTF8.GetBytes("{\"tags\":\"secret\"}\n"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var batch in NdjsonCodec.ReadAsync(input, contract, null, BatchOptions.Default, CancellationToken.None))
            {
                batch.Dispose();
            }
        });

        Assert.Equal("NDJSON line 1, column 'tags': value is not a valid varchar[]", ex.Message);
    }
}
