using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary>Nested <c>columns:</c> contract types through <c>pz run</c>: a csv whose cells hold JSON text and an
/// ndjson file with real objects both land <c>varchar[]</c>, <c>struct(...)</c>, <c>map(...)</c> and
/// <c>int[2]</c> columns typed all the way down, on the native scan (both) and the universal csv reader,
/// so pipeline SQL can index into them. A type that does not parse is PZ0368 at validate, every bad column
/// reported.</summary>
[Collection("console-and-env-serialized")]
public sealed class NestedContractReadRunTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-nested-contract-tests", Guid.NewGuid().ToString("N"));

    private const string Columns =
        "        columns:\n" +
        "          id: bigint\n" +
        "          tags: varchar[]\n" +
        "          person: struct(name varchar, langs varchar[])\n" +
        "          counts: map(varchar, bigint)\n" +
        "          pair: int[2]\n";

    public NestedContractReadRunTests()
    {
        Directory.CreateDirectory(Path.Combine(_work, "data"));
        Directory.CreateDirectory(Path.Combine(_work, "pipelines"));
        File.WriteAllText(Path.Combine(_work, "project.yml"),
            "name: nested_contract\nversion: 0.1.0\n\nconnectors:\n  - package: Pz.Connector.LocalFiles\n    version: 0.1.0\n");
        File.WriteAllText(Path.Combine(_work, "connections.yml"),
            "raw:\n  connector: localfiles\n  entities:\n" +
            "    from_csv:\n      read:\n        path: data/rows.csv\n        format: csv\n" + Columns +
            "    from_json:\n      read:\n        path: data/rows.ndjson\n        format: json\n" + Columns +
            "\nlake:\n  connector: localfiles\n");
        File.WriteAllText(Path.Combine(_work, "data", "rows.csv"),
            "id,tags,person,counts,pair\n" +
            "1,\"[\"\"a\"\",\"\"b\"\"]\",\"{\"\"name\"\":\"\"ann\"\",\"\"langs\"\":[\"\"en\"\"]}\",\"{\"\"x\"\":2}\",\"[1,2]\"\n" +
            "2,,,,\n");
        File.WriteAllText(Path.Combine(_work, "data", "rows.ndjson"),
            "{\"id\":1,\"tags\":[\"a\",\"b\"],\"person\":{\"name\":\"ann\",\"langs\":[\"en\"]},\"counts\":{\"x\":2},\"pair\":[1,2]}\n" +
            "{\"id\":2}\n");
        foreach (var source in new[] { "from_csv", "from_json" })
        {
            File.WriteAllText(Path.Combine(_work, "pipelines", $"{source}_out.sql"), $$$"""
                INSERT INTO {{ sink('lake', '{{{source}}}_out', strategy: 'replace', format: 'csv', path: 'out/{{{source}}}/') }}
                select id, tags[2] as second_tag, len(tags) as n_tags, person.name as name, person.langs[1] as lang,
                       counts['x'] as x, pair[2] as p2
                from {{ source('raw', '{{{source}}}') }}
                order by id
                """);
        }
    }

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private (int Exit, string Output) Cli(params string[] args)
    {
        var output = new StringWriter();
        var (originalErr, originalOut) = (Console.Error, Console.Out);
        Console.SetError(output);
        Console.SetOut(output);
        try
        {
            return (CliApp.Build().Parse([.. args, "--project", _work]).Invoke(), output.ToString());
        }
        finally
        {
            Console.SetError(originalErr);
            Console.SetOut(originalOut);
        }
    }

    private string Output(string source) =>
        File.ReadAllText(Directory.GetFiles(Path.Combine(_work, "out", source), "*.csv").Single()).ReplaceLineEndings("\n");

    private const string Expected = "id,second_tag,n_tags,name,lang,x,p2\n1,b,2,ann,en,2,2\n2,,,,,,\n";

    [Fact]
    public void Nested_contract_columns_are_typed_on_the_native_scan()
    {
        var (exit, output) = Cli("run", "--all");

        Assert.True(exit == 0, output);
        Assert.Equal(Expected, Output("from_csv"));
        Assert.Equal(Expected, Output("from_json"));
    }

    [Fact]
    public void Nested_contract_columns_are_typed_on_the_universal_reader()
    {
        // localfiles reads json natively only; the universal ndjson reader is covered in NdjsonCodec's tests.
        File.AppendAllText(Path.Combine(_work, "project.yml"), "\nengine:\n  force_universal: true\n");
        File.Delete(Path.Combine(_work, "pipelines", "from_json_out.sql"));
        File.WriteAllText(Path.Combine(_work, "connections.yml"), File.ReadAllText(Path.Combine(_work, "connections.yml"))
            [..File.ReadAllText(Path.Combine(_work, "connections.yml")).IndexOf("    from_json:", StringComparison.Ordinal)] +
            "\nlake:\n  connector: localfiles\n");

        var (exit, output) = Cli("run", "--all");

        Assert.True(exit == 0, output);
        Assert.Equal(Expected, Output("from_csv"));
    }

    [Fact]
    public void A_contract_type_that_does_not_parse_is_PZ0368_for_every_bad_column()
    {
        File.WriteAllText(Path.Combine(_work, "connections.yml"), File.ReadAllText(Path.Combine(_work, "connections.yml"))
            .Replace("tags: varchar[]", "tags: varchar[0]", StringComparison.Ordinal)
            .Replace("counts: map(varchar, bigint)", "counts: map(varchar[], bigint)", StringComparison.Ordinal));

        var (exit, output) = Cli("validate");

        Assert.Equal(2, exit);
        Assert.Contains("PZ0368", output, StringComparison.Ordinal);
        Assert.Contains("column 'tags': 'varchar[0]' is not a columns: contract type (array size '0' must be a positive whole number)", output, StringComparison.Ordinal);
        Assert.Contains("column 'counts': 'map(varchar[], bigint)' is not a columns: contract type (a map key must be a flat type)", output, StringComparison.Ordinal);
    }
}
