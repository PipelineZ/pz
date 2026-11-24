using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary>Nested columns written to a file through <c>pz run</c>'s native copy: csv, a format with no nested
/// types, gets each STRUCT, LIST or MAP cell as JSON text rather than DuckDB's literal syntax.</summary>
[Collection("console-and-env-serialized")]
public sealed class NestedFileOutputRunTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-nested-file-tests", Guid.NewGuid().ToString("N"));

    public NestedFileOutputRunTests()
    {
        Directory.CreateDirectory(Path.Combine(_work, "data"));
        Directory.CreateDirectory(Path.Combine(_work, "pipelines"));
        File.WriteAllText(Path.Combine(_work, "project.yml"),
            "name: nested_demo\nversion: 0.1.0\n\nconnectors:\n  - package: Pz.Connector.LocalFiles\n    version: 0.1.0\n");
        File.WriteAllText(Path.Combine(_work, "connections.yml"), """
            raw:
              connector: localfiles
              entities:
                lines:
                  read:
                    path: data/lines.csv
                    format: csv

            lake:
              connector: localfiles
            """);
        File.WriteAllText(Path.Combine(_work, "data", "lines.csv"), "id,customer,sku\n1,ann,A1\n1,ann,B2\n2,bob,C3\n");
        const string select = """
            select id, {'name': any_value(customer)} as customer, list(sku order by sku) as skus,
                   map([any_value(customer)], [count(*)]) as counts
            from {{ source('raw', 'lines') }}
            group by id order by id
            """;
        File.WriteAllText(Path.Combine(_work, "pipelines", "orders_csv.sql"),
            "INSERT INTO {{ sink('lake', 'orders_csv', strategy: 'replace', format: 'csv', path: 'out/csv/') }}\n" + select);
    }

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private (int Exit, string Output) Run()
    {
        var output = new StringWriter();
        var (originalErr, originalOut) = (Console.Error, Console.Out);
        Console.SetError(output);
        Console.SetOut(output);
        try
        {
            return (CliApp.Build().Parse(["run", "--all", "--project", _work]).Invoke(), output.ToString());
        }
        finally
        {
            Console.SetError(originalErr);
            Console.SetOut(originalOut);
        }
    }

    [Fact]
    public void A_csv_output_writes_nested_cells_as_JSON_text()
    {
        var (exit, output) = Run();

        Assert.True(exit == 0, output);
        var csv = File.ReadAllText(Directory.GetFiles(Path.Combine(_work, "out", "csv"), "*.csv").Single());
        Assert.Equal(
            "id,customer,skus,counts\n" +
            "1,\"{\"\"name\"\":\"\"ann\"\"}\",\"[\"\"A1\"\",\"\"B2\"\"]\",\"{\"\"ann\"\":2}\"\n" +
            "2,\"{\"\"name\"\":\"\"bob\"\"}\",\"[\"\"C3\"\"]\",\"{\"\"bob\"\":1}\"\n",
            csv.ReplaceLineEndings("\n"));
    }
}
