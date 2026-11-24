using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary>Nested columns written to files through <c>pz run</c>: csv, a format with no nested types, gets each
/// STRUCT, LIST or MAP cell as JSON text rather than DuckDB's literal syntax, on the native copy and on the
/// universal (Arrow) writer alike; the universal json writer keeps them as objects and arrays; the universal
/// parquet writer refuses them, naming the column, before writing anything.</summary>
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
                    columns: { id: bigint, customer: varchar, sku: varchar }

            lake:
              connector: localfiles
            """);
        File.WriteAllText(Path.Combine(_work, "data", "lines.csv"), "id,customer,sku\n1,ann,A1\n1,ann,B2\n2,bob,C3\n");
        File.WriteAllText(Path.Combine(_work, "pipelines", "orders.sql"), Select);
        File.WriteAllText(Path.Combine(_work, "pipelines", "orders_csv.sql"), Output("orders_csv", "csv"));
    }

    private const string Select = """
            select id, {'name': any_value(customer)} as customer, list(sku order by sku) as skus,
                   map([any_value(customer)], [count(*)]) as counts
            from {{ source('raw', 'lines') }}
            group by id order by id
            """;

    private const string ExpectedCsv =
        "id,customer,skus,counts\n" +
        "1,\"{\"\"name\"\":\"\"ann\"\"}\",\"[\"\"A1\"\",\"\"B2\"\"]\",\"{\"\"ann\"\":2}\"\n" +
        "2,\"{\"\"name\"\":\"\"bob\"\"}\",\"[\"\"C3\"\"]\",\"{\"\"bob\"\":1}\"\n";

    private static string Output(string name, string format) =>
        $"INSERT INTO {{{{ sink('lake', '{name}', strategy: 'replace', format: '{format}', path: 'out/{name}/') }}}}\n" +
        "select * from {{ ref('orders') }} order by id\n";

    private void ForceUniversal(params (string Name, string Format)[] outputs)
    {
        File.AppendAllText(Path.Combine(_work, "project.yml"), "\nengine:\n  force_universal: true\n");
        File.Delete(Path.Combine(_work, "pipelines", "orders_csv.sql"));
        foreach (var (name, format) in outputs)
        {
            File.WriteAllText(Path.Combine(_work, "pipelines", $"{name}.sql"), Output(name, format));
        }
    }

    private string ReadOutput(string name, string extension) =>
        File.ReadAllText(Directory.GetFiles(Path.Combine(_work, "out", name), "*." + extension).Single()).ReplaceLineEndings("\n");

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
        Assert.Equal(ExpectedCsv, ReadOutput("orders_csv", "csv"));
    }

    [Fact]
    public void The_universal_csv_writer_writes_the_same_JSON_cells_and_the_json_writer_real_objects()
    {
        ForceUniversal(("orders_csv", "csv"), ("orders_json", "json"));

        var (exit, output) = Run();

        Assert.True(exit == 0, output);
        Assert.Equal(ExpectedCsv, ReadOutput("orders_csv", "csv"));
        Assert.Equal(
            "{\"id\":1,\"customer\":{\"name\":\"ann\"},\"skus\":[\"A1\",\"B2\"],\"counts\":{\"ann\":2}}\n" +
            "{\"id\":2,\"customer\":{\"name\":\"bob\"},\"skus\":[\"C3\"],\"counts\":{\"bob\":1}}\n",
            ReadOutput("orders_json", "json"));
    }

    [Fact]
    public void The_universal_parquet_writer_refuses_a_nested_column_naming_it()
    {
        ForceUniversal(("orders_parquet", "parquet"));

        var (exit, output) = Run();

        Assert.NotEqual(0, exit);
        Assert.Contains("column 'customer'", output, StringComparison.Ordinal);
        Assert.Contains("cannot write a nested column to parquet", output, StringComparison.Ordinal);
        Assert.Contains("customer::JSON::VARCHAR", output, StringComparison.Ordinal);
        Assert.False(Directory.Exists(Path.Combine(_work, "out", "orders_parquet"))
            && Directory.GetFiles(Path.Combine(_work, "out", "orders_parquet"), "*.parquet").Length > 0);
    }
}
