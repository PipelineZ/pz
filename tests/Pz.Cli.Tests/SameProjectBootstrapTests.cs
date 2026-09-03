using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary>A project whose one flow writes a duckdb file and whose other flow reads it back has no
/// DAG edge between the two (edges come from ref/source/sink calls, and the read is a fresh source),
/// so on a fresh checkout the reader's plan-time refusal (PZ0353, file does not exist yet) used to
/// block even `pz run &lt;writer&gt;`. The planner now defers a refusal on a node outside the run's
/// effective set, so the writer bootstraps the file and the reader runs next.</summary>
// See the "console-and-env-serialized" collection definition in RestoreCommandTests.cs: this class
// redirects Console.Error to assert on CLI output.
[Collection("console-and-env-serialized")]
public sealed class SameProjectBootstrapTests : IDisposable
{
    private readonly string _work =
        Path.Combine(Path.GetTempPath(), "pz-bootstrap-tests", Guid.NewGuid().ToString("N"));

    public SameProjectBootstrapTests()
    {
        Directory.CreateDirectory(Path.Combine(_work, "pipelines"));
        Directory.CreateDirectory(Path.Combine(_work, "data"));
        File.WriteAllText(Path.Combine(_work, "project.yml"), "name: bootstrap\nversion: 0.1.0\n");
        File.WriteAllText(Path.Combine(_work, "connections.yml"), """
            raw:
              connector: localfiles
              entities:
                orders:
                  read:
                    path: data/orders.csv
                    format: csv
                    columns:
                      id: bigint
                      amount: double

            wh:
              connector: duckdb
              path: data/wh.duckdb
              entities:
                orders_current:
                  read:
                    columns:
                      id: bigint
                      amount: double

            out:
              connector: localfiles
              root: out
            """);
        File.WriteAllText(Path.Combine(_work, "data", "orders.csv"), "id,amount\n1,10.5\n2,20.0\n");
        File.WriteAllText(Path.Combine(_work, "pipelines", "load_orders.sql"), """
            INSERT INTO {{ sink('wh', 'orders_current', strategy: 'replace') }}
            select id, amount from {{ source('raw', 'orders') }}
            """);
        File.WriteAllText(Path.Combine(_work, "pipelines", "readback.sql"), """
            INSERT INTO {{ sink('out', 'readback', format: 'csv', strategy: 'replace') }}
            select id, amount from {{ source('wh', 'orders_current') }}
            """);
    }

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private string WarehouseFile => Path.Combine(_work, "data", "wh.duckdb");

    private static (int Exit, string Stderr) InvokeCapturingStderr(string[] args)
    {
        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        try
        {
            return (CliApp.Build().Parse(args).Invoke(), stderr.ToString());
        }
        finally
        {
            Console.SetError(original);
        }
    }

    [Fact]
    public void Run_all_on_a_fresh_project_still_refuses_the_read_PZ0353()
    {
        var (exit, stderr) = InvokeCapturingStderr(["run", "--all", "--project", _work]);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0353", stderr);
        Assert.False(File.Exists(WarehouseFile));
    }

    [Fact]
    public void Writer_flow_bootstraps_the_file_and_the_reader_flow_reads_it_next()
    {
        var (writerExit, writerStderr) = InvokeCapturingStderr(["run", "load_orders", "--project", _work]);
        Assert.Equal(ExitCodes.Ok, writerExit);
        Assert.DoesNotContain("PZ0353", writerStderr);
        Assert.True(File.Exists(WarehouseFile));

        var planJson = File.ReadAllText(Path.Combine(_work, ".pz", "target", "plan.json"));
        Assert.Contains("PZ0353 deferred because this node is not part of the run", planJson);

        var (readerExit, readerStderr) = InvokeCapturingStderr(["run", "readback", "--project", _work]);
        Assert.Equal(ExitCodes.Ok, readerExit);
        Assert.DoesNotContain("PZ0353", readerStderr);
        var outDir = Path.Combine(_work, "out", "readback");
        var written = Directory.GetFiles(outDir, "*.csv");
        Assert.Single(written);
        Assert.Equal(3, File.ReadAllLines(written[0]).Length);
    }

    [Fact]
    public void Plan_of_the_writer_flow_succeeds_and_records_the_deferred_read()
    {
        var (exit, stderr) = InvokeCapturingStderr(["plan", "load_orders", "--project", _work]);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.DoesNotContain("PZ0353", stderr);
        var planJson = File.ReadAllText(Path.Combine(_work, ".pz", "target", "plan.json"));
        Assert.Contains("PZ0353 deferred because this node is not part of the run", planJson);
        Assert.DoesNotContain("wh.duckdb", planJson);
    }
}
