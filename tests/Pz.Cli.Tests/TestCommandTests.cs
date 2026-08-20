using System.Text.Json;
using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary>`pz test`: CLI coverage against the shipped <c>templates/sample</c> tree (copied into the
/// test output as "TemplatesSample" -- see the .csproj), a real, working project with actual CSV data
/// files and no env-var-templated connection config, so these tests need no DATA_DIR/OUT_DIR juggling. Still joins
/// "console-and-env-serialized" (see the collection definition in RestoreCommandTests.cs) because
/// <c>Test_no_checks_exits_0</c> redirects the process-global <see cref="Console.Out"/> — the same
/// resource <c>ValidateCommandTests</c>/<c>RunCommandTests</c> serialize on, not just DATA_DIR/OUT_DIR.</summary>
[Collection("console-and-env-serialized")]
public class TestCommandTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-test-cmd-tests", Guid.NewGuid().ToString("N"));

    public TestCommandTests()
    {
        CopyTree(Path.Combine(AppContext.BaseDirectory, "TemplatesSample"), _work);
    }

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void Test_runs_only_checks_and_ancestors()
    {
        var exit = CliApp.Build().Parse(["test", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);

        var names = NodeNames(ReadRunResults(_work));

        Assert.Contains("check_orders_enriched_not_null_id_email", names);
        Assert.Contains("check_orders_enriched_unique_id", names);
        Assert.Contains("orders_enriched", names);       // the checks' owning pipeline
        Assert.Contains("stg_orders", names);             // orders_enriched's pipeline dependency
        Assert.Contains("src_raw__customers", names);      // ancestor source
        Assert.Contains("src_raw__orders", names);         // ancestor source (via stg_orders)
        Assert.DoesNotContain("lake.orders_curated", names); // the sink never runs
    }

    [Fact]
    public void Test_with_failing_check_exits_1_with_samples()
    {
        // Seed a duplicate order id: both rows clear stg_orders' `amount >= 10` filter, so
        // orders_enriched's own `id` column ends up with a duplicate -- failing `unique: [id]`
        // without touching `not_null` (every order still resolves a customer/email).
        var ordersPath = Path.Combine(_work, "data", "orders.csv");
        File.AppendAllText(ordersPath, "5,2,20.00,shipped\n");

        var exit = CliApp.Build().Parse(["test", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.NodeFailures, exit);

        var checkNode = ReadRunResults(_work).RootElement.GetProperty("nodes").EnumerateArray()
            .Single(n => n.GetProperty("name").GetString() == "check_orders_enriched_unique_id");

        Assert.Equal("failed", checkNode.GetProperty("status").GetString());
        Assert.True(checkNode.GetProperty("rows").GetInt64() > 0);

        var error = checkNode.GetProperty("error");
        Assert.Equal("PZ0510", error.GetProperty("code").GetString());
        var message = error.GetProperty("message").GetString()!;
        Assert.Contains("violation(s)", message);
        Assert.Contains("id=5", message);
    }

    /// <summary>Smoke test for the check vocabulary through the real CLI path (loader validation,
    /// DagCompiler naming, executor, exit code). custom_sql stands in for freshness because
    /// the sample template has no temporal column; the Check-node machinery is identical. The query returns
    /// every row, so the check fails by design.</summary>
    [Fact]
    public void Test_with_failing_custom_sql_check_exits_1()
    {
        var configPath = Path.Combine(_work, "pipelines", "configs", "orders_enriched.yml");
        File.WriteAllText(configPath, """
            pipeline: orders_enriched
            checks:
              - custom_sql:
                  name: no_rows_at_all
                  sql: select * from staging.orders_enriched
            """);

        var exit = CliApp.Build().Parse(["test", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.NodeFailures, exit);

        var checkNode = ReadRunResults(_work).RootElement.GetProperty("nodes").EnumerateArray()
            .Single(n => n.GetProperty("name").GetString() == "check_orders_enriched_no_rows_at_all");
        Assert.Equal("failed", checkNode.GetProperty("status").GetString());
        Assert.Equal("PZ0510", checkNode.GetProperty("error").GetProperty("code").GetString());
    }

    [Fact]
    public void Test_no_checks_exits_0()
    {
        var configPath = Path.Combine(_work, "pipelines", "configs", "orders_enriched.yml");
        File.WriteAllText(configPath, "pipeline: orders_enriched\nmaterialization: table\ntags: [daily, crm]\n");

        var stdout = new StringWriter();
        var originalOut = Console.Out;
        Console.SetOut(stdout);
        int exit;
        try
        {
            exit = CliApp.Build().Parse(["test", "--project", _work]).Invoke();
        }
        finally
        {
            Console.SetOut(originalOut);
        }

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("no checks defined", stdout.ToString());
        Assert.False(Directory.Exists(Path.Combine(_work, ".pz", "runs")));
    }

    private static HashSet<string> NodeNames(JsonDocument runResults) =>
        runResults.RootElement.GetProperty("nodes").EnumerateArray()
            .Select(n => n.GetProperty("name").GetString()!)
            .ToHashSet();

    private static JsonDocument ReadRunResults(string projectDir)
    {
        var runsDir = Path.Combine(projectDir, ".pz", "runs");
        var runDir = Directory.GetDirectories(runsDir).Single();
        var path = Path.Combine(runDir, "run_results.json");
        return JsonDocument.Parse(File.ReadAllBytes(path));
    }

    private static void CopyTree(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (var file in Directory.EnumerateFiles(from, "*", SearchOption.AllDirectories))
        {
            var dest = Path.Combine(to, Path.GetRelativePath(from, file));
            Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
            File.Copy(file, dest, overwrite: true);
        }
    }
}
