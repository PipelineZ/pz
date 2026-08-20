using System.Text.Json;
using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary>`pz plan`: shows the per-node strategy the engine would use and persists it to
/// `.pz/target/plan.json`, without executing anything. Uses the same `hello-pz` fixture (connector
/// "localfiles") as <see cref="RunCommandTests"/>.</summary>
// See the "console-and-env-serialized" collection definition in RestoreCommandTests.cs: this class
// redirects Console.Out to assert on CLI output and mutates the process-global DATA_DIR/OUT_DIR env
// vars, both of which must serialize against every other Console/env-swapping class in the assembly.
[Collection("console-and-env-serialized")]
public sealed class PlanCommandTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-plan-tests", Guid.NewGuid().ToString("N"));

    public PlanCommandTests()
    {
        Environment.SetEnvironmentVariable("DATA_DIR", "/tmp/pz-data");
        Environment.SetEnvironmentVariable("OUT_DIR", "/tmp/pz-out");
        CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", "hello-pz"), _work);
    }

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void Plan_verb_prints_strategies_and_reasons()
    {
        var stdout = new StringWriter();
        var originalOut = Console.Out;
        Console.SetOut(stdout);
        int exit;
        try
        {
            exit = CliApp.Build().Parse(["plan", "--project", _work]).Invoke();
        }
        finally
        {
            Console.SetOut(originalOut);
        }

        Assert.Equal(ExitCodes.Ok, exit);

        var output = stdout.ToString();
        // CsvSource.TryGetNativeScan never declines for csv, contract or no contract — DuckDB's own
        // auto_detect covers the no-contract case. The hello-pz fixture's 'customers' dataset declares
        // a columns: contract and 'orders' does not, yet both plan native_scan, so no dataset here
        // falls back to the universal (arrow_stream) reason. That reason string is covered at engine
        // level (ExecutionPlannerTests/PlanWriterTests) and by RunCommandTests' engine.force_universal
        // cases in this assembly.
        Assert.Contains("native scan: connector 'localfiles' provides read_csv over customers.csv", output);
        Assert.Contains("native scan: connector 'localfiles' provides read_csv over orders.csv", output);
        Assert.Contains("duckdb sql: executes in-engine", output);
        Assert.Contains("duck_sql", output);
        Assert.Contains("native_scan", output);
    }

    /// <summary>The console table is filtered by --select, but plan.json (written
    /// regardless) always covers the full project — the notice line must appear only when a selection
    /// is actually active, never on an unfiltered `pz plan`.</summary>
    [Fact]
    public void Plan_verb_prints_select_notice_only_when_selecting()
    {
        var withoutSelect = RunPlanCapturingStdout(["plan", "--project", _work]);
        Assert.DoesNotContain("note: table filtered by selection", withoutSelect);

        var withSelect = RunPlanCapturingStdout(["plan", "--project", _work, "--select", "stg_orders"]);
        Assert.Contains("note: table filtered by selection; plan.json covers the full project", withSelect);
    }

    private static string RunPlanCapturingStdout(string[] args)
    {
        var stdout = new StringWriter();
        var originalOut = Console.Out;
        Console.SetOut(stdout);
        try
        {
            var exit = CliApp.Build().Parse(args).Invoke();
            Assert.Equal(ExitCodes.Ok, exit);
        }
        finally
        {
            Console.SetOut(originalOut);
        }

        return stdout.ToString();
    }

    /// <summary>`pz plan` must be side-effect-free. ExecutionPlanner's TryGetNativeCopy probe reaches
    /// LocalFilesSink.ResolveFinalPath, which must not call Directory.CreateDirectory on the sink's
    /// configured output path — the hello-pz fixture's lake sink writes under "curated/orders/", which
    /// does not exist in the fixture, so its (non-)existence after a plan-only run is what this
    /// pins.</summary>
    [Fact]
    public void Plan_verb_creates_no_output_directories()
    {
        var outputDir = Path.Combine(_work, "curated");
        Assert.False(Directory.Exists(outputDir));

        var exit = CliApp.Build().Parse(["plan", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);

        Assert.False(Directory.Exists(outputDir));
    }

    /// <summary>`pz plan` prints the memory budget the same
    /// ExecutionPlanner call also writes into plan.json's additive `memoryBudget` object. The fixture's
    /// project.yml sets `duckdb.memory_limit: 1GiB`, so the duckdb component renders as a byte figure,
    /// not the unset-disclaimer text.</summary>
    [Fact]
    public void Plan_prints_memory_budget_line()
    {
        var output = RunPlanCapturingStdout(["plan", "--project", _work]);

        Assert.Contains("memory budget: ~", output);
        Assert.Contains("duckdb", output);
        Assert.Contains("channels", output);
        Assert.Contains("overhead 256MB", output);
    }

    [Fact]
    public void Plan_verb_writes_plan_json()
    {
        var exit = CliApp.Build().Parse(["plan", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);

        var planPath = Path.Combine(_work, ".pz", "target", "plan.json");
        Assert.True(File.Exists(planPath));

        var text = File.ReadAllText(planPath);
        Assert.Contains("\"version\": 1", text);

        using var document = JsonDocument.Parse(text);
        Assert.True(document.RootElement.GetProperty("nodes").GetArrayLength() > 0);
    }

    [Fact]
    public void Plan_verb_exits_config_error_on_broken_project()
    {
        var brokenDir = Path.Combine(Path.GetTempPath(), "pz-plan-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(brokenDir);
        try
        {
            var exit = CliApp.Build().Parse(["plan", "--project", brokenDir]).Invoke();
            Assert.Equal(ExitCodes.ConfigError, exit);
        }
        finally
        {
            Directory.Delete(brokenDir, recursive: true);
        }
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
