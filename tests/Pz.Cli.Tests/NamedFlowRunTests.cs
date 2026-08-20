using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary>End-to-end coverage for named flow runs over the two-flows
/// fixture: two disjoint localfiles chains, orders_clean -> lake.orders_out and
/// product_catalog -> lake.products_out.</summary>
// See the "console-and-env-serialized" collection definition in RestoreCommandTests.cs: this class
// redirects Console.Error to assert on CLI output, which must serialize against every other
// Console-swapping class in the assembly.
[Collection("console-and-env-serialized")]
public sealed class NamedFlowRunTests : IDisposable
{
    private readonly string _work =
        Path.Combine(Path.GetTempPath(), "pz-named-flow-tests", Guid.NewGuid().ToString("N"));

    public NamedFlowRunTests() =>
        CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", "two-flows"), _work);

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static void CopyTree(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (var dir in Directory.GetDirectories(from, "*", SearchOption.AllDirectories))
        {
            Directory.CreateDirectory(Path.Combine(to, Path.GetRelativePath(from, dir)));
        }

        foreach (var file in Directory.GetFiles(from, "*", SearchOption.AllDirectories))
        {
            File.Copy(file, Path.Combine(to, Path.GetRelativePath(from, file)));
        }
    }

    private string OrdersOut => Path.Combine(_work, "out", "orders", "orders_out.csv");
    private string ProductsOut => Path.Combine(_work, "out", "products", "products_out.csv");

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
    public void Bare_run_on_two_flow_project_is_PZ0215()
    {
        var (exit, stderr) = InvokeCapturingStderr(["run", "--project", _work]);
        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0215", stderr);
        Assert.Contains("--all", stderr);
        Assert.Contains("lake.orders_out", stderr);
        Assert.Contains("lake.products_out", stderr);
        // The gate fires before any staging session opens: no runs directory appears.
        Assert.False(Directory.Exists(Path.Combine(_work, ".pz", "runs")));
    }

    [Fact]
    public void Run_all_runs_both_flows()
    {
        var exit = CliApp.Build().Parse(["run", "--all", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);
        Assert.True(File.Exists(OrdersOut) && new FileInfo(OrdersOut).Length > 0);
        Assert.True(File.Exists(ProductsOut) && new FileInfo(ProductsOut).Length > 0);
    }

    [Fact]
    public void Named_flow_runs_end_to_end_and_leaves_the_other_flow_untouched()
    {
        var exit = CliApp.Build().Parse(["run", "orders_clean", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);
        // Both-direction closure: naming the TRANSFORM still drained the sink.
        Assert.True(File.Exists(OrdersOut) && new FileInfo(OrdersOut).Length > 0);
        Assert.False(File.Exists(ProductsOut));
    }

    [Fact]
    public void Two_names_run_both_flows()
    {
        var exit = CliApp.Build()
            .Parse(["run", "orders_clean", "product_catalog", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);
        Assert.True(File.Exists(OrdersOut));
        Assert.True(File.Exists(ProductsOut));
    }

    [Fact]
    public void Name_combined_with_select_is_PZ0216()
    {
        var (exit, stderr) = InvokeCapturingStderr(
            ["run", "orders_clean", "--select", "+product_catalog", "--project", _work]);
        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0216", stderr);
    }

    [Fact]
    public void Unknown_name_is_PZ0210_listing_the_flows()
    {
        var (exit, stderr) = InvokeCapturingStderr(["run", "nope", "--project", _work]);
        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0210", stderr);
        Assert.Contains("lake.products_out", stderr);
    }

    private static (int Exit, string Stdout) InvokeCapturingStdout(string[] args)
    {
        var stdout = new StringWriter();
        var original = Console.Out;
        Console.SetOut(stdout);
        try
        {
            return (CliApp.Build().Parse(args).Invoke(), stdout.ToString());
        }
        finally
        {
            Console.SetOut(original);
        }
    }

    [Fact]
    public void Bare_plan_on_two_flow_project_is_not_gated()
    {
        var (exit, stdout) = InvokeCapturingStdout(["plan", "--project", _work]);
        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("orders_clean", stdout);
        Assert.Contains("product_catalog", stdout);
    }

    [Fact]
    public void Named_plan_filters_the_table_to_that_flow()
    {
        var (exit, stdout) = InvokeCapturingStdout(["plan", "orders_clean", "--project", _work]);
        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("orders_clean", stdout);
        Assert.DoesNotContain("product_catalog", stdout);
        Assert.Contains("note: table filtered by selection; plan.json covers the full project", stdout);
    }
}
