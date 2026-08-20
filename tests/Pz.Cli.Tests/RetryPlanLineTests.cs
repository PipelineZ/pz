using Pz.Cli.Commands;
using Pz.Core.Model;

namespace Pz.Cli.Tests;

// See the "console-and-env-serialized" collection definition in RestoreCommandTests.cs: this class
// redirects Console.Out to assert on CLI output and must serialize against every other Console-swapping
// class in the assembly.
[Collection("console-and-env-serialized")]
public sealed class RetryPlanLineTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-retry-plan-line-tests", Guid.NewGuid().ToString("N"));

    public RetryPlanLineTests() => CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", "retry-plan-lines"), _work);

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    /// <summary>The pure-helper tests below don't drive PlanCommand.Execute's retry-line loop itself:
    /// sources-before-sinks ordering, instance-line-before-dataset/output-line ordering, the
    /// "{source}.{dataset}" name composition, and the "declares retry: at all" filter. This test
    /// runs the real `pz plan` code path against Fixtures/retry-plan-lines, which declares retry: at:
    /// source-instance level ('files'), dataset level cascading over the instance ('files.orders', which
    /// overrides max_attempts/base_delay but inherits max_delay from the instance), and output level on
    /// a sink with NO instance-level retry: ('lake.totals'), plus a dataset ('customers') and an
    /// output ('lake.raw') that declare no retry: at all, to pin the "no line" case.</summary>
    [Fact]
    public void Plan_prints_retry_lines_for_declared_blocks_in_source_before_sink_order()
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

        // Exact cascaded lines. 'files' instance declares the full block verbatim; 'files.orders'
        // overrides max_attempts/base_delay but has no max_delay of its own, so it inherits the
        // instance's 5m; 'lake.totals' has no sink-instance retry: to cascade from at all (nearest-only).
        const string instanceLine = "retry: source files max_attempts=8 base_delay=2s max_delay=5m";
        const string datasetLine = "retry: source files.orders max_attempts=10 base_delay=500ms max_delay=5m";
        const string outputLine = "retry: sink lake.totals max_attempts=4 base_delay=1s max_delay=90s";
        Assert.Contains(instanceLine, output);
        Assert.Contains(datasetLine, output);
        Assert.Contains(outputLine, output);

        // Ordering: source instance line before its own dataset line, and all source retry lines before
        // all sink retry lines.
        var instanceIndex = output.IndexOf(instanceLine, StringComparison.Ordinal);
        var datasetIndex = output.IndexOf(datasetLine, StringComparison.Ordinal);
        var outputIndex = output.IndexOf(outputLine, StringComparison.Ordinal);
        Assert.True(instanceIndex < datasetIndex,
            "source instance retry line must print before its dataset retry line");
        Assert.True(datasetIndex < outputIndex,
            "source retry lines must print before sink retry lines");

        // No retry: block declared -> no line, at every level: the 'customers' dataset, the 'lake' sink
        // instance itself (only its 'totals' output declares retry:), and the 'raw' output.
        Assert.DoesNotContain("retry: source files.customers", output);
        Assert.DoesNotContain("retry: sink lake max_attempts", output);
        Assert.DoesNotContain("retry: sink lake.raw", output);
    }

    /// <summary>The same fixture also declares `max_concurrency:` on the 'files' source
    /// instance (2) and the 'lake' sink instance (3) -- drives PlanCommand.Execute's max_concurrency loop
    /// (mirrors the retry-line loop above: sources before sinks, ordinal order, instance-only since
    /// max_concurrency has no per-dataset/output override to cascade).</summary>
    [Fact]
    public void Plan_prints_max_concurrency_lines_for_declaring_instances_in_source_before_sink_order()
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

        const string sourceLine = "max_concurrency: source files = 2";
        const string sinkLine = "max_concurrency: sink lake = 3";
        Assert.Contains(sourceLine, output);
        Assert.Contains(sinkLine, output);

        var sourceIndex = output.IndexOf(sourceLine, StringComparison.Ordinal);
        var sinkIndex = output.IndexOf(sinkLine, StringComparison.Ordinal);
        Assert.True(sourceIndex < sinkIndex, "source max_concurrency line must print before sink lines");
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

public class RetryPlanLineFormatterTests
{
    [Fact]
    public void Formats_effective_policy_with_defaults_overlaid()
    {
        var line = PlanCommand.FormatRetryLine("source", "pg_prod", null, new RetryDef(8, null, TimeSpan.FromMinutes(5)));
        Assert.Equal("retry: source pg_prod max_attempts=8 base_delay=1s max_delay=5m", line);
    }

    [Fact]
    public void Dataset_override_cascades_over_instance()
    {
        var line = PlanCommand.FormatRetryLine("source", "pg_prod.orders",
            new RetryDef(10, null, null), new RetryDef(5, TimeSpan.FromSeconds(2), null));
        Assert.Equal("retry: source pg_prod.orders max_attempts=10 base_delay=2s max_delay=30s", line);
    }

    [Fact]
    public void Formats_full_block_verbatim()
    {
        var line = PlanCommand.FormatRetryLine(
            "sink", "warehouse", null, new RetryDef(4, TimeSpan.FromMilliseconds(500), TimeSpan.FromSeconds(90)));
        Assert.Equal("retry: sink warehouse max_attempts=4 base_delay=500ms max_delay=90s", line);
    }

    [Theory]
    [InlineData(500, "500ms")]     // sub-second -> ms
    [InlineData(1_000, "1s")]
    [InlineData(90_000, "90s")]    // not a whole minute -> stays seconds
    [InlineData(300_000, "5m")]
    [InlineData(3_600_000, "1h")]
    [InlineData(86_400_000, "1d")]
    [InlineData(1_500, "1500ms")]  // fractional seconds -> ms
    public void FormatDuration_picks_largest_exact_unit(long ms, string expected)
    {
        Assert.Equal(expected, PlanCommand.FormatDuration(TimeSpan.FromMilliseconds(ms)));
    }
}

public class MaxConcurrencyPlanLineFormatterTests
{
    [Fact]
    public void Formats_source_instance_line()
    {
        Assert.Equal("max_concurrency: source pg_prod = 2", PlanCommand.FormatMaxConcurrencyLine("source", "pg_prod", 2));
    }

    [Fact]
    public void Formats_sink_instance_line()
    {
        Assert.Equal("max_concurrency: sink warehouse = 5", PlanCommand.FormatMaxConcurrencyLine("sink", "warehouse", 5));
    }
}
