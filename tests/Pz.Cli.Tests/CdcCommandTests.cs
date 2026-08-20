namespace Pz.Cli.Tests;

/// <summary>`pz cdc status` / `pz cdc drop` offline paths -- no docker needed since these fixtures declare
/// no cdc datasets at all (Fixtures/retry-basic, a plain localfiles project) or exercise pure CLI
/// usage/validation errors before any connector is ever opened. The against-a-real-connector paths (status
/// fields after a real run, drop tearing down slot/capture-instance state) are docker facts appended to the
/// Postgres/SqlServer cdc test suites.</summary>
// See the "console-and-env-serialized" collection definition in RestoreCommandTests.cs: several classes
// in this assembly redirect the process-global Console.Error/Console.Out to assert on CLI output, and
// must serialize against each other or their captures race. This class joins the same collection purely
// for that serialization (it does not itself touch DATA_DIR/OUT_DIR).
[Collection("console-and-env-serialized")]
public sealed class CdcCommandTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-cdc-cli-tests", Guid.NewGuid().ToString("N"));

    public CdcCommandTests() => CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", "retry-basic"), _work);

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void Status_with_no_cdc_datasets_prints_message_and_exits_ok()
    {
        var stdout = CaptureOut(() => CliApp.Build().Parse(["cdc", "status", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("no cdc datasets in this project", stdout);
    }

    [Fact]
    public void Status_errors_cleanly_outside_project()
    {
        var emptyDir = Path.Combine(Path.GetTempPath(), "pz-cdc-cli-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(emptyDir);
        try
        {
            var stderr = Capture(() => CliApp.Build().Parse(["cdc", "status", "--project", emptyDir]).Invoke(), out var exit);
            Assert.Equal(ExitCodes.ConfigError, exit);
            Assert.Contains("project.yml", stderr);
        }
        finally
        {
            Directory.Delete(emptyDir, recursive: true);
        }
    }

    [Fact]
    public void Drop_without_a_target_is_a_usage_error()
    {
        var stderr = Capture(() => CliApp.Build().Parse(["cdc", "drop", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0507", stderr);
        Assert.Contains("pz cdc drop <source>.<dataset>", stderr);
    }

    [Fact]
    public void Drop_with_two_targets_is_a_usage_error()
    {
        var stderr = Capture(() => CliApp.Build()
            .Parse(["cdc", "drop", "--project", _work, "files.orders", "files.other"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0507", stderr);
    }

    [Fact]
    public void Drop_with_a_malformed_target_is_a_usage_error()
    {
        var stderr = Capture(() => CliApp.Build()
            .Parse(["cdc", "drop", "--project", _work, "no-dot-here"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0507", stderr);
        Assert.Contains("no-dot-here", stderr);
    }

    [Fact]
    public void Drop_on_a_non_cdc_dataset_is_refused()
    {
        // Fixtures/retry-basic declares source 'files', dataset 'orders', with no sync: block at all.
        var stderr = Capture(() => CliApp.Build()
            .Parse(["cdc", "drop", "--project", _work, "files.orders"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0508", stderr);
        Assert.Contains("not a cdc dataset", stderr);
    }

    [Fact]
    public void Drop_on_an_unknown_source_is_refused()
    {
        var stderr = Capture(() => CliApp.Build()
            .Parse(["cdc", "drop", "--project", _work, "nosuch.dataset"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0508", stderr);
    }

    [Fact]
    public void Unknown_cdc_subverb_is_the_app_wide_unrecognized_command_behavior()
    {
        var stderr = Capture(() => CliApp.Build().Parse(["cdc", "bogus"]).Invoke(), out var exit);

        // System.CommandLine 2.0.9's app-wide behavior: an unrecognized token under a command that
        // itself requires a subcommand isn't reported as "unknown subcommand 'bogus'" -- it's dropped
        // as an unmatched token, leaving `cdc` with no subcommand at all, so the parser reports its
        // generic "a command was required" error instead. Same path every other multi-subcommand verb
        // in this CLI goes through; CdcCommand's own Status/Drop actions are never invoked.
        Assert.Equal(1, exit);
        Assert.Contains("Required command was not provided.", stderr, StringComparison.Ordinal);
    }

    private static string Capture(Func<int> action, out int exit)
    {
        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        try
        {
            exit = action();
        }
        finally
        {
            Console.SetError(original);
        }

        return stderr.ToString();
    }

    private static string CaptureOut(Func<int> action, out int exit)
    {
        var stdout = new StringWriter();
        var original = Console.Out;
        Console.SetOut(stdout);
        try
        {
            exit = action();
        }
        finally
        {
            Console.SetOut(original);
        }

        return stdout.ToString();
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
