using System.Text.Json;
using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary>`pz run --until-caught-up` end to end against Fixtures/windowed-basic: a localfiles source over
/// ids 1..6 with <c>initial: "0"</c>, <c>max_window: "2"</c> and <c>until: "6"</c>, which takes exactly
/// three slices — (0,2], (2,4], (4,6] — to catch up. The stop rule's edge cases are unit-tested in
/// <see cref="CaughtUpLoopTests"/>; this class proves the verb loops real runs and reports why it stopped.
///
/// See the "console-and-env-serialized" collection definition in RestoreCommandTests.cs: this class
/// redirects Console.Out/Console.Error, which must serialize against every other Console-swapping class
/// in the assembly.</summary>
[Collection("console-and-env-serialized")]
public sealed class UntilCaughtUpCliTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-until-caught-up-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void Repeats_the_run_until_the_window_reaches_until()
    {
        CopyFixture("windowed-basic");

        var (exit, stdout, _) = Run("run", "--project", _work, "--until-caught-up");

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Equal("6", WatermarkValue());
        Assert.Equal(3, RunDirCount());
        Assert.Contains("until-caught-up: 3 passes, stopped: caught up", stdout);
    }

    [Fact]
    public void Max_runs_stops_early_without_failing()
    {
        CopyFixture("windowed-basic");

        var (exit, stdout, _) = Run("run", "--project", _work, "--until-caught-up", "--max-runs", "2");

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Equal("4", WatermarkValue());
        Assert.Equal(2, RunDirCount());
        Assert.Contains("until-caught-up: 2 passes, stopped: max runs (2) reached", stdout);
    }

    [Fact]
    public void A_project_without_a_windowed_stop_runs_once()
    {
        CopyFixture("watermark-basic");

        var (exit, stdout, _) = Run("run", "--project", _work, "--until-caught-up");

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Equal(1, RunDirCount());
        Assert.Contains("until-caught-up: 1 pass, stopped: no windowed source with a stop", stdout);
    }

    [Fact]
    public void Json_mode_keeps_the_summary_off_stdout()
    {
        CopyFixture("windowed-basic");

        var (exit, stdout, stderr) = Run("run", "--project", _work, "--until-caught-up", "--log-format", "json");

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("until-caught-up: 3 passes, stopped: caught up", stderr);
        foreach (var line in stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries))
        {
            using var _ = JsonDocument.Parse(line);
        }
    }

    [Fact]
    public void Max_runs_without_until_caught_up_is_a_usage_error()
    {
        CopyFixture("windowed-basic");

        var (exit, _, stderr) = Run("run", "--project", _work, "--max-runs", "3");

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("--max-runs needs --until-caught-up", stderr);
        Assert.Equal(0, RunDirCount());
    }

    [Fact]
    public void Max_runs_below_one_is_a_usage_error()
    {
        CopyFixture("windowed-basic");

        var (exit, _, stderr) = Run("run", "--project", _work, "--until-caught-up", "--max-runs", "0");

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("--max-runs must be at least 1", stderr);
        Assert.Equal(0, RunDirCount());
    }

    private static (int Exit, string Stdout, string Stderr) Run(params string[] args)
    {
        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var originalOut = Console.Out;
        var originalErr = Console.Error;
        Console.SetOut(stdout);
        Console.SetError(stderr);
        try
        {
            var exit = CliApp.Build().Parse(args).Invoke();
            return (exit, stdout.ToString(), stderr.ToString());
        }
        finally
        {
            Console.SetOut(originalOut);
            Console.SetError(originalErr);
        }
    }

    private string WatermarkValue()
    {
        using var doc = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(_work, ".pz", "state", "watermarks.json")));
        return doc.RootElement.GetProperty("watermarks").GetProperty("files.orders").GetProperty("value").GetString()!;
    }

    private int RunDirCount()
    {
        var runs = Path.Combine(_work, ".pz", "runs");
        return Directory.Exists(runs) ? Directory.GetDirectories(runs).Length : 0;
    }

    private void CopyFixture(string name)
    {
        var from = Path.Combine(AppContext.BaseDirectory, "Fixtures", name);
        foreach (var file in Directory.EnumerateFiles(from, "*", SearchOption.AllDirectories))
        {
            var dest = Path.Combine(_work, Path.GetRelativePath(from, file));
            Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
            File.Copy(file, dest, overwrite: true);
        }
    }
}
