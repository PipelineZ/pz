using Pz.Engine.Execution;

namespace Pz.Cli.Tests;

/// <summary>The `pz clean` verb end to end. Entirely offline -- `pz clean` loads no project, opens no
/// connectors, and runs none of the eight phases, so these need no fixture beyond a project.yml and some
/// fabricated run directories.
///
/// Joins "console-and-env-serialized" (defined in RestoreCommandTests.cs) purely because it redirects
/// the process-global Console.Out/Error to assert on CLI output, and would otherwise race the other
/// classes that do.</summary>
[Collection("console-and-env-serialized")]
public sealed class CleanCommandTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-clean-cli-tests", Guid.NewGuid().ToString("N"));

    public CleanCommandTests()
    {
        Directory.CreateDirectory(_work);
        File.WriteAllText(Path.Combine(_work, "project.yml"), "name: clean_test\nversion: 1\n");
        Directory.CreateDirectory(Path.Combine(_work, ".pz", "state"));
        File.WriteAllText(Path.Combine(_work, ".pz", "state", "watermarks.json"), "{\"wm\":1}");
    }

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    /// <summary>Fabricates a run dir. Ids are ordinal-sortable, so a higher suffix is a newer run.</summary>
    private string MakeRun(string suffix, int stagingBytes = 512)
    {
        var runId = $"20260729T10331212{suffix[0]}Z-{suffix}";
        var dir = Path.Combine(_work, ".pz", "runs", runId);
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "run_results.json"), "{}");
        if (stagingBytes > 0)
        {
            File.WriteAllBytes(Path.Combine(dir, "staging.duckdb"), new byte[stagingBytes]);
        }

        return runId;
    }

    private string RunDir(string runId) => Path.Combine(_work, ".pz", "runs", runId);

    [Fact]
    public void Default_sweeps_staging_and_keeps_the_newest()
    {
        var older = MakeRun("1111");
        var newest = MakeRun("2222");

        var stdout = CaptureOut(() => CliApp.Build().Parse(["clean", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.True(File.Exists(Path.Combine(RunDir(newest), "staging.duckdb")));
        Assert.False(File.Exists(Path.Combine(RunDir(older), "staging.duckdb")));
        Assert.True(File.Exists(Path.Combine(RunDir(older), "run_results.json")));
        Assert.Contains("staging only", stdout);
    }

    [Fact]
    public void Nothing_to_clean_is_reported_and_exits_ok()
    {
        var stdout = CaptureOut(() => CliApp.Build().Parse(["clean", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("nothing to clean", stdout);
    }

    [Fact]
    public void Dry_run_deletes_nothing()
    {
        var older = MakeRun("1111");
        MakeRun("2222");

        var stdout = CaptureOut(
            () => CliApp.Build().Parse(["clean", "--project", _work, "--dry-run"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.True(File.Exists(Path.Combine(RunDir(older), "staging.duckdb")));
        Assert.Contains("dry-run:", stdout);
    }

    [Fact]
    public void Purge_removes_whole_directories()
    {
        var older = MakeRun("1111");
        var newest = MakeRun("2222");

        CaptureOut(() => CliApp.Build().Parse(["clean", "--project", _work, "--purge"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.False(Directory.Exists(RunDir(older)));
        Assert.True(Directory.Exists(RunDir(newest)));
    }

    [Fact]
    public void Keep_last_zero_with_purge_warns_about_retry()
    {
        MakeRun("1111");
        MakeRun("2222");

        var stdout = CaptureOut(
            () => CliApp.Build().Parse(["clean", "--project", _work, "--keep-last", "0", "--purge"]).Invoke(),
            out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("pz retry", stdout);
        Assert.Empty(Directory.EnumerateDirectories(Path.Combine(_work, ".pz", "runs")));
    }

    [Fact]
    public void State_directory_is_never_touched()
    {
        MakeRun("1111");
        MakeRun("2222");

        CliApp.Build().Parse(["clean", "--project", _work, "--keep-last", "0", "--purge"]).Invoke();

        Assert.Equal("{\"wm\":1}", File.ReadAllText(Path.Combine(_work, ".pz", "state", "watermarks.json")));
    }

    [Fact]
    public void Both_selectors_is_a_usage_error()
    {
        var stderr = Capture(
            () => CliApp.Build().Parse(["clean", "--project", _work, "--keep-last", "3", "--older-than", "7d"]).Invoke(),
            out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0511", stderr);
        Assert.Contains("--keep-last", stderr);
        Assert.Contains("--older-than", stderr);
    }

    [Theory]
    [InlineData("banana")]
    [InlineData("7")]
    [InlineData("0d")]
    [InlineData("-3d")]
    public void Unusable_older_than_is_a_usage_error(string duration)
    {
        var stderr = Capture(
            () => CliApp.Build().Parse(["clean", "--project", _work, "--older-than", duration]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0512", stderr);
    }

    [Fact]
    public void Negative_keep_last_is_a_usage_error()
    {
        var stderr = Capture(
            () => CliApp.Build().Parse(["clean", "--project", _work, "--keep-last", "-1"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0512", stderr);
    }

    [Fact]
    public void Outside_a_project_is_a_config_error()
    {
        var empty = Path.Combine(Path.GetTempPath(), "pz-clean-cli-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(empty);
        try
        {
            var stderr = Capture(
                () => CliApp.Build().Parse(["clean", "--project", empty]).Invoke(), out var exit);

            Assert.Equal(ExitCodes.ConfigError, exit);
            Assert.Contains("project.yml", stderr);
        }
        finally
        {
            Directory.Delete(empty, recursive: true);
        }
    }

    [Fact]
    public void Clean_is_registered_on_the_root_command()
    {
        Assert.Contains(CliApp.Build().Subcommands, c => c.Name == "clean");
    }

    [Fact]
    public void A_locked_run_survives_keep_last_zero_purge()
    {
        var locked = MakeRun("1111");
        MakeRun("2222");
        using var held = RunDirLock.Acquire(RunDir(locked));

        var stdout = CaptureOut(
            () => CliApp.Build().Parse(["clean", "--project", _work, "--keep-last", "0", "--purge"]).Invoke(),
            out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.True(Directory.Exists(RunDir(locked)));
        Assert.Contains("live run", stdout);
    }

    /// <summary>`pz clean` must not need the project to be VALID. The occasion for reaching for it is that
    /// something is already broken, and this file -- a connections.yml that does not parse -- is the exact
    /// scenario. Under `backend: local` (no `state:` block, no PZ_STATE_*) connections.yml is not read at
    /// all, so the sweep proceeds.</summary>
    [Fact]
    public void A_connections_file_that_does_not_parse_does_not_block_a_local_sweep()
    {
        File.WriteAllText(Path.Combine(_work, "connections.yml"), "files:\n  connector: localfiles\n   bad: [\n");
        var older = MakeRun("1111");
        MakeRun("2222");

        var stdout = CaptureOut(() => CliApp.Build().Parse(["clean", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.False(File.Exists(Path.Combine(RunDir(older), "staging.duckdb")));
        Assert.Contains("staging only", stdout);
    }

    /// <summary>The same file, on the read-only path: --dry-run must report rather than refuse.</summary>
    [Fact]
    public void A_connections_file_that_does_not_parse_does_not_block_a_local_dry_run()
    {
        File.WriteAllText(Path.Combine(_work, "connections.yml"), "files:\n  connector: localfiles\n   bad: [\n");
        var older = MakeRun("1111");
        MakeRun("2222");

        var stdout = CaptureOut(
            () => CliApp.Build().Parse(["clean", "--project", _work, "--dry-run"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.True(File.Exists(Path.Combine(RunDir(older), "staging.duckdb")));
        Assert.Contains("dry-run:", stdout);
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
}
