using System.Text.Json;
using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary>CLI-level proof that `pz run` actually wires
/// <c>RunCommand.ExecuteRun</c>'s <c>WatermarkStore</c>/<c>WatermarkAdvancement</c> plumbing end-to-end
/// against a real project (Fixtures/watermark-basic: a localfiles CSV source with a declared
/// <c>sync: {{ mode: incremental, cursor: id }}</c> dataset) -- the algorithmic edge cases (commit-gated advancement,
/// empty delta, full refresh, cancellation, unsupported cursor types) are already covered at the engine
/// level by <c>Pz.Engine.Tests.State.WatermarkFlowTests</c>; this test's job is only to prove the CLI
/// verb writes `.pz/state/watermarks.json` for real and that `--full-refresh` is accepted end-to-end.
///
/// See the "console-and-env-serialized" collection definition in RestoreCommandTests.cs: this class
/// redirects Console.Out to assert on CLI output, which must serialize against every other
/// Console-swapping class in the assembly.</summary>
[Collection("console-and-env-serialized")]
public sealed class WatermarkCliTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-watermark-cli-tests", Guid.NewGuid().ToString("N"));

    public WatermarkCliTests() => CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", "watermark-basic"), _work);

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void Run_writes_watermark_state_for_incremental_dataset()
    {
        var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);

        var wmPath = Path.Combine(_work, ".pz", "state", "watermarks.json");
        Assert.True(File.Exists(wmPath), "expected .pz/state/watermarks.json to exist after a successful incremental run");

        using var doc = JsonDocument.Parse(File.ReadAllBytes(wmPath));
        var entry = doc.RootElement.GetProperty("watermarks").GetProperty("files.orders");
        Assert.Equal("id", entry.GetProperty("cursor").GetString());
        Assert.Equal("bigint", entry.GetProperty("type").GetString());
        Assert.Equal("4", entry.GetProperty("value").GetString()); // orders.csv's max id
    }

    [Fact]
    public void Full_refresh_option_is_accepted_and_reestablishes_watermark()
    {
        var firstExit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, firstExit);

        var secondExit = CliApp.Build().Parse(["run", "--project", _work, "--full-refresh"]).Invoke();
        Assert.Equal(ExitCodes.Ok, secondExit);

        // --full-refresh must not be rejected as an unrecognized option, and the second run must have
        // actually re-extracted and re-established the watermark from the (unchanged) source data --
        // same cursor/type/value as a plain run, just a different runId (each run mints its own).
        var wmPath = Path.Combine(_work, ".pz", "state", "watermarks.json");
        using var doc = JsonDocument.Parse(File.ReadAllBytes(wmPath));
        var entry = doc.RootElement.GetProperty("watermarks").GetProperty("files.orders");
        Assert.Equal("id", entry.GetProperty("cursor").GetString());
        Assert.Equal("bigint", entry.GetProperty("type").GetString());
        Assert.Equal("4", entry.GetProperty("value").GetString());

        var runDirs = Directory.GetDirectories(Path.Combine(_work, ".pz", "runs"));
        Assert.Equal(2, runDirs.Length);
    }

    /// <summary><c>WatermarkAdvancement.Advance</c> runs at the very end of `pz run`,
    /// after every node result is already durably written. Per its own doc comment, watermark state
    /// "must never block or be blocked by" the run_results artifact -- so a persistence failure in
    /// <c>WatermarkStore.Set</c> (disk full, permission error, unwritable state path) must never flip an
    /// otherwise-successful run's status/exit code to Fatal. Forces that failure deterministically (no
    /// reliance on real filesystem permissions, which aren't reliably controllable in CI/sandboxes) by
    /// creating a DIRECTORY at the exact path <c>WatermarkStore</c> writes to
    /// (`.pz/state/watermarks.json`): <c>Set</c>'s own tmp-file write still succeeds, but its final
    /// <c>File.Move(tmpPath, path, overwrite: true)</c> onto an existing directory throws IOException,
    /// which is exactly the class of failure (a path that exists but cannot be written as a file) the
    /// fix must swallow into a "note: " line rather than a fatal exit code.</summary>
    [Fact]
    public void Watermark_persistence_failure_does_not_fail_an_otherwise_successful_run()
    {
        var statePath = Path.Combine(_work, ".pz", "state", "watermarks.json");
        Directory.CreateDirectory(statePath);

        var stdout = new StringWriter();
        var originalOut = Console.Out;
        Console.SetOut(stdout);
        int exit;
        try
        {
            exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
        }
        finally
        {
            Console.SetOut(originalOut);
        }

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.True(Directory.Exists(statePath), "the directory collision must remain in place -- Set must fail, not silently recover");

        var output = stdout.ToString();
        Assert.Contains("note: could not persist watermarks", output);
        Assert.Contains("the next run will re-extract from the previous watermark", output);
    }

    /// <summary>`--log-format json`'s documented contract is that every stdout line parses as JSON,
    /// including under the conditions that matter most -- the state-backend note, the corrupt-watermark
    /// notice and the watermark/sync persist-failure notes must not reach <c>Console.WriteLine</c>
    /// regardless of format, or a machine consumer's parser fails exactly when something has gone
    /// wrong. Reuses the same
    /// deterministic persistence failure as the test above (a directory at the watermark path), then
    /// asserts stdout stayed parseable AND that the note itself is still reported, on stderr, rather
    /// than silently dropped.</summary>
    [Fact]
    public void Json_log_format_keeps_stdout_parseable_when_a_run_time_note_fires()
    {
        Directory.CreateDirectory(Path.Combine(_work, ".pz", "state", "watermarks.json"));

        var stdout = new StringWriter();
        var stderr = new StringWriter();
        var originalOut = Console.Out;
        var originalErr = Console.Error;
        Console.SetOut(stdout);
        Console.SetError(stderr);
        int exit;
        try
        {
            exit = CliApp.Build().Parse(["run", "--project", _work, "--log-format", "json"]).Invoke();
        }
        finally
        {
            Console.SetOut(originalOut);
            Console.SetError(originalErr);
        }

        Assert.Equal(ExitCodes.Ok, exit);

        var lines = stdout.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.NotEmpty(lines);
        foreach (var line in lines)
        {
            // JsonDocument.Parse throws on anything that is not a JSON value -- which is the contract.
            using var doc = JsonDocument.Parse(line);
            Assert.Equal(JsonValueKind.Object, doc.RootElement.ValueKind);
        }

        Assert.Contains("note: could not persist watermarks", stderr.ToString(), StringComparison.Ordinal);
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
