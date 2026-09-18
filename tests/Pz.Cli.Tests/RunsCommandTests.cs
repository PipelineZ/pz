using System.Text.Json;

namespace Pz.Cli.Tests;

/// <summary>`pz runs` end to end. Entirely offline -- like `pz clean`/`pz state`, it loads no project, opens
/// no connectors, and runs none of the eight phases, so these need no fixture beyond a project.yml and
/// some fabricated run directories (same technique as `WriteFakePriorRun` in RetryCommandTests.cs).
///
/// Joins "console-and-env-serialized" (defined in RestoreCommandTests.cs) purely because it redirects
/// the process-global Console.Out/Error to assert on CLI output, and would otherwise race the other
/// classes that do.</summary>
[Collection("console-and-env-serialized")]
public sealed class RunsCommandTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-runs-cli-tests", Guid.NewGuid().ToString("N"));

    public RunsCommandTests()
    {
        Directory.CreateDirectory(_work);
        File.WriteAllText(Path.Combine(_work, "project.yml"), "name: runs_test\nversion: 1\n");
    }

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void Missing_project_yml_is_a_clean_config_error()
    {
        var empty = Path.Combine(Path.GetTempPath(), "pz-runs-cli-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(empty);
        try
        {
            var stderr = Capture(() => CliApp.Build().Parse(["runs", "--project", empty]).Invoke(), out var exit);
            Assert.Equal(ExitCodes.ConfigError, exit);
            Assert.Contains("project.yml is missing", stderr);
        }
        finally
        {
            try { Directory.Delete(empty, recursive: true); } catch { /* best-effort */ }
        }
    }

    [Fact]
    public void No_runs_dir_reports_no_runs_found()
    {
        var stdout = CaptureOut(() => CliApp.Build().Parse(["runs", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("no runs found", stdout);
    }

    [Fact]
    public void Lists_runs_newest_first_with_counts_and_provenance()
    {
        WriteRun("20260101T000000000Z-0001", "success", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:05.000Z",
        [
            ("a1", "src_a", "success", null),
            ("a2", "pipe_a", "success", "reused"),
        ]);
        WriteRun("20260102T000000000Z-0002", "completed_with_failures",
            "2026-01-02T00:00:00.000Z", "2026-01-02T00:00:01.500Z",
        [
            ("b1", "src_b", "failed", null),
            ("b2", "sink_b", "skipped", null),
        ]);

        var stdout = CaptureOut(() => CliApp.Build().Parse(["runs", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        var lines = stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        // Newest first: the second (later) run id appears before the first.
        var newestIndex = Array.FindIndex(lines, l => l.Contains("20260102T000000000Z-0002"));
        var olderIndex = Array.FindIndex(lines, l => l.Contains("20260101T000000000Z-0001"));
        Assert.True(newestIndex >= 0 && olderIndex >= 0 && newestIndex < olderIndex);
        Assert.Contains("1 reused", stdout);
    }

    [Fact]
    public void Duration_is_a_dash_while_the_run_is_still_running()
    {
        WriteRunningRun("20260101T000000000Z-0001", "2026-01-01T00:00:00.000Z",
        [
            ("a1", "src_a", "success", null),
        ]);

        var stdout = CaptureOut(() => CliApp.Build().Parse(["runs", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("running", stdout);
        // The DURATION column must show "-" for a run with no finishedAt yet -- not "0ms" or a crash.
        var line = stdout.Split('\n').Single(l => l.Contains("20260101T000000000Z-0001"));
        Assert.Contains(" - ", line);
    }

    [Fact]
    public void Json_output_is_one_object_per_line_with_explicit_fields()
    {
        WriteRun("20260101T000000000Z-0001", "success", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:05.000Z",
        [
            ("a1", "src_a", "success", "carried_forward"),
        ]);

        var stdout = CaptureOut(() => CliApp.Build().Parse(["runs", "--project", _work, "--json"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        var line = stdout.Trim();
        using var doc = JsonDocument.Parse(line);
        var root = doc.RootElement;
        Assert.Equal("20260101T000000000Z-0001", root.GetProperty("runId").GetString());
        Assert.Equal("success", root.GetProperty("status").GetString());
        Assert.Equal("2026-01-01T00:00:00.000Z", root.GetProperty("startedAt").GetString());
        Assert.Equal("2026-01-01T00:00:05.000Z", root.GetProperty("finishedAt").GetString());
        Assert.Equal(5000, root.GetProperty("durationMs").GetInt64());
        Assert.Equal(1, root.GetProperty("succeeded").GetInt32());
        Assert.Equal(0, root.GetProperty("failed").GetInt32());
        Assert.Equal(0, root.GetProperty("skipped").GetInt32());
        Assert.Equal(0, root.GetProperty("reused").GetInt32());
        Assert.Equal(1, root.GetProperty("carriedForward").GetInt32());
    }

    [Fact]
    public void Limit_restricts_to_the_n_most_recent_runs()
    {
        WriteRun("20260101T000000000Z-0001", "success", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:01.000Z", []);
        WriteRun("20260102T000000000Z-0002", "success", "2026-01-02T00:00:00.000Z", "2026-01-02T00:00:01.000Z", []);

        var stdout = CaptureOut(
            () => CliApp.Build().Parse(["runs", "--project", _work, "--limit", "1"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("20260102T000000000Z-0002", stdout);
        Assert.DoesNotContain("20260101T000000000Z-0001", stdout);
    }

    [Fact]
    public void Limit_below_one_is_a_config_error()
    {
        var stderr = Capture(
            () => CliApp.Build().Parse(["runs", "--project", _work, "--limit", "0"]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0534", stderr);
        Assert.Contains("--limit", stderr);
    }

    private void WriteRun(string runId, string status, string startedAt, string finishedAt,
        (string Id, string Name, string Status, string? Provenance)[] nodes)
    {
        var runDir = Path.Combine(_work, ".pz", "runs", runId);
        Directory.CreateDirectory(runDir);

        var nodesJson = string.Join(",", nodes.Select(n =>
            $$"""{"id":"{{n.Id}}","kind":"SourceLoad","name":"{{n.Name}}","status":"{{n.Status}}","rows":0,"durationMs":0,"error":null{{(n.Provenance is null ? "" : $",\"provenance\":\"{n.Provenance}\"")}}}"""));

        File.WriteAllText(Path.Combine(runDir, "run_results.json"),
            $$"""{"version":1,"runId":"{{runId}}","status":"{{status}}","startedAt":"{{startedAt}}","finishedAt":"{{finishedAt}}","nodes":[{{nodesJson}}]}""");
    }

    private void WriteRunningRun(string runId, string startedAt,
        (string Id, string Name, string Status, string? Provenance)[] nodes)
    {
        var runDir = Path.Combine(_work, ".pz", "runs", runId);
        Directory.CreateDirectory(runDir);

        var nodesJson = string.Join(",", nodes.Select(n =>
            $$"""{"id":"{{n.Id}}","kind":"SourceLoad","name":"{{n.Name}}","status":"{{n.Status}}","rows":0,"durationMs":0,"error":null}"""));

        File.WriteAllText(Path.Combine(runDir, "run_results.json"),
            $$"""{"version":1,"runId":"{{runId}}","status":"running","startedAt":"{{startedAt}}","nodes":[{{nodesJson}}]}""");
    }

    // A state store on another machine can be down. That is the store's own coded failure with its
    // next step -- exit 2, and never the "this is a bug in pz" report an escaped exception gets.
    [Fact]
    public void A_state_store_that_cannot_be_read_is_its_own_coded_error()
    {
        var stderr = Capture(() => Commands.RunsCommand.Report(new UnreachableStore(), json: false, limit: null), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0529", stderr);
        Assert.Contains("check the server", stderr);
    }

    private sealed class UnreachableStore : Pz.Engine.Artifacts.IRunArtifactStore
    {
        public IEnumerable<Pz.Engine.Artifacts.PriorRun> ReadAllNewestFirst()
        {
            yield return new Pz.Engine.Artifacts.PriorRun("20260702T101500123Z-0001", "success", []);
            throw new Pz.Core.Validation.PzConfigException(new Pz.Core.Validation.PzError(
                "PZ0529", "the state store was reached, but the operation failed", "project.yml", null,
                "check the server"));
        }

        public void WriteSnapshot(string runId, string startedAtIso,
            IReadOnlyList<Pz.Engine.Execution.NodeResult> completed, string status, long? eventsDropped = null) =>
            throw new NotSupportedException();

        public Pz.Engine.Artifacts.PriorRun? ReadLatest() => throw new NotSupportedException();

        public IReadOnlyList<Pz.Engine.Artifacts.RunCandidate> ListCandidates() => throw new NotSupportedException();

        public void Delete(string runId) => throw new NotSupportedException();
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
