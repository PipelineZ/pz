using System.Text.Json;
using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary>`pz retry` selection. These tests exercise the CLI verb against
/// a real, small, working project (Fixtures/retry-basic: one source/pipeline/sink lineage) so node ids
/// are genuine content hashes — the e2e suite (<c>RetryRunTests</c>) is what proves an INDEPENDENT
/// succeeded branch is excluded from a retry's effective set, which needs a two-branch fixture; this
/// linear one is enough to prove selection, notices, and the success/no-prior-run edge cases.</summary>
// See the "console-and-env-serialized" collection definition in RestoreCommandTests.cs: several classes
// in this assembly redirect the process-global Console.Error/Console.Out to assert on CLI output, and
// must serialize against each other or their captures race. This class joins the same collection purely
// for that serialization (it does not itself touch DATA_DIR/OUT_DIR).
[Collection("console-and-env-serialized")]
public sealed class RetryCommandTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-retry-cli-tests", Guid.NewGuid().ToString("N"));

    public RetryCommandTests() => CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", "retry-basic"), _work);

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void No_prior_run_is_a_clean_config_error()
    {
        var stderr = Capture(() => CliApp.Build().Parse(["retry", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0502", stderr);
        Assert.Contains("no prior run found", stderr);
    }

    [Fact]
    public void Nothing_to_retry_on_success_run()
    {
        var runExit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, runExit);

        var stdout = CaptureOut(() => CliApp.Build().Parse(["retry", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("nothing to retry", stdout);

        // Must not have created a new run dir -- retry short-circuits before any execution.
        Assert.Single(Directory.EnumerateDirectories(Path.Combine(_work, ".pz", "runs")));
    }

    [Fact]
    public void Running_prior_run_is_refused()
    {
        var byKind = RunOnceAndReadNodesByKind();

        // A crashed run's last-written snapshot is left with status "running" (see
        // RunResultsWriter.WriteSnapshot) -- even though every recorded node here shows "success",
        // `pz retry` must refuse rather than report "nothing to retry".
        WriteFakePriorRun(_work, "99999999T999999999Z-fake",
        [
            (byKind["SourceLoad"].Id, byKind["SourceLoad"].Name, "success"),
        ], "running");

        var stderr = Capture(() => CliApp.Build().Parse(["retry", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0503", stderr);
        Assert.Contains("prior run was interrupted", stderr);
        Assert.Contains("1 node(s) recorded before it stopped", stderr);
        Assert.Contains("re-run 'pz run'", stderr);
    }

    [Fact]
    public void Fatal_prior_run_is_refused()
    {
        var byKind = RunOnceAndReadNodesByKind();

        WriteFakePriorRun(_work, "99999999T999999999Z-fake",
        [
            (byKind["SourceLoad"].Id, byKind["SourceLoad"].Name, "success"),
        ], "fatal");

        var stderr = Capture(() => CliApp.Build().Parse(["retry", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0504", stderr);
        Assert.Contains("prior run ended fatally", stderr);
        Assert.Contains("re-run 'pz run'", stderr);
    }

    [Fact]
    public void Retry_selects_failed_and_skipped_ids()
    {
        var byKind = RunOnceAndReadNodesByKind();

        // A fixed runId far in the "future" lexicographically (real run ids are UTC-now timestamps, all
        // "2026..."; "9999..." always sorts greater) so RunResultsReader.ReadLatest picks this fake prior
        // run over the genuine successful one just produced above.
        WriteFakePriorRun(_work, "99999999T999999999Z-fake",
        [
            (byKind["SourceLoad"].Id, byKind["SourceLoad"].Name, "success"),
            (byKind["Pipeline"].Id, byKind["Pipeline"].Name, "failed"),
            (byKind["SinkWrite"].Id, byKind["SinkWrite"].Name, "skipped"),
        ], "completed_with_failures");

        var runsDir = Path.Combine(_work, ".pz", "runs");
        var before = Directory.EnumerateDirectories(runsDir).ToHashSet();

        var exit = CliApp.Build().Parse(["retry", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);

        var newRunDir = Directory.EnumerateDirectories(runsDir).Except(before).Single();
        var newNodes = ReadNodesByKind(newRunDir);

        Assert.Equal(3, newNodes.Count);
        Assert.All(newNodes.Values, n => Assert.Equal("success", n.Status));
    }

    [Fact]
    public void Retry_degrades_safely_when_prior_staging_deleted()
    {
        // RetryReusePlanner.Plan's File.Exists probe on the prior run's staging.duckdb is the fallback
        // guard -- WriteFakePriorRun below never creates that file for its fake run dir, which is
        // exactly equivalent to "the prior staging.duckdb was deleted". The retry must still succeed:
        // full re-extraction, no reuse/carry-forward notes, no "provenance" field in the new
        // run_results.json.
        var byKind = RunOnceAndReadNodesByKind();
        var runsDir = Path.Combine(_work, ".pz", "runs");
        var fakeRunId = "99999999T999999999Z-fake";

        WriteFakePriorRun(_work, fakeRunId,
        [
            (byKind["SourceLoad"].Id, byKind["SourceLoad"].Name, "success"),
            (byKind["Pipeline"].Id, byKind["Pipeline"].Name, "failed"),
            (byKind["SinkWrite"].Id, byKind["SinkWrite"].Name, "skipped"),
        ], "completed_with_failures");
        Assert.False(File.Exists(Path.Combine(runsDir, fakeRunId, "staging.duckdb")));

        var before = Directory.EnumerateDirectories(runsDir).ToHashSet();
        var stdout = CaptureOut(() => CliApp.Build().Parse(["retry", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.DoesNotContain("note: reusing", stdout);
        Assert.DoesNotContain("note: carrying forward", stdout);

        var newRunDir = Directory.EnumerateDirectories(runsDir).Except(before).Single();
        var newNodes = ReadNodesByKind(newRunDir);
        Assert.Equal(3, newNodes.Count);
        Assert.All(newNodes.Values, n => Assert.Equal("success", n.Status));
        Assert.DoesNotContain("\"provenance\"", File.ReadAllText(Path.Combine(newRunDir, "run_results.json")));
    }

    [Fact]
    public void Retry_reuses_staged_source_and_marks_provenance_reused()
    {
        // Reuse-positive path: unlike the fakes above, this prior run's staging.duckdb is a genuine copy
        // of a real run's staging (so the source's staged table/row-count actually matches), letting
        // RetryReusePlanner.Plan produce a non-empty manifest and SourceLoadExecutor's TryReuseAsync
        // succeed for real. Full end-to-end coverage (independent-branch carry-forward) lives in
        // RetryReuseEndToEndTests.
        var byKind = RunOnceAndReadNodesByKind();
        var runsDir = Path.Combine(_work, ".pz", "runs");
        var realRunDir = Directory.EnumerateDirectories(runsDir).Single();
        var sourceRows = ReadRows(realRunDir, byKind["SourceLoad"].Id);

        var fakeRunId = "99999999T999999999Z-fake";
        var fakeRunDir = Path.Combine(runsDir, fakeRunId);
        Directory.CreateDirectory(fakeRunDir);
        File.Copy(Path.Combine(realRunDir, "staging.duckdb"), Path.Combine(fakeRunDir, "staging.duckdb"));

        File.WriteAllText(Path.Combine(fakeRunDir, "run_results.json"), $$"""
            {"version":1,"runId":"{{fakeRunId}}","status":"completed_with_failures",
            "startedAt":"2026-01-01T00:00:00.000Z","nodes":[
            {"id":"{{byKind["SourceLoad"].Id}}","kind":"SourceLoad","name":"{{byKind["SourceLoad"].Name}}","status":"success","rows":{{sourceRows}},"durationMs":0,"error":null},
            {"id":"{{byKind["Pipeline"].Id}}","kind":"Pipeline","name":"{{byKind["Pipeline"].Name}}","status":"failed","rows":0,"durationMs":0,"error":null},
            {"id":"{{byKind["SinkWrite"].Id}}","kind":"SinkWrite","name":"{{byKind["SinkWrite"].Name}}","status":"skipped","rows":0,"durationMs":0,"error":null}
            ]}
            """);

        var before = Directory.EnumerateDirectories(runsDir).ToHashSet();
        var stdout = CaptureOut(() => CliApp.Build().Parse(["retry", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains($"note: reusing staged data for 1 source load(s) from run {fakeRunId}", stdout);
        Assert.DoesNotContain("note: carrying forward", stdout);

        var newRunDir = Directory.EnumerateDirectories(runsDir).Except(before).Single();
        var newJson = File.ReadAllText(Path.Combine(newRunDir, "run_results.json"));
        Assert.Contains("\"provenance\":\"reused\"", newJson);

        var newNodes = ReadNodesByKind(newRunDir);
        Assert.Equal(3, newNodes.Count);
        Assert.All(newNodes.Values, n => Assert.Equal("success", n.Status));
    }

    [Fact]
    public void Changed_failed_node_produces_notice()
    {
        var byKind = RunOnceAndReadNodesByKind();

        WriteFakePriorRun(_work, "99999999T999999999Z-fake",
        [
            (byKind["SourceLoad"].Id, byKind["SourceLoad"].Name, "success"),
            ("deadbeefdeadbeef", byKind["Pipeline"].Name, "failed"), // stale/rehashed id -- won't match
            (byKind["SinkWrite"].Id, byKind["SinkWrite"].Name, "skipped"),
        ], "completed_with_failures");

        var stderr = Capture(() => CliApp.Build().Parse(["retry", "--project", _work]).Invoke(), out var exit);

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains($"note: {byKind["Pipeline"].Name} changed since the failed run", stderr);
    }

    private Dictionary<string, (string Id, string Name, string Status)> RunOnceAndReadNodesByKind()
    {
        var exit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);
        var runDir = Directory.EnumerateDirectories(Path.Combine(_work, ".pz", "runs")).Single();
        return ReadNodesByKind(runDir);
    }

    private static Dictionary<string, (string Id, string Name, string Status)> ReadNodesByKind(string runDir)
    {
        using var doc = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(runDir, "run_results.json")));
        var byKind = new Dictionary<string, (string, string, string)>();
        foreach (var node in doc.RootElement.GetProperty("nodes").EnumerateArray())
        {
            byKind[node.GetProperty("kind").GetString()!] = (
                node.GetProperty("id").GetString()!,
                node.GetProperty("name").GetString()!,
                node.GetProperty("status").GetString()!);
        }

        return byKind;
    }

    private static long ReadRows(string runDir, string nodeId)
    {
        using var doc = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(runDir, "run_results.json")));
        foreach (var node in doc.RootElement.GetProperty("nodes").EnumerateArray())
        {
            if (node.GetProperty("id").GetString() == nodeId)
            {
                return node.GetProperty("rows").GetInt64();
            }
        }

        throw new InvalidOperationException($"node {nodeId} not found in {runDir}");
    }

    /// <summary>Hand-crafts a "prior run" snapshot under a fixed, lexicographically-maximal run id --
    /// lets these tests simulate "the last run failed" without needing real fault injection through the
    /// localfiles connector.</summary>
    private static void WriteFakePriorRun(
        string work, string runId, (string Id, string Name, string Status)[] nodes, string status)
    {
        var runDir = Path.Combine(work, ".pz", "runs", runId);
        Directory.CreateDirectory(runDir);

        var nodesJson = string.Join(",", nodes.Select(n =>
            $$"""{"id":"{{n.Id}}","kind":"SourceLoad","name":"{{n.Name}}","status":"{{n.Status}}","rows":0,"durationMs":0,"error":null}"""));

        File.WriteAllText(Path.Combine(runDir, "run_results.json"),
            $$"""{"version":1,"runId":"{{runId}}","status":"{{status}}","startedAt":"2026-01-01T00:00:00.000Z","nodes":[{{nodesJson}}]}""");
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
