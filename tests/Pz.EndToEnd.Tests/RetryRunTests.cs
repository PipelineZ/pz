using System.Runtime.Versioning;
using System.Text.Json;
using Pz.Cli;

namespace Pz.EndToEnd.Tests;

/// <summary>`pz retry` re-runs only the failure lineage from the last run,
/// never an independent succeeded branch. Fixture <c>retry-two-branches</c> has TWO fully independent
/// source-&gt;pipeline-&gt;sink lineages (A and B, six nodes total); this test breaks branch A's sink by
/// making its output directory unwritable, runs (fails), fixes the directory, then retries and asserts
/// the new run touches exactly branch A's three nodes -- branch B's nodes are entirely absent from the
/// retry's run_results.json, proving they were never re-executed. The first run passes <c>--all</c>: the
/// fixture's two branches are independent flows, so bare <c>pz run</c> is a PZ0215 config error by
/// design.
///
/// Unix permission bits only: <see cref="File.SetUnixFileMode"/> is a no-op fiction on Windows (and the
/// repo's CI/dev environment is Linux per https://pipelinez.dev/concepts/architecture-overview/), hence the platform attribute below.</summary>
[SupportedOSPlatform("linux")]
[Collection("console-redirection")]
public sealed class RetryRunTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-e2e-retry-tests", Guid.NewGuid().ToString("N"));

    public RetryRunTests() => CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", "retry-two-branches"), _work);

    public void Dispose()
    {
        // Best-effort: restore permissions first so recursive delete of a leftover unwritable dir
        // doesn't itself fail.
        var outADir = Path.Combine(_work, "out_a");
        if (Directory.Exists(outADir))
        {
            try { File.SetUnixFileMode(outADir, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute); }
            catch { /* best-effort */ }
        }

        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task Failed_run_then_retry_succeeds_and_reruns_only_the_failure_lineage()
    {
        var outADir = Path.Combine(_work, "out_a");
        Directory.CreateDirectory(outADir);
        // Read+execute but no write: LocalFilesSink can create the outer dir (already exists, no-op) but
        // fails creating its per-write temp subdirectory inside it.
        File.SetUnixFileMode(outADir, UnixFileMode.UserRead | UnixFileMode.UserExecute);

        var firstExit = CliApp.Build().Parse(["run", "--all", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.NodeFailures, firstExit);

        var firstRunDir = Directory.EnumerateDirectories(Path.Combine(_work, ".pz", "runs")).Single();
        var firstNodes = ReadNodesByName(firstRunDir);

        Assert.Equal(6, firstNodes.Count);
        Assert.Equal("success", firstNodes["src_files__orders_a"].Status);
        Assert.Equal("success", firstNodes["totals_a"].Status);
        Assert.Equal("failed", firstNodes["lake.totals_a"].Status);
        Assert.Equal("PZ0501", firstNodes["lake.totals_a"].ErrorCode);

        Assert.Equal("success", firstNodes["src_files__orders_b"].Status);
        Assert.Equal("success", firstNodes["totals_b"].Status);
        Assert.Equal("success", firstNodes["lake.totals_b"].Status);

        // Fix the sink and retry.
        File.SetUnixFileMode(outADir, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);

        var retryExit = CliApp.Build().Parse(["retry", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, retryExit);

        var runsDir = Path.Combine(_work, ".pz", "runs");
        var secondRunDir = Directory.EnumerateDirectories(runsDir).Single(d => d != firstRunDir);
        var secondNodes = ReadNodesByName(secondRunDir);

        // ONLY the failure lineage (source_a, pipeline_a, sink_a) re-ran -- never the full 6-node dag,
        // and never branch B, which succeeded independently the first time.
        Assert.Equal(3, secondNodes.Count);
        Assert.Equal("success", secondNodes["src_files__orders_a"].Status);
        Assert.Equal("success", secondNodes["totals_a"].Status);
        Assert.Equal("success", secondNodes["lake.totals_a"].Status);
        Assert.False(secondNodes.ContainsKey("src_files__orders_b"));
        Assert.False(secondNodes.ContainsKey("totals_b"));
        Assert.False(secondNodes.ContainsKey("lake.totals_b"));

        Assert.True(File.Exists(Path.Combine(_work, "out_a", "totals_a.parquet")));
    }

    private static Dictionary<string, (string Status, string? ErrorCode)> ReadNodesByName(string runDir)
    {
        using var doc = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(runDir, "run_results.json")));
        var byName = new Dictionary<string, (string, string?)>();
        foreach (var node in doc.RootElement.GetProperty("nodes").EnumerateArray())
        {
            var name = node.GetProperty("name").GetString()!;
            var status = node.GetProperty("status").GetString()!;
            var errorCode = node.GetProperty("error").ValueKind == JsonValueKind.Object
                ? node.GetProperty("error").GetProperty("code").GetString()
                : null;
            byName[name] = (status, errorCode);
        }

        return byName;
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
