using Pz.Cli.Commands;
using Pz.DuckDb;

namespace Pz.Cli.Tests;

/// <summary>`pz run` and `pz retry` leave the preview index in their own staging database, so a run
/// directory alone can answer `pz preview --run-dir`. See the "console-and-env-serialized" collection in
/// RestoreCommandTests.cs.</summary>
[Collection("console-and-env-serialized")]
public sealed class PreviewIndexRunTests : IDisposable
{
    private readonly string _work =
        PreviewFixture.Write(Path.Combine(Path.GetTempPath(), "pz-preview-index-tests", Guid.NewGuid().ToString("N")));

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static async Task<(string Nodes, string Excluded)> ReadIndex(string runDir)
    {
        await using var duck = DuckSession.Open(Path.Combine(runDir, "staging.duckdb"));
        var nodes = await duck.ScalarAsync<string>(
            "select string_agg(node || ':' || array_to_string(redacted_columns, ','), ';' order by node) " +
            "from pz_meta.preview_nodes");
        var excluded = await duck.ScalarAsync<string>(
            "select string_agg(node || ':' || kind, ';' order by node) from pz_meta.preview_excluded");
        return (nodes, excluded);
    }

    [Fact]
    public async Task Run_writes_preview_nodes_into_its_staging()
    {
        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["run", "--project", _work]).Invoke());

        var (nodes, excluded) = await ReadIndex(PreviewFixture.LatestRunDir(_work));

        Assert.Equal("out_customers:;src_files__customers:email;stg_customers:contact;v_customers:", nodes);
        Assert.Contains("eph_customers:ephemeral", excluded, StringComparison.Ordinal);
        Assert.Contains(":check", excluded, StringComparison.Ordinal);
        Assert.Contains(":sink_write", excluded, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Retry_writes_preview_nodes_into_its_own_staging()
    {
        // Block the sink's output directory with a plain file, as RetryReuseEndToEndTests does.
        var blocker = Path.Combine(_work, "out");
        File.WriteAllText(blocker, "blocks the sink's output directory");
        Assert.Equal(ExitCodes.NodeFailures, CliApp.Build().Parse(["run", "--project", _work]).Invoke());
        var firstRun = PreviewFixture.LatestRunDir(_work);
        File.Delete(blocker);

        var retryExit = await RetryCommand.Execute(
            _work, failFast: false, noLockCheck: true, logFormatRaw: "text", otelRaw: null, stateUrlRaw: null,
            fullRefresh: false, CancellationToken.None);
        Assert.Equal(ExitCodes.Ok, retryExit);

        var retryRun = Directory.EnumerateDirectories(Path.Combine(_work, ".pz", "runs")).Single(d => d != firstRun);
        var (nodes, _) = await ReadIndex(retryRun);
        Assert.Contains("stg_customers:contact", nodes, StringComparison.Ordinal);
    }
}
