using System.Text.Json;
using Pz.Cli;
using Pz.Cli.Commands;
using Pz.DuckDb;

namespace Pz.Cli.Tests;

/// <summary>The end-to-end regression net proving the three
/// headline claims of `pz retry`'s staging-reuse/carry-forward design TOGETHER, against a real
/// failed-then-retried run (a genuine `.pz/runs/&lt;id&gt;/staging.duckdb` from an actual failing `pz
/// run`, not a hand-crafted prior-run JSON like <see cref="RetryCommandTests"/>'s unit-level tests) --
/// (1) zero re-extraction: the source CSV is overwritten with completely different rows between the
/// run and the retry, so if the retry re-extracted instead of reusing staged data, those foreign rows
/// would leak into the flaky sink's eventual output; (2) the incremental watermark advances to the
/// ORIGINAL slice's candidate once the retry's reused source + carried-forward sink unblock commit-
/// gated advancement (<see cref="Pz.Engine.State.WatermarkAdvancement"/>); (3) the retry's
/// run_results.json records both provenance kinds -- "reused" for the SourceLoad, "carried_forward" for
/// the sink that already committed on the first run.
///
/// The flaky sink cannot be toggled through its `connection: { root: ${...} }` block: `root` is
/// validated by the connector's JSON schema but never consulted for path resolution (see
/// `LocalFilesConnector.ResolveBaseDir`) -- only the `base_dir` connection key is read, and every CLI
/// verb unconditionally overwrites `base_dir` with the project directory for every localfiles
/// source/sink. So this test toggles the failure by colliding a plain FILE with the sink's own fixed,
/// never-changing output path (<see cref="LocalFilesSink.BeginWriteAsync"/>'s
/// <c>Directory.CreateDirectory(outputDir)</c> throws when a file already occupies that name), which
/// gives the property the fixture needs: nothing about the project's YAML changes between the run and
/// the retry (so node ids stay stable), only external filesystem state the connector never hashes.
///
/// See the "console-and-env-serialized" collection definition in RestoreCommandTests.cs: this class
/// invokes the real CLI entry points, which print to the process-global Console.Out/Error other classes
/// in this assembly redirect and assert on.</summary>
[Collection("console-and-env-serialized")]
public sealed class RetryReuseEndToEndTests : IDisposable
{
    private readonly string _work =
        Path.Combine(Path.GetTempPath(), "pz-retry-reuse-e2e-tests", Guid.NewGuid().ToString("N"));

    public RetryReuseEndToEndTests() =>
        CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", "retry-reuse-e2e"), _work);

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task Retry_reuses_staging_carries_forward_committed_sink_and_advances_watermark()
    {
        var flakyOutputDir = Path.Combine(_work, "out_flaky");
        var ordersCsvPath = Path.Combine(_work, "orders.csv");
        var wmPath = Path.Combine(_work, ".pz", "state", "watermarks.json");
        var runsDir = Path.Combine(_work, ".pz", "runs");

        // Run 1: block the flaky sink's own output directory with a plain file at that exact path.
        // LocalFilesSink.BeginWriteAsync's Directory.CreateDirectory(outputDir) throws IOException
        // because a file already occupies the name it needs as a directory -- the ok sink's separate
        // path is untouched, so it commits normally.
        File.WriteAllText(flakyOutputDir, "blocks the flaky sink's output directory");

        var runExit = CliApp.Build().Parse(["run", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.NodeFailures, runExit);

        var runDirsAfterRun1 = Directory.EnumerateDirectories(runsDir).ToHashSet();
        Assert.Single(runDirsAfterRun1);
        Assert.False(WatermarkHasEntry(wmPath, "crm.orders"),
            "watermark advancement must stay blocked while the flaky sink hasn't committed");

        // Mutate the world: if the retry re-extracts instead of reusing staged data, these foreign rows
        // (100/150/200) would leak into the flaky sink's eventual output.
        File.WriteAllText(ordersCsvPath, "id,name\n100,zed\n150,yara\n200,xena\n");

        // Unblock the flaky sink for the retry.
        File.Delete(flakyOutputDir);

        // No process-wide env var to restore here (see the class doc comment on why a connection-root
        // env toggle isn't used) -- _work's cleanup in Dispose() is enough.
        var retryExit = await RetryCommand.Execute(
            _work, failFast: false, noLockCheck: true, logFormatRaw: "text", otelEndpointRaw: null, stateUrlRaw: null,
            fullRefresh: false, CancellationToken.None);
        Assert.Equal(ExitCodes.Ok, retryExit);

        // Claim 1: zero re-extraction -- the flaky sink's parquet holds exactly the ORIGINAL ids 1-5,
        // never the mutated 100/150/200 rows. write.strategy: append (PZ0335 forbids replace fed by an
        // incremental dataset) names the file `flaky-<runGuid>.parquet` (LocalFilesSink.BeginWriteAsync),
        // not a fixed name, so locate it by pattern instead of a literal path.
        var flakyParquetPath = Assert.Single(Directory.GetFiles(flakyOutputDir, "flaky-*.parquet"));

        await using var duck = DuckSession.Open(
            Path.Combine(Path.GetTempPath(), $"pz-retry-reuse-e2e-readback-{Guid.NewGuid():N}.duckdb"));
        var quoted = flakyParquetPath.Replace("'", "''");
        var rowCount = await duck.ScalarAsync<long>($"select count(*) from read_parquet('{quoted}')");
        var minId = await duck.ScalarAsync<long>($"select min(id) from read_parquet('{quoted}')");
        var maxId = await duck.ScalarAsync<long>($"select max(id) from read_parquet('{quoted}')");
        Assert.Equal(5, rowCount);
        Assert.Equal(1, minId);
        Assert.Equal(5, maxId);

        // Claim 2: the watermark advances to the ORIGINAL slice's candidate (max id 5) -- the carried-
        // forward ok sink plus the actually-retried flaky sink together unblock commit-gated advancement.
        using var wmDoc = JsonDocument.Parse(File.ReadAllBytes(wmPath));
        var entry = wmDoc.RootElement.GetProperty("watermarks").GetProperty("crm.orders");
        Assert.Equal("id", entry.GetProperty("cursor").GetString());
        Assert.Equal("bigint", entry.GetProperty("type").GetString());
        Assert.Equal("5", entry.GetProperty("value").GetString());

        // Claim 3: the retry's run_results.json records both provenance kinds.
        var retryRunDir = Directory.EnumerateDirectories(runsDir).Except(runDirsAfterRun1).Single();
        using var retryDoc = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(retryRunDir, "run_results.json")));
        var nodes = retryDoc.RootElement.GetProperty("nodes").EnumerateArray().ToList();

        var reused = nodes.Where(n => n.TryGetProperty("provenance", out var p) && p.GetString() == "reused").ToList();
        var carriedForward = nodes
            .Where(n => n.TryGetProperty("provenance", out var p) && p.GetString() == "carried_forward").ToList();

        Assert.Single(reused);
        Assert.Equal("SourceLoad", reused[0].GetProperty("kind").GetString());
        Assert.Equal("success", reused[0].GetProperty("status").GetString());

        Assert.Single(carriedForward);
        Assert.Equal("SinkWrite", carriedForward[0].GetProperty("kind").GetString());
        Assert.Equal("ok.ok", carriedForward[0].GetProperty("name").GetString());
        Assert.Equal("success", carriedForward[0].GetProperty("status").GetString());
    }

    private static bool WatermarkHasEntry(string wmPath, string key)
    {
        if (!File.Exists(wmPath))
        {
            return false;
        }

        using var doc = JsonDocument.Parse(File.ReadAllBytes(wmPath));
        return doc.RootElement.GetProperty("watermarks").TryGetProperty(key, out _);
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
