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
            _work, failFast: false, noLockCheck: true, logFormatRaw: "text", otelRaw: null, stateUrlRaw: null,
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

    /// <summary>`--log-sql` through the real CLI: no <c>node_sql</c> line without the flag; with it, one for the
    /// pipeline and none for the SourceLoad or SinkWrite nodes; on `pz retry --log-sql` the reused SourceLoad
    /// publishes nothing and only pipelines publish.</summary>
    [Fact]
    public async Task Log_sql_emits_node_sql_for_pipelines_only_on_run_and_retry()
    {
        var flakyOutputDir = Path.Combine(_work, "out_flaky");
        File.WriteAllText(flakyOutputDir, "blocks the flaky sink's output directory");

        var (exitWithout, plain) = CaptureStdout(() =>
            CliApp.Build().Parse(["run", "--project", _work, "--log-format", "json"]).Invoke());
        Assert.Equal(ExitCodes.NodeFailures, exitWithout);
        Assert.Empty(Events(plain, "node_sql"));

        var (exitWith, logged) = CaptureStdout(() =>
            CliApp.Build().Parse(["run", "--project", _work, "--log-format", "json", "--log-sql"]).Invoke());
        Assert.Equal(ExitCodes.NodeFailures, exitWith);
        var sql = Assert.Single(Events(logged, "node_sql"));
        Assert.Equal("pipeline", sql.GetProperty("kind").GetString());
        Assert.Equal("stg_orders", sql.GetProperty("name").GetString());
        Assert.Equal(1, sql.GetProperty("attempt").GetInt32());
        Assert.StartsWith("CREATE OR REPLACE TABLE staging.stg_orders AS (", sql.GetProperty("sql").GetString());
        Assert.False(sql.GetProperty("truncated").GetBoolean());

        File.Delete(flakyOutputDir);
        var (retryExit, retried) = await CaptureStdoutAsync(() => RetryCommand.Execute(
            _work, failFast: false, noLockCheck: true, logFormatRaw: "json", otelRaw: null, stateUrlRaw: null,
            fullRefresh: false, CancellationToken.None, logSql: true));
        Assert.Equal(ExitCodes.Ok, retryExit);

        var reusedIds = Events(retried, "node_completed")
            .Where(e => e.TryGetProperty("provenance", out var p) && p.GetString() == "reused")
            .Select(e => e.GetProperty("nodeId").GetString())
            .ToList();
        Assert.NotEmpty(reusedIds);
        var retrySql = Events(retried, "node_sql");
        Assert.All(retrySql, e => Assert.Equal("pipeline", e.GetProperty("kind").GetString()));
        Assert.DoesNotContain(retrySql, e => reusedIds.Contains(e.GetProperty("nodeId").GetString()));
    }

    /// <summary>Resolved SQL can carry a value env() inlined, so it is written only to the NDJSON stream: `--log-sql`
    /// with text output is refused before anything runs, on both verbs.</summary>
    [Theory]
    [InlineData("run", null)]
    [InlineData("run", "text")]
    [InlineData("retry", null)]
    [InlineData("retry", "text")]
    public void Log_sql_without_the_json_log_format_is_a_config_error(string verb, string? format)
    {
        string[] args = format is null
            ? [verb, "--project", _work, "--log-sql"]
            : [verb, "--project", _work, "--log-format", format, "--log-sql"];
        var original = Console.Error;
        using var stderr = new StringWriter();
        Console.SetError(stderr);
        int exit;
        try
        {
            exit = CliApp.Build().Parse(args).Invoke();
        }
        finally
        {
            Console.SetError(original);
        }

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains(RunCommand.LogSqlNeedsJson, stderr.ToString(), StringComparison.Ordinal);
        Assert.False(Directory.Exists(Path.Combine(_work, ".pz", "runs")), "nothing may run");
    }

    /// <summary>The persisted stream (state.events: true) never carries node_sql.</summary>
    [Fact]
    public async Task The_sql_event_renderer_skips_node_sql()
    {
        var connection = new Pz.State.SqlServer.SqlStateConnection("Server=unused;Database=unused;Connect Timeout=1;", "pz");
        var renderer = new Pz.Cli.Rendering.SqlEventRenderer(
            new Pz.State.SqlServer.SqlEventSink(connection, "run-1", TimeProvider.System));

        renderer.Render(new Pz.Diagnostics.Events.NodeSqlEvent(DateTimeOffset.UnixEpoch, "run-1", "n1", "pipeline", "p", 1,
            "select 1", false, 8));
        await renderer.DisposeAsync();

        Assert.Equal(0L, renderer.Dropped);
    }

    private static List<JsonElement> Events(string ndjson, string name) =>
    [
        .. ndjson.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Where(line => line.StartsWith('{'))
            .Select(line => JsonDocument.Parse(line).RootElement)
            .Where(e => e.GetProperty("event").GetString() == name),
    ];

    private static (int Exit, string Stdout) CaptureStdout(Func<int> action)
    {
        var original = Console.Out;
        using var stdout = new StringWriter();
        Console.SetOut(stdout);
        try
        {
            var exit = action();
            return (exit, stdout.ToString());
        }
        finally
        {
            Console.SetOut(original);
        }
    }

    private static async Task<(int Exit, string Stdout)> CaptureStdoutAsync(Func<Task<int>> action)
    {
        var original = Console.Out;
        using var stdout = new StringWriter();
        Console.SetOut(stdout);
        try
        {
            var exit = await action();
            return (exit, stdout.ToString());
        }
        finally
        {
            Console.SetOut(original);
        }
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
