using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;
using Pz.Cli;
using Pz.Engine.State;
using Pz.TestSupport;
using Testcontainers.PostgreSql;

namespace Pz.EndToEnd.Tests;

/// <summary>The windowed-backfill end-to-end proof -- a real
/// `postgres orders (bounded window: max_window/initial/until) -&gt; transform -&gt; postgres sink (merge,
/// keys=[id])` project, run through the real CLI entry point (<see cref="CliApp"/>, exactly like <see
/// cref="IncrementalSyncTests"/>) against a Testcontainers postgres instance. Demonstrates, end to end:
/// windowing (config surface, PZ0213, and engine window-bounds arithmetic), empty-slice watermark
/// advancement through a genuine data gap, caught-up semantics, Postgres bounded-window pushdown,
/// and merge idempotency on replay. The candidate-cap rule (an
/// over-extracting connector must never advance the watermark past the window upper bound) is NOT
/// exercised here -- it is covered at unit level by <c>WatermarkFlowTests</c>' over-extraction seam,
/// which can construct that scenario directly against a stub rather than needing a real connector to
/// misbehave.
///
/// Source rows: cursor (bigint `id`) 1..20 and 31..50 -- a genuine gap at 21..30, so the (20,30] window
/// is a real empty slice, not an artifact of `until`/`max_window` alignment. With `max_window: "10"`,
/// `initial: "0"`, `until: "45"`, the backfill takes exactly 5 runs before the dataset is caught up:
/// (0,10] (10 rows), (10,20] (10 rows), (20,30] (0 rows -- the gap; the watermark must still advance
/// 20-&gt;30 per the empty-slice rule), (30,40] (10 rows), (40,45] (5 rows). The 6th run (`upper == lower
/// == 45`) moves zero rows and leaves the watermark unchanged. Reverting the empty-slice rule (making a
/// windowed empty slice behave like the unwindowed case -- no candidate, watermark stands) would leave
/// the watermark stuck at 20 after the 3rd run, so this test's per-slice watermark assertions (and its
/// iteration cap, see below) are mutation-sensitive to exactly that regression.
///
/// Joins the "console-redirection" collection for the same reason <see cref="IncrementalSyncTests"/>
/// does (see that class's doc comment) -- `pz run` writes to the real, process-global <see
/// cref="Console.Out"/>.</summary>
[Collection("console-redirection")]
public sealed class WindowedBackfillTests : IClassFixture<WindowedBackfillTests.PostgresFixture>, IDisposable
{
    private const string SourceName = "pgsrc";
    private const string DatasetName = SourceTable;
    private const string SinkName = "pgsink";
    private const string OutputName = SinkTable;
    private const string SourceTable = "orders_backfill_src";
    private const string SinkTable = "orders_backfill_sink";
    private const string WatermarkKey = $"{SourceName}.{DatasetName}";
    private const string Until = "45";

    // (SourceLoad rows, expected watermark value) after each real (non-caught-up) slice, in order -- the
    // load-bearing shape of the whole backfill: rows 1..20 and 31..50 exist (a genuine gap at 21..30) /
    // max_window 10 / until 45 slices into exactly (0,10],(10,20],(20,30],(30,40],(40,45]. The 3rd slice
    // (20,30] lands ZERO rows (the gap) -- boundary "30" comes from the empty-slice advancement rule
    // (SourceLoadExecutor's windowed empty-slice branch), not from any row landing, which is exactly
    // what makes this array mutation-sensitive to that rule regressing.
    private static readonly (long Rows, string Watermark)[] ExpectedSlices =
    [
        (10, "10"), (10, "20"), (0, "30"), (10, "40"), (5, "45"),
    ];

    /// <summary>Hard cap on the number of real (non-caught-up) `pz run` invocations the backfill loop
    /// below will attempt before failing fast. Set comfortably above <see cref="ExpectedSlices"/>'s
    /// length (5): if the empty-slice advancement rule regresses, the (20,30] gap window would leave the
    /// watermark stuck at "20" and every subsequent run would re-extract the SAME empty window forever --
    /// this cap turns that failure mode into a fast, diagnostic <c>Assert.Fail</c> instead of a
    /// hang.</summary>
    private const int MaxBackfillIterations = 10;

    private readonly PostgresFixture _fx;
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-e2e-windowed-backfill-tests", Guid.NewGuid().ToString("N"));

    public WindowedBackfillTests(PostgresFixture fixture) => _fx = fixture;

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [SkippableFact]
    public async Task Windowed_backfill_extracts_bounded_slices_then_catches_up_then_replays_idempotently()
    {
        var adminConn = _fx.ConnectionString;

        await ExecuteAsync(adminConn, $"drop table if exists public.{SourceTable}");
        await ExecuteAsync(adminConn, $"drop table if exists public.{SinkTable}");
        await ExecuteAsync(adminConn, $"""
            create table public.{SourceTable} (
                id bigint primary key,
                payload varchar not null
            )
            """);
        // Rows 1..20 and 31..50, cursor (id) -- a genuine gap at 21..30 so the (20,30] window is a real
        // empty slice: this exercises the empty-slice advancement rule, not just the caught-up path a
        // dense 1..50 seed would take.
        await ExecuteAsync(adminConn, $"""
            insert into public.{SourceTable} (id, payload)
            select i, 'payload-' || i
            from generate_series(1, 20) as i
            """);
        await ExecuteAsync(adminConn, $"""
            insert into public.{SourceTable} (id, payload)
            select i, 'payload-' || i
            from generate_series(31, 50) as i
            """);

        WriteProject(_work);

        var seenRunDirs = new HashSet<string>();

        // --- (a)/(b): drive the backfill slice by slice UNTIL caught up (watermark reaches `until`),
        // collecting each run's (SourceLoad row count, resulting watermark) rather than asserting them
        // one at a time -- so a stalled watermark (the empty-slice rule regressing) keeps the loop
        // running, rather than being caught immediately by a per-iteration assertion, all the way to
        // MaxBackfillIterations, where it fails fast with a diagnostic message instead of hanging. The
        // full collected sequence is then compared, in one shot, against the exact expected shape
        // computed above. ---
        var actualSlices = new List<(long Rows, string Watermark)>();
        for (var i = 0; i < MaxBackfillIterations; i++)
        {
            var (exit, results) = await RunOnceAsync(_work, seenRunDirs);
            Assert.Equal(ExitCodes.Ok, exit);

            var rows = ByKind(results)["SourceLoad"].GetProperty("rows").GetInt64();
            Assert.True(rows <= 10, $"run {i + 1}: SourceLoad moved {rows} rows, expected <= max_window (10)");

            var watermark = ReadWatermarkEntry(_work, WatermarkKey);
            Assert.Equal("id", watermark.GetProperty("cursor").GetString());
            Assert.Equal("bigint", watermark.GetProperty("type").GetString());
            var watermarkValue = watermark.GetProperty("value").GetString()!;

            actualSlices.Add((rows, watermarkValue));

            if (watermarkValue == Until)
            {
                break;
            }

            if (i == MaxBackfillIterations - 1)
            {
                Assert.Fail(
                    $"windowed backfill loop did not converge to watermark '{Until}' within " +
                    $"{MaxBackfillIterations} runs -- loop did not converge, watermark stalled at " +
                    $"'{watermarkValue}' (likely an empty-slice advancement regression on the (20,30] gap window)");
            }
        }

        Assert.Equal(ExpectedSlices, actualSlices);

        // --- (c): final sink content = rows 1..20 union 31..45 exactly once (35 rows -- the 21..30 gap
        // never existed at the source), vs an independent oracle. The oracle applies upper(payload)
        // itself -- the SAME transform the pipeline applies -- so the digest proves the sink holds the
        // transformed content, not merely a passthrough of the source. ---
        var oracleSql45 = $"select id, upper(payload) as payload from public.{SourceTable} where id <= 45";
        var (oracleCount, oracleDigest) = await DigestAsync(adminConn, oracleSql45);
        Assert.Equal(35, oracleCount);
        var (sinkCount, sinkDigest) = await DigestAsync(adminConn, SelectAllSinkSql());
        Assert.Equal(oracleCount, sinkCount);
        Assert.Equal(oracleDigest, sinkDigest);

        // --- (d): the caught-up run moves 0 rows and leaves the watermark at `until` (unchanged). ---
        var (caughtUpExit, caughtUpResults) = await RunOnceAsync(_work, seenRunDirs);
        Assert.Equal(ExitCodes.Ok, caughtUpExit);
        Assert.Equal(0, ByKind(caughtUpResults)["SourceLoad"].GetProperty("rows").GetInt64());
        var caughtUpWatermark = ReadWatermarkEntry(_work, WatermarkKey);
        Assert.Equal(Until, caughtUpWatermark.GetProperty("value").GetString());

        // Sink content is unchanged by the caught-up run (it moved nothing).
        var (sinkCountAfterCaughtUp, sinkDigestAfterCaughtUp) = await DigestAsync(adminConn, SelectAllSinkSql());
        Assert.Equal(sinkCount, sinkCountAfterCaughtUp);
        Assert.Equal(sinkDigest, sinkDigestAfterCaughtUp);

        // --- (e): mid-loop re-run of an already-committed slice (merge idempotency). Rewind the stored
        // watermark back ONE slice (45 -> 40, the boundary before the last committed (40,45] slice) via
        // the real WatermarkStore.Set API (not raw JSON surgery -- exercises the exact write path the
        // engine itself uses), then run once more. The (40,45] slice is re-extracted (5 rows) and
        // re-merged; because the sink's merge is keyed on `id` (ON CONFLICT DO UPDATE), the replay must
        // NOT create duplicates -- final content still matches the same 35-row oracle, and the watermark
        // advances back to 45.
        var store = WatermarkStore.Local(Path.Combine(_work, ".pz", "state"));
        store.Set(WatermarkKey, new Watermark("id", "bigint", "40", "test-rewind-one-slice"));

        var (replayExit, replayResults) = await RunOnceAsync(_work, seenRunDirs);
        Assert.Equal(ExitCodes.Ok, replayExit);
        Assert.Equal(5, ByKind(replayResults)["SourceLoad"].GetProperty("rows").GetInt64());

        var replayWatermark = ReadWatermarkEntry(_work, WatermarkKey);
        Assert.Equal(Until, replayWatermark.GetProperty("value").GetString());

        var (sinkCountAfterReplay, sinkDigestAfterReplay) = await DigestAsync(adminConn, SelectAllSinkSql());
        // Same row COUNT (no duplicates -- a broken merge would double the 5 replayed rows, 35 -> 40) and
        // the SAME content digest as before the replay (re-merging identical rows is a no-op on content).
        Assert.Equal(oracleCount, sinkCountAfterReplay);
        Assert.Equal(sinkDigest, sinkDigestAfterReplay);
    }

    private static string SelectAllSinkSql() => $"select id, payload from public.{SinkTable}";

    private void WriteProject(string work)
    {
        Directory.CreateDirectory(Path.Combine(work, "pipelines"));

        File.WriteAllText(Path.Combine(work, "project.yml"), """
            name: windowed_backfill_e2e
            version: 0.1.0
            connectors:
              - package: Pz.Connector.Postgres
                version: 0.1.0
            engine:
              threads: 2
            """);

        // A real transform, not a bare passthrough -- upper-cases payload between source and sink, so
        // this genuinely exercises "source -> transform -> sink", not just "source -> sink".
        File.WriteAllText(Path.Combine(work, "pipelines", "orders_out.sql"),
            $"INSERT INTO {{{{ sink('{SinkName}', '{OutputName}', strategy: 'merge', keys: ['id'], " +
            $"schema_policy: 'fail_on_change') }}}}\nselect id, upper(payload) as payload\n" +
            $"from {{{{ source('{SourceName}', '{DatasetName}') }}}}\n");

        File.WriteAllText(Path.Combine(work, "connections.yml"), $"""
            {SourceName}:
              connector: postgres
              host: {_fx.Host}
              port: {_fx.Port}
              database: {_fx.Database}
              user: {_fx.User}
              password: {_fx.Password}
              entities:
                {DatasetName}:
                  read:
                    columns:
                      id: bigint
                      payload: varchar
                    sync:
                      mode: incremental
                      cursor: id
                      max_window: "10"
                      initial: "0"
                      until: "{Until}"
            """);

        File.AppendAllText(Path.Combine(work, "connections.yml"), $"""

            {SinkName}:
              connector: postgres
              host: {_fx.Host}
              port: {_fx.Port}
              database: {_fx.Database}
              user: {_fx.User}
              password: {_fx.Password}
            """);
    }

    /// <summary>Runs `pz run` via the real CLI entry point and returns the exit code plus the run's
    /// parsed run_results.json. Mirrors <c>IncrementalSyncTests.RunOnceAsync</c> exactly.</summary>
    private static Task<(int Exit, JsonDocument Results)> RunOnceAsync(string work, HashSet<string> seenRunDirs)
    {
        var exit = CliApp.Build().Parse(["run", "--project", work]).Invoke();

        var runsDir = Path.Combine(work, ".pz", "runs");
        var newDir = Directory.EnumerateDirectories(runsDir).Single(d => seenRunDirs.Add(d));
        var resultsPath = Path.Combine(newDir, "run_results.json");
        Assert.True(File.Exists(resultsPath));
        var results = JsonDocument.Parse(File.ReadAllBytes(resultsPath));
        return Task.FromResult((exit, results));
    }

    private static Dictionary<string, JsonElement> ByKind(JsonDocument results)
    {
        var byKind = new Dictionary<string, JsonElement>();
        foreach (var node in results.RootElement.GetProperty("nodes").EnumerateArray())
        {
            byKind[node.GetProperty("kind").GetString()!] = node;
        }

        return byKind;
    }

    private static JsonElement ReadWatermarkEntry(string work, string key)
    {
        var path = Path.Combine(work, ".pz", "state", "watermarks.json");
        using var doc = JsonDocument.Parse(File.ReadAllBytes(path));
        return doc.RootElement.GetProperty("watermarks").GetProperty(key).Clone();
    }

    private static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    /// <summary>Order-insensitive SHA-256 digest over every row's canonical rendering -- an INDEPENDENT
    /// oracle (plain ADO.NET reads via raw SQL, never touching the watermark/connector code path under
    /// test). Mirrors <c>IncrementalSyncTests.DigestAsync</c> exactly.</summary>
    private static async Task<(long Count, string Digest)> DigestAsync(string connectionString, string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

        var rows = new List<string>();
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            var parts = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                parts[i] = reader.IsDBNull(i) ? "<NULL>" : CanonicalValue(reader.GetValue(i));
            }

            rows.Add(string.Join('\u0001', parts));
        }

        rows.Sort(StringComparer.Ordinal);
        var digestBytes = SHA256.HashData(Encoding.UTF8.GetBytes(string.Join('\n', rows)));
        return (rows.Count, Convert.ToHexString(digestBytes));
    }

    private static string CanonicalValue(object value) => value switch
    {
        long l => l.ToString(CultureInfo.InvariantCulture),
        int n => n.ToString(CultureInfo.InvariantCulture),
        double d => d.ToString("R", CultureInfo.InvariantCulture),
        decimal m => m.ToString(CultureInfo.InvariantCulture),
        string s => s,
        _ => value.ToString() ?? "",
    };

    /// <summary>One Testcontainers postgres instance for this class -- mirrors
    /// <c>IncrementalSyncTests.PostgresFixture</c> exactly (a separate container, not shared with that
    /// class: neither file declares an <c>ICollectionFixture</c>, so this is the established
    /// per-e2e-class convention, not a new one).</summary>
    public sealed class PostgresFixture : IAsyncLifetime
    {
        private PostgreSqlContainer? _container;

        public PostgresFixture() => DockerFacts.SkipUnlessDocker();

        public string Host { get; private set; } = "";

        public int Port { get; private set; }

        public string Database { get; private set; } = "";

        public string User { get; private set; } = "";

        public string Password { get; private set; } = "";

        public string ConnectionString { get; private set; } = "";

        public async Task InitializeAsync()
        {
            _container = new PostgreSqlBuilder("postgres:16-alpine")
                .WithDatabase("pz")
                .WithUsername("pz")
                .WithPassword("pz")
                .Build();
            await _container.StartAsync().ConfigureAwait(false);

            ConnectionString = _container.GetConnectionString();
            var csb = new NpgsqlConnectionStringBuilder(ConnectionString);
            Host = csb.Host!;
            Port = csb.Port;
            Database = csb.Database!;
            User = csb.Username!;
            Password = csb.Password!;
        }

        public async Task DisposeAsync()
        {
            if (_container is not null)
            {
                await _container.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}
