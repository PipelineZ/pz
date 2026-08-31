using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;
using Pz.Cli;
using Pz.TestSupport;
using Testcontainers.PostgreSql;

namespace Pz.EndToEnd.Tests;

/// <summary>The flagship incremental-sync end-to-end proof -- a real
/// `postgres orders (incremental cursor=updated_at) -&gt; transform -&gt; postgres sink (merge, keys=[id])`
/// project, run through the real CLI entry point (<see cref="CliApp"/>, exactly like every other class in
/// this project) against a Testcontainers postgres instance, demonstrating that watermark threading +
/// commit-gated advancement, postgres source cursor pushdown, and postgres sink binary-COPY + ON CONFLICT
/// merge compose correctly end to end.
///
/// Each of the three facts below is fully self-contained: it builds its own temp project directory, seeds
/// its own uniquely-named source/sink table pair in the SHARED container (<see cref="PostgresFixture"/> is
/// one container for the whole class, started once -- container startup is the expensive part, mirrored
/// from <c>Pz.Connector.Postgres.Tests/PostgresContainerFixture</c>), and drives however many `pz run`
/// invocations its scenario needs. Distinct table names per fact (scn1/scn2/scn3 suffixes) mean the three
/// facts never interfere with each other even though they share one postgres instance.
///
/// Joins the "console-redirection" collection (see <c>JsonLogFormatTests.ConsoleRedirectionCollection</c>):
/// like every other class in this assembly, `pz run` writes to the real, process-global <see
/// cref="Console.Out"/>/<see cref="Console.Error"/> -- without the shared collection, xunit's default
/// cross-class parallelism would let those writes race another class's Console-redirecting test.
///
/// All cursor timestamps below are explicit, hand-picked literals (never wall-clock/<c>Thread.Sleep</c>):
/// each scenario's "original" rows land on one anchor date and the "delta" rows land on a distinctly later
/// anchor date, so "extract only what's newer than the watermark" is proven deterministically.</summary>
[Collection("console-redirection")]
public sealed class IncrementalSyncTests : IClassFixture<IncrementalSyncTests.PostgresFixture>, IDisposable
{
    private const string SourceName = "pgsrc";
    private const string SinkName = "pgsink";

    private readonly PostgresFixture _fx;
    /// <summary>The connection block for the read side, kept so WriteSink can rewrite the whole
    /// file: both directions share connections.yml, and a second WriteSink call
    /// (the limited-credentials scenarios) would otherwise duplicate a YAML key.</summary>
    private string _sourceBlock = string.Empty;

    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-e2e-incremental-tests", Guid.NewGuid().ToString("N"));

    public IncrementalSyncTests(PostgresFixture fixture)
    {
        DockerFacts.SkipUnlessDocker();
        _fx = fixture;
    }

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [SkippableFact]
    public async Task Second_run_extracts_only_the_delta_and_merges()
    {
        const string sourceTable = "orders_scn1";
        const string sinkTable = "orders_synced_scn1";
        var adminConn = _fx.ConnectionString;

        await ExecuteAsync(adminConn, $"drop table if exists public.{sourceTable}");
        await ExecuteAsync(adminConn, $"drop table if exists public.{sinkTable}");
        await ExecuteAsync(adminConn, $"""
            create table public.{sourceTable} (
                id bigint primary key,
                customer text not null,
                amount double precision not null,
                updated_at timestamp not null
            )
            """);
        // 50 original rows: id 1..50, updated_at = 2026-01-01T00:00:00 + (id-1) minutes -- max is id=50 at
        // 2026-01-01T00:49:00.
        await ExecuteAsync(adminConn, $"""
            insert into public.{sourceTable} (id, customer, amount, updated_at)
            select i, 'cust-' || i, i * 1.5, timestamp '2026-01-01 00:00:00' + ((i - 1) || ' minutes')::interval
            from generate_series(1, 50) as i
            """);

        WriteProject(_work, sourceTable, sinkTable, _fx.User, _fx.Password);

        var seenRunDirs = new HashSet<string>();
        var (exit1, results1) = await RunOnceAsync(_work, seenRunDirs);
        Assert.Equal(ExitCodes.Ok, exit1);

        var byKind1 = ByKind(results1);
        Assert.Equal("success", byKind1["SourceLoad"].GetProperty("status").GetString());
        Assert.Equal(50, byKind1["SourceLoad"].GetProperty("rows").GetInt64());

        var (sourceCount1, sourceDigest1) = await DigestAsync(adminConn, SelectAllSql(sourceTable));
        var (sinkCount1, sinkDigest1) = await DigestAsync(adminConn, SelectAllSql(sinkTable));
        Assert.Equal(50, sourceCount1);
        Assert.Equal(sourceCount1, sinkCount1);
        Assert.Equal(sourceDigest1, sinkDigest1);

        // Delta: mutate 5 existing rows (non-key column + updated_at bumped) and insert 10 brand-new rows,
        // all dated on 2026-01-03 -- strictly after every original row's 2026-01-01 updated_at. Max delta
        // value lands on id=60 at 2026-01-03T00:19:00 (10th inserted row, offset 9 minutes past 00:10:00).
        await ExecuteAsync(adminConn, $"""
            update public.{sourceTable}
            set customer = customer || '-updated',
                updated_at = timestamp '2026-01-03 00:00:00' + ((id - 1) || ' minutes')::interval
            where id between 1 and 5
            """);
        await ExecuteAsync(adminConn, $"""
            insert into public.{sourceTable} (id, customer, amount, updated_at)
            select i, 'cust-' || i, i * 1.5, timestamp '2026-01-03 00:10:00' + ((i - 51) || ' minutes')::interval
            from generate_series(51, 60) as i
            """);

        var (exit2, results2) = await RunOnceAsync(_work, seenRunDirs);
        Assert.Equal(ExitCodes.Ok, exit2);

        var byKind2 = ByKind(results2);
        Assert.Equal("success", byKind2["SourceLoad"].GetProperty("status").GetString());
        // The load-bearing assertion: EXACTLY the 15-row delta (5 updated + 10 inserted), never the full
        // 60 -- proving the stored watermark genuinely gated the second extract's SELECT.
        Assert.Equal(15, byKind2["SourceLoad"].GetProperty("rows").GetInt64());

        var (sourceCount2, sourceDigest2) = await DigestAsync(adminConn, SelectAllSql(sourceTable));
        var (sinkCount2, sinkDigest2) = await DigestAsync(adminConn, SelectAllSql(sinkTable));
        Assert.Equal(60, sourceCount2); // 45 untouched + 5 updated + 10 new -- 60 DISTINCT ids, no duplicates
        Assert.Equal(sourceCount2, sinkCount2);
        Assert.Equal(sourceDigest2, sinkDigest2);

        var watermark = ReadWatermarkEntry(_work, $"{SourceName}.{sourceTable}");
        Assert.Equal("updated_at", watermark.GetProperty("cursor").GetString());
        Assert.Equal("timestamp", watermark.GetProperty("type").GetString());
        Assert.Equal("2026-01-03T00:19:00.000000", watermark.GetProperty("value").GetString());
    }

    /// <summary>Fault injection: a DEDICATED low-privilege postgres role (created fresh for this fact --
    /// revoking from the shared Testcontainers bootstrap user is a no-op since that user is a superuser,
    /// which bypasses every grant). The role is granted exactly
    /// enough to open a write session and get as far as the finalize step -- SELECT on the sink table
    /// (needed both for `create temp table ... (like target)`, which postgres requires SELECT for, and for
    /// the pre-existing-merge-target unique-constraint probe, which only reads system catalogs) -- but
    /// never INSERT/UPDATE, so `PostgresSinkWriteSession.CommitAsync`'s finalize `insert ... on conflict ...
    /// do update` fails deterministically with a permission-denied error. Crucially this happens AFTER
    /// SourceLoad has already succeeded (extraction is a separate, already-committed-to-staging node), so
    /// the run is a genuine "crash between extract and commit", not a compile-time or connection-level
    /// failure.</summary>
    [SkippableFact]
    public async Task Crash_between_extract_and_commit_does_not_advance_watermark()
    {
        const string sourceTable = "orders_scn2";
        const string sinkTable = "orders_synced_scn2";
        const string limitedRole = "scn2_limited_role";
        const string limitedPassword = "scn2_limited_pw";
        var adminConn = _fx.ConnectionString;

        await ExecuteAsync(adminConn, $"drop table if exists public.{sourceTable}");
        await ExecuteAsync(adminConn, $"drop table if exists public.{sinkTable}");
        await ExecuteAsync(adminConn, $"drop role if exists {limitedRole}");
        await ExecuteAsync(adminConn, $"""
            create table public.{sourceTable} (
                id bigint primary key,
                customer text not null,
                amount double precision not null,
                updated_at timestamp not null
            )
            """);
        // 20 original rows: 2026-02-01T00:00 + (id-1) minutes -- max is id=20 at 2026-02-01T00:19:00.
        await ExecuteAsync(adminConn, $"""
            insert into public.{sourceTable} (id, customer, amount, updated_at)
            select i, 'cust-' || i, i * 1.5, timestamp '2026-02-01 00:00:00' + ((i - 1) || ' minutes')::interval
            from generate_series(1, 20) as i
            """);

        WriteProject(_work, sourceTable, sinkTable, _fx.User, _fx.Password);

        var seenRunDirs = new HashSet<string>();
        var (exit1, results1) = await RunOnceAsync(_work, seenRunDirs);
        Assert.Equal(ExitCodes.Ok, exit1);
        Assert.Equal(20, ByKind(results1)["SourceLoad"].GetProperty("rows").GetInt64());

        var watermarkPath = Path.Combine(_work, ".pz", "state", "watermarks.json");
        var beforeBytes = await File.ReadAllBytesAsync(watermarkPath);

        // Delta: 3 mutated + 4 inserted, all dated 2026-02-03 -- strictly after the established watermark.
        await ExecuteAsync(adminConn, $"""
            update public.{sourceTable}
            set customer = customer || '-updated',
                updated_at = timestamp '2026-02-03 00:00:00' + ((id - 1) || ' minutes')::interval
            where id between 1 and 3
            """);
        await ExecuteAsync(adminConn, $"""
            insert into public.{sourceTable} (id, customer, amount, updated_at)
            select i, 'cust-' || i, i * 1.5, timestamp '2026-02-03 00:10:00' + ((i - 21) || ' minutes')::interval
            from generate_series(21, 24) as i
            """);

        // Dedicated low-privilege role: SELECT on the (already-created-by-run-1) sink table, but no
        // INSERT/UPDATE -- see this method's doc comment for exactly which finalize step this breaks.
        await ExecuteAsync(adminConn, $"create role {limitedRole} login password '{limitedPassword}'");
        await ExecuteAsync(adminConn, $"grant connect on database \"{_fx.Database}\" to {limitedRole}");
        await ExecuteAsync(adminConn, $"grant usage on schema public to {limitedRole}");
        await ExecuteAsync(adminConn, $"grant select on public.{sinkTable} to {limitedRole}");

        WriteSink(_work, sinkTable, limitedRole, limitedPassword);

        var (exit2, results2) = await RunOnceAsync(_work, seenRunDirs);
        Assert.Equal(ExitCodes.NodeFailures, exit2);

        var byKind2 = ByKind(results2);
        // Extraction genuinely succeeded (the delta was read into staging) -- only the sink's commit
        // failed, proving this is a post-extract, pre-commit fault, not an earlier one.
        Assert.Equal("success", byKind2["SourceLoad"].GetProperty("status").GetString());
        var deltaRows = byKind2["SourceLoad"].GetProperty("rows").GetInt64();
        Assert.Equal(7, deltaRows); // 3 updated + 4 inserted
        Assert.Equal("failed", byKind2["SinkWrite"].GetProperty("status").GetString());

        var afterFailureBytes = await File.ReadAllBytesAsync(watermarkPath);
        // The load-bearing assertion: byte-for-byte identical -- WatermarkAdvancement never even attempted
        // a write (advancement is gated on every downstream sink committing), so the file was never
        // touched, not merely "logically the same value".
        Assert.Equal(beforeBytes, afterFailureBytes);

        // Heal: grant the missing privileges to the SAME role (proves the fix is the privilege, not a
        // connection swap) and re-run with the identical (still-unadvanced) watermark.
        await ExecuteAsync(adminConn, $"grant insert, update on public.{sinkTable} to {limitedRole}");

        var (exit3, results3) = await RunOnceAsync(_work, seenRunDirs);
        Assert.Equal(ExitCodes.Ok, exit3);

        var byKind3 = ByKind(results3);
        Assert.Equal("success", byKind3["SinkWrite"].GetProperty("status").GetString());
        // Re-extracts the EXACT same slice the failed run attempted -- the watermark never advanced, so
        // there was nothing else it could have re-extracted.
        Assert.Equal(deltaRows, byKind3["SourceLoad"].GetProperty("rows").GetInt64());

        var (sourceCount, sourceDigest) = await DigestAsync(adminConn, SelectAllSql(sourceTable));
        var (sinkCount, sinkDigest) = await DigestAsync(adminConn, SelectAllSql(sinkTable));
        Assert.Equal(24, sourceCount);
        Assert.Equal(sourceCount, sinkCount);
        Assert.Equal(sourceDigest, sinkDigest);

        var watermark = ReadWatermarkEntry(_work, $"{SourceName}.{sourceTable}");
        Assert.Equal("2026-02-03T00:13:00.000000", watermark.GetProperty("value").GetString());
    }

    [SkippableFact]
    public async Task Full_refresh_rebuilds_identically()
    {
        const string sourceTable = "orders_scn3";
        const string sinkTable = "orders_synced_scn3";
        var adminConn = _fx.ConnectionString;

        await ExecuteAsync(adminConn, $"drop table if exists public.{sourceTable}");
        await ExecuteAsync(adminConn, $"drop table if exists public.{sinkTable}");
        await ExecuteAsync(adminConn, $"""
            create table public.{sourceTable} (
                id bigint primary key,
                customer text not null,
                amount double precision not null,
                updated_at timestamp not null
            )
            """);
        // 30 original rows: 2026-03-01T00:00 + (id-1) minutes -- max is id=30 at 2026-03-01T00:29:00.
        await ExecuteAsync(adminConn, $"""
            insert into public.{sourceTable} (id, customer, amount, updated_at)
            select i, 'cust-' || i, i * 1.5, timestamp '2026-03-01 00:00:00' + ((i - 1) || ' minutes')::interval
            from generate_series(1, 30) as i
            """);

        WriteProject(_work, sourceTable, sinkTable, _fx.User, _fx.Password);

        var seenRunDirs = new HashSet<string>();
        var (exit1, results1) = await RunOnceAsync(_work, seenRunDirs);
        Assert.Equal(ExitCodes.Ok, exit1);
        Assert.Equal(30, ByKind(results1)["SourceLoad"].GetProperty("rows").GetInt64());

        // Delta: 5 new rows dated 2026-03-02 -- strictly after the established watermark. Max lands on
        // id=35 at 2026-03-02T00:04:00.
        await ExecuteAsync(adminConn, $"""
            insert into public.{sourceTable} (id, customer, amount, updated_at)
            select i, 'cust-' || i, i * 1.5, timestamp '2026-03-02 00:00:00' + ((i - 31) || ' minutes')::interval
            from generate_series(31, 35) as i
            """);

        var (exit2, results2) = await RunOnceAsync(_work, seenRunDirs);
        Assert.Equal(ExitCodes.Ok, exit2);
        Assert.Equal(5, ByKind(results2)["SourceLoad"].GetProperty("rows").GetInt64());

        var (sourceCountBefore, sourceDigestBefore) = await DigestAsync(adminConn, SelectAllSql(sourceTable));
        var (_, sinkDigestBefore) = await DigestAsync(adminConn, SelectAllSql(sinkTable));
        Assert.Equal(35, sourceCountBefore);
        Assert.Equal(sourceDigestBefore, sinkDigestBefore);

        var (exit3, results3) = await RunOnceAsync(_work, seenRunDirs, fullRefresh: true);
        Assert.Equal(ExitCodes.Ok, exit3);

        var byKind3 = ByKind(results3);
        // --full-refresh ignores the stored watermark on read -- every row is re-extracted, not just
        // whatever's newer than the last cursor.
        Assert.Equal(35, byKind3["SourceLoad"].GetProperty("rows").GetInt64());

        var (sourceCountAfter, sourceDigestAfter) = await DigestAsync(adminConn, SelectAllSql(sourceTable));
        var (sinkCountAfter, sinkDigestAfter) = await DigestAsync(adminConn, SelectAllSql(sinkTable));
        Assert.Equal(sourceCountBefore, sourceCountAfter);
        Assert.Equal(sourceCountAfter, sinkCountAfter);
        Assert.Equal(sourceDigestAfter, sinkDigestAfter);
        // Merge is idempotent: re-merging every row leaves the sink's content byte-for-byte the same
        // digest as before the full refresh.
        Assert.Equal(sinkDigestBefore, sinkDigestAfter);

        var watermark = ReadWatermarkEntry(_work, $"{SourceName}.{sourceTable}");
        Assert.Equal("updated_at", watermark.GetProperty("cursor").GetString());
        Assert.Equal("timestamp", watermark.GetProperty("type").GetString());
        Assert.Equal("2026-03-02T00:04:00.000000", watermark.GetProperty("value").GetString());
    }

    private static string SelectAllSql(string table) => $"select id, customer, amount, updated_at from public.{table}";

    private void WriteProject(string work, string sourceTable, string sinkTable, string sinkUser, string sinkPassword)
    {
        Directory.CreateDirectory(Path.Combine(work, "pipelines"));

        File.WriteAllText(Path.Combine(work, "project.yml"), """
            name: incremental_sync_e2e
            version: 0.1.0
            connectors:
              - package: Pz.Connector.Postgres
                version: 0.1.0
            engine:
              threads: 2
            """);

        File.WriteAllText(Path.Combine(work, "pipelines", "orders_out.sql"),
            $"INSERT INTO {{{{ sink('{SinkName}', '{sinkTable}', strategy: 'merge', keys: ['id'], " +
            $"schema_policy: 'fail_on_change') }}}}\nselect id, customer, amount, updated_at\n" +
            $"from {{{{ source('{SourceName}', '{sourceTable}') }}}}\n");

        _sourceBlock = $"""
            {SourceName}:
              connector: postgres
              host: {_fx.Host}
              port: {_fx.Port}
              database: {_fx.Database}
              user: {_fx.User}
              password: {_fx.Password}
              entities:
                {sourceTable}:
                  read:
                    columns:
                      id: bigint
                      customer: varchar
                      amount: double
                      updated_at: timestamp
                    sync:
                      mode: incremental
                      cursor: updated_at
            """;

        WriteSink(work, sinkTable, sinkUser, sinkPassword);
    }

    private void WriteSink(string work, string sinkTable, string sinkUser, string sinkPassword)
    {
        File.WriteAllText(Path.Combine(work, "connections.yml"), _sourceBlock + "\n\n" + $"""
            {SinkName}:
              connector: postgres
              host: {_fx.Host}
              port: {_fx.Port}
              database: {_fx.Database}
              user: {sinkUser}
              password: {sinkPassword}
            """);
    }

    /// <summary>Runs `pz run` (optionally `--full-refresh`) via the real CLI entry point and returns the
    /// exit code plus the run's parsed run_results.json. <paramref name="seenRunDirs"/> is mutated in
    /// place so successive calls within one test can each identify exactly the one NEW `.pz/runs/&lt;id&gt;`
    /// directory this call produced (mirrors <c>RetryRunTests</c>' "the directory that wasn't there
    /// before" pattern, generalized to more than two runs).</summary>
    private static Task<(int Exit, JsonDocument Results)> RunOnceAsync(string work, HashSet<string> seenRunDirs, bool fullRefresh = false)
    {
        var args = fullRefresh
            ? new[] { "run", "--project", work, "--full-refresh" }
            : new[] { "run", "--project", work };
        var exit = CliApp.Build().Parse(args).Invoke();

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
    /// oracle: plain ADO.NET reads via raw SQL, never touching the watermark/connector code path under
    /// test. Used both as the "oracle" (queried against the source table with a hand-written predicate)
    /// and to read back the sink table's committed content, so a match proves the two are byte-for-byte
    /// the same row set, not merely a plausible one. Mirrors
    /// <c>PostgresWatermarkTests.ReadRowsAndDigestAsync</c>'s digest style, adapted to plain ADO.NET rows
    /// (no Arrow batches) since this test drives everything through the real `pz run` CLI, not the
    /// connector API directly.</summary>
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
        // Both the source's `timestamp` (no tz) and the sink's `timestamptz` columns round-trip to a
        // .NET DateTime here; the container's session TimeZone is UTC (same discipline
        // PostgresWatermarkTests's type matrix relies on), so formatting the wall-clock digits alone
        // (ignoring Kind) yields identical strings for the same instant on both sides.
        DateTime dt => dt.ToString("yyyy-MM-ddTHH:mm:ss.ffffff", CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };

    /// <summary>One Testcontainers postgres instance for the whole class (container startup is the
    /// expensive part) -- mirrors <c>Pz.Connector.Postgres.Tests/PostgresContainerFixture</c>, including
    /// the constructor-level <see cref="DockerFacts.SkipUnlessDocker"/> call whose thrown
    /// <see cref="SkipException"/> xunit records and re-throws for every dependent <see
    /// cref="IClassFixture{TFixture}"/>-using test class construction, which is why the three facts above
    /// need no redundant per-method skip check.</summary>
    public sealed class PostgresFixture : IAsyncLifetime
    {
        private PostgreSqlContainer? _container;

        public string Host { get; private set; } = "";

        public int Port { get; private set; }

        public string Database { get; private set; } = "";

        public string User { get; private set; } = "";

        public string Password { get; private set; } = "";

        public string ConnectionString { get; private set; } = "";

        public async Task InitializeAsync()
        {
            if (!DockerFacts.IsAvailable)
            {
                return;
            }

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
