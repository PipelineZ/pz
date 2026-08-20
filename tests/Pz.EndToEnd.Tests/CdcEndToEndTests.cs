using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;
using Pz.Cli;
using Pz.TestSupport;
using Testcontainers.PostgreSql;

namespace Pz.EndToEnd.Tests;

/// <summary>The flagship cdc end-to-end proof -- a real `postgres orders
/// (sync: mode: cdc) -&gt; transform -&gt; postgres sink (merge, keys=[id], on_delete: delete)` project run
/// through the real CLI entry point (<see cref="CliApp"/>) against a <c>wal_level=logical</c>
/// Testcontainers postgres. Exercises the whole cdc path end to end: the first-run snapshot, the bounded
/// pgoutput poll (insert/update/delete), delete propagation through the merge sink, an idempotent
/// no-change poll, a --full-refresh re-snapshot, and (test 2) a commit-gated failure replay where a failing
/// sink leaves the sync token unadvanced so the next run replays the same window and converges.
///
/// Joins the "console-redirection" collection like every other class here (pz run writes to the
/// process-global Console). Distinct table names per fact keep the two facts independent in the shared
/// container.</summary>
[Collection("console-redirection")]
public sealed class CdcEndToEndTests : IClassFixture<CdcEndToEndTests.CdcPostgresFixture>, IDisposable
{
    private const string SourceName = "pgsrc";
    private const string SinkName = "pgsink";
    private const string Publication = $"pz_{SourceName}"; // connector default: pz_{source}

    private readonly CdcPostgresFixture _fx;
    /// <summary>The connection block for the read side, kept so WriteSink can rewrite the whole
    /// file: both directions share connections.yml, and a second WriteSink call
    /// (the limited-credentials scenarios) would otherwise duplicate a YAML key.</summary>
    private string _sourceBlock = string.Empty;

    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-e2e-cdc-tests", Guid.NewGuid().ToString("N"));

    public CdcEndToEndTests(CdcPostgresFixture fixture) => _fx = fixture;

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [SkippableFact]
    public async Task Snapshot_then_poll_propagates_inserts_updates_and_deletes()
    {
        const string sourceTable = "cdc_orders_scn1";
        const string sinkTable = "cdc_orders_synced_scn1";
        var admin = _fx.ConnectionString;

        await SeedSourceAsync(admin, sourceTable, rows: 3);
        await CreatePublicationAsync(admin, sourceTable);
        await DropSlotAsync(admin, sourceTable);
        WriteProject(sourceTable, sinkTable, _fx.User /* superuser sink */, _fx.Password);

        var seen = new HashSet<string>();

        // Run 1: first-run snapshot -> target mirrors the seeded source.
        var (exit1, _) = RunOnce(seen);
        Assert.Equal(ExitCodes.Ok, exit1);
        await AssertMirrorsAsync(admin, sourceTable, sinkTable);

        // Mutate: insert a new key, update an existing key's column, delete a key.
        await ExecuteAsync(admin, $"insert into public.{sourceTable} (id, name) values (4, 'four')");
        await ExecuteAsync(admin, $"update public.{sourceTable} set name = 'one-updated' where id = 1");
        await ExecuteAsync(admin, $"delete from public.{sourceTable} where id = 2");

        // Run 2: bounded poll -> target mirrors source again, delete propagated.
        var (exit2, results2) = RunOnce(seen);
        Assert.Equal(ExitCodes.Ok, exit2);
        Assert.Equal("success", ByKind(results2)["SourceLoad"].GetProperty("status").GetString());
        await AssertMirrorsAsync(admin, sourceTable, sinkTable);
        Assert.Equal(3, await CountAsync(admin, sinkTable)); // 1(updated),3,4 -- 2 was deleted

        // Run 3: no new changes -> succeeds and stays converged (at-least-once boundary replay is a no-op
        // through the idempotent merge + delete-apply).
        var (exit3, _) = RunOnce(seen);
        Assert.Equal(ExitCodes.Ok, exit3);
        await AssertMirrorsAsync(admin, sourceTable, sinkTable);

        // Drop the target, then --full-refresh -> fresh snapshot rebuilds it from the current source.
        await ExecuteAsync(admin, $"drop table if exists public.{sinkTable}");
        var (exit4, _) = RunOnce(seen, fullRefresh: true);
        Assert.Equal(ExitCodes.Ok, exit4);
        await AssertMirrorsAsync(admin, sourceTable, sinkTable);
    }

    [SkippableFact]
    public async Task Sink_failure_leaves_token_unadvanced_and_next_run_replays_and_converges()
    {
        const string sourceTable = "cdc_orders_scn2";
        const string sinkTable = "cdc_orders_synced_scn2";
        const string limitedRole = "cdc_scn2_role";
        const string limitedPassword = "cdc_scn2_pw";
        var admin = _fx.ConnectionString;

        await SeedSourceAsync(admin, sourceTable, rows: 3);
        await CreatePublicationAsync(admin, sourceTable);
        await DropSlotAsync(admin, sourceTable);
        await ExecuteAsync(admin, $"drop role if exists {limitedRole}");
        WriteProject(sourceTable, sinkTable, _fx.User /* superuser sink for run 1 */, _fx.Password);

        var seen = new HashSet<string>();
        var (exit1, _) = RunOnce(seen);
        Assert.Equal(ExitCodes.Ok, exit1);
        await AssertMirrorsAsync(admin, sourceTable, sinkTable);

        var statePath = Path.Combine(_work, ".pz", "state", "sync-state.json");
        var beforeBytes = await File.ReadAllBytesAsync(statePath);

        // Mutate, then point the sink at a role that can create the temp table (SELECT) but cannot apply
        // the merge/deletes (no INSERT/UPDATE/DELETE) -- the sink commit fails AFTER SourceLoad succeeded.
        await ExecuteAsync(admin, $"insert into public.{sourceTable} (id, name) values (4, 'four')");
        await ExecuteAsync(admin, $"delete from public.{sourceTable} where id = 2");
        await ExecuteAsync(admin, $"create role {limitedRole} login password '{limitedPassword}'");
        await ExecuteAsync(admin, $"grant connect on database \"{_fx.Database}\" to {limitedRole}");
        await ExecuteAsync(admin, $"grant usage on schema public to {limitedRole}");
        await ExecuteAsync(admin, $"grant select on public.{sinkTable} to {limitedRole}");
        WriteSink(sinkTable, limitedRole, limitedPassword);

        var (exit2, results2) = RunOnce(seen);
        Assert.Equal(ExitCodes.NodeFailures, exit2);
        var byKind2 = ByKind(results2);
        Assert.Equal("success", byKind2["SourceLoad"].GetProperty("status").GetString());
        Assert.Equal("failed", byKind2["SinkWrite"].GetProperty("status").GetString());

        // Commit-gated: the sync token never advanced -- the state file is byte-for-byte unchanged.
        var afterFailureBytes = await File.ReadAllBytesAsync(statePath);
        Assert.Equal(beforeBytes, afterFailureBytes);

        // Heal the role and re-run: the same window replays (token unadvanced) and the target converges --
        // the delete of the already-absent key is a no-op, the merge idempotent.
        await ExecuteAsync(admin, $"grant insert, update, delete on public.{sinkTable} to {limitedRole}");
        var (exit3, results3) = RunOnce(seen);
        Assert.Equal(ExitCodes.Ok, exit3);
        Assert.Equal("success", ByKind(results3)["SinkWrite"].GetProperty("status").GetString());
        await AssertMirrorsAsync(admin, sourceTable, sinkTable);
        Assert.Equal(3, await CountAsync(admin, sinkTable)); // 1,3,4 -- 2 deleted
    }

    // ---- project + helpers ----

    private void WriteProject(string sourceTable, string sinkTable, string sinkUser, string sinkPassword)
    {
        Directory.CreateDirectory(Path.Combine(_work, "pipelines"));

        File.WriteAllText(Path.Combine(_work, "project.yml"), """
            name: cdc_e2e
            version: 0.1.0
            connectors:
              - package: Pz.Connector.Postgres
                version: 0.1.0
            engine:
              threads: 2
            """);

        File.WriteAllText(Path.Combine(_work, "pipelines", "orders_out.sql"),
            $"INSERT INTO {{{{ sink('{SinkName}', '{sinkTable}', strategy: 'merge', keys: ['id'], " +
            $"on_delete: 'delete', schema_policy: 'fail_on_change') }}}}\nselect id, name\n" +
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
                    sync:
                      mode: cdc
            """;

        WriteSink(sinkTable, sinkUser, sinkPassword);
    }

    private void WriteSink(string sinkTable, string sinkUser, string sinkPassword)
    {
        File.WriteAllText(Path.Combine(_work, "connections.yml"), _sourceBlock + "\n\n" + $"""
            {SinkName}:
              connector: postgres
              host: {_fx.Host}
              port: {_fx.Port}
              database: {_fx.Database}
              user: {sinkUser}
              password: {sinkPassword}
            """);
    }

    private (int Exit, JsonDocument Results) RunOnce(HashSet<string> seenRunDirs, bool fullRefresh = false)
    {
        var args = fullRefresh
            ? new[] { "run", "--project", _work, "--full-refresh" }
            : new[] { "run", "--project", _work };
        var exit = CliApp.Build().Parse(args).Invoke();

        var runsDir = Path.Combine(_work, ".pz", "runs");
        var newDir = Directory.EnumerateDirectories(runsDir).Single(d => seenRunDirs.Add(d));
        var resultsPath = Path.Combine(newDir, "run_results.json");
        Assert.True(File.Exists(resultsPath));
        return (exit, JsonDocument.Parse(File.ReadAllBytes(resultsPath)));
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

    private static async Task SeedSourceAsync(string admin, string table, int rows)
    {
        await ExecuteAsync(admin, $"drop table if exists public.{table} cascade");
        await ExecuteAsync(admin, $"create table public.{table} (id integer primary key, name text not null)");
        await ExecuteAsync(admin,
            $"insert into public.{table} (id, name) select i, 'row-' || i from generate_series(1, {rows}) i");
    }

    private static async Task CreatePublicationAsync(string admin, string table)
    {
        await ExecuteAsync(admin, $"drop publication if exists {Publication}");
        await ExecuteAsync(admin, $"create publication {Publication} for table public.{table}");
    }

    private static Task DropSlotAsync(string admin, string dataset) =>
        ExecuteAsync(admin,
            $"select pg_drop_replication_slot(slot_name) from pg_replication_slots " +
            $"where slot_name = 'pz_{SourceName}_{dataset}'");

    private static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
    }

    private static async Task<long> CountAsync(string admin, string table)
    {
        await using var connection = new NpgsqlConnection(admin);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand($"select count(*) from public.{table}", connection);
        return (long)(await command.ExecuteScalarAsync().ConfigureAwait(false))!;
    }

    // Independent oracle: order-insensitive digest of (id, name) read via plain ADO.NET, asserted equal
    // between the source table and the sink table -- so a match proves byte-for-byte the same row set.
    private static async Task AssertMirrorsAsync(string admin, string sourceTable, string sinkTable)
    {
        var (srcCount, srcDigest) = await DigestAsync(admin, sourceTable);
        var (sinkCount, sinkDigest) = await DigestAsync(admin, sinkTable);
        Assert.Equal(srcCount, sinkCount);
        Assert.Equal(srcDigest, sinkDigest);
    }

    private static async Task<(long Count, string Digest)> DigestAsync(string admin, string table)
    {
        await using var connection = new NpgsqlConnection(admin);
        await connection.OpenAsync().ConfigureAwait(false);
        await using var command = new NpgsqlCommand($"select id, name from public.{table}", connection);
        await using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);

        var rows = new List<string>();
        while (await reader.ReadAsync().ConfigureAwait(false))
        {
            var id = reader.GetInt32(0).ToString(CultureInfo.InvariantCulture);
            var name = reader.IsDBNull(1) ? "<NULL>" : reader.GetString(1);
            rows.Add($"{id}{name}");
        }

        rows.Sort(StringComparer.Ordinal);
        var digest = SHA256.HashData(Encoding.UTF8.GetBytes(string.Join('\n', rows)));
        return (rows.Count, Convert.ToHexString(digest));
    }

    /// <summary>One <c>wal_level=logical</c> Testcontainers postgres for the whole class -- mirrors
    /// <c>PostgresCdcContainerFixture</c>. The bootstrap superuser has REPLICATION, so the only cdc
    /// prerequisite each test arranges itself is the publication.</summary>
    public sealed class CdcPostgresFixture : IAsyncLifetime
    {
        private PostgreSqlContainer? _container;

        public CdcPostgresFixture() => DockerFacts.SkipUnlessDocker();

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
                .WithCommand("-c", "wal_level=logical")
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
