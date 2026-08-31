using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Npgsql;
using Pz.Cli;
using Pz.TestSupport;
using Testcontainers.PostgreSql;

namespace Pz.EndToEnd.Tests;

/// <summary>SQL-declared incremental: the flagship SQL-declared-watermark end-to-end
/// proof. Unlike <see cref="IncrementalSyncTests"/> (whose source YAML carries an <c>incremental:</c>
/// block), here the source YAML declares ONLY a <c>columns:</c> contract with NO <c>incremental:</c> block
/// at all -- the cursor is declared purely in pipeline SQL via <c>{{ watermark('pg', 'orders') }}</c>. The
/// DuckDbSqlAstReader infers the incremental from that expression at compile time, the executor pushes the
/// evaluated bound to the postgres source, and the pipeline's NULL-guard applies the cut -- driven through
/// the real CLI entry point (<see cref="CliApp"/>) against a Testcontainers postgres instance.
///
/// Runs `pz run` twice: run 1 lands every seeded row (no stored watermark -> unbounded); new rows are
/// inserted between runs; run 2 lands ONLY those new rows (the stored cursor gated the extract); the sink
/// is a postgres <c>merge</c> (keys=[id]) mirroring IncrementalSyncTests' simplest committed sink, and
/// <c>.pz/state/watermarks.json</c> ends up holding the advanced cursor keyed <c>pg.orders</c>.
///
/// Joins the "console-redirection" collection (like every class in this assembly): `pz run` writes to the
/// process-global <see cref="Console.Out"/>/<see cref="Console.Error"/>, so the shared collection serializes
/// it against other Console-redirecting classes.</summary>
[Collection("console-redirection")]
public sealed class SqlWatermarkEndToEndTests : IClassFixture<SqlWatermarkEndToEndTests.PostgresFixture>, IDisposable
{
    private const string SourceName = "pg";
    private const string DatasetName = "orders_src";
    private const string SinkName = "out";
    private const string OutputName = "orders_out";
    private const string WatermarkKey = $"{SourceName}.{DatasetName}";

    private readonly PostgresFixture _fx;
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-e2e-sqlwatermark-tests", Guid.NewGuid().ToString("N"));

    public SqlWatermarkEndToEndTests(PostgresFixture fixture)
    {
        DockerFacts.SkipUnlessDocker();
        _fx = fixture;
    }

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [SkippableFact]
    public async Task Sql_declared_watermark_extracts_delta_on_second_run()
    {
        const string sourceTable = DatasetName;
        const string sinkTable = OutputName;
        var adminConn = _fx.ConnectionString;

        await ExecuteAsync(adminConn, $"drop table if exists public.{sourceTable}");
        await ExecuteAsync(adminConn, $"drop table if exists public.{sinkTable}");
        await ExecuteAsync(adminConn, $"""
            create table public.{sourceTable} (
                id bigint primary key,
                amount double precision not null,
                updated_at timestamp not null
            )
            """);
        // 40 seeded rows: id 1..40, updated_at = 2026-01-01T00:00:00 + (id-1) minutes -- max is id=40 at
        // 2026-01-01T00:39:00.
        await ExecuteAsync(adminConn, $"""
            insert into public.{sourceTable} (id, amount, updated_at)
            select i, i * 1.5, timestamp '2026-01-01 00:00:00' + ((i - 1) || ' minutes')::interval
            from generate_series(1, 40) as i
            """);

        WriteProject(_work, sourceTable, sinkTable);

        var seenRunDirs = new HashSet<string>();

        // Run 1: no stored watermark -> the compiled NULL-guard passes every row; all 40 land.
        var (exit1, results1) = await RunOnceAsync(_work, seenRunDirs);
        Assert.Equal(ExitCodes.Ok, exit1);

        var byKind1 = ByKind(results1);
        Assert.Equal("success", byKind1["SourceLoad"].GetProperty("status").GetString());
        Assert.Equal(40, byKind1["SourceLoad"].GetProperty("rows").GetInt64());

        var (sourceCount1, sourceDigest1) = await DigestAsync(adminConn, SelectAllSql(sourceTable));
        var (sinkCount1, sinkDigest1) = await DigestAsync(adminConn, SelectAllSql(sinkTable));
        Assert.Equal(40, sourceCount1);
        Assert.Equal(sourceCount1, sinkCount1);
        Assert.Equal(sourceDigest1, sinkDigest1);

        // The watermark advanced to the max seen updated_at after run 1's sink committed.
        var wm1 = ReadWatermarkEntry(_work, WatermarkKey);
        Assert.Equal("updated_at", wm1.GetProperty("cursor").GetString());
        Assert.Equal("timestamp", wm1.GetProperty("type").GetString());
        Assert.Equal("2026-01-01T00:39:00.000000", wm1.GetProperty("value").GetString());

        // Insert 8 brand-new rows dated 2026-01-03 -- strictly after every original row's 2026-01-01
        // updated_at. Max delta value lands on id=48 at 2026-01-03T00:07:00.
        await ExecuteAsync(adminConn, $"""
            insert into public.{sourceTable} (id, amount, updated_at)
            select i, i * 1.5, timestamp '2026-01-03 00:00:00' + ((i - 41) || ' minutes')::interval
            from generate_series(41, 48) as i
            """);

        // Run 2: the SQL-declared watermark gates the extract -- ONLY the 8 new rows, never all 48.
        var (exit2, results2) = await RunOnceAsync(_work, seenRunDirs);
        Assert.Equal(ExitCodes.Ok, exit2);

        var byKind2 = ByKind(results2);
        Assert.Equal("success", byKind2["SourceLoad"].GetProperty("status").GetString());
        Assert.Equal(8, byKind2["SourceLoad"].GetProperty("rows").GetInt64());

        var (sourceCount2, sourceDigest2) = await DigestAsync(adminConn, SelectAllSql(sourceTable));
        var (sinkCount2, sinkDigest2) = await DigestAsync(adminConn, SelectAllSql(sinkTable));
        Assert.Equal(48, sourceCount2); // 40 original + 8 new, all merged into distinct-keyed rows
        Assert.Equal(sourceCount2, sinkCount2);
        Assert.Equal(sourceDigest2, sinkDigest2);

        // The advanced cursor, keyed pg.orders, now holds the delta's max.
        var wm2 = ReadWatermarkEntry(_work, WatermarkKey);
        Assert.Equal("2026-01-03T00:07:00.000000", wm2.GetProperty("value").GetString());
    }

    private static string SelectAllSql(string table) => $"select id, amount, updated_at from public.{table}";

    private void WriteProject(string work, string sourceTable, string sinkTable)
    {
        Directory.CreateDirectory(Path.Combine(work, "pipelines"));

        File.WriteAllText(Path.Combine(work, "project.yml"), """
            name: sql_watermark_e2e
            version: 0.1.0
            connectors:
              - package: Pz.Connector.Postgres
                version: 0.1.0
            engine:
              threads: 2
            """);

        // The cursor is declared ONLY here, in SQL, via watermark() -- the source YAML below has NO
        // incremental: block. This is the SQL-declared route the whole test exercises.
        File.WriteAllText(Path.Combine(work, "pipelines", "orders_load.sql"),
            $"INSERT INTO {{{{ sink('{SinkName}', '{OutputName}', strategy: 'merge', keys: ['id'], " +
            $"schema_policy: 'fail_on_change') }}}}\n" +
            $"select o.id, o.amount, o.updated_at\n" +
            $"from {{{{ source('{SourceName}', '{DatasetName}') }}}} as o\n" +
            $"where o.updated_at > {{{{ watermark('{SourceName}', '{DatasetName}') }}}}\n");

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
                      amount: double
                      updated_at: timestamp
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

    private static Task<(int Exit, JsonDocument Results)> RunOnceAsync(string work, HashSet<string> seenRunDirs)
    {
        var exit = CliApp.Build().Parse(new[] { "run", "--project", work }).Invoke();

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

    /// <summary>Order-insensitive SHA-256 digest over every row -- an INDEPENDENT oracle (plain ADO.NET,
    /// never the connector/watermark code path under test). Mirrors <see cref="IncrementalSyncTests"/>'s
    /// digest so a source/sink match proves byte-for-byte equal row sets.</summary>
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

            rows.Add(string.Join('', parts));
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
        DateTime dt => dt.ToString("yyyy-MM-ddTHH:mm:ss.ffffff", CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };

    /// <summary>One Testcontainers postgres instance for the whole class -- mirrors
    /// <see cref="IncrementalSyncTests.PostgresFixture"/>, including the constructor-level
    /// <see cref="DockerFacts.SkipUnlessDocker"/> whose <see cref="SkipException"/> xunit re-throws for every
    /// dependent test-class construction, so the fact above needs no redundant per-method skip check.</summary>
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
