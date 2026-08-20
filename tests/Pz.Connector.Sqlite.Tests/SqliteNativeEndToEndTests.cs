using Microsoft.Data.Sqlite;
using Pz.Connectors.Abstractions;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.TestSupport;

namespace Pz.Connector.Sqlite.Tests;

/// <summary>Real-file proof of the native-only connector's whole data plane; sqlite is a file, so this
/// suite needs no docker at all. Every test drives the REAL <see cref="ISource"/>/<see cref="ISink"/>
/// surface returned from <see cref="SqliteConnector"/>.OpenAsync -- TryGetNativeScan/TryGetNativeCopy
/// fragments run through <see cref="NativeSetup.ExecuteSetupAsync"/> against a live
/// <see cref="DuckSession"/>. Gated on <c>PZ_TESTS_OFFLINE</c> only: <c>install sqlite</c> needs the
/// extension repo on first run.
///
/// The one thing the connector's own sink cannot author is a sqlite schema with declared
/// DATE/DATETIME column types (the extension's DDL translation flattens them to TEXT), so the
/// typed-schema read test seeds through Microsoft.Data.Sqlite, test-only.</summary>
public sealed class SqliteNativeEndToEndTests : IDisposable
{
    private readonly string dir = Directory.CreateTempSubdirectory("pz-sqlite-e2e-").FullName;

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        try
        {
            Directory.Delete(dir, recursive: true);
        }
        catch (IOException)
        {
            // best-effort cleanup
        }
    }

    private string DbPath(string name = "app.db") => Path.Combine(dir, name);

    private ConnectorConfig Config(string name = "app.db") =>
        new(new Dictionary<string, object?> { ["path"] = DbPath(name) });

    private TempDuckSession OpenDuck()
    {
        var dbPath = Path.Combine(dir, $"scratch-{Guid.NewGuid():N}.duckdb");
        return new TempDuckSession(DuckSession.Open(dbPath), dbPath);
    }

    /// <summary>Writes <paramref name="selectSql"/>'s rows into sqlite table <paramref name="table"/>
    /// through the connector's own <see cref="ISink.TryGetNativeCopy"/> -- staging table substituted
    /// for <c>{{source}}</c> exactly as SinkWriteExecutor does.</summary>
    private async Task SeedAsync(DuckSession duck, string dbName, string table, string selectSql, string mode = "replace")
    {
        await using var sink = await ((ISinkConnector)new SqliteConnector()).OpenAsync(Config(dbName), CancellationToken.None);
        var spec = new OutputSpec("wh", table, mode, "fail_on_change", new Dictionary<string, object?>());
        Assert.True(sink.TryGetNativeCopy(spec, out var copy));

        foreach (var setup in copy!.SetupStatements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }

        var staging = "stage_" + Guid.NewGuid().ToString("N");
        await duck.ExecuteAsync($"create table {staging} as {selectSql}");
        await duck.ExecuteAsync(copy.CopySql.Replace("{{source}}", staging, StringComparison.Ordinal));
    }

    private async Task<string> MaterializeScanAsync(DuckSession duck, string dbName, DatasetSpec spec)
    {
        await using var source = await ((ISourceConnector)new SqliteConnector()).OpenAsync(Config(dbName), CancellationToken.None);
        Assert.True(source.TryGetNativeScan(spec, out var scan));

        foreach (var setup in scan!.SetupStatements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }

        var landed = "landed_" + Guid.NewGuid().ToString("N");
        await duck.ExecuteAsync($"create table {landed} as select * from {scan.SqlFragment}");
        return landed;
    }

    [SkippableFact]
    public async Task Write_then_read_round_trips_rows_including_quotes_and_nulls()
    {
        DockerFacts.SkipIfOffline();
        await using var session = OpenDuck();
        var duck = session.Duck;

        await SeedAsync(duck, "app.db", "events", """
            (select 1 as id, 'O''Brien' as name, 12.5 as amount
             union all
             select 2, NULL, 13.5)
            """);

        var landed = await MaterializeScanAsync(duck, "app.db",
            new DatasetSpec("appdb", "events", new Dictionary<string, object?>()));

        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
        Assert.Equal("O'Brien", await duck.ScalarAsync<string>($"select name from {landed} where id = 1"));
        Assert.True(await duck.ScalarAsync<bool>($"select name is null from {landed} where id = 2"));
        Assert.Equal(13.5, await duck.ScalarAsync<double>($"select amount from {landed} where id = 2"));
    }

    [SkippableFact]
    public async Task Sink_written_dates_flatten_to_text_and_round_trip_as_iso_strings()
    {
        // Documented degradation: the extension stores DATE/TIMESTAMP as TEXT, so a pz-created table
        // reads back VARCHAR -- with the VALUES intact in ISO form.
        DockerFacts.SkipIfOffline();
        await using var session = OpenDuck();
        var duck = session.Duck;

        await SeedAsync(duck, "app.db", "dated",
            "(select 1 as id, date '2026-03-27' as placed_on, timestamp '2026-03-27 10:30:00' as created_at)");

        var landed = await MaterializeScanAsync(duck, "app.db",
            new DatasetSpec("appdb", "dated", new Dictionary<string, object?>()));

        Assert.Equal("VARCHAR", await duck.ScalarAsync<string>(
            $"select column_type from (describe {landed}) where column_name = 'placed_on'"));
        Assert.Equal("2026-03-27", await duck.ScalarAsync<string>($"select placed_on from {landed}"));
        Assert.Equal("2026-03-27 10:30:00", await duck.ScalarAsync<string>($"select created_at from {landed}"));
    }

    [SkippableFact]
    public async Task A_sqlite_authored_typed_schema_lands_typed()
    {
        // The read-side mapping against a schema sqlite itself authored: declared DATE/DATETIME
        // columns surface as real DuckDB DATE/TIMESTAMP in staging.
        DockerFacts.SkipIfOffline();
        var dbPath = DbPath("native.db");
        await using (var sq = new SqliteConnection($"Data Source={dbPath}"))
        {
            await sq.OpenAsync();
            var cmd = sq.CreateCommand();
            cmd.CommandText = """
                create table events (id integer primary key, happened_on date, updated_at datetime);
                insert into events values
                  (1, '2026-08-01', '2026-08-01 10:00:00'),
                  (2, '2026-08-15', '2026-08-15 11:30:00');
                """;
            await cmd.ExecuteNonQueryAsync();
        }

        SqliteConnection.ClearAllPools();
        await using var session = OpenDuck();
        var duck = session.Duck;

        var landed = await MaterializeScanAsync(duck, "native.db",
            new DatasetSpec("appdb", "events", new Dictionary<string, object?>()));

        Assert.Equal("DATE", await duck.ScalarAsync<string>(
            $"select column_type from (describe {landed}) where column_name = 'happened_on'"));
        Assert.Equal("TIMESTAMP", await duck.ScalarAsync<string>(
            $"select column_type from (describe {landed}) where column_name = 'updated_at'"));
        Assert.Equal(new DateOnly(2026, 8, 1),
            await duck.ScalarAsync<DateOnly>($"select happened_on from {landed} where id = 1"));
    }

    [SkippableFact]
    public async Task Declared_columns_contract_prunes_the_read()
    {
        DockerFacts.SkipIfOffline();
        await using var session = OpenDuck();
        var duck = session.Duck;

        await SeedAsync(duck, "app.db", "pruned",
            "(select 1 as id, 'alice' as name, 999 as extra_col union all select 2, 'bob', 998)");

        var spec = new DatasetSpec("appdb", "pruned", new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        });
        var landed = await MaterializeScanAsync(duck, "app.db", spec);

        Assert.Equal(2, await duck.ScalarAsync<long>(
            $"select count(*) from (describe {landed})"));
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
    }

    [SkippableFact]
    public async Task Append_creates_the_file_on_first_write_then_appends()
    {
        DockerFacts.SkipIfOffline();
        await using var session = OpenDuck();
        var duck = session.Duck;
        Assert.False(File.Exists(DbPath("fresh.db")));

        await SeedAsync(duck, "fresh.db", "log", "(select 1 as id)", mode: "append");
        Assert.True(File.Exists(DbPath("fresh.db")));
        await SeedAsync(duck, "fresh.db", "log", "(select 2 as id)", mode: "append");

        var landed = await MaterializeScanAsync(duck, "fresh.db",
            new DatasetSpec("appdb", "log", new Dictionary<string, object?>()));
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
    }

    [SkippableFact]
    public async Task Replace_overwrites_previous_contents()
    {
        DockerFacts.SkipIfOffline();
        await using var session = OpenDuck();
        var duck = session.Duck;

        await SeedAsync(duck, "app.db", "snap", "(select 1 as id union all select 2)", mode: "replace");
        await SeedAsync(duck, "app.db", "snap", "(select 9 as id)", mode: "replace");

        var landed = await MaterializeScanAsync(duck, "app.db",
            new DatasetSpec("appdb", "snap", new Dictionary<string, object?>()));
        Assert.Equal(1, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
        Assert.Equal(9L, await duck.ScalarAsync<long>($"select id from {landed}"));
    }

    [SkippableFact]
    public async Task Watermark_and_window_bounds_confine_the_extraction()
    {
        DockerFacts.SkipIfOffline();
        await using var session = OpenDuck();
        var duck = session.Duck;

        await SeedAsync(duck, "app.db", "wm",
            "(select 1 as id union all select 2 union all select 3 union all select 4)");

        var incremental = await MaterializeScanAsync(duck, "app.db",
            new DatasetSpec("appdb", "wm", new Dictionary<string, object?>()) { WatermarkCursor = "id", WatermarkValue = "2" });
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {incremental}"));
        Assert.Equal(3L, await duck.ScalarAsync<long>($"select min(id) from {incremental}"));

        var windowed = await MaterializeScanAsync(duck, "app.db",
            new DatasetSpec("appdb", "wm", new Dictionary<string, object?>())
            {
                WatermarkCursor = "id",
                WatermarkValue = "1",
                WatermarkUpperBound = "3",
            });
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {windowed}"));
        Assert.Equal(3L, await duck.ScalarAsync<long>($"select max(id) from {windowed}"));
    }

    [SkippableFact]
    public async Task A_text_stored_timestamp_cursor_compares_lexically_with_the_space_form_literal()
    {
        // Why the T→space conversion is load-bearing here: the cursor column a pz sink creates is
        // TEXT, and the space-form literal must confine the read lexically.
        DockerFacts.SkipIfOffline();
        await using var session = OpenDuck();
        var duck = session.Duck;

        await SeedAsync(duck, "app.db", "textcursor", """
            (select 1 as id, timestamp '2026-08-01 10:00:00' as updated_at
             union all
             select 2, timestamp '2026-08-15 11:30:00')
            """);

        var landed = await MaterializeScanAsync(duck, "app.db",
            new DatasetSpec("appdb", "textcursor", new Dictionary<string, object?>())
            {
                WatermarkCursor = "updated_at",
                WatermarkValue = "2026-08-01T10:00:00",
            });

        Assert.Equal(1, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
        Assert.Equal(2L, await duck.ScalarAsync<long>($"select id from {landed}"));
    }
}

/// <summary>A scratch DuckDB file, deleted on dispose (best-effort). Test-support types are
/// replicated per-suite rather than shared, matching the connector rule.</summary>
internal sealed class TempDuckSession(DuckSession duck, string dbPath) : IAsyncDisposable
{
    public DuckSession Duck => duck;

    public async ValueTask DisposeAsync()
    {
        await duck.DisposeAsync().ConfigureAwait(false);
        try
        {
            File.Delete(dbPath);
        }
        catch
        {
            // Suppressed by design: best-effort temp-file cleanup.
        }
    }
}
