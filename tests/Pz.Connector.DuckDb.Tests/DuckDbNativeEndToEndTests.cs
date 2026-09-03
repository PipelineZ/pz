using Pz.Connectors.Abstractions;
using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Connector.DuckDb.Tests;

/// <summary>Real-file proof of the native-only connector's whole data plane. Every test drives the
/// REAL <see cref="ISource"/>/<see cref="ISink"/> surface returned from
/// <see cref="DuckDbConnector"/>.OpenAsync -- TryGetNativeScan/TryGetNativeCopy fragments run
/// through <see cref="NativeSetup.ExecuteSetupAsync"/> against a live <see cref="DuckSession"/>
/// exactly as the executors do. No docker, no network: nothing to install.</summary>
public sealed class DuckDbNativeEndToEndTests : IDisposable
{
    private readonly string dir = Directory.CreateTempSubdirectory("pz-duckdb-e2e-").FullName;

    public void Dispose()
    {
        try
        {
            Directory.Delete(dir, recursive: true);
        }
        catch (IOException)
        {
            // best-effort cleanup
        }
    }

    private string DbPath(string name = "app.duckdb") => Path.Combine(dir, name);

    private ConnectorConfig Config(string name = "app.duckdb") =>
        new(new Dictionary<string, object?> { ["path"] = DbPath(name) });

    private TempDuckSession OpenDuck()
    {
        var dbPath = Path.Combine(dir, $"scratch-{Guid.NewGuid():N}.duckdb");
        return new TempDuckSession(DuckSession.Open(dbPath), dbPath);
    }

    /// <summary>Writes <paramref name="selectSql"/>'s rows into <paramref name="entity"/> through the
    /// connector's own <see cref="ISink.TryGetNativeCopy"/> -- staging table substituted for
    /// <c>{{source}}</c> exactly as SinkWriteExecutor does.</summary>
    private async Task WriteAsync(DuckSession duck, string dbName, string entity, string selectSql,
        string mode = "replace", IReadOnlyList<string>? keys = null)
    {
        await using var sink = await ((ISinkConnector)new DuckDbConnector()).OpenAsync(Config(dbName), CancellationToken.None);
        var spec = new OutputSpec("wh", entity, mode, "fail_on_change", new Dictionary<string, object?>())
        {
            Keys = keys ?? [],
        };
        Assert.True(sink.TryGetNativeCopy(spec, out var copy));

        foreach (var setup in copy!.SetupStatements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }

        var staging = "stage_" + Guid.NewGuid().ToString("N");
        await duck.ExecuteAsync($"create table {staging} as {selectSql}");
        await duck.ExecuteAsync(copy.CopySql.Replace("{{source}}", staging, StringComparison.Ordinal));
    }

    private async Task<string> ReadAsync(DuckSession duck, string dbName, DatasetSpec spec)
    {
        await using var source = await ((ISourceConnector)new DuckDbConnector()).OpenAsync(Config(dbName), CancellationToken.None);
        Assert.True(source.TryGetNativeScan(spec, out var scan));

        foreach (var setup in scan!.SetupStatements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }

        var landed = "landed_" + Guid.NewGuid().ToString("N");
        await duck.ExecuteAsync($"create table {landed} as select * from {scan.SqlFragment}");
        return landed;
    }

    private static DatasetSpec Spec(string entity = "events") => new("wh", entity, new Dictionary<string, object?>());

    [Fact]
    public async Task Write_then_read_round_trips_rows_including_quotes_and_nulls()
    {
        await using var session = OpenDuck();
        var duck = session.Duck;

        await WriteAsync(duck, "app.duckdb", "events", """
            (select 1 as id, 'O''Brien' as name, 12.5 as amount
             union all
             select 2, NULL, 13.5)
            """);

        var landed = await ReadAsync(duck, "app.duckdb", Spec());

        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
        Assert.Equal("O'Brien", await duck.ScalarAsync<string>($"select name from {landed} where id = 1"));
        Assert.True(await duck.ScalarAsync<bool>($"select name is null from {landed} where id = 2"));
        Assert.Equal(13.5, await duck.ScalarAsync<double>($"select amount from {landed} where id = 2"));
    }

    [Fact]
    public async Task Types_survive_the_round_trip()
    {
        await using var session = OpenDuck();
        var duck = session.Duck;

        await WriteAsync(duck, "app.duckdb", "dated",
            "(select 1 as id, date '2026-03-27' as placed_on, timestamp '2026-03-27 10:30:00' as created_at)");

        var landed = await ReadAsync(duck, "app.duckdb", Spec("dated"));

        Assert.Equal("DATE", await duck.ScalarAsync<string>(
            $"select column_type from (describe {landed}) where column_name = 'placed_on'"));
        Assert.Equal("TIMESTAMP", await duck.ScalarAsync<string>(
            $"select column_type from (describe {landed}) where column_name = 'created_at'"));
    }

    [Fact]
    public async Task Append_creates_the_file_on_first_write_then_appends()
    {
        await using var session = OpenDuck();
        var duck = session.Duck;
        Assert.False(File.Exists(DbPath("fresh.duckdb")));

        await WriteAsync(duck, "fresh.duckdb", "log", "(select 1 as id)", mode: "append");
        Assert.True(File.Exists(DbPath("fresh.duckdb")));
        await WriteAsync(duck, "fresh.duckdb", "log", "(select 2 as id)", mode: "append");

        var landed = await ReadAsync(duck, "fresh.duckdb", Spec("log"));
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
    }

    [Fact]
    public async Task Replace_overwrites_previous_contents()
    {
        await using var session = OpenDuck();
        var duck = session.Duck;

        await WriteAsync(duck, "app.duckdb", "snap", "(select 1 as id union all select 2)", mode: "replace");
        await WriteAsync(duck, "app.duckdb", "snap", "(select 9 as id)", mode: "replace");

        var landed = await ReadAsync(duck, "app.duckdb", Spec("snap"));
        Assert.Equal(1, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
        Assert.Equal(9L, await duck.ScalarAsync<long>($"select id from {landed}"));
    }

    [Fact]
    public async Task Merge_updates_matched_rows_and_inserts_the_rest()
    {
        await using var session = OpenDuck();
        var duck = session.Duck;

        await WriteAsync(duck, "app.duckdb", "dim", "(select 1 as id, 'a' as name union all select 2, 'b')",
            mode: "merge", keys: ["id"]);
        await WriteAsync(duck, "app.duckdb", "dim", "(select 2 as id, 'B' as name union all select 3, 'c')",
            mode: "merge", keys: ["id"]);

        var landed = await ReadAsync(duck, "app.duckdb", Spec("dim"));
        Assert.Equal(3, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
        Assert.Equal("B", await duck.ScalarAsync<string>($"select name from {landed} where id = 2"));
        Assert.Equal("c", await duck.ScalarAsync<string>($"select name from {landed} where id = 3"));
    }

    // Two staged rows sharing a key within one batch collapse to one connector-determined survivor,
    // whether the key is new to the target (both rows would otherwise insert) or already held.
    [Fact]
    public async Task Merge_collapses_duplicate_keys_within_a_batch()
    {
        await using var session = OpenDuck();
        var duck = session.Duck;

        await WriteAsync(duck, "app.duckdb", "dupes", "(select 1 as id, 'a' as name)", mode: "merge", keys: ["id"]);
        await WriteAsync(duck, "app.duckdb", "dupes",
            "(select 1 as id, 'b' as name union all select 1, 'c' union all select 2, 'd' union all select 2, 'e')",
            mode: "merge", keys: ["id"]);

        var landed = await ReadAsync(duck, "app.duckdb", Spec("dupes"));
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
        Assert.Equal(1, await duck.ScalarAsync<long>($"select count(*) from {landed} where id = 1"));
        Assert.Equal(1, await duck.ScalarAsync<long>($"select count(*) from {landed} where id = 2"));
        Assert.NotEqual("a", await duck.ScalarAsync<string>($"select name from {landed} where id = 1"));
    }

    [Fact]
    public async Task Schema_qualified_entities_write_and_read_inside_that_schema()
    {
        await using var session = OpenDuck();
        var duck = session.Duck;

        // The connector does not create schemas; the target schema must already exist.
        await WriteAsync(duck, "app.duckdb", "bootstrap", "(select 1 as x)");
        await duck.ExecuteAsync($"create schema {DuckDbSql.Alias("wh")}.raw");

        await WriteAsync(duck, "app.duckdb", "raw.events", "(select 7 as id)");
        var landed = await ReadAsync(duck, "app.duckdb", Spec("raw.events"));
        Assert.Equal(7L, await duck.ScalarAsync<long>($"select id from {landed}"));
    }

    [Fact]
    public async Task Declared_columns_contract_prunes_the_read()
    {
        await using var session = OpenDuck();
        var duck = session.Duck;

        await WriteAsync(duck, "app.duckdb", "pruned",
            "(select 1 as id, 'alice' as name, 999 as extra_col union all select 2, 'bob', 998)");

        var spec = new DatasetSpec("wh", "pruned", new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        });
        var landed = await ReadAsync(duck, "app.duckdb", spec);

        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from (describe {landed})"));
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
    }

    [Fact]
    public async Task Watermark_and_window_bounds_confine_the_extraction()
    {
        await using var session = OpenDuck();
        var duck = session.Duck;

        await WriteAsync(duck, "app.duckdb", "wm",
            "(select 1 as id union all select 2 union all select 3 union all select 4)");

        var incremental = await ReadAsync(duck, "app.duckdb",
            Spec("wm") with { WatermarkCursor = "id", WatermarkValue = "2" });
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {incremental}"));
        Assert.Equal(3L, await duck.ScalarAsync<long>($"select min(id) from {incremental}"));

        var windowed = await ReadAsync(duck, "app.duckdb",
            Spec("wm") with { WatermarkCursor = "id", WatermarkValue = "1", WatermarkUpperBound = "3" });
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {windowed}"));
        Assert.Equal(3L, await duck.ScalarAsync<long>($"select max(id) from {windowed}"));
    }

    [Fact]
    public async Task A_timestamp_cursor_accepts_the_space_form_literal()
    {
        await using var session = OpenDuck();
        var duck = session.Duck;

        await WriteAsync(duck, "app.duckdb", "tscursor", """
            (select 1 as id, timestamp '2026-08-01 10:00:00' as updated_at
             union all
             select 2, timestamp '2026-08-15 11:30:00')
            """);

        var landed = await ReadAsync(duck, "app.duckdb",
            Spec("tscursor") with { WatermarkCursor = "updated_at", WatermarkValue = "2026-08-01T10:00:00" });

        Assert.Equal(1, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
        Assert.Equal(2L, await duck.ScalarAsync<long>($"select id from {landed}"));
    }

    [Fact]
    public async Task A_read_against_a_missing_file_is_refused_and_creates_no_file()
    {
        // The shared alias is read-write (writes must be able to create the file), so an unguarded
        // `attach if not exists` against a missing file would silently create an empty database on a
        // read -- indistinguishable from an empty table and a likely path typo (F5).
        Assert.False(File.Exists(DbPath("absent.duckdb")));

        await using var source = await ((ISourceConnector)new DuckDbConnector()).OpenAsync(Config("absent.duckdb"), CancellationToken.None);
        var ex = Assert.Throws<PzConnectorException>(() => source.TryGetNativeScan(Spec(), out _));

        Assert.False(ex.IsTransient);
        Assert.Contains("events", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(DbPath("absent.duckdb"), ex.Message, StringComparison.Ordinal);
        Assert.False(File.Exists(DbPath("absent.duckdb")));
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
