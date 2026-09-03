using Pz.Connectors.Abstractions;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.TestSupport;

namespace Pz.Connector.DuckLake.Tests;

/// <summary>Real-lake proof of the native-only data plane over the two file catalogs (no docker).
/// Every test drives the REAL ISource/ISink surface: TryGetNativeScan/TryGetNativeCopy fragments
/// run through <see cref="NativeSetup.ExecuteSetupAsync"/> against a live <see cref="DuckSession"/>
/// exactly as the executors do. Gated on PZ_TESTS_OFFLINE: `install ducklake`/`install sqlite`
/// need the extension repository on first use.</summary>
public sealed class DuckLakeNativeEndToEndTests : IDisposable
{
    private readonly string dir = Directory.CreateTempSubdirectory("pz-ducklake-e2e-").FullName;

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch (IOException) { /* best-effort cleanup */ }
    }

    private ConnectorConfig Config(string catalog) => new(new Dictionary<string, object?>
    {
        ["catalog"] = catalog,
        ["path"] = Path.Combine(dir, catalog == "sqlite" ? "catalog.sqlite" : "catalog.ducklake"),
        ["data_path"] = Path.Combine(dir, "data-" + catalog),
    });

    private TempDuckSession OpenDuck() =>
        new(DuckSession.Open(Path.Combine(dir, $"scratch-{Guid.NewGuid():N}.duckdb")));

    private async Task WriteAsync(DuckSession duck, string catalog, string entity, string selectSql,
        string mode = "replace", IReadOnlyList<string>? keys = null)
    {
        await using var sink = await ((ISinkConnector)new DuckLakeConnector()).OpenAsync(Config(catalog), CancellationToken.None);
        var spec = new OutputSpec("wh", entity, mode, "fail_on_change", new Dictionary<string, object?>()) { Keys = keys ?? [] };
        Assert.True(sink.TryGetNativeCopy(spec, out var copy));
        foreach (var setup in copy!.SetupStatements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }

        var staging = "stage_" + Guid.NewGuid().ToString("N");
        await duck.ExecuteAsync($"create table {staging} as {selectSql}");
        await duck.ExecuteAsync(copy.CopySql.Replace("{{source}}", staging, StringComparison.Ordinal));
    }

    private async Task<string> ReadAsync(DuckSession duck, string catalog, DatasetSpec spec)
    {
        await using var source = await ((ISourceConnector)new DuckLakeConnector()).OpenAsync(Config(catalog), CancellationToken.None);
        Assert.True(source.TryGetNativeScan(spec, out var scan));
        foreach (var setup in scan!.SetupStatements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }

        var landed = "landed_" + Guid.NewGuid().ToString("N");
        await duck.ExecuteAsync($"create table {landed} as select * from {scan.SqlFragment}");
        return landed;
    }

    private static DatasetSpec Spec(string entity, Dictionary<string, object?>? options = null) => new("wh", entity, options ?? []);

    [SkippableTheory]
    [InlineData("duckdb")]
    [InlineData("sqlite")]
    public async Task Write_then_read_round_trips_rows_including_quotes_and_nulls(string catalog)
    {
        DockerFacts.SkipIfOffline();
        await using var session = OpenDuck();
        var duck = session.Duck;

        await WriteAsync(duck, catalog, "events", """
            (select 1 as id, 'O''Brien' as name, 12.5 as amount union all select 2, NULL, 13.5)
            """);
        var landed = await ReadAsync(duck, catalog, Spec("events"));

        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
        Assert.Equal("O'Brien", await duck.ScalarAsync<string>($"select name from {landed} where id = 1"));
        Assert.True(await duck.ScalarAsync<bool>($"select name is null from {landed} where id = 2"));
        Assert.True(Directory.Exists(Path.Combine(dir, "data-" + catalog)));
    }

    [SkippableTheory]
    [InlineData("duckdb")]
    [InlineData("sqlite")]
    public async Task Append_replace_and_merge_behave(string catalog)
    {
        DockerFacts.SkipIfOffline();
        await using var session = OpenDuck();
        var duck = session.Duck;

        await WriteAsync(duck, catalog, "log", "(select 1 as id)", mode: "append");
        await WriteAsync(duck, catalog, "log", "(select 2 as id)", mode: "append");
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {await ReadAsync(duck, catalog, Spec("log"))}"));

        await WriteAsync(duck, catalog, "snap", "(select 1 as id union all select 2)");
        await WriteAsync(duck, catalog, "snap", "(select 9 as id)");
        Assert.Equal(9L, await duck.ScalarAsync<long>($"select id from {await ReadAsync(duck, catalog, Spec("snap"))}"));

        await WriteAsync(duck, catalog, "dim", "(select 1 as id, 'a' as name union all select 2, 'b')", mode: "merge", keys: ["id"]);
        await WriteAsync(duck, catalog, "dim", "(select 2 as id, 'B' as name union all select 3, 'c')", mode: "merge", keys: ["id"]);
        var dim = await ReadAsync(duck, catalog, Spec("dim"));
        Assert.Equal(3, await duck.ScalarAsync<long>($"select count(*) from {dim}"));
        Assert.Equal("B", await duck.ScalarAsync<string>($"select name from {dim} where id = 2"));
    }

    [SkippableFact]
    public async Task Version_option_reads_an_older_snapshot()
    {
        DockerFacts.SkipIfOffline();
        await using var session = OpenDuck();
        var duck = session.Duck;

        await WriteAsync(duck, "duckdb", "tt", "(select 1 as id)");
        var firstSnapshot = await duck.ScalarAsync<long>($"select max(snapshot_id) from {DuckLakeSql.Alias("wh")}.snapshots()");
        await WriteAsync(duck, "duckdb", "tt", "(select 1 as id union all select 2)");

        var latest = await ReadAsync(duck, "duckdb", Spec("tt"));
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {latest}"));

        var pinned = await ReadAsync(duck, "duckdb", Spec("tt", new Dictionary<string, object?> { ["version"] = firstSnapshot }));
        Assert.Equal(1, await duck.ScalarAsync<long>($"select count(*) from {pinned}"));
    }

    [SkippableFact]
    public async Task Timestamp_option_reads_the_snapshot_current_at_that_instant()
    {
        DockerFacts.SkipIfOffline();
        await using var session = OpenDuck();
        var duck = session.Duck;

        await WriteAsync(duck, "duckdb", "tt2", "(select 1 as id)");
        var firstTime = await duck.ScalarAsync<DateTime>(
            $"select max(snapshot_time)::timestamp from {DuckLakeSql.Alias("wh")}.snapshots()");
        await WriteAsync(duck, "duckdb", "tt2", "(select 1 as id union all select 2)");

        var pinned = await ReadAsync(duck, "duckdb",
            Spec("tt2", new Dictionary<string, object?> { ["timestamp"] = firstTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff") }));
        Assert.Equal(1, await duck.ScalarAsync<long>($"select count(*) from {pinned}"));
    }

    [SkippableFact]
    public async Task Schema_qualified_entities_and_contract_pruning_and_watermarks()
    {
        DockerFacts.SkipIfOffline();
        await using var session = OpenDuck();
        var duck = session.Duck;

        await WriteAsync(duck, "duckdb", "bootstrap", "(select 1 as x)");
        await duck.ExecuteAsync($"create schema {DuckLakeSql.Alias("wh")}.raw");
        await WriteAsync(duck, "duckdb", "raw.events", "(select 1 as id, 'a' as name, 9 as extra union all select 2, 'b', 8 union all select 3, 'c', 7)");

        var pruned = await ReadAsync(duck, "duckdb", Spec("raw.events", new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        }));
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from (describe {pruned})"));

        var windowed = await ReadAsync(duck, "duckdb",
            Spec("raw.events") with { WatermarkCursor = "id", WatermarkValue = "1", WatermarkUpperBound = "2" });
        Assert.Equal(1, await duck.ScalarAsync<long>($"select count(*) from {windowed}"));
        Assert.Equal(2L, await duck.ScalarAsync<long>($"select id from {windowed}"));
    }

    [SkippableTheory]
    [InlineData("duckdb")]
    [InlineData("sqlite")]
    public async Task A_read_against_a_missing_catalog_is_refused_and_creates_no_file(string catalog)
    {
        // The shared alias is read-write (writes must be able to create the catalog), so an unguarded
        // `attach if not exists` against a missing file-backed catalog would silently create an empty
        // catalog and "succeed" at reading zero rows -- indistinguishable from an empty table and a
        // likely path typo. Never touches DuckDB (no extensions to install), so no offline gate.
        var catalogPath = (string)Config(catalog).Values["path"]!;
        Assert.False(File.Exists(catalogPath));

        await using var source = await ((ISourceConnector)new DuckLakeConnector()).OpenAsync(Config(catalog), CancellationToken.None);
        var ex = Assert.Throws<PzConnectorException>(() => source.TryGetNativeScan(Spec("events"), out _));

        Assert.False(ex.IsTransient);
        Assert.Contains("events", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(catalogPath, ex.Message, StringComparison.Ordinal);
        Assert.False(File.Exists(catalogPath));
    }
}

/// <summary>A scratch DuckDB session whose file is deleted on dispose (best-effort).</summary>
internal sealed class TempDuckSession(DuckSession duck) : IAsyncDisposable
{
    public DuckSession Duck => duck;

    public async ValueTask DisposeAsync() => await duck.DisposeAsync().ConfigureAwait(false);
}
