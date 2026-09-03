using Pz.Connectors.Abstractions;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.TestSupport;

namespace Pz.Connector.Iceberg.Tests;

/// <summary>Real-catalog proof of the native-only data plane over a REST catalog with an S3
/// (MinIO) warehouse. Every test drives the REAL ISource/ISink surface: TryGetNativeScan/
/// TryGetNativeCopy fragments run through <see cref="NativeSetup.ExecuteSetupAsync"/> against a live
/// <see cref="DuckSession"/> exactly as the executors do. SKIPs without docker; also gated on
/// PZ_TESTS_OFFLINE (`install iceberg`/`install httpfs` need the extension repository on first use).</summary>
[Collection("iceberg-rest")]
public sealed class IcebergRestCatalogTests(IcebergRestCatalogFixture catalog) : IDisposable
{
    private readonly string dir = Directory.CreateTempSubdirectory("pz-iceberg-e2e-").FullName;

    // One namespace per test class instance so the shared catalog never sees a stale table.
    private readonly string ns = "t" + Guid.NewGuid().ToString("N")[..8];

    public void Dispose()
    {
        try { Directory.Delete(dir, recursive: true); } catch (IOException) { /* best-effort cleanup */ }
    }

    private ConnectorConfig Config() => new(new Dictionary<string, object?>
    {
        ["catalog"] = "rest",
        ["endpoint"] = catalog.Endpoint,
        // A name, never the warehouse URL: DuckDB attaches a URL-shaped iceberg path read-only.
        ["warehouse"] = "wh",
        ["token"] = "unchecked-by-the-fixture",
        ["storage_key_id"] = catalog.AccessKey,
        ["storage_secret_key"] = catalog.SecretKey,
        ["storage_endpoint"] = catalog.StorageEndpoint,
        ["storage_url_style"] = "path",
        ["storage_use_ssl"] = false,
    });

    private ConnectorConfig FilesConfig() => new(new Dictionary<string, object?>
    {
        ["catalog"] = "files",
        ["root"] = catalog.Warehouse,
        ["storage_key_id"] = catalog.AccessKey,
        ["storage_secret_key"] = catalog.SecretKey,
        ["storage_endpoint"] = catalog.StorageEndpoint,
        ["storage_url_style"] = "path",
        ["storage_use_ssl"] = false,
    });

    private DuckSession OpenDuck() => DuckSession.Open(Path.Combine(dir, $"scratch-{Guid.NewGuid():N}.duckdb"));

    private static async Task RunSetupAsync(DuckSession duck, IReadOnlyList<string> statements)
    {
        foreach (var setup in statements)
        {
            await NativeSetup.ExecuteSetupAsync(duck, setup, CancellationToken.None);
        }
    }

    private async Task WriteAsync(DuckSession duck, string table, string selectSql,
        string mode = "replace", IReadOnlyList<string>? keys = null)
    {
        await using var sink = await ((ISinkConnector)new IcebergConnector()).OpenAsync(Config(), CancellationToken.None);
        var spec = new OutputSpec("wh", $"{ns}.{table}", mode, "fail_on_change", new Dictionary<string, object?>()) { Keys = keys ?? [] };
        Assert.True(sink.TryGetNativeCopy(spec, out var copy));
        await RunSetupAsync(duck, copy!.SetupStatements);

        var staging = "stage_" + Guid.NewGuid().ToString("N");
        await duck.ExecuteAsync($"create table {staging} as {selectSql}");
        await duck.ExecuteAsync(copy.CopySql.Replace("{{source}}", staging, StringComparison.Ordinal));
    }

    private async Task<string> ReadAsync(DuckSession duck, DatasetSpec spec, ConnectorConfig? config = null, string connection = "wh")
    {
        await using var source = await ((ISourceConnector)new IcebergConnector()).OpenAsync(config ?? Config(), CancellationToken.None);
        Assert.True(source.TryGetNativeScan(spec, out var scan));
        await RunSetupAsync(duck, scan!.SetupStatements);

        var landed = "landed_" + Guid.NewGuid().ToString("N");
        await duck.ExecuteAsync($"create table {landed} as select * from {scan.SqlFragment}");
        return landed;
    }

    private DatasetSpec Spec(string table, Dictionary<string, object?>? options = null, string connection = "wh") =>
        new(connection, $"{ns}.{table}", options ?? []);

    private string Snapshots(string table) => $"iceberg_snapshots('{IcebergSql.Alias("wh")}.{ns}.{table}')";

    [SkippableFact]
    public async Task Write_then_read_round_trips_rows_including_quotes_and_nulls()
    {
        DockerFacts.SkipUnlessDocker();
        await using var duck = OpenDuck();

        await WriteAsync(duck, "events", """
            (select 1 as id, 'O''Brien' as name, 12.5 as amount union all select 2, NULL, 13.5)
            """);
        var landed = await ReadAsync(duck, Spec("events"));

        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
        Assert.Equal("O'Brien", await duck.ScalarAsync<string>($"select name from {landed} where id = 1"));
        Assert.True(await duck.ScalarAsync<bool>($"select name is null from {landed} where id = 2"));
    }

    [SkippableFact]
    public async Task Append_replace_and_merge_behave()
    {
        DockerFacts.SkipUnlessDocker();
        await using var duck = OpenDuck();

        await WriteAsync(duck, "log", "(select 1 as id)", mode: "append");
        await WriteAsync(duck, "log", "(select 2 as id)", mode: "append");
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {await ReadAsync(duck, Spec("log"))}"));

        await WriteAsync(duck, "snap", "(select 1 as id union all select 2)");
        var beforeReplace = await duck.ScalarAsync<long>($"select count(*) from {Snapshots("snap")}");
        await WriteAsync(duck, "snap", "(select 9 as id)");
        Assert.Equal(9L, await duck.ScalarAsync<long>($"select id from {await ReadAsync(duck, Spec("snap"))}"));
        // DuckDB's iceberg extension commits one snapshot per DML statement -- there is no
        // single-snapshot atomic overwrite it can be asked for (verified against the live REST
        // fixture: even one MERGE INTO with a "when not matched by source then delete" clause
        // still produces a separate delete snapshot and a separate append snapshot; DuckDB also
        // refuses CREATE OR REPLACE against an attached iceberg catalog outright). What the
        // wrapping transaction actually buys is that the delete and the insert land together or
        // not at all -- exactly two new snapshots appear, a delete immediately followed by an
        // append, never a delete on its own.
        var newSnapshotCount = await duck.ScalarAsync<long>($"select count(*) from {Snapshots("snap")}") - beforeReplace;
        Assert.Equal(2, newSnapshotCount);
        var newOperations = await duck.ScalarAsync<string>(
            $"select string_agg(operation, ',' order by sequence_number) from {Snapshots("snap")} " +
            $"where sequence_number > {beforeReplace}");
        Assert.Equal("delete,append", newOperations);

        await WriteAsync(duck, "dim", "(select 1 as id, 'a' as name union all select 2, 'b')", mode: "merge", keys: ["id"]);
        await WriteAsync(duck, "dim", "(select 2 as id, 'B' as name union all select 3, 'c')", mode: "merge", keys: ["id"]);
        var dim = await ReadAsync(duck, Spec("dim"));
        Assert.Equal(3, await duck.ScalarAsync<long>($"select count(*) from {dim}"));
        Assert.Equal("B", await duck.ScalarAsync<string>($"select name from {dim} where id = 2"));

        // Duplicate keys within one batch collapse to one survivor per key, new or already held.
        await WriteAsync(duck, "dim", "(select 3 as id, 'C' as name union all select 3, 'CC' union all select 4, 'd' union all select 4, 'dd')", mode: "merge", keys: ["id"]);
        var deduped = await ReadAsync(duck, Spec("dim"));
        Assert.Equal(4, await duck.ScalarAsync<long>($"select count(*) from {deduped}"));
        Assert.Equal(1, await duck.ScalarAsync<long>($"select count(*) from {deduped} where id = 3"));
        Assert.Equal(1, await duck.ScalarAsync<long>($"select count(*) from {deduped} where id = 4"));
    }

    [SkippableFact]
    public async Task Version_and_timestamp_options_read_an_older_snapshot()
    {
        DockerFacts.SkipUnlessDocker();
        await using var duck = OpenDuck();

        await WriteAsync(duck, "tt", "(select 1 as id)", mode: "append");
        var firstSnapshot = await duck.ScalarAsync<ulong>($"select snapshot_id from {Snapshots("tt")} order by sequence_number limit 1");
        var firstTime = await duck.ScalarAsync<DateTime>($"select timestamp_ms::timestamp from {Snapshots("tt")} order by sequence_number limit 1");
        await WriteAsync(duck, "tt", "(select 2 as id)", mode: "append");

        var latest = await ReadAsync(duck, Spec("tt"));
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {latest}"));

        var pinned = await ReadAsync(duck, Spec("tt", new Dictionary<string, object?> { ["version"] = firstSnapshot }));
        Assert.Equal(1, await duck.ScalarAsync<long>($"select count(*) from {pinned}"));

        var pinnedSubquery = await ReadAsync(duck,
            Spec("tt", new Dictionary<string, object?>
            {
                ["version"] = firstSnapshot,
                ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
            }) with { WatermarkCursor = "id", WatermarkValue = "0" });
        Assert.Equal(1, await duck.ScalarAsync<long>($"select count(*) from {pinnedSubquery}"));

        var atTime = await ReadAsync(duck,
            Spec("tt", new Dictionary<string, object?> { ["timestamp"] = firstTime.ToString("yyyy-MM-dd HH:mm:ss.ffffff") }));
        Assert.Equal(1, await duck.ScalarAsync<long>($"select count(*) from {atTime}"));
    }

    [SkippableFact]
    public async Task Contract_pruning_and_watermark_windows_apply()
    {
        DockerFacts.SkipUnlessDocker();
        await using var duck = OpenDuck();

        await WriteAsync(duck, "events", "(select 1 as id, 'a' as name, 9 as extra union all select 2, 'b', 8 union all select 3, 'c', 7)");

        var pruned = await ReadAsync(duck, Spec("events", new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        }));
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from (describe {pruned})"));

        var windowed = await ReadAsync(duck,
            Spec("events") with { WatermarkCursor = "id", WatermarkValue = "1", WatermarkUpperBound = "2" });
        Assert.Equal(1, await duck.ScalarAsync<long>($"select count(*) from {windowed}"));
        Assert.Equal(2L, await duck.ScalarAsync<long>($"select id from {windowed}"));
    }

    [SkippableFact]
    public async Task A_files_catalog_reads_the_same_table_directly_from_the_warehouse()
    {
        DockerFacts.SkipUnlessDocker();
        await using var duck = OpenDuck();

        await WriteAsync(duck, "direct", "(select 1 as id union all select 2 union all select 3)");
        var metadataVersion = await catalog.LatestMetadataVersionAsync(ns, "direct");

        var landed = await ReadAsync(duck,
            Spec("direct", new Dictionary<string, object?> { ["metadata_version"] = metadataVersion }, connection: "lake")
                with { WatermarkCursor = "id", WatermarkValue = "1" },
            FilesConfig(), connection: "lake");
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from {landed}"));
    }
}
