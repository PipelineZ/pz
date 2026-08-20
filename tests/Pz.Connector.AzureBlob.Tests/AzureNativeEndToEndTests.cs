using Pz.Connectors.Abstractions;
using Pz.DuckDb;
using Pz.TestSupport;

namespace Pz.Connector.AzureBlob.Tests;

/// <summary>Azurite Testcontainers e2e (docker+network gated -- see <see cref="AzuriteFixture"/>): drives a
/// real <see cref="DuckSession"/> through the azure connector's native COPY (<see
/// cref="AzureSink.TryGetNativeCopy"/>) to write a parquet blob to a live Azurite instance, then through
/// its native scan (<see cref="AzureSource.TryGetNativeScan"/>) to read it back -- proving the whole round
/// trip through the DuckDB <c>azure</c> extension against the emulator, not just statement shape (mirrors
/// <c>MinioEndToEndTests</c>, minus the full <c>pz run</c> wrapper -- this drives DuckDB directly).</summary>
[Collection("azurite")]
public sealed class AzureNativeEndToEndTests(AzuriteFixture fixture)
{
    private static ConnectorConfig Config(AzuriteFixture fixture) => new(new Dictionary<string, object?>
    {
        ["auth"] = "connection_string",
        ["connection_string"] = fixture.ConnectionString,
    });

    [SkippableFact]
    public async Task Azure_native_copy_roundtrips_through_azurite()
    {
        DockerFacts.SkipUnlessDocker();

        var dbPath = Path.Combine(Path.GetTempPath(), $"pz-azure-e2e-{Guid.NewGuid():N}.duckdb");
        try
        {
            await using var duck = DuckSession.Open(dbPath);

            var sink = new AzureSink(Config(fixture));
            var outputSpec = new OutputSpec("lake", "data", "replace", "fail_on_change",
                new Dictionary<string, object?> { ["container"] = AzuriteFixture.Container, ["path"] = "out", ["format"] = "parquet" });
            Assert.True(sink.TryGetNativeCopy(outputSpec, out var copy));

            foreach (var setup in copy!.SetupStatements)
            {
                await duck.ExecuteAsync(setup);
            }

            const string ThreeRows = "(select 1 as id, 'alice' as customer union all " +
                "select 2, 'bob' union all select 3, 'carol')";
            var copySql = copy.CopySql.Replace("{{source}}", ThreeRows, StringComparison.Ordinal);
            await duck.ExecuteAsync(copySql);

            var source = new AzureSource(Config(fixture));
            var datasetSpec = new DatasetSpec("lake", "data",
                new Dictionary<string, object?> { ["container"] = AzuriteFixture.Container, ["path"] = "out/data.parquet", ["format"] = "parquet" });
            Assert.True(source.TryGetNativeScan(datasetSpec, out var scan));

            foreach (var setup in scan!.SetupStatements)
            {
                await duck.ExecuteAsync(setup);
            }

            var rowCount = await duck.ScalarAsync<long>($"select count(*) from {scan.SqlFragment}");

            Assert.Equal(3, rowCount);
        }
        finally
        {
            try { File.Delete(dbPath); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>Proves the native json tier (<see cref="AzureSink.TryGetNativeCopy"/>'s <c>(format json)</c>
    /// COPY + <see cref="AzureSource.TryGetNativeScan"/>'s <c>read_json(..., format = 'newline_delimited')</c>)
    /// round-trips through a live Azurite -- <see cref="AzureSqlGenTests.NativeScan_json_renders_columns_map_when_full_contract_present"/>
    /// and <c>NativeCopy_json_emits_format_json</c> only assert the generated SQL's shape without ever executing
    /// it; this proves DuckDB's `azure` extension actually reads back what it wrote, mirroring
    /// <see cref="Azure_native_copy_roundtrips_through_azurite"/> for parquet. Declares a full `columns:`
    /// contract on the read side (native json does not require one -- see
    /// <see cref="Azure_native_json_scan_infers_schema_without_contract"/> for the contract-less case).</summary>
    [SkippableFact]
    public async Task Azure_native_json_copy_roundtrips_through_azurite()
    {
        DockerFacts.SkipUnlessDocker();

        var dbPath = Path.Combine(Path.GetTempPath(), $"pz-azure-e2e-json-{Guid.NewGuid():N}.duckdb");
        var prefix = $"json-native-{Guid.NewGuid():N}";
        try
        {
            await using var duck = DuckSession.Open(dbPath);

            var sink = new AzureSink(Config(fixture));
            var outputSpec = new OutputSpec("lake", "data", "replace", "fail_on_change",
                new Dictionary<string, object?> { ["container"] = AzuriteFixture.Container, ["path"] = prefix, ["format"] = "json" });
            Assert.True(sink.TryGetNativeCopy(outputSpec, out var copy));

            foreach (var setup in copy!.SetupStatements)
            {
                await duck.ExecuteAsync(setup);
            }

            const string ThreeRows = "(select 1 as id, 'alice' as customer union all " +
                "select 2, 'bob' union all select 3, 'carol')";
            var copySql = copy.CopySql.Replace("{{source}}", ThreeRows, StringComparison.Ordinal);
            await duck.ExecuteAsync(copySql);

            var source = new AzureSource(Config(fixture));
            var columns = new Dictionary<string, string> { ["id"] = "bigint", ["customer"] = "varchar" };
            var datasetSpec = new DatasetSpec("lake", "data", new Dictionary<string, object?>
            {
                ["container"] = AzuriteFixture.Container,
                ["path"] = $"{prefix}/data.json",
                ["format"] = "json",
                ["columns"] = columns,
            });
            Assert.True(source.TryGetNativeScan(datasetSpec, out var scan));
            Assert.Contains("read_json(", scan!.SqlFragment, StringComparison.Ordinal);

            foreach (var setup in scan.SetupStatements)
            {
                await duck.ExecuteAsync(setup);
            }

            var rowCount = await duck.ScalarAsync<long>($"select count(*) from {scan.SqlFragment}");
            Assert.Equal(3, rowCount);

            var customers = await duck.ScalarAsync<string>(
                $"select string_agg(customer, ',' order by id) from {scan.SqlFragment}");
            Assert.Equal("alice,bob,carol", customers);
        }
        finally
        {
            try { File.Delete(dbPath); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>Proves the csv native scan tier actually lands data against a live Azurite when NO
    /// `columns:` contract is declared at all, unlike <see
    /// cref="Azure_native_copy_roundtrips_through_azurite"/>/<c>Azure_native_json_copy_roundtrips_through_azurite</c>
    /// which both declare a full contract. Asserts actual landed VALUES rather than exact DuckDB type names:
    /// which type DuckDB infers is DuckDB's call, not pz's.</summary>
    [SkippableFact]
    public async Task Azure_native_csv_scan_infers_schema_without_contract()
    {
        DockerFacts.SkipUnlessDocker();

        var dbPath = Path.Combine(Path.GetTempPath(), $"pz-azure-e2e-csv-infer-{Guid.NewGuid():N}.duckdb");
        var prefix = $"csv-infer-{Guid.NewGuid():N}";
        try
        {
            await using var duck = DuckSession.Open(dbPath);

            var sink = new AzureSink(Config(fixture));
            var outputSpec = new OutputSpec("lake", "data", "replace", "fail_on_change",
                new Dictionary<string, object?> { ["container"] = AzuriteFixture.Container, ["path"] = prefix, ["format"] = "csv" });
            Assert.True(sink.TryGetNativeCopy(outputSpec, out var copy));

            foreach (var setup in copy!.SetupStatements)
            {
                await duck.ExecuteAsync(setup);
            }

            const string ThreeRows = "(select 1 as id, 'alice' as customer union all " +
                "select 2, 'bob' union all select 3, 'carol')";
            var copySql = copy.CopySql.Replace("{{source}}", ThreeRows, StringComparison.Ordinal);
            await duck.ExecuteAsync(copySql);

            var source = new AzureSource(Config(fixture));
            var datasetSpec = new DatasetSpec("lake", "data", new Dictionary<string, object?>
            {
                ["container"] = AzuriteFixture.Container,
                ["path"] = $"{prefix}/data.csv",
                ["format"] = "csv",
                // No "columns" key at all -- the contract-less case.
            });
            Assert.True(source.TryGetNativeScan(datasetSpec, out var scan));
            Assert.Contains("auto_detect = true", scan!.SqlFragment, StringComparison.Ordinal);
            Assert.DoesNotContain("columns = {", scan.SqlFragment, StringComparison.Ordinal);

            foreach (var setup in scan.SetupStatements)
            {
                await duck.ExecuteAsync(setup);
            }

            var rowCount = await duck.ScalarAsync<long>($"select count(*) from {scan.SqlFragment}");
            Assert.Equal(3, rowCount);

            var customers = await duck.ScalarAsync<string>(
                $"select string_agg(customer, ',' order by id) from {scan.SqlFragment}");
            Assert.Equal("alice,bob,carol", customers);
        }
        finally
        {
            try { File.Delete(dbPath); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>json's contract-less branch (<c>auto_detect = true</c>, no `columns=`/`types=` map -- see <see
    /// cref="AzureSqlGenTests.NativeScan_json_auto_detects_without_columns_contract"/> for the SQL-shape
    /// assertion) is the ONE json case that behaves like csv's; the declared-contract case diverges (<see
    /// cref="AzureSqlGenTests.ReadJson_has_no_types_named_parameter"/>). Worth its own live e2e fact rather
    /// than leaning on csv's alone: nothing else proves this shape actually lands data against a live
    /// Azurite.</summary>
    [SkippableFact]
    public async Task Azure_native_json_scan_infers_schema_without_contract()
    {
        DockerFacts.SkipUnlessDocker();

        var dbPath = Path.Combine(Path.GetTempPath(), $"pz-azure-e2e-json-infer-{Guid.NewGuid():N}.duckdb");
        var prefix = $"json-infer-{Guid.NewGuid():N}";
        try
        {
            await using var duck = DuckSession.Open(dbPath);

            var sink = new AzureSink(Config(fixture));
            var outputSpec = new OutputSpec("lake", "data", "replace", "fail_on_change",
                new Dictionary<string, object?> { ["container"] = AzuriteFixture.Container, ["path"] = prefix, ["format"] = "json" });
            Assert.True(sink.TryGetNativeCopy(outputSpec, out var copy));

            foreach (var setup in copy!.SetupStatements)
            {
                await duck.ExecuteAsync(setup);
            }

            const string ThreeRows = "(select 1 as id, 'alice' as customer union all " +
                "select 2, 'bob' union all select 3, 'carol')";
            var copySql = copy.CopySql.Replace("{{source}}", ThreeRows, StringComparison.Ordinal);
            await duck.ExecuteAsync(copySql);

            var source = new AzureSource(Config(fixture));
            var datasetSpec = new DatasetSpec("lake", "data", new Dictionary<string, object?>
            {
                ["container"] = AzuriteFixture.Container,
                ["path"] = $"{prefix}/data.json",
                ["format"] = "json",
                // No "columns" key at all -- the contract-less case.
            });
            Assert.True(source.TryGetNativeScan(datasetSpec, out var scan));
            Assert.Contains("auto_detect = true", scan!.SqlFragment, StringComparison.Ordinal);
            Assert.DoesNotContain("columns = {", scan.SqlFragment, StringComparison.Ordinal);

            foreach (var setup in scan.SetupStatements)
            {
                await duck.ExecuteAsync(setup);
            }

            var rowCount = await duck.ScalarAsync<long>($"select count(*) from {scan.SqlFragment}");
            Assert.Equal(3, rowCount);

            var customers = await duck.ScalarAsync<string>(
                $"select string_agg(customer, ',' order by id) from {scan.SqlFragment}");
            Assert.Equal("alice,bob,carol", customers);
        }
        finally
        {
            try { File.Delete(dbPath); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>Proves the watermark-window minimal cover (<see cref="AzureSource.TryGetNativeScan"/> +
    /// <c>PathTemplate.WindowCover</c>) actually prunes reads against a live Azurite: seeds three
    /// day-folders (07-11, 07-12, 07-13) under a unique per-run prefix, then runs a native scan whose
    /// watermark window spans only the middle day. The generated scan SQL is asserted to name only the
    /// 07-12 folder (never 07-11/07-13), and -- the stronger, data-level proof -- the query actually
    /// executed against DuckDB returns only the two rows seeded under 07-12, with the 07-11/07-13 rows
    /// entirely absent. No <see cref="DatasetSpec.WatermarkCursor"/> is set, so <c>AzureWindowSql.Wrap</c>
    /// adds no row-level predicate: the row set is pruned by the folder-level cover alone.</summary>
    [SkippableFact]
    public async Task Native_pruning_reads_only_window_day()
    {
        DockerFacts.SkipUnlessDocker();
        DockerFacts.SkipIfOffline();

        var dbPath = Path.Combine(Path.GetTempPath(), $"pz-azure-e2e-prune-{Guid.NewGuid():N}.duckdb");
        var prefix = $"events-{Guid.NewGuid():N}";
        try
        {
            await using var duck = DuckSession.Open(dbPath);

            await SeedDayAsync(duck, "2026/07/11", "d11", "(select 101 as id, '2026-07-11' as day)", prefix);
            await SeedDayAsync(duck, "2026/07/12", "d12",
                "(select 201 as id, '2026-07-12' as day union all select 202, '2026-07-12')", prefix);
            await SeedDayAsync(duck, "2026/07/13", "d13", "(select 301 as id, '2026-07-13' as day)", prefix);

            var source = new AzureSource(Config(fixture));
            var datasetSpec = new DatasetSpec("lake", "events", new Dictionary<string, object?>
            {
                ["container"] = AzuriteFixture.Container,
                ["path"] = $"{prefix}/{{yyyy}}/{{MM}}/{{dd}}/*.parquet",
                ["format"] = "parquet",
            })
            {
                WatermarkValue = "2026-07-12T00:00:00.000000",
                WatermarkUpperBound = "2026-07-12T23:59:59.000000",
            };

            Assert.True(source.TryGetNativeScan(datasetSpec, out var scan));
            Assert.Contains($"{prefix}/2026/07/12/*.parquet", scan!.SqlFragment, StringComparison.Ordinal);
            Assert.DoesNotContain("2026/07/11", scan.SqlFragment, StringComparison.Ordinal);
            Assert.DoesNotContain("2026/07/13", scan.SqlFragment, StringComparison.Ordinal);

            foreach (var setup in scan.SetupStatements)
            {
                await duck.ExecuteAsync(setup);
            }

            var rowCount = await duck.ScalarAsync<long>($"select count(*) from {scan.SqlFragment}");
            Assert.Equal(2, rowCount);

            var ids = await duck.ScalarAsync<string>(
                $"select string_agg(id::varchar, ',' order by id) from {scan.SqlFragment}");
            Assert.Equal("201,202", ids);

            var distinctDays = await duck.ScalarAsync<long>(
                $"select count(distinct day) from {scan.SqlFragment}");
            Assert.Equal(1, distinctDays);

            var day = await duck.ScalarAsync<string>($"select min(day) from {scan.SqlFragment}");
            Assert.Equal("2026-07-12", day);
        }
        finally
        {
            try { File.Delete(dbPath); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>Writes one day-folder's parquet blob via the sink's native COPY (mirrors the seeding shape
    /// in <see cref="Azure_native_copy_roundtrips_through_azurite"/>) at
    /// <c>{prefix}/{datePath}/{outputName}.parquet</c>.</summary>
    private async Task SeedDayAsync(DuckSession duck, string datePath, string outputName, string selectSql, string prefix)
    {
        var sink = new AzureSink(Config(fixture));
        var outputSpec = new OutputSpec("lake", outputName, "replace", "fail_on_change",
            new Dictionary<string, object?> { ["container"] = AzuriteFixture.Container, ["path"] = $"{prefix}/{datePath}", ["format"] = "parquet" });
        Assert.True(sink.TryGetNativeCopy(outputSpec, out var copy));

        foreach (var setup in copy!.SetupStatements)
        {
            await duck.ExecuteAsync(setup);
        }

        var copySql = copy.CopySql.Replace("{{source}}", selectSql, StringComparison.Ordinal);
        await duck.ExecuteAsync(copySql);
    }
}
