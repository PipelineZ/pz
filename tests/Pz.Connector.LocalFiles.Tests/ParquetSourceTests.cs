using Apache.Arrow;
using Apache.Arrow.Types;
using Parquet;
using Parquet.Schema;
using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Planning;
using Pz.Engine.State;
using Pz.Engine.Validation;

namespace Pz.Connector.LocalFiles.Tests;

/// <summary>Parquet source via native <c>read_parquet</c> scan. <see cref="ParquetSource"/> is
/// <c>internal</c> (no <c>InternalsVisibleTo</c> to this test assembly, mirroring
/// <see cref="LocalFilesConnectorTests"/>' convention) -- every test here drives it only through the
/// public <see cref="ISourceConnector"/>/<see cref="ISource"/> surface, like the CSV tests.</summary>
public sealed class ParquetSourceTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-parquet-source-tests", Guid.NewGuid().ToString("N"));

    public ParquetSourceTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private ConnectorConfig Config => new(new Dictionary<string, object?> { ["base_dir"] = _dir });

    [Fact]
    public async Task Parquet_source_reports_native_scan_strategy()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("localfiles", new LocalFilesConnector());

        var sourceDef = new ConnectionDef("files", "localfiles", new Dictionary<string, object?>(),
            [new DatasetDef("orders", new Dictionary<string, object?> { ["path"] = "orders.parquet", ["format"] = "parquet" }, null)],
            "sources/files.yml");
        var loadNode = new DagNode(new NodeId("1010101010101010"), NodeKind.SourceLoad, "src_files__orders",
            [], null, new SourceDatasetDef(sourceDef, sourceDef.Datasets[0]));
        var dag = new CompiledDag([loadNode]);

        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: false, CancellationToken.None);

        Assert.Equal(EdgeStrategy.NativeScan, plan.StrategyFor(loadNode.Id));
    }

    [Fact]
    public async Task Parquet_schema_read_from_footer_without_contract()
    {
        var path = Path.Combine(_dir, "matrix.parquet");
        await WriteMatrixParquetAsync(path);

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        // No "columns" key at all: parquet is self-describing, so no declared contract is required
        // for GetSchemaAsync to work.
        var spec = new DatasetSpec("files", "matrix", new Dictionary<string, object?>
        {
            ["path"] = "matrix.parquet",
            ["format"] = "parquet",
        });

        var schema = await source.GetSchemaAsync(spec, CancellationToken.None);

        var byName = schema.Schema.FieldsList.ToDictionary(f => f.Name);
        Assert.Equal(8, schema.Schema.FieldsList.Count);
        Assert.Equal(Int32Type.Default, byName["c_int"].DataType);
        Assert.Equal(Int64Type.Default, byName["c_bigint"].DataType);
        Assert.Equal(DoubleType.Default, byName["c_double"].DataType);
        // Decimal128Type/TimestampType don't override Equals (verified empirically -- same convention
        // ConnectivityValidatorTests/ContractTypes.ArrowTypesEqual already work around), so compare the
        // properties that actually matter instead of the whole type object.
        var dec = Assert.IsType<Decimal128Type>(byName["c_dec"].DataType);
        Assert.Equal(38, dec.Precision);
        Assert.Equal(9, dec.Scale);
        Assert.Equal(StringType.Default, byName["c_varchar"].DataType);
        Assert.Equal(BooleanType.Default, byName["c_bool"].DataType);
        Assert.Equal(Date32Type.Default, byName["c_date"].DataType);
        var ts = Assert.IsType<TimestampType>(byName["c_ts"].DataType);
        Assert.Equal(TimeUnit.Microsecond, ts.Unit);
        Assert.Equal("+00:00", ts.Timezone);
    }

    [Fact]
    public async Task Parquet_schema_read_missing_file_is_permanent_error()
    {
        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "missing", new Dictionary<string, object?>
        {
            ["path"] = "does-not-exist.parquet",
            ["format"] = "parquet",
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.GetSchemaAsync(spec, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("does-not-exist.parquet", ex.Message);
    }

    [Fact]
    public async Task Parquet_native_scan_succeeds_without_columns_contract()
    {
        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "orders", new Dictionary<string, object?>
        {
            ["path"] = "orders.parquet",
            ["format"] = "parquet",
        });

        var ok = source.TryGetNativeScan(spec, out var scan);

        Assert.True(ok);
        Assert.NotNull(scan);
        Assert.Equal("read_parquet", scan!.Mechanism);
        Assert.Contains("read_parquet('", scan.SqlFragment);
        Assert.Contains(Path.Combine(_dir, "orders.parquet"), scan.SqlFragment);
    }

    /// <summary>Same wrapping contract as
    /// <c>LocalFilesConnectorTests.Native_scan_wraps_fragment_with_window_bounds</c>, applied to
    /// <see cref="ParquetSource"/>'s read_parquet(...) fragment.</summary>
    [Fact]
    public async Task Parquet_native_scan_wraps_fragment_with_window_bounds()
    {
        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "orders", new Dictionary<string, object?>
        {
            ["path"] = "orders.parquet",
            ["format"] = "parquet",
        })
        {
            WatermarkCursor = "id",
            WatermarkValue = "10",
            WatermarkUpperBound = "20",
        };

        var ok = source.TryGetNativeScan(spec, out var scan);

        Assert.True(ok);
        Assert.NotNull(scan);
        Assert.StartsWith("(select * from read_parquet(", scan!.SqlFragment);
        Assert.Contains("\"id\" > '10' and \"id\" <= '20'", scan.SqlFragment);
        Assert.EndsWith(")", scan.SqlFragment);
    }

    /// <summary>Byte-identical guarantee (mirrors the CSV counterpart): no watermark fields, or a plain
    /// (unwindowed) lower-bound-only incremental spec, both produce the bare unwrapped fragment.</summary>
    [Fact]
    public async Task Parquet_native_scan_without_upper_bound_is_byte_identical_to_unwindowed()
    {
        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var baseSpec = new DatasetSpec("files", "orders", new Dictionary<string, object?>
        {
            ["path"] = "orders.parquet",
            ["format"] = "parquet",
        });

        Assert.True(source.TryGetNativeScan(baseSpec, out var unwindowedScan));

        var lowerBoundOnlySpec = baseSpec with { WatermarkCursor = "id", WatermarkValue = "10" };
        Assert.True(source.TryGetNativeScan(lowerBoundOnlySpec, out var lowerBoundOnlyScan));

        Assert.Equal(unwindowedScan!.SqlFragment, lowerBoundOnlyScan!.SqlFragment);
        Assert.DoesNotContain("where", unwindowedScan.SqlFragment, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Functional proof via a real DuckDB CTAS, mirroring
    /// <c>LocalFilesConnectorTests.Native_scan_window_bounds_filter_rows_in_duckdb</c>.</summary>
    [Fact]
    public async Task Parquet_native_scan_window_bounds_filter_rows_in_duckdb()
    {
        var path = Path.Combine(_dir, "orders.parquet");
        await WriteIdsParquetAsync(path, Enumerable.Range(1, 30).Select(i => (long)i).ToArray());

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "orders", new Dictionary<string, object?>
        {
            ["path"] = "orders.parquet",
            ["format"] = "parquet",
        })
        {
            WatermarkCursor = "id",
            WatermarkValue = "10",
            WatermarkUpperBound = "20",
        };

        var ok = source.TryGetNativeScan(spec, out var scan);
        Assert.True(ok);

        await using var duck = DuckSession.Open(Path.Combine(_dir, "window.duckdb"));
        await duck.ExecuteAsync("create schema if not exists staging");
        await duck.ExecuteAsync($"create table staging.t as select * from {scan!.SqlFragment}");

        Assert.Equal(10, await duck.ScalarAsync<long>("select count(*) from staging.t"));
        Assert.Equal(11L, await duck.ScalarAsync<long>("select min(id) from staging.t"));
        Assert.Equal(20L, await duck.ScalarAsync<long>("select max(id) from staging.t"));
    }

    [Fact]
    public async Task Parquet_force_universal_errors_native_only()
    {
        var registry = new ConnectorRegistry();
        registry.AddSource("localfiles", new LocalFilesConnector());

        var sourceDef = new ConnectionDef("files", "localfiles", new Dictionary<string, object?>(),
            [new DatasetDef("orders", new Dictionary<string, object?> { ["path"] = "orders.parquet", ["format"] = "parquet" }, null)],
            "sources/files.yml");
        var loadNode = new DagNode(new NodeId("2020202020202020"), NodeKind.SourceLoad, "src_files__orders",
            [], null, new SourceDatasetDef(sourceDef, sourceDef.Datasets[0]));
        var dag = new CompiledDag([loadNode]);

        // Under force_universal, the planner reports the universal (ArrowStream) strategy for every
        // source -- there is no compile-time INativeOnlySink-style marker for a per-dataset native-only
        // SOURCE (unlike sinks, a LocalFiles source's native-only-ness is per-dataset format, not
        // per-connector), so the actual failure is deferred to the universal read path itself.
        var plan = await new ExecutionPlanner(registry).PlanAsync(dag, forceUniversal: true, CancellationToken.None);
        Assert.Equal(EdgeStrategy.ArrowStream, plan.StrategyFor(loadNode.Id));

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);
        var spec = new DatasetSpec("files", "orders", new Dictionary<string, object?>
        {
            ["path"] = "orders.parquet",
            ["format"] = "parquet",
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("PZ0312", ex.Message);
        Assert.Contains("force_universal", ex.Message);
        Assert.Contains("orders", ex.Message);
    }

    [Fact]
    public async Task Parquet_declared_contract_drift_detected_at_connect()
    {
        var path = Path.Combine(_dir, "orders.parquet");
        await WriteSimpleParquetAsync(path); // writes "amount" as a double column

        var registry = new ConnectorRegistry();
        registry.AddSource("localfiles", new LocalFilesConnector());

        var dataset = new DatasetDef("orders", new Dictionary<string, object?>
        {
            ["path"] = "orders.parquet",
            ["format"] = "parquet",
        }, new Dictionary<string, string> { ["amount"] = "int" }); // declared int, footer actually has double

        var sourceDef = new ConnectionDef("files", "localfiles",
            new Dictionary<string, object?> { ["base_dir"] = _dir }, [dataset], "sources/files.yml");

        var project = new PzProject("proj", "0.1.0", new EngineConfig(), new Dictionary<string, object?>(),
            [], [sourceDef], []);

        var result = await ConnectivityValidator.RunAsync(project, registry, CancellationToken.None);

        var error = Assert.Single(result.Errors, e => e.Code == PzErrorCode.SchemaDrift);
        Assert.Contains("amount", error.Message, StringComparison.Ordinal);
    }

    /// <summary>An honest limitation, not a bug: a parquet source's watermark does CAPTURE
    /// and ADVANCES correctly (tier-agnostic capture against the landed staging table), but the native
    /// scan never consults it -- so a second run with an already-advanced watermark still extracts every
    /// row. Mirrors <c>WatermarkFlowTests.Capture_works_on_native_tier_too</c>'s harness shape, but
    /// against the REAL <see cref="LocalFilesConnector"/> instead of a stub, and asserts the row count
    /// specifically to prove there is no pushdown between run 1 and run 2.</summary>
    [Fact]
    public async Task Parquet_incremental_captures_but_does_not_pushdown()
    {
        var path = Path.Combine(_dir, "orders.parquet");
        await WriteIdsParquetAsync(path, [1, 2, 3, 4, 5]);

        var dataset = new DatasetDef("orders", new Dictionary<string, object?>
        {
            ["path"] = "orders.parquet",
            ["format"] = "parquet",
        }, null, new SyncModeDef(SyncMode.Incremental, new IncrementalDef("id")));
        var sourceDef = new ConnectionDef("files", "localfiles", new Dictionary<string, object?> { ["base_dir"] = _dir },
            [dataset], "sources/files.yml");
        var node = new DagNode(new NodeId("3030303030303030"), NodeKind.SourceLoad, "src_files__orders",
            [], null, new SourceDatasetDef(sourceDef, dataset));

        var registry = new ConnectorRegistry();
        registry.AddSource("localfiles", new LocalFilesConnector());

        var plan = new ExecutionPlan(
            [new PlannedNode(node.Id, node.Kind, node.Name, EdgeStrategy.NativeScan, 1, "test")],
            MemoryBudget.Compute(new EngineConfig()));

        var storeDir = Path.Combine(_dir, "state");
        var store = WatermarkStore.Local(storeDir);

        // Run 1: no prior watermark -- full scan, 5 rows, watermark captured at max(id) = 5.
        await using (var duck1 = DuckSession.Open(Path.Combine(_dir, "run1.duckdb")))
        {
            await duck1.ExecuteAsync("create schema if not exists staging");
            var ctx1 = new RunContext(duck1, registry, new RunPaths(_dir, "run1"), NullRunEvents.Instance, plan);
            var result1 = await new KindDispatchingExecutor().ExecuteAsync(node, ctx1, CancellationToken.None);

            Assert.Equal(NodeStatus.Success, result1.Status);
            Assert.Equal(5, result1.RowsMoved);
            Assert.NotNull(result1.WatermarkCandidate);
            Assert.Equal("id", result1.WatermarkCandidate!.Cursor);
            Assert.Equal("5", result1.WatermarkCandidate.Value);

            // Commit-gated advancement, simulated directly (no downstream sink node in this focused
            // test) -- WatermarkFlowTests.Dataset_with_no_sink_advances_on_source_success establishes
            // that a sourceload with no sink advances on source success alone, which is exactly this
            // shape.
            store.Set(WatermarkStore.Key("files", "orders"), result1.WatermarkCandidate);
        }

        // Run 2: a real prior watermark (id > 5) is now available, but the native read_parquet(...)
        // scan never looks at it -- every row is re-extracted anyway (the limitation above).
        await using (var duck2 = DuckSession.Open(Path.Combine(_dir, "run2.duckdb")))
        {
            await duck2.ExecuteAsync("create schema if not exists staging");
            var ctx2 = new RunContext(duck2, registry, new RunPaths(_dir, "run2"), NullRunEvents.Instance,
                plan, Watermarks: store);
            var result2 = await new KindDispatchingExecutor().ExecuteAsync(node, ctx2, CancellationToken.None);

            Assert.Equal(NodeStatus.Success, result2.Status);
            Assert.Equal(5, result2.RowsMoved); // full scan again, NOT zero/reduced -- no pushdown
            Assert.NotNull(result2.WatermarkCandidate);
            Assert.Equal("5", result2.WatermarkCandidate!.Value); // idempotent: same max, re-derived
        }
    }

    private static async Task WriteSimpleParquetAsync(string path)
    {
        var fields = new DataField[] { new DataField("amount", typeof(double?)) };
        var schema = new ParquetSchema(fields);
        await using var stream = new FileStream(path, FileMode.Create, FileAccess.Write);
        await using var writer = await ParquetWriter.CreateAsync(schema, stream);
        using var rowGroup = writer.CreateRowGroup();
        await rowGroup.WriteAsync<double>(fields[0], new double?[] { 10.5, 20.25 }, cancellationToken: default);
    }

    private static async Task WriteIdsParquetAsync(string path, long[] ids)
    {
        var fields = new DataField[] { new DataField("id", typeof(long?)) };
        var schema = new ParquetSchema(fields);
        await using var stream = new FileStream(path, FileMode.Create, FileAccess.Write);
        await using var writer = await ParquetWriter.CreateAsync(schema, stream);
        using var rowGroup = writer.CreateRowGroup();
        await rowGroup.WriteAsync<long>(fields[0], ids.Select(i => (long?)i).ToArray(), cancellationToken: default);
    }

    /// <summary>Covers every v0 type name in <see cref="ParquetTypeMap.ToV0TypeName"/>'s matrix, written
    /// directly via Parquet.Net (not through <c>LocalFilesSink</c>, which refuses decimal128 on write,
    /// so this is the only way to exercise decimal on the read side).</summary>
    private static async Task WriteMatrixParquetAsync(string path)
    {
        var fields = new DataField[]
        {
            new DataField("c_int", typeof(int?)),
            new DataField("c_bigint", typeof(long?)),
            new DataField("c_double", typeof(double?)),
            new DecimalDataField("c_dec", precision: 38, scale: 9, forceByteArrayEncoding: false, isNullable: true),
            new DataField("c_varchar", typeof(string)),
            new DataField("c_bool", typeof(bool?)),
            new DateTimeDataField("c_date", DateTimeFormat.Date, isNullable: true),
            new DateTimeDataField("c_ts", DateTimeFormat.DateAndTime, isAdjustedToUTC: true,
                unit: DateTimeTimeUnit.Micros, isNullable: true),
        };
        var schema = new ParquetSchema(fields);

        await using var stream = new FileStream(path, FileMode.Create, FileAccess.Write);
        await using var writer = await ParquetWriter.CreateAsync(schema, stream);
        using var rowGroup = writer.CreateRowGroup();
        await rowGroup.WriteAsync<int>(fields[0], new int?[] { 1 }, cancellationToken: default);
        await rowGroup.WriteAsync<long>(fields[1], new long?[] { 2L }, cancellationToken: default);
        await rowGroup.WriteAsync<double>(fields[2], new double?[] { 3.5 }, cancellationToken: default);
        await rowGroup.WriteAsync<decimal>(fields[3], new decimal?[] { 12.5m }, cancellationToken: default);
        await rowGroup.WriteAsync(fields[4], new List<string?> { "hello" });
        await rowGroup.WriteAsync<bool>(fields[5], new bool?[] { true }, cancellationToken: default);
        await rowGroup.WriteAsync<DateTime>(fields[6], new DateTime?[] { new DateTime(2026, 1, 15) }, cancellationToken: default);
        await rowGroup.WriteAsync<DateTime>(fields[7], new DateTime?[] { new DateTime(2026, 1, 15, 10, 30, 0, DateTimeKind.Utc) }, cancellationToken: default);
    }
}
