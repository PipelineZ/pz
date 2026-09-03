using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Planning;
using Pz.Engine.Dispatch;
using Pz.Engine.State;

namespace Pz.Engine.Tests.Execution;

/// <summary>Executor-level coverage for the native scan/copy branches: real DuckDB, stub
/// connectors whose universal-path members (GetSchemaAsync/PlanReadAsync/BeginWriteAsync) throw so
/// any accidental fall-through to the universal path fails loudly.</summary>
public sealed class NativePathTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-native-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static DagNode SourceLoadNode(string sourceName, string datasetName)
    {
        var source = new ConnectionDef(sourceName, "stub", new Dictionary<string, object?>(),
            [new DatasetDef(datasetName, new Dictionary<string, object?>(), null)], $"sources/{sourceName}.yml");
        return new DagNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, $"src_{sourceName}__{datasetName}",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    private static DagNode SinkWriteNode(string input)
    {
        var sink = new ConnectionDef("stub", "stub", new Dictionary<string, object?>(), [], "sinks/stub.yml") { Outputs = [new OutputDef("out", input, "replace", "fail_on_change", new Dictionary<string, object?>())] };
        return new DagNode(new NodeId("bbbbbbbbbbbbbbbb"), NodeKind.SinkWrite, "stub.out",
            [], null, new SinkOutputDef(sink, sink.Outputs[0]));
    }

    private static ExecutionPlan SingleNodePlan(DagNode node, EdgeStrategy strategy) =>
        new([new PlannedNode(node.Id, node.Kind, node.Name, strategy, 1, "test")], MemoryBudget.Compute(new EngineConfig()));

    [Fact]
    public async Task Native_scan_lands_rows_without_connector_batches()
    {
        var node = SourceLoadNode("stub", "t");
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new ConfigurableNativeSource("(values (1),(2),(3)) t(x)", []));
        var plan = SingleNodePlan(node, EdgeStrategy.NativeScan);
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "run"), NullRunEvents.Instance, plan);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(3, result.RowsMoved);
        Assert.Equal(3, await _duck.ScalarAsync<long>("select count(*) from staging.src_stub__t"));
    }

    [Fact]
    public async Task Native_setup_statements_run_in_order_before_scan()
    {
        var node = SourceLoadNode("stub", "t");
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new ConfigurableNativeSource(
            "(values (1)) t(x)", ["create schema if not exists s1", "create schema if not exists s2"]));
        var plan = SingleNodePlan(node, EdgeStrategy.NativeScan);
        var recording = new RecordingDuckSession(_duck);
        var ctx = new RunContext(recording, registry, new RunPaths(_dir, "run"), NullRunEvents.Instance, plan);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(3, recording.ExecutedStatements.Count);
        Assert.Equal("create schema if not exists s1", recording.ExecutedStatements[0]);
        Assert.Equal("create schema if not exists s2", recording.ExecutedStatements[1]);
        Assert.Contains("create or replace table staging.src_stub__t as select * from (values (1)) t(x)",
            recording.ExecutedStatements[2]);
    }

    /// <summary>Proves the wiring, not just the ledger in isolation: two nodes of the same run
    /// (a SourceLoad and a SinkWrite, both native-only) reach <see cref="NativeSetup.ExecuteSetupAsync"/>
    /// through the same <see cref="RunContext.SetupLedger"/> rather than each running its own copy of
    /// the loop that used to live in the executors.</summary>
    [Fact]
    public async Task Executors_reach_setup_through_the_ledger()
    {
        const string statement = "create table ledger_reach_marker(x int)";
        await _duck.ExecuteAsync("create table staging.p5 as select 1 as x");

        var sourceNode = SourceLoadNode("src5", "t5");
        var sinkNode = SinkWriteNode("p5");

        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new ConfigurableNativeSource("(values (1)) t(x)", [statement]));
        registry.AddSink("stub", new ConfigurableNativeSink(Path.Combine(_dir, "out5.tmp"), Path.Combine(_dir, "out5.csv"), [statement]));

        var plan = new ExecutionPlan(
            [
                new PlannedNode(sourceNode.Id, sourceNode.Kind, sourceNode.Name, EdgeStrategy.NativeScan, 1, "test"),
                new PlannedNode(sinkNode.Id, sinkNode.Kind, sinkNode.Name, EdgeStrategy.NativeCopy, 1, "test"),
            ],
            MemoryBudget.Compute(new EngineConfig()));

        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "run"), NullRunEvents.Instance, plan);

        var sourceResult = await new KindDispatchingExecutor().ExecuteAsync(sourceNode, ctx, CancellationToken.None);
        var sinkResult = await new KindDispatchingExecutor().ExecuteAsync(sinkNode, ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, sourceResult.Status);
        Assert.Equal(NodeStatus.Success, sinkResult.Status);
        Assert.Equal(1, await _duck.ScalarAsync<long>(
            "select count(*) from duckdb_tables() where table_name = 'ledger_reach_marker'"));
    }

    [Fact]
    public async Task Native_setup_failure_is_PZ0311_with_redacted_statement()
    {
        // "not_a_real_type" is not a recognized DuckDB secret type, so this fails deterministically
        // (bind-time error) without any network access — unlike a real object-store secret type,
        // which could legitimately try to auto-install httpfs first.
        var node = SourceLoadNode("stub", "t");
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new ConfigurableNativeSource(
            "(values (1)) t(x)", ["create secret pz_test (type not_a_real_type, secret 'SECRET_VALUE')"]));
        var plan = SingleNodePlan(node, EdgeStrategy.NativeScan);
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "run"), NullRunEvents.Instance, plan);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Error);
        Assert.Contains("PZ0311", result.Error!.Message);
        Assert.Contains("CREATE SECRET …", result.Error.Message);
        Assert.DoesNotContain("SECRET_VALUE", result.Error.Message);
    }

    /// <summary>A malformed setup statement that ALSO embeds a
    /// secret literal must never echo that secret into the NodeResult error — DuckDB's Parser Error
    /// echoes the whole offending statement (including the secret) in its "LINE 1: ..." context block,
    /// which <see cref="NativeStatementRedactor.SanitizeEngineMessage"/> must strip. This exact
    /// statement shape reproduces with the local duckdb binary: `duckdb -c "create secret pz_x (type s3
    /// secret 'SECRET_VALUE')"` -> "Parser Error: syntax error at or near "secret" ... LINE 1: create
    /// secret pz_x (type s3 secret 'SECRET_VALUE')".</summary>
    [Fact]
    public async Task Parser_error_never_leaks_setup_statement()
    {
        var node = SourceLoadNode("stub", "t");
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new ConfigurableNativeSource(
            "(values (1)) t(x)", ["create secret pz_x (type s3 secret 'SECRET_VALUE')"]));
        var plan = SingleNodePlan(node, EdgeStrategy.NativeScan);
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "run"), NullRunEvents.Instance, plan);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Error);
        Assert.DoesNotContain("SECRET_VALUE", result.Error!.Message);
        Assert.Contains("Parser Error", result.Error.Message);
    }

    /// <summary>A native scan fragment that fails partway through evaluation (here, DuckDB's error()
    /// scalar function firing on the third row) must leave no partial staging table behind — CTAS
    /// ("create or replace table ... as select ...") is transactional, and this pins that
    /// guarantee.</summary>
    [Fact]
    public async Task Native_scan_failure_leaves_no_staging_table()
    {
        var node = SourceLoadNode("stub", "t");
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new ConfigurableNativeSource(
            "(select case when x = 2 then error('boom') else x end as v from range(5) t(x))", []));
        var plan = SingleNodePlan(node, EdgeStrategy.NativeScan);
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "run"), NullRunEvents.Instance, plan);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal(0, await _duck.ScalarAsync<long>(
            "select count(*) from duckdb_tables() where table_name = 'src_stub__t'"));
    }

    [Fact]
    public async Task Native_copy_applies_finalizations_atomically()
    {
        await _duck.ExecuteAsync("create table staging.t as select 1 as x");
        var finalPath = Path.Combine(_dir, "final.parquet");
        var tempPath = Path.Combine(_dir, "temp.parquet");
        Assert.False(File.Exists(finalPath));

        var node = SinkWriteNode("t");
        var registry = new ConnectorRegistry();
        registry.AddSink("stub", new ConfigurableNativeSink(tempPath, finalPath));
        var plan = SingleNodePlan(node, EdgeStrategy.NativeCopy);
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "run"), NullRunEvents.Instance, plan);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(1, result.RowsMoved);
        Assert.True(File.Exists(finalPath));
        Assert.False(File.Exists(tempPath));
    }

    /// <summary>The litter guarantee: if the COPY statement itself
    /// fails, whatever's sitting at the `.pz-native-*` temp path must be deleted best-effort by the
    /// engine — independent of whatever cleanup DuckDB does internally. Simulated here by pre-seeding
    /// the temp path with a stray file and pointing the sink at a CopySql that is guaranteed to fail
    /// (a nonexistent relation) before this test's <see cref="File.Exists"/> assertion runs.</summary>
    [Fact]
    public async Task Native_copy_deletes_temp_file_on_copy_failure()
    {
        await _duck.ExecuteAsync("create table staging.t as select 1 as x");
        var tempPath = Path.Combine(_dir, "leftover.parquet");
        var finalPath = Path.Combine(_dir, "final-never-reached.parquet");
        await File.WriteAllTextAsync(tempPath, "stray litter from a previous, failed attempt");

        var node = SinkWriteNode("t");
        var registry = new ConnectorRegistry();
        registry.AddSink("stub", new FailingCopySink(tempPath, finalPath));
        var plan = SingleNodePlan(node, EdgeStrategy.NativeCopy);
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "run"), NullRunEvents.Instance, plan);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.False(File.Exists(tempPath));
        Assert.False(File.Exists(finalPath));
    }

    /// <summary>The drift gate covers both data-plane tiers
    /// uniformly -- native scan never surfaces Arrow batches to .NET, but it always produces a
    /// staging table, and that materialized table is the gate's only input. Proves the native
    /// branch's success return is wired through <see cref="SchemaDriftGate"/> exactly like the two
    /// universal-tier epilogues: a contract-less dataset under `warn` seeds a baseline from the
    /// native-landed table and attaches <see cref="NodeResult.Observed"/>.</summary>
    [Fact]
    public async Task Native_scan_success_routes_through_the_schema_drift_gate()
    {
        var node = SourceLoadNode("stub", "t");
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new ConfigurableNativeSource("(values (1),(2),(3)) t(x)", []));
        var plan = SingleNodePlan(node, EdgeStrategy.NativeScan);
        var store = SchemaBaselineStore.Local(Path.Combine(_dir, "state"));
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "run"), NullRunEvents.Instance, plan,
            SchemaBaselines: store, OnSourceDrift: DriftPolicy.Warn);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.NotNull(result.Observed);
        Assert.Equal(["x"], result.Observed!.Columns.Select(c => c.Name).ToArray());
        Assert.NotNull(store.Get(SchemaBaselineStore.Key("stub", "t")));
    }

    /// <summary>A native scan that declared
    /// <see cref="NativeScan.SchemaInferred"/> (contract-less csv/json auto-detect) routes its staged
    /// table through <see cref="IntegerInferenceLint"/>, so a DOUBLE column of &gt;2^53 integers —
    /// auto-detect's silently-lossy shape for a &gt;int64 integer column — warns loudly.</summary>
    [Fact]
    public async Task Schema_inferred_native_scan_runs_the_integer_inference_lint()
    {
        var node = SourceLoadNode("stub", "t");
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new ConfigurableNativeSource(
            "(select 12345678901234567890::double as id) t", [], schemaInferred: true));
        var plan = SingleNodePlan(node, EdgeStrategy.NativeScan);
        var events = new LintRecordingEvents();
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "run"), events, plan);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        var lint = Assert.Single(events.Lints);
        Assert.Equal("stub", lint.Connection);
        Assert.Equal("t", lint.Entity);
        Assert.Equal(["id"], lint.Columns);
        Assert.Empty(events.DateLints); // no sniff fragment declared -> ambiguous-date lint skipped
    }

    /// <summary>The lint keys off the connector's own declaration: a native scan that did NOT declare
    /// <see cref="NativeScan.SchemaInferred"/> (database sources, parquet, declared contracts) never
    /// runs it — a Postgres DOUBLE column holding big integral values was already a double at the
    /// source, so warning would be a false positive.</summary>
    [Fact]
    public async Task Non_inferred_native_scan_skips_the_integer_inference_lint()
    {
        var node = SourceLoadNode("stub", "t");
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new ConfigurableNativeSource(
            "(select 12345678901234567890::double as id) t", []));
        var plan = SingleNodePlan(node, EdgeStrategy.NativeScan);
        var events = new LintRecordingEvents();
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "run"), events, plan);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Empty(events.Lints);
    }

    /// <summary>Records every <see cref="IRunEvents.LossyIntegerInferenceDetected"/> call — every
    /// other member is a no-op, mirroring <c>SchemaDriftGateTests.RecordingEvents</c>'s shape.</summary>
    private sealed class LintRecordingEvents : IRunEvents
    {
        public readonly List<(string Connection, string Entity, IReadOnlyList<string> Columns)> Lints = [];

        public void RunStarted(string runId, string projectName, int nodeCount) { }
        public void NodeStarted(DagNode node) { }
        public void NodeProgress(DagNode node, long rowsSoFar, long bytesSoFar, long batchesSoFar) { }
        public void RetryScheduled(DagNode node, int attempt, int maxAttempts, TimeSpan delay, string reason) { }
        public void BreakerStateChanged(string instance, string oldState, string newState, string trigger,
            TimeSpan coolDown) { }
        public void SourceDriftDetected(DagNode node, string connection, string entity, string policy,
            IReadOnlyList<Pz.Engine.State.SchemaDriftDiffer.Change> changes,
            IReadOnlyList<Pz.Engine.State.SchemaColumn> observed, string hintsHash) { }
        public void MergeKeyDuplicatesDetected(DagNode node, string output, IReadOnlyList<string> keys,
            long duplicateGroups, long extraRows) { }
        public void LossyIntegerInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns) => Lints.Add((connection, entity, columns));
        public void AmbiguousDateInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns, string format) => DateLints.Add((connection, entity, columns, format));
        public readonly List<(string Connection, string Entity, IReadOnlyList<string> Columns, string Format)> DateLints = [];
        public void NodeCompleted(NodeResult result) { }
        public void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped, TimeSpan duration) { }
    }

    /// <summary>A schema-inferred native scan that
    /// also carries a sniff fragment routes its staged table through <see cref="AmbiguousDateLint"/> —
    /// an all-ambiguous date column (every day and month &le; 12) warns; the sniff fragment is the
    /// connector's own <c>sniff_csv</c> over the same file the scan read.</summary>
    [Fact]
    public async Task Schema_inferred_native_scan_with_sniff_fragment_runs_the_ambiguous_date_lint()
    {
        var csv = Path.Combine(_dir, "amb.csv");
        await File.WriteAllTextAsync(csv, "when,amount\n01/02/2024,10\n03/04/2024,20\n");
        var escaped = csv.Replace("'", "''");
        var node = SourceLoadNode("stub", "t");
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new ConfigurableNativeSource(
            $"read_csv('{escaped}', header = true, auto_detect = true)", [],
            schemaInferred: true, sniffFragment: $"sniff_csv('{escaped}')"));
        var plan = SingleNodePlan(node, EdgeStrategy.NativeScan);
        var events = new LintRecordingEvents();
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "run"), events, plan);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        var lint = Assert.Single(events.DateLints);
        Assert.Equal(["when"], lint.Columns);
        Assert.StartsWith("%d/%m", lint.Format, StringComparison.Ordinal);
    }

    /// <summary>Default `ignore` policy: the native branch's byte-identical-artifacts guarantee
    /// holds exactly like the two universal-tier epilogues -- no DESCRIBE, no baseline touch,
    /// Observed stays null.</summary>
    [Fact]
    public async Task Native_scan_under_ignore_policy_never_populates_Observed()
    {
        var node = SourceLoadNode("stub", "t");
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new ConfigurableNativeSource("(values (1),(2),(3)) t(x)", []));
        var plan = SingleNodePlan(node, EdgeStrategy.NativeScan);
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "run"), NullRunEvents.Instance, plan);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Null(result.Observed);
    }

    /// <summary>Gate wiring on the universal-tier LEGACY epilogue (SourceLoadExecutor's channel/ingest
    /// branch), the twin of <c>Native_scan_success_routes_through_the_schema_drift_gate</c>.
    /// <see cref="PlainFakeSource"/>/<see cref="GateStubConnector"/> (OperationGateWiringTests' fixture)
    /// declare no <see cref="ConnectorCapabilities.StablePartitionIds"/> and no native scan, so with no
    /// <see cref="ExecutionPlan"/> at all this runs the SAME legacy branch as any ordinary universal-tier
    /// connector in production.</summary>
    [Fact]
    public async Task Universal_legacy_epilogue_success_routes_through_the_schema_drift_gate()
    {
        var node = SourceLoadNode("stub", "t");
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new GateStubConnector(new PlainFakeSource()));
        var store = SchemaBaselineStore.Local(Path.Combine(_dir, "state"));
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "run"), NullRunEvents.Instance,
            SchemaBaselines: store, OnSourceDrift: DriftPolicy.Warn);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.NotNull(result.Observed);
        Assert.Equal(["id"], result.Observed!.Columns.Select(c => c.Name).ToArray());
        Assert.NotNull(store.Get(SchemaBaselineStore.Key("stub", "t")));
    }

    /// <summary>Partition side: proves the universal-tier PARTITION epilogue
    /// (SourceLoadExecutor's <c>ExecutePartitionModeAsync</c>) also routes through the gate. <see cref="ListStubConnector"/>/<see cref="ListStubSource"/>/
    /// <see cref="IdentifiedStubPartition"/> (PartitionModeTests' fixture) declare
    /// <see cref="ConnectorCapabilities.StablePartitionIds"/>, which is exactly what routes a
    /// contract-less dataset through <c>PartitionModeLoader</c> instead of the legacy channel path.
    /// <see cref="NodeResult.Partitions"/> being non-null confirms the partition branch (not the legacy
    /// one) actually ran.</summary>
    [Fact]
    public async Task Universal_partition_epilogue_success_routes_through_the_schema_drift_gate()
    {
        var source = new ConnectionDef("mem", "liststub", new Dictionary<string, object?>(),
            [new DatasetDef("numbers", new Dictionary<string, object?>(), null)], "sources/mem.yml");
        var node = new DagNode(new NodeId("cccccccccccccccc"), NodeKind.SourceLoad, "src_mem__numbers",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
        var registry = new ConnectorRegistry();
        var listSource = new ListStubSource(
            [new IdentifiedStubPartition("a", [1, 2]), new IdentifiedStubPartition("b", [3])]);
        registry.AddSource("liststub",
            new ListStubConnector(listSource, ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds));
        var store = SchemaBaselineStore.Local(Path.Combine(_dir, "state"));
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "run"), NullRunEvents.Instance,
            SchemaBaselines: store, OnSourceDrift: DriftPolicy.Warn);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.NotNull(result.Partitions);
        Assert.NotNull(result.Observed);
        Assert.Equal(["id"], result.Observed!.Columns.Select(c => c.Name).ToArray());
        Assert.NotNull(store.Get(SchemaBaselineStore.Key("mem", "numbers")));
    }

    [Fact]
    public async Task Universal_path_untouched_when_plan_absent()
    {
        var node = SourceLoadNode("dual", "numbers");
        var registry = new ConnectorRegistry();
        registry.AddSource("stub", new DualPathSource());
        // ctx.Plan defaults to null: the executor must fall back to the universal path even though
        // the connector's native scan would otherwise succeed.
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "run"), NullRunEvents.Instance);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(2, result.RowsMoved);
        Assert.Equal(2, await _duck.ScalarAsync<long>("select count(*) from staging.src_dual__numbers"));
    }

    /// <summary>Source whose native scan is fully configurable (fragment + setup statements) and
    /// whose universal-path members throw — any accidental universal fall-through fails loudly.</summary>
    private sealed class ConfigurableNativeSource(string fragment, IReadOnlyList<string> setupStatements,
        bool schemaInferred = false, string? sniffFragment = null)
        : ISourceConnector, ISource
    {
        public ConnectorInfo Info => new("stub-native", "0.1.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.NativeScan;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);
        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));
        public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

        public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
            throw new InvalidOperationException("universal path must not be used");

        public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
        {
            scan = new NativeScan(fragment, setupStatements)
            {
                Mechanism = "stub_scan",
                SchemaInferred = schemaInferred,
                SniffFragment = sniffFragment,
            };
            return true;
        }

        public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
            throw new InvalidOperationException("universal path must not be used");

        public ValueTask DisposeAsync() => default;
    }

    /// <summary>Sink whose native copy is fully configurable (temp/final path, setup statements) and
    /// whose universal write path throws — any accidental universal fall-through fails loudly.</summary>
    private sealed class ConfigurableNativeSink(string tempPath, string finalPath, IReadOnlyList<string>? setupStatements = null)
        : ISinkConnector, ISink
    {
        public ConnectorInfo Info => new("stub-native-sink", "0.1.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.NativeCopy;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);
        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));
        public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

        public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
        {
            copy = new NativeCopy($"copy (select 1) to '{tempPath}'", setupStatements ?? [])
            {
                Mechanism = "stub_copy",
                Finalizations = [new FileMove(tempPath, finalPath)],
            };
            return true;
        }

        public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
            throw new InvalidOperationException("universal path must not be used");

        public ValueTask DisposeAsync() => default;
    }

    /// <summary>Sink whose native copy always resolves to a CopySql that DuckDB is guaranteed to
    /// reject (a nonexistent relation) — used to exercise the engine's post-failure temp-file cleanup
    /// independent of whatever DuckDB itself does on a failed COPY.</summary>
    private sealed class FailingCopySink(string tempPath, string finalPath) : ISinkConnector, ISink
    {
        public ConnectorInfo Info => new("stub-failing-copy-sink", "0.1.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.NativeCopy;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);
        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));
        public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

        public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
        {
            copy = new NativeCopy("copy (select * from staging.__does_not_exist__) to '" + tempPath + "'", [])
            {
                Mechanism = "stub_failing_copy",
                Finalizations = [new FileMove(tempPath, finalPath)],
            };
            return true;
        }

        public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
            throw new InvalidOperationException("universal path must not be used");

        public ValueTask DisposeAsync() => default;
    }

    /// <summary>Source whose native scan WOULD succeed but whose universal-path members are also
    /// real and succeed — proves <see cref="Universal_path_untouched_when_plan_absent"/> exercises the
    /// universal path specifically, not merely "something succeeded".</summary>
    private sealed class DualPathSource : ISourceConnector, ISource
    {
        private static readonly Schema OneColumnSchema = new([new Field("x", Int32Type.Default, nullable: true)], null);

        public ConnectorInfo Info => new("dual", "0.1.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.NativeScan;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);
        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));
        public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

        public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
            new(new DatasetSchema(OneColumnSchema));

        public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
        {
            scan = new NativeScan("(values (1),(2)) t(x)", []) { Mechanism = "stub_scan" };
            return true;
        }

        public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
        {
            IReadOnlyList<IDatasetPartition> partitions = [new OnePartition()];
            return new ValueTask<IReadOnlyList<IDatasetPartition>>(partitions);
        }

        public ValueTask DisposeAsync() => default;

        private sealed class OnePartition : IDatasetPartition
        {
            public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
            {
                var builder = new Int32Array.Builder();
                builder.Append(1);
                builder.Append(2);
                yield return new RecordBatch(OneColumnSchema, [builder.Build()], 2);
                await Task.CompletedTask;
            }
        }
    }

    /// <summary>Wraps a real <see cref="IDuckSession"/>, recording every <see cref="ExecuteAsync"/>
    /// call's SQL in order — used to prove setup statements run before the scan/copy statement.</summary>
    private sealed class RecordingDuckSession(IDuckSession inner) : IDuckSession
    {
        public List<string> ExecutedStatements { get; } = [];

        public async Task ExecuteAsync(string sql, CancellationToken ct = default)
        {
            ExecutedStatements.Add(sql);
            await inner.ExecuteAsync(sql, ct).ConfigureAwait(false);
        }

        public Task<T> ScalarAsync<T>(string sql, CancellationToken ct = default) => inner.ScalarAsync<T>(sql, ct);

        public Task<long> IngestArrowAsync(string targetTable, Schema schema, IAsyncEnumerable<RecordBatch> batches,
            CancellationToken ct = default) => inner.IngestArrowAsync(targetTable, schema, batches, ct);

        public IAsyncEnumerable<RecordBatch> QueryArrowAsync(string sql, int targetBatchBytes = 32 * 1024 * 1024,
            CancellationToken ct = default) => inner.QueryArrowAsync(sql, targetBatchBytes, ct);

        public Task<Schema> GetResultSchemaAsync(string sql, CancellationToken ct = default) =>
            inner.GetResultSchemaAsync(sql, ct);

        public Task CreateEmptyTableAsync(string targetTable, Schema schema, CancellationToken ct = default) =>
            inner.CreateEmptyTableAsync(targetTable, schema, ct);

        public Task<long> AppendArrowBatchAsync(string targetTable, RecordBatch batch, CancellationToken ct = default) =>
            inner.AppendArrowBatchAsync(targetTable, batch, ct);

        public Task ExecuteTransactionAsync(IReadOnlyList<string> statements, CancellationToken ct = default) =>
            inner.ExecuteTransactionAsync(statements, ct);

        public ValueTask DisposeAsync() => default; // the real session is owned/disposed by the test fixture
    }
}
