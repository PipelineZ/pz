using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Dispatch;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Engine.Tests.Execution;

/// <summary><see cref="SchemaDriftGate"/> unit tests. Fixture mirrors
/// <c>StagingReuseTests</c> (temp dir, <see cref="DuckSession.Open"/>, real
/// <see cref="SchemaBaselineStore.Local"/>) — the gate is called directly (internal, not through
/// <see cref="SourceLoadExecutor"/>) with a hand-built staging table and success <see cref="NodeResult"/>,
/// exactly the shape both SourceLoad epilogues hand it.</summary>
public sealed class SchemaDriftGateTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-schema-drift-gate-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        var paths = new RunPaths(_dir, "current");
        Directory.CreateDirectory(paths.RunDir);
        _duck = DuckSession.Open(paths.StagingDbPath);
        await _duck.ExecuteAsync("create schema if not exists staging");
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static SourceDatasetDef Def(string sourceName, string datasetName,
        IReadOnlyDictionary<string, string>? columns = null)
    {
        var dataset = new DatasetDef(datasetName, new Dictionary<string, object?>(), columns);
        var source = new ConnectionDef(sourceName, "inmemory", new Dictionary<string, object?>(), [dataset],
            $"sources/{sourceName}.yml");
        return new SourceDatasetDef(source, dataset);
    }

    private static DagNode Node(SourceDatasetDef def, string idHex = "1111111111111111") =>
        new(new NodeId(idHex), NodeKind.SourceLoad, $"src_{def.Source.Name}__{def.Dataset.Name}", [], null, def);

    private static NodeResult SuccessResult(DagNode node, long rows = 3,
        Watermark? watermark = null, SyncState? syncState = null) =>
        new(node.Id, node.Kind, node.Name, NodeStatus.Success, rows, TimeSpan.Zero, null,
            WatermarkCandidate: watermark, SyncStateCandidate: syncState);

    private RunContext Ctx(DriftPolicy policy, SchemaBaselineStore? store, IRunEvents? events = null,
        Action<string>? notice = null) =>
        new(_duck, new ConnectorRegistry(), new RunPaths(_dir, "current"), events ?? NullRunEvents.Instance,
            SchemaBaselines: store, OnSourceDrift: policy, Notice: notice);

    private Task CreateStagingTableAsync(string tableName, string selectList, bool zeroRows = false) =>
        _duck.ExecuteAsync($"create or replace table {tableName} as select {selectList}" +
            (zeroRows ? " where false" : ""));

    /// <summary>Records every <see cref="IRunEvents.SourceDriftDetected"/> call — every other member is
    /// a no-op, mirroring <c>NodeExecutorTests.CountingEvents</c>'s shape.</summary>
    private sealed class RecordingEvents : IRunEvents
    {
        public readonly List<(DagNode Node, string Connection, string Entity, string Policy,
            IReadOnlyList<SchemaDriftDiffer.Change> Changes, IReadOnlyList<SchemaColumn> Observed,
            string HintsHash)> Drifts = [];

        public void RunStarted(string runId, string projectName, int nodeCount) { }
        public void NodeStarted(DagNode node) { }
        public void NodeProgress(DagNode node, long rowsSoFar, long bytesSoFar, long batchesSoFar) { }
        public void RetryScheduled(DagNode node, int attempt, int maxAttempts, TimeSpan delay, string reason) { }
        public void BreakerStateChanged(string instance, string oldState, string newState, string trigger,
            TimeSpan coolDown) { }

        public void SourceDriftDetected(DagNode node, string connection, string entity, string policy,
            IReadOnlyList<SchemaDriftDiffer.Change> changes, IReadOnlyList<SchemaColumn> observed, string hintsHash) =>
            Drifts.Add((node, connection, entity, policy, changes, observed, hintsHash));
        public void MergeKeyDuplicatesDetected(DagNode node, string output, IReadOnlyList<string> keys,
            long duplicateGroups, long extraRows) { }
        public void LossyIntegerInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns) { }
        public void AmbiguousDateInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns, string format) { }

        public void NodeCompleted(NodeResult result) { }
        public void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped, TimeSpan duration) { }
    }

    [Fact]
    public async Task Ignore_policy_never_reads_or_writes_the_baseline_and_leaves_Observed_null()
    {
        var def = Def("crm", "orders");
        var node = Node(def);
        var tableName = StagingNames.ForSourceLoad("crm", "orders");
        await CreateStagingTableAsync(tableName, "1::bigint as id, 'a'::varchar as name");

        var store = SchemaBaselineStore.Local(Path.Combine(_dir, "state"));
        var success = SuccessResult(node);

        var result = await SchemaDriftGate.ApplyAsync(success, node, def, ReadHints.None, tableName,
            Ctx(DriftPolicy.Ignore, store), CancellationToken.None);

        Assert.Same(success, result);
        Assert.Null(result.Observed);
        Assert.Null(store.Get(SchemaBaselineStore.Key("crm", "orders")));
    }

    [Fact]
    public async Task Warn_with_no_baseline_seeds_silently_and_fires_no_event()
    {
        var def = Def("crm", "orders");
        var node = Node(def);
        var tableName = StagingNames.ForSourceLoad("crm", "orders");
        await CreateStagingTableAsync(tableName, "1::bigint as id, 'a'::varchar as name");

        var store = SchemaBaselineStore.Local(Path.Combine(_dir, "state"));
        var events = new RecordingEvents();
        var success = SuccessResult(node);

        var result = await SchemaDriftGate.ApplyAsync(success, node, def, ReadHints.None, tableName,
            Ctx(DriftPolicy.Warn, store, events), CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.NotNull(result.Observed);
        Assert.Equal(["id", "name"], result.Observed!.Columns.Select(c => c.Name).ToArray());
        Assert.Equal(SchemaDriftDiffer.HashHints(ReadHints.None), result.Observed.HintsHash);
        Assert.Empty(events.Drifts);

        var baseline = store.Get(SchemaBaselineStore.Key("crm", "orders"));
        Assert.NotNull(baseline);
        Assert.Equal(result.Observed.Columns, baseline!.Columns);
        Assert.Equal(result.Observed.HintsHash, baseline.HintsHash);
        Assert.Equal("current", baseline.RunId);
    }

    [Fact]
    public async Task Warn_with_matching_baseline_fires_no_event_and_leaves_baseline_untouched()
    {
        var def = Def("crm", "orders");
        var node = Node(def);
        var tableName = StagingNames.ForSourceLoad("crm", "orders");
        await CreateStagingTableAsync(tableName, "1::bigint as id, 'a'::varchar as name");

        var stateDir = Path.Combine(_dir, "state");
        var store = SchemaBaselineStore.Local(stateDir);
        var key = SchemaBaselineStore.Key("crm", "orders");
        var hintsHash = SchemaDriftDiffer.HashHints(ReadHints.None);
        var seeded = new SchemaBaseline(
            [new SchemaColumn("id", "BIGINT"), new SchemaColumn("name", "VARCHAR")], hintsHash, "prior-run");
        store.Set(key, seeded);
        var bytesBefore = File.ReadAllBytes(Path.Combine(stateDir, "schemas.json"));

        var events = new RecordingEvents();
        var success = SuccessResult(node);

        var result = await SchemaDriftGate.ApplyAsync(success, node, def, ReadHints.None, tableName,
            Ctx(DriftPolicy.Warn, store, events), CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.NotNull(result.Observed);
        Assert.Empty(events.Drifts);

        var bytesAfter = File.ReadAllBytes(Path.Combine(stateDir, "schemas.json"));
        Assert.Equal(bytesBefore, bytesAfter);
    }

    [Fact]
    public async Task Warn_with_retyped_column_fires_once_per_run_and_never_updates_the_baseline()
    {
        var def = Def("crm", "orders");
        var node = Node(def);
        var tableName = StagingNames.ForSourceLoad("crm", "orders");
        // Staging landed `id` as BIGINT; the accepted baseline says INTEGER -> a "retyped" change.
        await CreateStagingTableAsync(tableName, "1::bigint as id");

        var store = SchemaBaselineStore.Local(Path.Combine(_dir, "state"));
        var key = SchemaBaselineStore.Key("crm", "orders");
        var hintsHash = SchemaDriftDiffer.HashHints(ReadHints.None);
        store.Set(key, new SchemaBaseline([new SchemaColumn("id", "INTEGER")], hintsHash, "prior-run"));

        var events = new RecordingEvents();
        var success = SuccessResult(node);
        var ctx = Ctx(DriftPolicy.Warn, store, events);

        var result1 = await SchemaDriftGate.ApplyAsync(success, node, def, ReadHints.None, tableName, ctx,
            CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result1.Status);
        var drift1 = Assert.Single(events.Drifts);
        Assert.Equal("warn", drift1.Policy);
        var change1 = Assert.Single(drift1.Changes);
        Assert.Equal("retyped", change1.Kind);
        Assert.Equal("id", change1.Column);
        Assert.Equal("INTEGER", change1.From);
        Assert.Equal("BIGINT", change1.To);

        var baselineAfterFirst = store.Get(key);
        Assert.Equal("INTEGER", Assert.Single(baselineAfterFirst!.Columns).Type);

        // A second identical run: the baseline is still stale, so the gate warns again.
        var result2 = await SchemaDriftGate.ApplyAsync(success, node, def, ReadHints.None, tableName, ctx,
            CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result2.Status);
        Assert.Equal(2, events.Drifts.Count);

        var baselineAfterSecond = store.Get(key);
        Assert.Equal("INTEGER", Assert.Single(baselineAfterSecond!.Columns).Type);
    }

    [Fact]
    public async Task Fail_with_drift_returns_failed_result_with_PZ0331_and_clears_advancement_candidates()
    {
        var def = Def("crm", "orders");
        var node = Node(def);
        var tableName = StagingNames.ForSourceLoad("crm", "orders");
        await CreateStagingTableAsync(tableName, "1::bigint as id");

        var store = SchemaBaselineStore.Local(Path.Combine(_dir, "state"));
        var key = SchemaBaselineStore.Key("crm", "orders");
        var hintsHash = SchemaDriftDiffer.HashHints(ReadHints.None);
        store.Set(key, new SchemaBaseline([new SchemaColumn("id", "INTEGER")], hintsHash, "prior-run"));

        var events = new RecordingEvents();
        var watermark = new Watermark("id", "bigint", "1", "current");
        var syncState = new SyncState("token", "current");
        var success = SuccessResult(node, watermark: watermark, syncState: syncState);

        var result = await SchemaDriftGate.ApplyAsync(success, node, def, ReadHints.None, tableName,
            Ctx(DriftPolicy.Fail, store, events), CancellationToken.None);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Null(result.WatermarkCandidate);
        Assert.Null(result.SyncStateCandidate);
        Assert.NotNull(result.Error);
        Assert.Equal(PzErrorCode.SchemaDrift, result.Error!.Code);
        Assert.Contains("crm", result.Error.Message, StringComparison.Ordinal);
        Assert.Contains("orders", result.Error.Message, StringComparison.Ordinal);
        Assert.Contains("retyped", result.Error.Message, StringComparison.Ordinal);
        Assert.Contains("pz schema accept", result.Error.Hint, StringComparison.Ordinal);

        var drift = Assert.Single(events.Drifts);
        Assert.Equal("fail", drift.Policy);
    }

    [Fact]
    public async Task Hints_hash_change_reseeds_silently_with_no_event()
    {
        var def = Def("crm", "orders");
        var node = Node(def);
        var tableName = StagingNames.ForSourceLoad("crm", "orders");
        await CreateStagingTableAsync(tableName, "1::bigint as id, 'a'::varchar as name");

        var store = SchemaBaselineStore.Local(Path.Combine(_dir, "state"));
        var key = SchemaBaselineStore.Key("crm", "orders");
        // Stored under a stale hints hash (as if the pipeline's projected column set changed) -- even
        // though the columns below would otherwise "drift" (missing "name"), a hash mismatch reseeds
        // silently rather than comparing against a baseline captured under a different read shape.
        store.Set(key, new SchemaBaseline([new SchemaColumn("id", "BIGINT")], "stale-hash", "prior-run"));

        var events = new RecordingEvents();
        var success = SuccessResult(node);
        var hints = ReadHints.None;

        var result = await SchemaDriftGate.ApplyAsync(success, node, def, hints, tableName,
            Ctx(DriftPolicy.Warn, store, events), CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Empty(events.Drifts);

        var reseeded = store.Get(key);
        Assert.NotNull(reseeded);
        Assert.Equal(SchemaDriftDiffer.HashHints(hints), reseeded!.HintsHash);
        Assert.Equal(["id", "name"], reseeded.Columns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public async Task Contract_dataset_is_a_pass_through_even_under_fail_policy()
    {
        var def = Def("crm", "orders", new Dictionary<string, string> { ["id"] = "bigint" });
        var node = Node(def);
        var tableName = StagingNames.ForSourceLoad("crm", "orders");

        var store = SchemaBaselineStore.Local(Path.Combine(_dir, "state"));
        var events = new RecordingEvents();
        var success = SuccessResult(node);

        var result = await SchemaDriftGate.ApplyAsync(success, node, def, ReadHints.None, tableName,
            Ctx(DriftPolicy.Fail, store, events), CancellationToken.None);

        Assert.Same(success, result);
        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Null(result.Observed);
        Assert.Empty(events.Drifts);
        Assert.Null(store.Get(SchemaBaselineStore.Key("crm", "orders")));
    }

    [Fact]
    public async Task Zero_row_staged_table_seeds_and_compares_columns_normally()
    {
        var def = Def("crm", "orders");
        var node = Node(def);
        var tableName = StagingNames.ForSourceLoad("crm", "orders");
        await CreateStagingTableAsync(tableName, "1::bigint as id, 'a'::varchar as name", zeroRows: true);
        Assert.Equal(0L, await _duck.ScalarAsync<long>($"select count(*) from {tableName}", default));

        var store = SchemaBaselineStore.Local(Path.Combine(_dir, "state"));
        var events = new RecordingEvents();
        var success = SuccessResult(node, rows: 0);

        var result = await SchemaDriftGate.ApplyAsync(success, node, def, ReadHints.None, tableName,
            Ctx(DriftPolicy.Warn, store, events), CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.NotNull(result.Observed);
        Assert.Equal(["id", "name"], result.Observed!.Columns.Select(c => c.Name).ToArray());
        Assert.Empty(events.Drifts);

        var baseline = store.Get(SchemaBaselineStore.Key("crm", "orders"));
        Assert.NotNull(baseline);
        Assert.Equal(["id", "name"], baseline!.Columns.Select(c => c.Name).ToArray());
    }
}
