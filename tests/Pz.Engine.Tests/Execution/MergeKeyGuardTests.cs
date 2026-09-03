using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Planning;

namespace Pz.Engine.Tests.Execution;

/// <summary>A <c>strategy: merge</c> output whose staged input carries a NULL in a declared merge key is
/// refused (PZ0521) before any sink session opens. A merge cannot match a NULL key, so such rows silently
/// collapse within the batch and re-insert (duplicate) every run — the guard turns that silent
/// loss/duplication into a loud failure, mirroring the CDC-deletes PZ0340 guard. The guard sits ahead of
/// both tiers: a native-copy sink's MERGE never joins on NULL either, so the null-key refusal and the
/// duplicate-key warning fire before the COPY statement exactly as they do before a universal session
/// opens. Harness reuses the stub sink and real-staging setup from <see cref="CdcDrainTests"/>.</summary>
public sealed class MergeKeyGuardTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-merge-key-guard-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;

    private const string Canonical = "staging.stg_orders";

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

    private RunContext Context(PlainSinkConnector sink, IRunEvents? events = null) => new(
        _duck, Registry(sink), new RunPaths(_dir, "test-run"), events ?? NullRunEvents.Instance,
        Batch: new BatchOptions(TargetBatchBytes: 256));

    /// <summary>Records every <see cref="IRunEvents.MergeKeyDuplicatesDetected"/> call — every other
    /// member is a no-op, mirroring <c>SchemaDriftGateTests.RecordingEvents</c>.</summary>
    private sealed class RecordingEvents : IRunEvents
    {
        public readonly List<(string Output, IReadOnlyList<string> Keys, long Groups, long ExtraRows)> DuplicateWarnings = [];

        public void MergeKeyDuplicatesDetected(DagNode node, string output, IReadOnlyList<string> keys,
            long duplicateGroups, long extraRows) =>
            DuplicateWarnings.Add((output, keys, duplicateGroups, extraRows));
        public void LossyIntegerInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns) { }
        public void AmbiguousDateInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns, string format) { }

        public void RunStarted(string runId, string projectName, int nodeCount) { }
        public void NodeStarted(DagNode node) { }
        public void NodeProgress(DagNode node, long rowsSoFar, long bytesSoFar, long batchesSoFar) { }
        public void RetryScheduled(DagNode node, int attempt, int maxAttempts, TimeSpan delay, string reason) { }
        public void BreakerStateChanged(string instance, string oldState, string newState, string trigger, TimeSpan coolDown) { }
        public void SourceDriftDetected(DagNode node, string connection, string entity, string policy,
            IReadOnlyList<Pz.Engine.State.SchemaDriftDiffer.Change> changes,
            IReadOnlyList<Pz.Engine.State.SchemaColumn> observed, string hintsHash) { }
        public void NodeCompleted(NodeResult result) { }
        public void RunCompleted(string runId, Pz.Engine.Dispatch.RunStatus status, int succeeded, int failed,
            int skipped, TimeSpan duration) { }
    }

    private static ConnectorRegistry Registry(ISinkConnector sink)
    {
        var reg = new ConnectorRegistry();
        reg.AddSink("cdcdelete", sink);
        return reg;
    }

    private RunContext NativeContext(NativeMergeSink sink, DagNode node, IRunEvents? events = null) => new(
        _duck, Registry(sink), new RunPaths(_dir, "test-run"), events ?? NullRunEvents.Instance,
        Plan: new ExecutionPlan([new PlannedNode(node.Id, node.Kind, node.Name, EdgeStrategy.NativeCopy, 1, "test")],
            MemoryBudget.Compute(new EngineConfig())),
        Batch: new BatchOptions(TargetBatchBytes: 256));

    /// <summary>A native-copy merge sink whose COPY materializes the staged relation into
    /// <c>staging.native_out</c>; the universal path must never be entered.</summary>
    private sealed class NativeMergeSink : ISinkConnector, ISink
    {
        public ConnectorInfo Info => new("cdcdelete", "0.1.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.NativeCopy | ConnectorCapabilities.Merge;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";

        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            new(ValidationResult.Success);
        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            new(new ConnectionCheck(true));
        public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

        public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
        {
            copy = new NativeCopy("create table staging.native_out as select * from {{source}}", []) { Mechanism = "stub_merge" };
            return true;
        }

        public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
            throw new InvalidOperationException("universal path must not be used");

        public ValueTask DisposeAsync() => default;
    }

    // A plain merge (no CDC): OnDelete is null, so the CDC-deletes guard is skipped and this test isolates
    // the merge-upsert guard.
    private static DagNode MergeSinkNode()
    {
        var sink = new ConnectionDef("cap", "cdcdelete", new Dictionary<string, object?>(), [],
            "sinks/cap.yml") { Outputs = [new OutputDef("out", "stg_orders", "merge", "fail_on_change",
                new Dictionary<string, object?>(), ["id"])] };
        var def = new SinkOutputDef(sink, sink.Outputs[0]);
        return new DagNode(new NodeId("ffffffffffffffff"), NodeKind.SinkWrite, "cap.out", [], null, def);
    }

    [Fact]
    public async Task Null_merge_key_value_fails_PZ0521_before_opening_a_session()
    {
        await _duck.ExecuteAsync(
            $"create table {Canonical} as select * from (values (1, 'a'), (CAST(NULL AS BIGINT), 'b')) t(id, name)");
        var connector = new PlainSinkConnector();

        var result = await new SinkWriteExecutor().ExecuteAsync(MergeSinkNode(), Context(connector), default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Error);
        Assert.Equal(PzErrorCode.MergeKeyNull, result.Error!.Code);
        Assert.Contains("id", result.Error.Message);
        Assert.Null(connector.Session); // no session opened — guard ran before BeginWriteAsync
    }

    [Fact]
    public async Task All_non_null_merge_keys_pass_the_guard_and_open_a_session()
    {
        await _duck.ExecuteAsync(
            $"create table {Canonical} as select range as id, 'x' as name from range(3)");
        var connector = new PlainSinkConnector();

        var result = await new SinkWriteExecutor().ExecuteAsync(MergeSinkNode(), Context(connector), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.NotNull(connector.Session); // guard passed — session opened and drained
    }

    // Duplicate merge keys within one staged batch collapse to a single
    // connector-determined survivor (physical staging order, not cursor order — the documented Absorb
    // contract), so a stale row can silently win over a newer one while the watermark still advances
    // past both. The executor must make that loud: a MergeKeyDuplicatesDetected event with the group and
    // extra-row counts — but never fail the node, since event-log-shaped inputs legitimately carry
    // duplicate keys.
    [Fact]
    public async Task Duplicate_merge_keys_publish_a_warning_event_but_do_not_fail()
    {
        await _duck.ExecuteAsync(
            $"create table {Canonical} as select * from (values (1, 'newer'), (1, 'older'), (2, 'only')) t(id, name)");
        var connector = new PlainSinkConnector();
        var events = new RecordingEvents();

        var result = await new SinkWriteExecutor().ExecuteAsync(MergeSinkNode(), Context(connector, events), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.NotNull(connector.Session); // warning only — the write still happens
        var warning = Assert.Single(events.DuplicateWarnings);
        Assert.Equal("out", warning.Output);
        Assert.Equal(["id"], warning.Keys);
        Assert.Equal(1, warning.Groups);
        Assert.Equal(1, warning.ExtraRows);
    }

    [Fact]
    public async Task Unique_merge_keys_publish_no_duplicate_warning()
    {
        await _duck.ExecuteAsync(
            $"create table {Canonical} as select range as id, 'x' as name from range(3)");
        var connector = new PlainSinkConnector();
        var events = new RecordingEvents();

        var result = await new SinkWriteExecutor().ExecuteAsync(MergeSinkNode(), Context(connector, events), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Empty(events.DuplicateWarnings);
    }

    [Fact]
    public async Task Native_copy_null_merge_key_fails_PZ0521_before_the_copy_runs()
    {
        await _duck.ExecuteAsync(
            $"create table {Canonical} as select * from (values (1, 'a'), (CAST(NULL AS BIGINT), 'b')) t(id, name)");
        var node = MergeSinkNode();

        var result = await new SinkWriteExecutor().ExecuteAsync(node, NativeContext(new NativeMergeSink(), node), default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal(PzErrorCode.MergeKeyNull, result.Error!.Code);
        Assert.Equal(0L, await _duck.ScalarAsync<long>(
            "select count(*) from information_schema.tables where table_schema = 'staging' and table_name = 'native_out'"));
    }

    [Fact]
    public async Task Native_copy_duplicate_merge_keys_warn_and_the_copy_still_runs()
    {
        await _duck.ExecuteAsync(
            $"create table {Canonical} as select * from (values (1, 'newer'), (1, 'older'), (2, 'only')) t(id, name)");
        var node = MergeSinkNode();
        var events = new RecordingEvents();

        var result = await new SinkWriteExecutor().ExecuteAsync(node, NativeContext(new NativeMergeSink(), node, events), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(3L, await _duck.ScalarAsync<long>("select count(*) from staging.native_out"));
        var warning = Assert.Single(events.DuplicateWarnings);
        Assert.Equal(1, warning.Groups);
        Assert.Equal(1, warning.ExtraRows);
    }
}
