using Pz.Connectors.TestKit.Reference;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Dispatch;

namespace Pz.Engine.Tests.Execution;

public sealed class NodeExecutorTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;
    private InMemoryConnector _mem = null!;
    private RunContext _ctx = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
        _mem = new InMemoryConnector();
        var reg = new ConnectorRegistry();
        reg.AddSource("inmemory", _mem);
        reg.AddSink("inmemory", _mem);
        _ctx = new RunContext(_duck, reg, new RunPaths(_dir, "test-run"), NullRunEvents.Instance);
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    private static DagNode SourceLoadNode(long rows, Dictionary<string, object?>? extra = null)
    {
        var options = new Dictionary<string, object?> { ["rows"] = rows };
        foreach (var (k, v) in extra ?? []) options[k] = v;
        var source = new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(),
            [new DatasetDef("numbers", options, null)], "sources/mem.yml");
        return new DagNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_mem__numbers",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    [Fact]
    public async Task Source_load_lands_all_rows()
    {
        var result = await new KindDispatchingExecutor().ExecuteAsync(SourceLoadNode(1_000), _ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(1_000, result.RowsMoved);
        Assert.Equal(1_000, await _duck.ScalarAsync<long>("select count(*) from staging.src_mem__numbers"));
    }

    [Fact]
    public async Task Pipeline_creates_table_with_transform_applied()
    {
        await new KindDispatchingExecutor().ExecuteAsync(SourceLoadNode(100), _ctx, default);
        var pipeline = new PipelineDef("evens", "select * from staging.src_mem__numbers where flag",
            "table", [], [], "pipelines/evens.sql");
        var node = new DagNode(new NodeId("bbbbbbbbbbbbbbbb"), NodeKind.Pipeline, "evens",
            [new NodeId("aaaaaaaaaaaaaaaa")], pipeline.RawSql, pipeline);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, _ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(50, result.RowsMoved);
        Assert.Equal(50, await _duck.ScalarAsync<long>("select count(*) from staging.evens"));
    }

    /// <summary>Check nodes dispatch through <see cref="KindDispatchingExecutor"/> like every other
    /// kind.</summary>
    [Fact]
    public async Task Check_node_dispatches_to_CheckExecutor()
    {
        await new KindDispatchingExecutor().ExecuteAsync(SourceLoadNode(100), _ctx, default);
        var pipeline = new PipelineDef("evens", "select * from staging.src_mem__numbers where flag",
            "table", [], [], "pipelines/evens.sql");
        var pipelineNode = new DagNode(new NodeId("bbbbbbbbbbbbbbbb"), NodeKind.Pipeline, "evens",
            [new NodeId("aaaaaaaaaaaaaaaa")], pipeline.RawSql, pipeline);
        await new KindDispatchingExecutor().ExecuteAsync(pipelineNode, _ctx, default);

        var check = new CheckDef("row_count", [], new Dictionary<string, object?> { ["min"] = 1L });
        var checkNode = new DagNode(new NodeId("eeeeeeeeeeeeeeee"), NodeKind.Check, "check_evens_row_count",
            [pipelineNode.Id], null, new CheckNodeDef("evens", check));

        var result = await new KindDispatchingExecutor().ExecuteAsync(checkNode, _ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
    }

    private DagNode SinkNode(string input, Dictionary<string, object?>? outputOptions = null)
    {
        var sink = new ConnectionDef("cap", "inmemory", new Dictionary<string, object?>(), [], "sinks/cap.yml") { Outputs = [new OutputDef("out", input, "replace", "fail_on_change", outputOptions ?? [])] };
        return new DagNode(new NodeId("cccccccccccccccc"), NodeKind.SinkWrite, "cap.out",
            [new NodeId("bbbbbbbbbbbbbbbb")], null, new SinkOutputDef(sink, sink.Outputs[0]));
    }

    [Fact]
    public async Task Sink_receives_committed_batches()
    {
        await new KindDispatchingExecutor().ExecuteAsync(SourceLoadNode(200), _ctx, default);

        var result = await new KindDispatchingExecutor().ExecuteAsync(
            SinkNode("src_mem__numbers"), _ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(200, result.RowsMoved);
        var write = Assert.Single(_mem.Committed);
        Assert.Equal(200, write.Result.RowsWritten);
    }

    [Fact]
    public async Task Sink_failure_calls_abort_not_commit()
    {
        await new KindDispatchingExecutor().ExecuteAsync(SourceLoadNode(500), _ctx, default);
        // fail_write_at_batch: use the InMemory sink's existing fault-injection option (read
        // InMemorySink.cs for the exact key) to fail the first written batch.
        var result = await new KindDispatchingExecutor().ExecuteAsync(
            SinkNode("src_mem__numbers", new Dictionary<string, object?> { ["fail_write_at_batch"] = 0 }), _ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Error);
        Assert.Equal("PZ0501", result.Error!.Code);
        Assert.Empty(_mem.Committed);
        Assert.Equal(1, _mem.AbortedSessions);
    }

    [Fact]
    public async Task Consumer_failure_cancels_partition_pump_promptly()
    {
        // Deadlock guard: if IngestArrowAsync fails for a reason unrelated to any partition
        // (simulated here via DuckSession.OnBatchConsumedForTests throwing mid-stream), and nothing
        // cancels the still-running partition producers, they block forever on the bounded(4) channel's
        // WriteAsync (nobody left to drain it) and SourceLoadExecutor's `finally { await pump; }` hangs.
        // Many partitions (each producing exactly one batch, well over the channel's capacity of 4)
        // guarantee several producers are still blocked on WriteAsync when the consumer fails at batch 2.
        var node = SourceLoadNode(50_000, new Dictionary<string, object?> { ["partitions"] = 20 });

        var consumedBatches = 0;
        _duck.OnBatchConsumedForTests = () =>
        {
            if (Interlocked.Increment(ref consumedBatches) == 2)
            {
                throw new InvalidOperationException("injected consumer failure");
            }
        };

        try
        {
            var execute = new KindDispatchingExecutor().ExecuteAsync(node, _ctx, default);
            var winner = await Task.WhenAny(execute, Task.Delay(TimeSpan.FromSeconds(15)));

            Assert.True(ReferenceEquals(winner, execute),
                "executor hung: consumer failure did not cancel the partition pump promptly (this must " +
                "reproduce the deadlock on the pre-fix executor, and must pass post-fix)");

            var result = await execute;
            Assert.Equal(NodeStatus.Failed, result.Status);
            Assert.NotNull(result.Error);
            Assert.Equal("PZ0501", result.Error!.Code);
        }
        finally
        {
            _duck.OnBatchConsumedForTests = null;
        }
    }

    [Fact]
    public async Task Partition_failure_cancels_healthy_sibling_promptly()
    {
        // Sibling fail-fast must be real, not illusory: the tiny (1-row) partition fails at its own first
        // batch almost instantly; the huge (2,000,000-row) sibling is still deep in its read loop at that
        // point. If cancellation only happened after Task.WhenAll observed both tasks (too late by
        // definition), the healthy sibling would run to completion — 2,000,000 rows read. The
        // rows_read_hook lets us see it was cut off far short of that.
        var maxRowsSeenBySibling = 0L;
        var options = new Dictionary<string, object?>
        {
            ["partition_sizes"] = new long[] { 1L, 2_000_000L },
            ["fail_read_at_batch"] = 0,
            ["rows_read_hook"] = (Action<long>)(n =>
            {
                long current;
                do { current = Volatile.Read(ref maxRowsSeenBySibling); }
                while (n > current && Interlocked.CompareExchange(ref maxRowsSeenBySibling, n, current) != current);
            }),
        };
        var node = SourceLoadNode(0, options);

        var execute = new KindDispatchingExecutor().ExecuteAsync(node, _ctx, default);
        var winner = await Task.WhenAny(execute, Task.Delay(TimeSpan.FromSeconds(15)));
        Assert.True(ReferenceEquals(winner, execute), "executor hung waiting for the partition pump");

        var result = await execute;
        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal("PZ0501", result.Error!.Code);
        Assert.True(maxRowsSeenBySibling < 1_000_000,
            $"expected the healthy sibling to be cancelled well before finishing 2,000,000 rows, " +
            $"but it read {maxRowsSeenBySibling} — sibling cancellation is not prompt");
    }

    [Fact]
    public async Task Commit_failure_does_not_trigger_abort()
    {
        await new KindDispatchingExecutor().ExecuteAsync(SourceLoadNode(300), _ctx, default);

        var result = await new KindDispatchingExecutor().ExecuteAsync(
            SinkNode("src_mem__numbers", new Dictionary<string, object?> { ["fail_commit"] = true }), _ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Error);
        Assert.Equal("PZ0501", result.Error!.Code);
        Assert.Empty(_mem.Committed);
        Assert.Equal(0, _mem.AbortedSessions);
        Assert.Equal(1, _mem.CommitAttempts);
    }

    [Fact]
    public async Task Executor_reports_rows_and_duration()
    {
        var result = await new KindDispatchingExecutor().ExecuteAsync(SourceLoadNode(1_000), _ctx, default);
        Assert.True(result.Duration > TimeSpan.Zero);
        Assert.Equal(1_000, result.RowsMoved);
    }

    [Fact]
    public async Task Missing_connector_fails_with_PZ0305()
    {
        var source = new ConnectionDef("pg", "postgres", new Dictionary<string, object?>(),
            [new DatasetDef("t", new Dictionary<string, object?>(), null)], "sources/pg.yml");
        var node = new DagNode(new NodeId("dddddddddddddddd"), NodeKind.SourceLoad, "src_pg__t",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, _ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal("PZ0305", result.Error!.Code);
        Assert.Contains("postgres", result.Error.Message);
    }

    [Fact]
    public void StagingNames_ForSinkInput_maps_bare_pipeline_name_to_its_staging_relation()
    {
        Assert.Equal("staging.evens", StagingNames.ForSinkInput("evens"));
    }

    private sealed class CountingEvents : IRunEvents
    {
        private int _completedCount;
        public int CompletedCount => Volatile.Read(ref _completedCount);
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
            IReadOnlyList<string> columns) { }
        public void AmbiguousDateInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns, string format) { }
        public void NodeCompleted(NodeResult result) => Interlocked.Increment(ref _completedCount);
        public void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped, TimeSpan duration) { }
    }

    /// <summary>RunOrchestrator is the ONLY publisher of NodeCompleted —
    /// KindDispatchingExecutor must not also fire it, or every node's completion would be reported
    /// twice to IRunEvents subscribers.</summary>
    [Fact]
    public async Task Orchestrator_is_the_single_NodeCompleted_publisher()
    {
        var recorder = new CountingEvents();
        var recordingCtx = _ctx with { Events = recorder };
        var dag = new CompiledDag([SourceLoadNode(50)]);

        var result = await new RunOrchestrator(new KindDispatchingExecutor(), recordingCtx)
            .ExecuteAsync(dag, new RunOptions(), default);

        Assert.Equal(NodeStatus.Success, Assert.Single(result.Nodes).Status);
        Assert.Equal(1, recorder.CompletedCount);
    }
}
