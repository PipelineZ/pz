using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit.Reference;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Planning;
using Pz.Engine.Dispatch;
using Pz.Engine.State;

namespace Pz.Engine.Tests.State;

/// <summary>End-to-end coverage of the engine watermark flow: capture, the read-side watermark
/// overload, the InMemory delta lever, and the commit-gated advancement rule -- all driven through
/// the real dispatcher/executor stack
/// (<see cref="RunOrchestrator"/>/<see cref="KindDispatchingExecutor"/>) plus a real <see cref="DuckSession"/>,
/// exactly as `pz run` would, but without needing the CLI/project-loading layer.</summary>
public sealed class WatermarkFlowTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-watermark-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;
    private InMemoryConnector _mem = null!;
    private ConnectorRegistry _registry = null!;
    private WatermarkStore _store = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
        _mem = new InMemoryConnector();
        _registry = new ConnectorRegistry();
        _registry.AddSource("inmemory", _mem);
        _registry.AddSink("inmemory", _mem);
        _store = WatermarkStore.Local(Path.Combine(_dir, "state"));
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private RunContext Ctx(bool fullRefresh = false) =>
        new(_duck, _registry, new RunPaths(_dir, "test-run"), NullRunEvents.Instance,
            Watermarks: _store, FullRefresh: fullRefresh);

    /// <summary>A fresh, uniquely-named staging DuckDB session + matching <see cref="RunContext"/> --
    /// mirrors what a real `pz run`/`pz retry` opens each time (a brand-new <c>staging.duckdb</c> per
    /// run id). Needed only by tests that simulate two sequential "runs" over the SAME source/sink
    /// pair: reusing one DuckDB session across two runs would hit "table already exists" on the second
    /// CREATE TABLE, which is a test-fixture artifact, not the behavior under test.</summary>
    private async Task<(DuckSession Duck, RunContext Ctx)> NewRunAsync(string runId, bool fullRefresh = false)
    {
        var duck = DuckSession.Open(Path.Combine(_dir, $"{runId}.duckdb"));
        await duck.ExecuteAsync("create schema if not exists staging");
        var ctx = new RunContext(duck, _registry, new RunPaths(_dir, runId), NullRunEvents.Instance,
            Watermarks: _store, FullRefresh: fullRefresh);
        return (duck, ctx);
    }

    private static DagNode SourceLoadNode(
        NodeId id, string sourceName, string datasetName, long rows, string cursor,
        Dictionary<string, object?>? extraOptions = null,
        IncrementalDef? incremental = null,
        IReadOnlyDictionary<string, string>? columns = null)
    {
        var options = new Dictionary<string, object?> { ["rows"] = rows };
        foreach (var (k, v) in extraOptions ?? [])
        {
            options[k] = v;
        }

        var dataset = new DatasetDef(datasetName, options, columns,
            new SyncModeDef(SyncMode.Incremental, incremental ?? new IncrementalDef(cursor)));
        var source = new ConnectionDef(sourceName, "inmemory", new Dictionary<string, object?>(), [dataset], $"sources/{sourceName}.yml");
        return new DagNode(id, NodeKind.SourceLoad, $"src_{sourceName}__{datasetName}", [], null, new SourceDatasetDef(source, dataset));
    }

    /// <summary>Shorthand for the windowed dataset shape used throughout: <c>columns:
    /// {id: bigint}</c> + <c>incremental: {cursor: id, max_window, initial, until}</c>. DagCompiler
    /// canonicalizes Initial/Until for a real compiled DAG; these tests build defs by hand, so the
    /// literals passed here must already be canonical (plain digit strings for a bigint cursor).</summary>
    private static DagNode WindowedSourceLoadNode(
        NodeId id, string sourceName, string datasetName, long rows, string maxWindow, string? initial, string? until = null) =>
        SourceLoadNode(id, sourceName, datasetName, rows, "id",
            incremental: new IncrementalDef("id", maxWindow, initial, until),
            columns: new Dictionary<string, string> { ["id"] = "bigint" });

    private static DagNode SinkNode(NodeId id, NodeId dependsOn, string input, Dictionary<string, object?>? outputOptions = null,
        string sinkName = "cap", string outputName = "out")
    {
        var sink = new ConnectionDef(sinkName, "inmemory", new Dictionary<string, object?>(), [], $"sinks/{sinkName}.yml") { Outputs = [new OutputDef(outputName, input, "replace", "fail_on_change", outputOptions ?? [])] };
        return new DagNode(id, NodeKind.SinkWrite, $"{sinkName}.{outputName}", [dependsOn], null, new SinkOutputDef(sink, sink.Outputs[0]));
    }

    [Fact]
    public async Task Watermark_advances_only_when_all_downstream_sinks_commit()
    {
        var sourceId = new NodeId("aaaaaaaaaaaaaaaa");
        var sinkId = new NodeId("bbbbbbbbbbbbbbbb");
        var key = WatermarkStore.Key("mem", "numbers");

        // Run 1: sink fails commit -- the source succeeds and captures a candidate, but advancement
        // must not touch the store because a downstream (effective-set) sink did not succeed.
        var dag1 = new CompiledDag([
            SourceLoadNode(sourceId, "mem", "numbers", 10, "id"),
            SinkNode(sinkId, sourceId, "src_mem__numbers", new Dictionary<string, object?> { ["fail_commit"] = true }),
        ]);
        var (duck1, ctx1) = await NewRunAsync("run-1");
        await using (duck1)
        {
            var result1 = await new RunOrchestrator(new KindDispatchingExecutor(), ctx1).ExecuteAsync(dag1, new RunOptions(), default);
            Assert.Equal(RunStatus.CompletedWithFailures, result1.Status);
            WatermarkAdvancement.Advance(dag1, result1.Nodes, _store);
        }

        Assert.Null(_store.Get(key));

        // Run 2: same DAG shape (a fresh staging session, exactly as a real second `pz run` would open),
        // sink now commits -- advancement must apply the captured candidate.
        var dag2 = new CompiledDag([
            SourceLoadNode(sourceId, "mem", "numbers", 10, "id"),
            SinkNode(sinkId, sourceId, "src_mem__numbers"),
        ]);
        var (duck2, ctx2) = await NewRunAsync("run-2");
        await using (duck2)
        {
            var result2 = await new RunOrchestrator(new KindDispatchingExecutor(), ctx2).ExecuteAsync(dag2, new RunOptions(), default);
            Assert.Equal(RunStatus.Success, result2.Status);
            WatermarkAdvancement.Advance(dag2, result2.Nodes, _store);
        }

        var stored = _store.Get(key);
        Assert.NotNull(stored);
        Assert.Equal("9", stored!.Value); // ids 0..9, max = 9
        Assert.Equal("id", stored.Cursor);
    }

    [Fact]
    public async Task Watermark_does_not_advance_when_a_structural_sink_is_deselected()
    {
        var sourceId = new NodeId("1010101010101010");
        var sinkAId = new NodeId("2020202020202020");
        var sinkBId = new NodeId("3030303030303030");
        var key = WatermarkStore.Key("mem", "fanout");

        var sourceNode = SourceLoadNode(sourceId, "mem", "fanout", 10, "id");
        // Structurally, "fanout" fans out to TWO sinks -- exactly the fan-out shape from the bug report
        // (orders(incremental) -> sink_A and -> sink_B).
        var sinkANode = SinkNode(sinkAId, sourceId, "src_mem__fanout", sinkName: "cap_a", outputName: "out_a");
        var sinkBNode = SinkNode(sinkBId, sourceId, "src_mem__fanout", sinkName: "cap_b", outputName: "out_b");

        // fullDag: the real, structural project DAG (what RunCommand.ExecuteRun passes to
        // WatermarkAdvancement) -- the source's descendants include BOTH sinks.
        var fullDag = new CompiledDag([sourceNode, sinkANode, sinkBNode]);

        // The effective/selected dag a `pz run --select sink_a` would actually execute: source + sink_a
        // only. sink_b is a structural descendant of the source but was never selected, so this run
        // produces no NodeResult for it at all.
        var selectedDag = new CompiledDag([sourceNode, sinkANode]);
        var result = await new RunOrchestrator(new KindDispatchingExecutor(), Ctx()).ExecuteAsync(selectedDag, new RunOptions(), default);
        Assert.Equal(RunStatus.Success, result.Status);

        // Mirrors RunCommand.ExecuteRun exactly: pass the FULL structural dag (not the selected subset)
        // together with this run's (partial) effective results.
        WatermarkAdvancement.Advance(fullDag, result.Nodes, _store);

        // sink_b -- a structural sink descendant that never ran this run -- must block advancement, else
        // the next full run permanently misses the delta sink_b never received.
        Assert.Null(_store.Get(key));
    }

    // -- Carry-forward soundness is enforced at ADVANCEMENT time. The
    // planner only seeds a sink as carried-forward when every SourceLoad ancestor WILL be reused, but
    // SourceLoadExecutor.TryReuseAsync can fall back to re-extraction at execution time (attach failure,
    // missing table, row-count mismatch) -- returning a normally-executed result (Provenance == null)
    // that may capture a NEWER, higher candidate than the slice the carried sink already committed.
    // WatermarkAdvancement must NOT advance that dataset, or the carried sink permanently misses the
    // delta between the old committed slice and the new candidate. These build NodeResults directly
    // (not through the dispatcher) because the interaction is a pure function of provenance + candidate.

    [Fact]
    public void Carried_forward_sink_blocks_advancement_when_its_source_fell_back()
    {
        var sourceId = new NodeId("f1f1f1f1f1f1f1f1");
        var sinkId = new NodeId("f2f2f2f2f2f2f2f2");
        var key = WatermarkStore.Key("mem", "fellback");

        var dag = new CompiledDag([
            SourceLoadNode(sourceId, "mem", "fellback", 10, "id"),
            SinkNode(sinkId, sourceId, "src_mem__fellback"),
        ]);

        // SourceLoad fell back to re-extraction (Provenance == null, not Reused) and captured a NEW,
        // higher candidate than the slice the carried sink recorded.
        var sourceResult = new NodeResult(sourceId, NodeKind.SourceLoad, "src_mem__fellback",
            NodeStatus.Success, 10, TimeSpan.Zero, null,
            WatermarkCandidate: new Watermark("id", "bigint", "99", "retry-run"));
        // The carried-forward sink: Success recorded from the PRIOR slice, never actually ran this retry.
        var sinkResult = new NodeResult(sinkId, NodeKind.SinkWrite, "cap.out",
            NodeStatus.Success, 5, TimeSpan.Zero, null, Provenance: NodeProvenance.CarriedForward);

        WatermarkAdvancement.Advance(dag, [sourceResult, sinkResult], _store);

        // The carried sink vouches only for the prior slice, which re-extraction did not reproduce.
        Assert.Null(_store.Get(key));
    }

    [Fact]
    public void Reused_source_with_carried_forward_sink_advances()
    {
        var sourceId = new NodeId("f3f3f3f3f3f3f3f3");
        var sinkId = new NodeId("f4f4f4f4f4f4f4f4");
        var key = WatermarkStore.Key("mem", "reusedok");

        var dag = new CompiledDag([
            SourceLoadNode(sourceId, "mem", "reusedok", 10, "id"),
            SinkNode(sinkId, sourceId, "src_mem__reusedok"),
        ]);

        // Counterpart: the SourceLoad genuinely reused the prior slice (Provenance == Reused), so the
        // carried sink's recorded success DOES vouch for exactly this run's slice -> advancement is sound.
        var sourceResult = new NodeResult(sourceId, NodeKind.SourceLoad, "src_mem__reusedok",
            NodeStatus.Success, 10, TimeSpan.Zero, null,
            WatermarkCandidate: new Watermark("id", "bigint", "99", "retry-run"),
            Provenance: NodeProvenance.Reused);
        var sinkResult = new NodeResult(sinkId, NodeKind.SinkWrite, "cap.out",
            NodeStatus.Success, 5, TimeSpan.Zero, null, Provenance: NodeProvenance.CarriedForward);

        WatermarkAdvancement.Advance(dag, [sourceResult, sinkResult], _store);

        Assert.Equal("99", _store.Get(key)!.Value);
    }

    [Fact]
    public void Reused_A_advances_but_fallenback_B_with_its_own_carried_sink_is_blocked()
    {
        var srcAId = new NodeId("aaaaaaaa11111111");
        var sinkAId = new NodeId("aaaaaaaa22222222");
        var srcBId = new NodeId("bbbbbbbb11111111");
        var sinkBId = new NodeId("bbbbbbbb22222222");
        var keyA = WatermarkStore.Key("mem", "dsa");
        var keyB = WatermarkStore.Key("mem", "dsb");

        var dag = new CompiledDag([
            SourceLoadNode(srcAId, "mem", "dsa", 10, "id"),
            SinkNode(sinkAId, srcAId, "src_mem__dsa"),
            SourceLoadNode(srcBId, "mem", "dsb", 10, "id"),
            SinkNode(sinkBId, srcBId, "src_mem__dsb"),
        ]);

        // A: genuinely reused -> its carried sink is sound -> advance. B: fell back (Provenance null) ->
        // its own carried sink only vouches for the prior slice -> blocked. Per-source: A is unaffected.
        var results = new List<NodeResult>
        {
            new(srcAId, NodeKind.SourceLoad, "src_mem__dsa", NodeStatus.Success, 10, TimeSpan.Zero, null,
                WatermarkCandidate: new Watermark("id", "bigint", "50", "retry-run"),
                Provenance: NodeProvenance.Reused),
            new(sinkAId, NodeKind.SinkWrite, "capa.out", NodeStatus.Success, 5, TimeSpan.Zero, null,
                Provenance: NodeProvenance.CarriedForward),
            new(srcBId, NodeKind.SourceLoad, "src_mem__dsb", NodeStatus.Success, 10, TimeSpan.Zero, null,
                WatermarkCandidate: new Watermark("id", "bigint", "77", "retry-run")),
            new(sinkBId, NodeKind.SinkWrite, "capb.out", NodeStatus.Success, 5, TimeSpan.Zero, null,
                Provenance: NodeProvenance.CarriedForward),
        };

        WatermarkAdvancement.Advance(dag, results, _store);

        Assert.Equal("50", _store.Get(keyA)!.Value); // reused-A: advances
        Assert.Null(_store.Get(keyB));               // fallen-back-B: blocked
    }

    [Fact]
    public async Task Dataset_with_no_sink_advances_on_source_success()
    {
        var sourceId = new NodeId("cccccccccccccccc");
        var key = WatermarkStore.Key("mem", "solo");
        var dag = new CompiledDag([SourceLoadNode(sourceId, "mem", "solo", 5, "id")]);

        var result = await new RunOrchestrator(new KindDispatchingExecutor(), Ctx()).ExecuteAsync(dag, new RunOptions(), default);
        Assert.Equal(RunStatus.Success, result.Status);

        WatermarkAdvancement.Advance(dag, result.Nodes, _store);

        var stored = _store.Get(key);
        Assert.NotNull(stored);
        Assert.Equal("4", stored!.Value); // ids 0..4, max = 4
    }

    [Fact]
    public async Task Empty_delta_keeps_previous_watermark()
    {
        var sourceId = new NodeId("dddddddddddddddd");
        var key = WatermarkStore.Key("mem", "empties");
        var previous = new Watermark("id", "bigint", "9", "prior-run");
        _store.Set(key, previous);

        // Decision 10 lever: the stored watermark (9) is >= every id InMemorySource would produce for
        // rows=10 (ids 0..9), so every row is filtered out -- an empty extract, NULL staging MAX.
        var dag = new CompiledDag([SourceLoadNode(sourceId, "mem", "empties", 10, "id")]);
        var result = await new RunOrchestrator(new KindDispatchingExecutor(), Ctx()).ExecuteAsync(dag, new RunOptions(), default);

        Assert.Equal(RunStatus.Success, result.Status);
        var sourceResult = Assert.Single(result.Nodes);
        Assert.Equal(0, sourceResult.RowsMoved);
        Assert.Null(sourceResult.WatermarkCandidate);

        WatermarkAdvancement.Advance(dag, result.Nodes, _store);
        Assert.Equal(previous, _store.Get(key));
    }

    [Fact]
    public async Task Full_refresh_ignores_stored_watermark_but_reestablishes()
    {
        var sourceId = new NodeId("eeeeeeeeeeeeeeee");
        var key = WatermarkStore.Key("mem", "refreshed");
        // Would filter out every row (ids 0..9) if the read side honored it -- proving --full-refresh
        // truly ignores the stored watermark rather than merely happening to produce the same result.
        _store.Set(key, new Watermark("id", "bigint", "9", "prior-run"));

        var dag = new CompiledDag([SourceLoadNode(sourceId, "mem", "refreshed", 10, "id")]);
        var result = await new RunOrchestrator(new KindDispatchingExecutor(), Ctx(fullRefresh: true))
            .ExecuteAsync(dag, new RunOptions(), default);

        Assert.Equal(RunStatus.Success, result.Status);
        var sourceResult = Assert.Single(result.Nodes);
        Assert.Equal(10, sourceResult.RowsMoved); // full extract, not filtered by the stale watermark
        Assert.NotNull(sourceResult.WatermarkCandidate);

        WatermarkAdvancement.Advance(dag, result.Nodes, _store);
        var stored = _store.Get(key);
        Assert.NotNull(stored);
        Assert.Equal("9", stored!.Value); // re-established from the full extract's own max
    }

    [Fact]
    public async Task Retry_reextracts_from_unchanged_watermark()
    {
        var sourceId = new NodeId("aaaaaaaabbbbbbbb");
        var sinkId = new NodeId("bbbbbbbbaaaaaaaa");
        var key = WatermarkStore.Key("mem", "retried");

        // "Run 1": sink fails, so the watermark is never advanced.
        var dag1 = new CompiledDag([
            SourceLoadNode(sourceId, "mem", "retried", 100, "id"),
            SinkNode(sinkId, sourceId, "src_mem__retried", new Dictionary<string, object?> { ["fail_commit"] = true }),
        ]);
        var (duck1, ctx1) = await NewRunAsync("run-1");
        await using (duck1)
        {
            var result1 = await new RunOrchestrator(new KindDispatchingExecutor(), ctx1).ExecuteAsync(dag1, new RunOptions(), default);
            Assert.Equal(RunStatus.CompletedWithFailures, result1.Status);
            var firstAttempt = Assert.Single(result1.Nodes, n => n.Kind == NodeKind.SourceLoad);
            Assert.Equal(100, firstAttempt.RowsMoved);
            WatermarkAdvancement.Advance(dag1, result1.Nodes, _store);
        }

        Assert.Null(_store.Get(key));

        // "pz retry": a fresh staging session + RunContext (same store instance, same as a new process
        // would open) reads the SAME (still-absent) watermark and re-extracts the identical full range --
        // not some incrementally-advanced slice -- because nothing was ever committed to the store.
        var dag2 = new CompiledDag([
            SourceLoadNode(sourceId, "mem", "retried", 100, "id"),
            SinkNode(sinkId, sourceId, "src_mem__retried"),
        ]);
        var (duck2, ctx2) = await NewRunAsync("run-2");
        await using (duck2)
        {
            var result2 = await new RunOrchestrator(new KindDispatchingExecutor(), ctx2).ExecuteAsync(dag2, new RunOptions(), default);
            Assert.Equal(RunStatus.Success, result2.Status);
            var retryAttempt = Assert.Single(result2.Nodes, n => n.Kind == NodeKind.SourceLoad);
            Assert.Equal(100, retryAttempt.RowsMoved); // identical to firstAttempt -- proves re-extraction, not resumption
            WatermarkAdvancement.Advance(dag2, result2.Nodes, _store);
        }

        var stored = _store.Get(key);
        Assert.NotNull(stored);
        Assert.Equal("99", stored!.Value);
    }

    [Fact]
    public async Task Cancellation_leaves_watermark_unchanged()
    {
        var sourceId = new NodeId("ffffffffffffffff");
        var key = WatermarkStore.Key("mem", "cancelled");
        var previous = new Watermark("id", "bigint", "1", "prior-run");
        _store.Set(key, previous);

        using var cts = new CancellationTokenSource();
        // Gate-based (no wall-clock sleep): the hook fires synchronously per row read, so cancelling on
        // the 10th of 1_000 rows deterministically lands mid-extraction, well before natural completion.
        var options = new Dictionary<string, object?>
        {
            ["rows_read_hook"] = (Action<long>)(n =>
            {
                if (n == 10)
                {
                    cts.Cancel();
                }
            }),
        };
        var dag = new CompiledDag([SourceLoadNode(sourceId, "mem", "cancelled", 1_000, "id", options)]);

        var result = await new RunOrchestrator(new KindDispatchingExecutor(), Ctx()).ExecuteAsync(dag, new RunOptions(), cts.Token);

        Assert.Equal(RunStatus.Fatal, result.Status);
        var sourceResult = Assert.Single(result.Nodes);
        Assert.Equal(NodeStatus.Skipped, sourceResult.Status);
        Assert.Null(sourceResult.WatermarkCandidate);

        WatermarkAdvancement.Advance(dag, result.Nodes, _store);
        Assert.Equal(previous, _store.Get(key));
    }

    [Fact]
    public async Task Capture_works_on_native_tier_too()
    {
        // Native tier (CTAS branch), not the universal Arrow-channel path -- proves capture is
        // tier-agnostic (both call the same CaptureWatermarkAsync helper against the staging table).
        var node = SourceLoadNode(new NodeId("1111111111111111"), "files", "orders", 0, "id");
        var registry = new ConnectorRegistry();
        registry.AddSource("inmemory", new ConfigurableNativeSource(
            "(values (cast(1 as bigint),'a'), (cast(2 as bigint),'b'), (cast(5 as bigint),'c')) t(id, name)"));
        var plan = new ExecutionPlan(
            [new PlannedNode(node.Id, node.Kind, node.Name, EdgeStrategy.NativeScan, 1, "test")],
            MemoryBudget.Compute(new Pz.Core.Model.EngineConfig()));
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "native-run"), NullRunEvents.Instance, plan);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(3, result.RowsMoved);
        Assert.NotNull(result.WatermarkCandidate);
        Assert.Equal("id", result.WatermarkCandidate!.Cursor);
        Assert.Equal("bigint", result.WatermarkCandidate.TypeName);
        Assert.Equal("5", result.WatermarkCandidate.Value);
        Assert.Equal("native-run", result.WatermarkCandidate.RunId);
    }

    [Fact]
    public async Task Unsupported_cursor_type_fails_node_cleanly()
    {
        var node = SourceLoadNode(new NodeId("2222222222222222"), "files", "textcursor", 0, "id");
        var registry = new ConnectorRegistry();
        registry.AddSource("inmemory", new ConfigurableNativeSource("(values ('a'), ('b')) t(id)"));
        var plan = new ExecutionPlan(
            [new PlannedNode(node.Id, node.Kind, node.Name, EdgeStrategy.NativeScan, 1, "test")],
            MemoryBudget.Compute(new Pz.Core.Model.EngineConfig()));
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "bad-type-run"), NullRunEvents.Instance, plan);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Error);
        Assert.Equal("PZ0505", result.Error!.Code);
        Assert.Contains("id", result.Error.Message);
        Assert.Contains("VARCHAR", result.Error.Message);
        Assert.Contains("int, bigint, decimal, date, timestamp", result.Error.Message);
        // Clean failure, never a crash: staging table left behind by the CTAS is irrelevant here, but the
        // important invariant is that this returned a NodeResult at all instead of throwing.
        Assert.Null(result.WatermarkCandidate);
    }

    // -- Window bounds computation, windowed capture rules, caught-up notice. --
    // Windowed dataset shape throughout: columns: {id: bigint} + incremental: {cursor: id, max_window,
    // initial[, until]} via WindowedSourceLoadNode. InMemorySource seeds ids 0..(rows-1) (0-indexed) and
    // filters `lower < id <= upper` (lower exclusive, upper inclusive) -- see InMemorySource.cs.

    [Fact]
    public async Task Windowed_first_run_extracts_initial_slice_and_advances_to_max()
    {
        var sourceId = new NodeId("a1a1a1a1a1a1a1a1");
        var key = WatermarkStore.Key("mem", "w1");
        var dag = new CompiledDag([WindowedSourceLoadNode(sourceId, "mem", "w1", 25, maxWindow: "10", initial: "0")]);

        var result = await new RunOrchestrator(new KindDispatchingExecutor(), Ctx()).ExecuteAsync(dag, new RunOptions(), default);

        Assert.Equal(RunStatus.Success, result.Status);
        var sourceResult = Assert.Single(result.Nodes);
        Assert.Equal(10, sourceResult.RowsMoved); // lower=0 (exclusive), upper=10 (inclusive) -> ids 1..10
        Assert.NotNull(sourceResult.WatermarkCandidate);
        Assert.Equal("10", sourceResult.WatermarkCandidate!.Value); // MAX of the landed slice

        WatermarkAdvancement.Advance(dag, result.Nodes, _store);
        var stored = _store.Get(key);
        Assert.NotNull(stored);
        Assert.Equal("10", stored!.Value);
    }

    [Fact]
    public async Task Windowed_second_run_continues_from_stored_watermark()
    {
        var sourceId = new NodeId("a2a2a2a2a2a2a2a2");
        var key = WatermarkStore.Key("mem", "w2");
        _store.Set(key, new Watermark("id", "bigint", "10", "prior-run"));

        var dag = new CompiledDag([WindowedSourceLoadNode(sourceId, "mem", "w2", 25, maxWindow: "10", initial: "0")]);
        var result = await new RunOrchestrator(new KindDispatchingExecutor(), Ctx()).ExecuteAsync(dag, new RunOptions(), default);

        Assert.Equal(RunStatus.Success, result.Status);
        var sourceResult = Assert.Single(result.Nodes);
        Assert.Equal(10, sourceResult.RowsMoved); // lower=10, upper=20 -> ids 11..20
        Assert.Equal("20", sourceResult.WatermarkCandidate!.Value);

        WatermarkAdvancement.Advance(dag, result.Nodes, _store);
        Assert.Equal("20", _store.Get(key)!.Value);
    }

    [Fact]
    public async Task Windowed_empty_slice_advances_watermark_to_upper_bound()
    {
        var sourceId = new NodeId("a3a3a3a3a3a3a3a3");
        var key = WatermarkStore.Key("mem", "w3");
        // 30 is beyond the data's real max id (24, since rows=25 seeds ids 0..24) -- every row is
        // filtered out by the lower bound alone, an empty extract.
        _store.Set(key, new Watermark("id", "bigint", "30", "prior-run"));

        var dag = new CompiledDag([WindowedSourceLoadNode(sourceId, "mem", "w3", 25, maxWindow: "10", initial: "0")]);
        var result = await new RunOrchestrator(new KindDispatchingExecutor(), Ctx()).ExecuteAsync(dag, new RunOptions(), default);

        Assert.Equal(RunStatus.Success, result.Status);
        var sourceResult = Assert.Single(result.Nodes);
        Assert.Equal(0, sourceResult.RowsMoved);
        // An empty slice on a windowed, non-caught-up dataset still advances the watermark to the
        // window's upper bound (40 = 30 + max_window 10), unlike the unwindowed rule.
        Assert.NotNull(sourceResult.WatermarkCandidate);
        Assert.Equal("40", sourceResult.WatermarkCandidate!.Value);

        WatermarkAdvancement.Advance(dag, result.Nodes, _store);
        Assert.Equal("40", _store.Get(key)!.Value);
    }

    [Fact]
    public async Task Unwindowed_empty_slice_leaves_watermark_untouched()
    {
        var sourceId = new NodeId("a4a4a4a4a4a4a4a4");
        var key = WatermarkStore.Key("mem", "w4");
        var previous = new Watermark("id", "bigint", "9", "prior-run");
        _store.Set(key, previous);

        // No MaxWindow -> not a windowed dataset; the unwindowed rule (previous watermark stands on an
        // empty slice) must be untouched by the windowed rules.
        var dag = new CompiledDag([SourceLoadNode(sourceId, "mem", "w4", 10, "id")]);
        var result = await new RunOrchestrator(new KindDispatchingExecutor(), Ctx()).ExecuteAsync(dag, new RunOptions(), default);

        Assert.Equal(RunStatus.Success, result.Status);
        var sourceResult = Assert.Single(result.Nodes);
        Assert.Equal(0, sourceResult.RowsMoved);
        Assert.Null(sourceResult.WatermarkCandidate);

        WatermarkAdvancement.Advance(dag, result.Nodes, _store);
        Assert.Equal(previous, _store.Get(key));
    }

    [Fact]
    public async Task Until_clamps_the_upper_bound()
    {
        var sourceId = new NodeId("a5a5a5a5a5a5a5a5");
        var key = WatermarkStore.Key("mem", "w5");
        _store.Set(key, new Watermark("id", "bigint", "10", "prior-run"));

        var dag = new CompiledDag(
            [WindowedSourceLoadNode(sourceId, "mem", "w5", 25, maxWindow: "10", initial: "0", until: "15")]);
        var result = await new RunOrchestrator(new KindDispatchingExecutor(), Ctx()).ExecuteAsync(dag, new RunOptions(), default);

        Assert.Equal(RunStatus.Success, result.Status);
        var sourceResult = Assert.Single(result.Nodes);
        Assert.Equal(5, sourceResult.RowsMoved); // upper = min(10+10, 15) = 15 -> ids 11..15
        Assert.Equal("15", sourceResult.WatermarkCandidate!.Value);

        WatermarkAdvancement.Advance(dag, result.Nodes, _store);
        Assert.Equal("15", _store.Get(key)!.Value);
    }

    [Fact]
    public async Task Caught_up_dataset_extracts_nothing_and_notices()
    {
        var sourceId = new NodeId("a6a6a6a6a6a6a6a6");
        var key = WatermarkStore.Key("mem", "w6");
        _store.Set(key, new Watermark("id", "bigint", "15", "prior-run"));

        var notices = new List<string>();
        var ctx = new RunContext(_duck, _registry, new RunPaths(_dir, "test-run"), NullRunEvents.Instance,
            Watermarks: _store, Notice: notices.Add);

        // until = "15" = the stored watermark -> upper (min(15+10, 15) = 15) equals lower (15): caught up.
        var dag = new CompiledDag(
            [WindowedSourceLoadNode(sourceId, "mem", "w6", 25, maxWindow: "10", initial: "0", until: "15")]);
        var result = await new RunOrchestrator(new KindDispatchingExecutor(), ctx).ExecuteAsync(dag, new RunOptions(), default);

        Assert.Equal(RunStatus.Success, result.Status);
        var sourceResult = Assert.Single(result.Nodes);
        Assert.Equal(0, sourceResult.RowsMoved);
        Assert.Null(sourceResult.WatermarkCandidate); // caught up -> no advancement, unlike test 3's empty slice
        Assert.Contains(notices, n => n.Contains("caught up", StringComparison.Ordinal));

        // Extraction STILL ran with these (empty) bounds -- the staging table must exist (for downstream
        // SQL) even though it holds zero rows. A missing table would make this query throw.
        var stagingRowCount = await _duck.ScalarAsync<long>("select count(*) from staging.src_mem__w6", default);
        Assert.Equal(0, stagingRowCount);

        WatermarkAdvancement.Advance(dag, result.Nodes, _store);
        Assert.Equal("15", _store.Get(key)!.Value); // unchanged
    }

    [Fact]
    public async Task Windowed_candidate_never_exceeds_upper_bound()
    {
        // Candidate capping: InMemorySource honestly enforces WatermarkUpperBound, so it cannot
        // construct an over-extraction scenario without contorting its seeding. The native-tier stub
        // (ConfigurableNativeSource, already used by Capture_works_on_native_tier_too above) hands DuckDB
        // a raw SQL fragment with NO bound awareness at all -- exactly the "connector ignores/misapplies
        // the bound" shape this rule exists to guard against -- so the cap is exercised through that
        // existing seam instead, at the CaptureWatermarkAsync/native-CTAS level.
        //
        // The MAX probe is scoped to `cursor > lower and cursor <= upper`, so the over-extracted id=22
        // (beyond upper=20) is not visible to the MAX at all -- WindowMath.Min's cap is belt-and-braces,
        // not the mechanism that keeps this invariant. The
        // reported candidate is therefore the true max of the in-window rows (1, 2 -> 2), not an
        // artificial "20" ceiling over an out-of-window value that should never have counted regardless.
        var node = SourceLoadNode(new NodeId("a7a7a7a7a7a7a7a7"), "files", "w7", 0, "id",
            incremental: new IncrementalDef("id", MaxWindow: "20", Initial: "0"),
            columns: new Dictionary<string, string> { ["id"] = "bigint" });
        var registry = new ConnectorRegistry();
        registry.AddSource("inmemory", new ConfigurableNativeSource(
            "(values (cast(1 as bigint),'a'), (cast(2 as bigint),'b'), (cast(22 as bigint),'c')) t(id, name)"));
        var plan = new ExecutionPlan(
            [new PlannedNode(node.Id, node.Kind, node.Name, EdgeStrategy.NativeScan, 1, "test")],
            MemoryBudget.Compute(new Pz.Core.Model.EngineConfig()));
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "native-cap-run"), NullRunEvents.Instance, plan);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(3, result.RowsMoved); // all 3 rows still land in staging -- scoping affects the MAX probe, not the CTAS
        Assert.NotNull(result.WatermarkCandidate);
        Assert.Equal("2", result.WatermarkCandidate!.Value); // window-scoped MAX never sees id=22; still <= upper (20)
    }

    [Fact]
    public async Task Windowed_raw_mode_dataset_resolves_cursor_type_from_dataset_options()
    {
        // Http-style raw envelope: no columns: contract; the cursor is typed by the connector's
        // `cursor`/`cursor_type` dataset options. Bounds must compute from those, not NPE on Columns.
        var node = SourceLoadNode(new NodeId("b1b1b1b1b1b1b1b1"), "mem", "rawwin", 25, "id",
            extraOptions: new() { ["cursor"] = "id", ["cursor_type"] = "bigint" },
            incremental: new IncrementalDef("id", MaxWindow: "10", Initial: "0"),
            columns: null);

        var result = await new RunOrchestrator(new KindDispatchingExecutor(), Ctx())
            .ExecuteAsync(new CompiledDag([node]), new RunOptions(), default);

        Assert.Equal(RunStatus.Success, result.Status);
        var sourceResult = Assert.Single(result.Nodes);
        Assert.Equal(10, sourceResult.RowsMoved); // lower=initial(0), upper=10 -> ids 1..10
        Assert.Equal("10", sourceResult.WatermarkCandidate!.Value);
    }

    [Fact]
    public async Task Full_refresh_on_windowed_dataset_restarts_from_initial_not_unbounded()
    {
        var sourceId = new NodeId("a8a8a8a8a8a8a8a8");
        var key = WatermarkStore.Key("mem", "w8");
        _store.Set(key, new Watermark("id", "bigint", "20", "prior-run"));

        var dag = new CompiledDag([WindowedSourceLoadNode(sourceId, "mem", "w8", 25, maxWindow: "10", initial: "0")]);
        var result = await new RunOrchestrator(new KindDispatchingExecutor(), Ctx(fullRefresh: true))
            .ExecuteAsync(dag, new RunOptions(), default);

        Assert.Equal(RunStatus.Success, result.Status);
        var sourceResult = Assert.Single(result.Nodes);
        Assert.Equal(10, sourceResult.RowsMoved); // stored wm 20 ignored -> lower=initial(0), upper=10 -> ids 1..10
        Assert.Equal("10", sourceResult.WatermarkCandidate!.Value);

        WatermarkAdvancement.Advance(dag, result.Nodes, _store);
        Assert.Equal("10", _store.Get(key)!.Value); // re-established at 10, not unbounded past the stale 20
    }

    [Fact]
    public async Task Caught_up_never_regresses_watermark_even_when_connector_over_extracts()
    {
        // Stored watermark 20, `until: 15` (legal -- DagCompiler cannot see stored state at compile
        // time) -> a zero-width, caught-up window. A connector that ignores
        // DatasetSpec.WatermarkUpperBound entirely and lands rows past `until` anyway (the native-tier
        // stub) must NOT be able to regress the stored
        // watermark: caught-up means NO candidate, unconditionally -- not "no candidate only when the
        // slice happens to come back empty".
        var node = SourceLoadNode(new NodeId("a9a9a9a9a9a9a9a9"), "mem", "w9", 0, "id",
            incremental: new IncrementalDef("id", MaxWindow: "100", Until: "15"),
            columns: new Dictionary<string, string> { ["id"] = "bigint" });
        var key = WatermarkStore.Key("mem", "w9");
        _store.Set(key, new Watermark("id", "bigint", "20", "prior-run"));

        var registry = new ConnectorRegistry();
        registry.AddSource("inmemory", new ConfigurableNativeSource(
            "(values (cast(1 as bigint),'a'), (cast(2 as bigint),'b'), (cast(22 as bigint),'c')) t(id, name)"));
        var plan = new ExecutionPlan(
            [new PlannedNode(node.Id, node.Kind, node.Name, EdgeStrategy.NativeScan, 1, "test")],
            MemoryBudget.Compute(new Pz.Core.Model.EngineConfig()));
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "caughtup-regress-run"), NullRunEvents.Instance,
            plan, Watermarks: _store);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(3, result.RowsMoved); // all 3 rows land -- the connector ignored the bound
        Assert.Null(result.WatermarkCandidate); // caught up -> no candidate, no matter what landed

        var dag = new CompiledDag([node]);
        WatermarkAdvancement.Advance(dag, [result], _store);
        Assert.Equal("20", _store.Get(key)!.Value); // never regressed to 15
    }

    [Fact]
    public async Task Windowed_declared_type_mismatch_fails_node_cleanly()
    {
        // A windowed dataset declares `columns: {c: timestamp}` (the type the
        // window bounds above were computed with), but the native fragment actually lands `c` as DATE.
        // WindowMath.Min/AddWindow would otherwise throw a raw FormatException on the next run's bound
        // computation once this mismatched candidate got stored -- this must fail the node cleanly with
        // a PZ-coded error instead, naming both the declared and landed types.
        var node = SourceLoadNode(new NodeId("b1b1b1b1b1b1b1b1"), "mem", "mismatched", 0, "c",
            incremental: new IncrementalDef("c", MaxWindow: "1d", Initial: "2024-01-01T00:00:00.000000"),
            columns: new Dictionary<string, string> { ["c"] = "timestamp" });
        var registry = new ConnectorRegistry();
        registry.AddSource("inmemory", new ConfigurableNativeSource("(values (date '2024-01-02')) t(c)"));
        var plan = new ExecutionPlan(
            [new PlannedNode(node.Id, node.Kind, node.Name, EdgeStrategy.NativeScan, 1, "test")],
            MemoryBudget.Compute(new Pz.Core.Model.EngineConfig()));
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "type-mismatch-run"), NullRunEvents.Instance, plan);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Error);
        Assert.Equal(PzErrorCode.UnsupportedCursorType, result.Error!.Code);
        Assert.Contains("timestamp", result.Error.Message);
        Assert.Contains("DATE", result.Error.Message);
        Assert.Contains("align the columns", result.Error.Hint);
        Assert.Null(result.WatermarkCandidate);
    }

    [Fact]
    public async Task Stored_watermark_type_differs_from_declared_type_fails_node_cleanly()
    {
        // An older backfill stored a `date`-typed watermark; the dataset's columns: contract declares
        // the same cursor as `bigint`/`timestamp`. Without a guard,
        // WindowMath.AddWindow(declaredType, lowerWm.Value, ...) parses the STORED (date-shaped) value
        // using the DECLARED (bigint) format and throws a raw FormatException -- caught only by the
        // dispatcher's generic safety net, surfacing as an unhelpful PZ0501. This must instead fail
        // cleanly, same-run, before any bounds are computed, naming both types.
        var sourceId = new NodeId("b2b2b2b2b2b2b2b2");
        var key = WatermarkStore.Key("mem", "w10");
        _store.Set(key, new Watermark("id", "date", "2024-01-02", "prior-run"));

        var dag = new CompiledDag([WindowedSourceLoadNode(sourceId, "mem", "w10", 25, maxWindow: "10", initial: "0")]);
        var result = await new RunOrchestrator(new KindDispatchingExecutor(), Ctx()).ExecuteAsync(dag, new RunOptions(), default);

        var sourceResult = Assert.Single(result.Nodes);
        Assert.Equal(NodeStatus.Failed, sourceResult.Status);
        Assert.NotNull(sourceResult.Error);
        Assert.Equal(PzErrorCode.UnsupportedCursorType, sourceResult.Error!.Code); // PZ0505, not the generic PZ0501
        Assert.Contains("date", sourceResult.Error.Message, StringComparison.Ordinal);
        Assert.Contains("bigint", sourceResult.Error.Message, StringComparison.Ordinal);
        Assert.Contains("mem", sourceResult.Error.Message, StringComparison.Ordinal);
        Assert.Contains("w10", sourceResult.Error.Message, StringComparison.Ordinal);
        Assert.NotNull(sourceResult.Error.Hint);
        Assert.Contains("--full-refresh", sourceResult.Error.Hint, StringComparison.Ordinal);
        Assert.Contains("align", sourceResult.Error.Hint, StringComparison.Ordinal);
        Assert.Null(sourceResult.WatermarkCandidate);

        // No exception escaped the node -- the run itself completed (with a failed node), never crashed.
        Assert.Equal(RunStatus.CompletedWithFailures, result.Status);
    }

    [Fact]
    public async Task Windowed_capture_max_is_scoped_to_the_window_and_ignores_stale_landed_rows()
    {
        // Mirrors the over-extraction seam test above
        // (Windowed_candidate_never_exceeds_upper_bound), but on the OTHER side of the window -- a
        // connector that legally ignores the LOWER bound (MAY-apply, not MUST) or a force_universal tier
        // switch can land rows BELOW the stored watermark. An unscoped MAX(cursor) over the whole staging
        // table would then report a landed max that is itself stale (below the stored watermark), and
        // WatermarkAdvancement.Set's unconditional write would REGRESS the cursor. The window-scoped MAX
        // (cursor > lower and cursor <= upper) must yield NO raw-max candidate here, so the run instead
        // falls through to the existing empty-slice rule and advances to windowUpper.
        var node = SourceLoadNode(new NodeId("b3b3b3b3b3b3b3b3"), "mem", "w11", 0, "id",
            incremental: new IncrementalDef("id", MaxWindow: "10"),
            columns: new Dictionary<string, string> { ["id"] = "bigint" });
        var key = WatermarkStore.Key("mem", "w11");
        _store.Set(key, new Watermark("id", "bigint", "20", "prior-run")); // lower=20 -> upper=30

        var registry = new ConnectorRegistry();
        registry.AddSource("inmemory", new ConfigurableNativeSource(
            // Every landed row is stale: id 1..5, all <= the stored lower bound (20).
            "(values (cast(1 as bigint)), (cast(2 as bigint)), (cast(3 as bigint)), " +
            "(cast(4 as bigint)), (cast(5 as bigint))) t(id)"));
        var plan = new ExecutionPlan(
            [new PlannedNode(node.Id, node.Kind, node.Name, EdgeStrategy.NativeScan, 1, "test")],
            MemoryBudget.Compute(new Pz.Core.Model.EngineConfig()));
        var ctx = new RunContext(_duck, registry, new RunPaths(_dir, "stale-landing-run"), NullRunEvents.Instance,
            plan, Watermarks: _store);

        var result = await new KindDispatchingExecutor().ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(5, result.RowsMoved); // all 5 stale rows still land in staging
        Assert.NotNull(result.WatermarkCandidate);
        Assert.Equal("30", result.WatermarkCandidate!.Value); // empty-slice rule: advances to windowUpper, not 5

        var dag = new CompiledDag([node]);
        WatermarkAdvancement.Advance(dag, [result], _store);
        Assert.Equal("30", _store.Get(key)!.Value); // never regressed to "5", never stalled at "20"
    }

    [Fact]
    public async Task Windowed_universal_path_trims_staging_to_the_window_when_connector_over_delivers()
    {
        // The InMemory source honors DatasetSpec.WatermarkLowerBound/UpperBound, so an ordinary run
        // cannot exercise the universal-path over-delivery this backstop guards against.
        // `ignore_watermark_bounds: true` (a TestKit option, following InMemorySource's fault-injection
        // idiom) makes it a misbehaving universal-tier connector: it ships every row in its range
        // (0..29) with NO regard for the engine-computed bounds. Stored watermark "10" + max_window
        // "10" -> window (10, 20]. The candidate-cap/scoped-MAX caps the WATERMARK at "20" regardless
        // (defense-in-depth layer 1) -- this test's job is to prove staging CONTENT is also trimmed to
        // exactly the window (layer 2).
        var sourceId = new NodeId("c1c1c1c1c1c1c1c1");
        var key = WatermarkStore.Key("mem", "w12");
        _store.Set(key, new Watermark("id", "bigint", "10", "prior-run"));

        var node = SourceLoadNode(sourceId, "mem", "w12", 30, "id",
            extraOptions: new Dictionary<string, object?> { ["ignore_watermark_bounds"] = true },
            incremental: new IncrementalDef("id", MaxWindow: "10"),
            columns: new Dictionary<string, string> { ["id"] = "bigint" });

        var dag = new CompiledDag([node]);
        var result = await new RunOrchestrator(new KindDispatchingExecutor(), Ctx())
            .ExecuteAsync(dag, new RunOptions(), default);

        Assert.Equal(RunStatus.Success, result.Status);
        var sourceResult = Assert.Single(result.Nodes);
        Assert.Equal(NodeStatus.Success, sourceResult.Status);
        Assert.Equal(10, sourceResult.RowsMoved); // POST-trim count, not the 30 the connector actually shipped
        Assert.NotNull(sourceResult.WatermarkCandidate);
        Assert.Equal("20", sourceResult.WatermarkCandidate!.Value);

        var stagingTable = StagingNames.ForSourceLoad("mem", "w12");
        var stagingCount = await _duck.ScalarAsync<long>($"select count(*) from {stagingTable}", default);
        var stagingMin = await _duck.ScalarAsync<long>($"select min(id) from {stagingTable}", default);
        var stagingMax = await _duck.ScalarAsync<long>($"select max(id) from {stagingTable}", default);
        Assert.Equal(10, stagingCount); // EXACTLY rows 11..20 survive the trim -- not the 30 that landed
        Assert.Equal(11, stagingMin);
        Assert.Equal(20, stagingMax);

        WatermarkAdvancement.Advance(dag, result.Nodes, _store);
        Assert.Equal("20", _store.Get(key)!.Value);
    }

    [Fact]
    public async Task Caught_up_over_delivering_universal_source_lands_empty_staging()
    {
        // When a windowed dataset is caught up (upper <= lower), the staging window-trim in
        // SourceLoadExecutor must land EMPTY — the catch for universal-path over-delivery at caught-up
        // boundaries. Mirrors Windowed_universal_path_trims_staging_to_the_window_when_connector_over_delivers
        // but forces the CAUGHT-UP case by setting stored watermark AT until. The connector over-delivers
        // (25 rows) via ignore_watermark_bounds, proving the trim is necessary even when caught up.
        var sourceId = new NodeId("c2c2c2c2c2c2c2c2");
        var key = WatermarkStore.Key("mem", "w13");
        _store.Set(key, new Watermark("id", "bigint", "15", "prior-run"));

        var notices = new List<string>();
        var ctx = new RunContext(_duck, _registry, new RunPaths(_dir, "test-run"), NullRunEvents.Instance,
            Watermarks: _store, Notice: notices.Add);

        // until = "15" = stored watermark -> upper (min(15+10, 15) = 15) equals lower (15): caught up.
        var node = SourceLoadNode(sourceId, "mem", "w13", 25, "id",
            extraOptions: new Dictionary<string, object?> { ["ignore_watermark_bounds"] = true },
            incremental: new IncrementalDef("id", MaxWindow: "10", Until: "15"),
            columns: new Dictionary<string, string> { ["id"] = "bigint" });

        var dag = new CompiledDag([node]);
        var result = await new RunOrchestrator(new KindDispatchingExecutor(), ctx)
            .ExecuteAsync(dag, new RunOptions(), default);

        Assert.Equal(RunStatus.Success, result.Status);
        var sourceResult = Assert.Single(result.Nodes);
        Assert.Equal(NodeStatus.Success, sourceResult.Status);
        Assert.Equal(0, sourceResult.RowsMoved); // caught up -> trim to empty window (15, 15]
        Assert.Null(sourceResult.WatermarkCandidate); // caught up -> no candidate, no advancement

        var stagingTable = StagingNames.ForSourceLoad("mem", "w13");
        var stagingCount = await _duck.ScalarAsync<long>($"select count(*) from {stagingTable}", default);
        Assert.Equal(0, stagingCount); // staging exists but contains 0 rows after trim

        Assert.Contains(notices, n => n.Contains("caught up", StringComparison.Ordinal));

        WatermarkAdvancement.Advance(dag, result.Nodes, _store);
        Assert.Equal("15", _store.Get(key)!.Value); // unchanged
    }

    /// <summary>Native-scan-only stub source (mirrors <c>NativePathTests.ConfigurableNativeSource</c>):
    /// its universal-path members throw so any accidental fall-through fails loudly.</summary>
    private sealed class ConfigurableNativeSource(string fragment) : ISourceConnector, ISource
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
            scan = new NativeScan(fragment, []) { Mechanism = "stub_scan" };
            return true;
        }

        public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
            throw new InvalidOperationException("universal path must not be used");

        public ValueTask DisposeAsync() => default;
    }
}
