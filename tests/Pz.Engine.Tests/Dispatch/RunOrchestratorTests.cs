using System.Collections.Concurrent;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Engine.Execution;
using Pz.Engine.Dispatch;

namespace Pz.Engine.Tests.Dispatch;

/// <summary>Scriptable executor: per-node delay, outcome, and a live max-parallelism probe.</summary>
internal sealed class FakeExecutor : INodeExecutor
{
    private int _inFlight; private int _maxInFlight;
    public readonly Dictionary<string, TimeSpan> Delays = [];
    public readonly HashSet<string> FailNodes = [];
    public readonly List<string> Executed = [];
    public int MaxObservedParallelism => _maxInFlight;

    public async Task<NodeResult> ExecuteAsync(DagNode node, RunContext ctx, CancellationToken ct)
    {
        var now = Interlocked.Increment(ref _inFlight);
        InterlockedExtensions.Max(ref _maxInFlight, now);
        try
        {
            lock (Executed) Executed.Add(node.Name);
            await Task.Delay(Delays.GetValueOrDefault(node.Name, TimeSpan.FromMilliseconds(20)), ct);
            return FailNodes.Contains(node.Name)
                ? new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0, TimeSpan.Zero,
                    new Core.Validation.PzError("PZ0501", "boom", null, null, null))
                : new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Success, 1, TimeSpan.Zero, null);
        }
        finally { Interlocked.Decrement(ref _inFlight); }
    }
}

internal static class InterlockedExtensions
{
    public static void Max(ref int location, int value)
    {
        int current;
        while (value > (current = Volatile.Read(ref location)) &&
               Interlocked.CompareExchange(ref location, value, current) != current) { }
    }
}

/// <summary>Gate-based stub for <c>Instance_cap_bounds_concurrent_nodes_of_one_source</c> below: no
/// wall-clock sleeps, only <see cref="TaskCompletionSource"/> gates the test controls explicitly.
/// <see cref="Entered"/>/<see cref="Completed"/> are signalled (lazily, per node name) so the test can
/// deterministically await "this node's executor call has started/returned" instead of polling.
/// <see cref="MaxObservedParallelism"/> only counts nodes named in <c>trackedNames</c> at construction
/// (the capped source's nodes) — an uncapped node running concurrently alongside a capped one is
/// expected and must not pollute this high-water mark.</summary>
internal sealed class GatedExecutor(IEnumerable<string> trackedNames) : INodeExecutor
{
    private readonly HashSet<string> _tracked = [.. trackedNames];
    private int _inFlight;
    private int _maxInFlight;

    public int MaxObservedParallelism => Volatile.Read(ref _maxInFlight);

    /// <summary>Node name -> gate. A node with no entry here proceeds without blocking.</summary>
    public readonly Dictionary<string, TaskCompletionSource> Gates = [];

    private readonly ConcurrentDictionary<string, TaskCompletionSource> _entered = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource> _completed = new();

    /// <summary>Resolves once this node's executor call has started -- safe to await before the call
    /// happens (lazily creates the gate on first access from either side, test or executor).</summary>
    public Task EnteredTask(string name) => TcsFor(_entered, name).Task;

    /// <summary>Resolves once this node's executor call has returned.</summary>
    public Task CompletedTask(string name) => TcsFor(_completed, name).Task;

    private static TaskCompletionSource TcsFor(ConcurrentDictionary<string, TaskCompletionSource> map, string name) =>
        map.GetOrAdd(name, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

    public async Task<NodeResult> ExecuteAsync(DagNode node, RunContext ctx, CancellationToken ct)
    {
        if (_tracked.Contains(node.Name))
        {
            var now = Interlocked.Increment(ref _inFlight);
            InterlockedExtensions.Max(ref _maxInFlight, now);
        }

        TcsFor(_entered, node.Name).TrySetResult();
        try
        {
            if (Gates.TryGetValue(node.Name, out var gate))
            {
                await gate.Task;
            }

            return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Success, 1, TimeSpan.Zero, null);
        }
        finally
        {
            if (_tracked.Contains(node.Name))
            {
                Interlocked.Decrement(ref _inFlight);
            }

            TcsFor(_completed, node.Name).TrySetResult();
        }
    }
}

public sealed class RunOrchestratorTests
{
    // Diamond: a -> b, a -> c, b+c -> d ; plus independent e
    private static CompiledDag Dag()
    {
        DagNode N(string id, string name, params string[] deps) => new(new NodeId(id.PadLeft(16, '0')),
            NodeKind.Pipeline, name, [.. deps.Select(d => new NodeId(d.PadLeft(16, '0')))], null, name);
        return new CompiledDag([N("a", "a"), N("b", "b", "a"), N("c", "c", "a"),
            N("d", "d", "b", "c"), N("e", "e")]);
    }

    private static RunContext Ctx() => new(null!, new ConnectorRegistry(),
        new RunPaths(Path.GetTempPath(), "t"), NullRunEvents.Instance);

    private static RunContext Ctx(IRunEvents events) => new(null!, new ConnectorRegistry(),
        new RunPaths(Path.GetTempPath(), "t"), events);

    /// <summary>Records NodeCompleted invocations; optionally throws for a named node — a throwing
    /// IRunEvents handler must not be able to silently orphan descendants.</summary>
    private sealed class RecordingEvents(string? throwForNode = null) : IRunEvents
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
        public void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped, TimeSpan duration) { }

        public void NodeCompleted(NodeResult result)
        {
            Interlocked.Increment(ref _completedCount);
            if (result.Name == throwForNode)
            {
                throw new InvalidOperationException("injected NodeCompleted handler failure");
            }
        }
    }

    /// <summary>Records call order (RunStarted, NodeCompleted) so ordering assertions can be made
    /// without wall-clock timing -- built for the seeded-results test below, which must observe
    /// NodeCompleted("carried-1") strictly after RunStarted (events.md: exactly one run_started opens
    /// the stream).</summary>
    private sealed class RecordingRunEvents : IRunEvents
    {
        private readonly List<NodeResult?> _calls = []; // null entry marks the RunStarted call
        private int _runStartedIndex = -1;

        public int RunStartedIndex => Volatile.Read(ref _runStartedIndex);
        public IReadOnlyList<NodeResult> Completed { get { lock (_calls) return [.. _calls.OfType<NodeResult>()]; } }

        public int IndexOfNodeCompleted(string idValue)
        {
            lock (_calls)
            {
                for (var i = 0; i < _calls.Count; i++)
                {
                    if (_calls[i] is { } r && r.Id.Value == idValue)
                    {
                        return i;
                    }
                }

                return -1;
            }
        }

        public void RunStarted(string runId, string projectName, int nodeCount)
        {
            lock (_calls)
            {
                _runStartedIndex = _calls.Count;
                _calls.Add(null);
            }
        }

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
        public void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped, TimeSpan duration) { }

        public void NodeCompleted(NodeResult result)
        {
            lock (_calls) _calls.Add(result);
        }
    }

    /// <summary>A seeded (carried-forward) result is recorded verbatim
    /// into the run's result list -- outside the effective set, never dispatched -- but still fires
    /// NodeCompleted exactly once, and only after RunStarted (which must remain the sole event opening
    /// the stream).</summary>
    [Fact]
    public async Task Seeded_results_are_recorded_before_dispatch_and_reported_once()
    {
        DagNode N(string id, string name, params string[] deps) => new(new NodeId(id.PadLeft(16, '0')),
            NodeKind.Pipeline, name, [.. deps.Select(d => new NodeId(d.PadLeft(16, '0')))], null, name);
        var dag = new CompiledDag([N("a", "a")]);

        var seeded = new NodeResult(new NodeId("carried-1"), NodeKind.SinkWrite, "sink_ok",
            NodeStatus.Success, 7, TimeSpan.Zero, null, Provenance: NodeProvenance.CarriedForward);
        var events = new RecordingRunEvents();
        var fake = new FakeExecutor();

        var result = await new RunOrchestrator(fake, Ctx(events))
            .ExecuteAsync(dag, new RunOptions(4, false, null, "t", [seeded]), CancellationToken.None);

        Assert.Contains(result.Nodes, n => n.Id.Value == "carried-1" &&
            n.Status == NodeStatus.Success && n.Provenance == NodeProvenance.CarriedForward);
        Assert.Equal(1, events.Completed.Count(r => r.Id.Value == "carried-1"));
        // run_started must still open the stream: the seeded completion is observed after it.
        Assert.True(events.RunStartedIndex < events.IndexOfNodeCompleted("carried-1"));
    }

    /// <summary>The planner guarantees seeded ids are disjoint
    /// from the run's effective set, but RunOrchestrator is a public API -- a caller that violates the
    /// precondition must fail loudly (ArgumentException naming the offending id), not silently drive a
    /// child-readiness decision from a divergent outcome or sweep descendants to Skipped. The guard must
    /// fire BEFORE RunStarted (no run_started for a run that never legally started).</summary>
    [Fact]
    public async Task Seeded_id_inside_effective_set_throws()
    {
        var dag = Dag();
        var aId = dag.Nodes.Single(n => n.Name == "a").Id;
        var seeded = new NodeResult(aId, NodeKind.Pipeline, "a", NodeStatus.Success, 1, TimeSpan.Zero, null,
            Provenance: NodeProvenance.CarriedForward);
        var events = new RecordingRunEvents();
        var fake = new FakeExecutor();

        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            new RunOrchestrator(fake, Ctx(events))
                .ExecuteAsync(dag, new RunOptions(4, false, null, "t", [seeded]), CancellationToken.None));

        Assert.Contains(aId.Value, ex.Message);
        Assert.Equal(-1, events.RunStartedIndex); // guard fires before run_started is ever emitted
    }

    [Fact]
    public async Task Independent_nodes_run_concurrently_up_to_limit()
    {
        var fake = new FakeExecutor();
        foreach (var n in new[] { "b", "c", "e" }) fake.Delays[n] = TimeSpan.FromMilliseconds(150);
        var result = await new RunOrchestrator(fake, Ctx())
            .ExecuteAsync(Dag(), new RunOptions(MaxConcurrency: 2), default);

        Assert.Equal(RunStatus.Success, result.Status);
        Assert.Equal(2, fake.MaxObservedParallelism); // capped at 2 despite 3 ready nodes (b, c, e after a)
    }

    /// <summary>`max_concurrency: 1` on one source instance must serialize its own
    /// SourceLoad nodes (three datasets of the SAME source, all independent/ready at once, global
    /// MaxConcurrency 4 -- they WOULD overlap if the instance cap weren't enforced) without starving a
    /// concurrently-ready node from a DIFFERENT, uncapped source of a global permit while the capped
    /// ones queue. Gate-based: every synchronization point is an explicit TaskCompletionSource the test
    /// controls, never a wall-clock sleep.</summary>
    [Fact]
    public async Task Instance_cap_bounds_concurrent_nodes_of_one_source()
    {
        var cappedSource = new ConnectionDef("capped", "test", new Dictionary<string, object?>(),
            [], "sources/capped.yml", MaxConcurrency: 1);
        var freeSource = new ConnectionDef("free", "test", new Dictionary<string, object?>(),
            [], "sources/free.yml");

        DagNode CapNode(string id, string name) => new(new NodeId(id.PadLeft(16, '0')), NodeKind.SourceLoad, name,
            [], null, new SourceDatasetDef(cappedSource, new DatasetDef(name, new Dictionary<string, object?>(), null)));
        DagNode FreeNode(string id, string name) => new(new NodeId(id.PadLeft(16, '0')), NodeKind.SourceLoad, name,
            [], null, new SourceDatasetDef(freeSource, new DatasetDef(name, new Dictionary<string, object?>(), null)));

        var dag = new CompiledDag(
            [CapNode("1", "cap-a"), CapNode("2", "cap-b"), CapNode("3", "cap-c"), FreeNode("4", "free-d")]);

        var executor = new GatedExecutor(trackedNames: ["cap-a", "cap-b", "cap-c"]);
        executor.Gates["cap-a"] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        executor.Gates["cap-b"] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        executor.Gates["cap-c"] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // "free-d" gets no gate at all -- it should run to completion unimpeded by the capped queue.

        // Global cap 3 (not 4) is load-bearing: with instance-before-global acquisition, the two queued
        // capped nodes hold NO global permits, so free-d always gets a slot. If the acquisition order
        // ever flipped to global-first, the queued capped nodes would pin 2 of 3 global permits and
        // free-d's await below would hang — this test then fails by timeout instead of passing silently.
        var runTask = new RunOrchestrator(executor, Ctx())
            .ExecuteAsync(dag, new RunOptions(MaxConcurrency: 3), default);

        await executor.EnteredTask("cap-a");
        await executor.CompletedTask("free-d"); // global slots not starved by the capped queue

        executor.Gates["cap-a"].SetResult();
        await executor.EnteredTask("cap-b"); // only unblocks once cap-a released the instance permit

        executor.Gates["cap-b"].SetResult();
        await executor.EnteredTask("cap-c");

        // The uncapped node finished well before we released the LAST capped gate.
        Assert.True(executor.CompletedTask("free-d").IsCompletedSuccessfully);

        executor.Gates["cap-c"].SetResult();

        var result = await runTask;

        Assert.Equal(RunStatus.Success, result.Status);
        Assert.Equal(4, result.Nodes.Count(r => r.Status == NodeStatus.Success));
        Assert.Equal(1, executor.MaxObservedParallelism); // never 2+ despite all 3 being ready at once
    }

    [Fact]
    public async Task Failed_node_skips_descendants_and_continues_siblings()
    {
        var fake = new FakeExecutor { FailNodes = { "b" } };
        var result = await new RunOrchestrator(fake, Ctx()).ExecuteAsync(Dag(), new RunOptions(), default);

        Assert.Equal(RunStatus.CompletedWithFailures, result.Status);
        var byName = result.Nodes.ToDictionary(r => r.Name);
        Assert.Equal(NodeStatus.Failed, byName["b"].Status);
        Assert.Equal(NodeStatus.Skipped, byName["d"].Status);   // descendant of b
        Assert.Equal(NodeStatus.Success, byName["c"].Status);   // sibling continues
        Assert.Equal(NodeStatus.Success, byName["e"].Status);   // independent continues
        Assert.Equal(5, result.Nodes.Count);
    }

    [Fact]
    public async Task FailFast_cancels_running_nodes()
    {
        var fake = new FakeExecutor { FailNodes = { "b" } };
        fake.Delays["c"] = TimeSpan.FromSeconds(30);
        fake.Delays["e"] = TimeSpan.FromSeconds(30);
        var sw = System.Diagnostics.Stopwatch.StartNew();

        var result = await new RunOrchestrator(fake, Ctx())
            .ExecuteAsync(Dag(), new RunOptions(FailFast: true), default);
        sw.Stop();

        Assert.Equal(RunStatus.CompletedWithFailures, result.Status);
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10), "FailFast did not cancel long-running siblings");
    }

    [Fact]
    public async Task Selection_limits_execution_to_selected_and_required_ancestors()
    {
        var fake = new FakeExecutor();
        var dag = Dag();
        var dId = dag.Nodes.Single(n => n.Name == "d").Id;

        var result = await new RunOrchestrator(fake, Ctx())
            .ExecuteAsync(dag, new RunOptions(Selection: new HashSet<NodeId> { dId }), default);

        Assert.Equal(RunStatus.Success, result.Status);
        Assert.Equal(["a", "b", "c", "d"], result.Nodes.Select(r => r.Name).OrderBy(n => n, StringComparer.Ordinal));
        Assert.DoesNotContain("e", fake.Executed); // not selected, not an ancestor
    }

    [Fact]
    public async Task Ctrl_c_token_produces_graceful_partial_result()
    {
        var fake = new FakeExecutor();
        fake.Delays["a"] = TimeSpan.FromSeconds(30);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        var result = await new RunOrchestrator(fake, Ctx()).ExecuteAsync(Dag(), new RunOptions(), cts.Token);

        Assert.Equal(RunStatus.Fatal, result.Status);
    }

    /// <summary>FailFast completeness. A node ("a") that fails immediately holds
    /// the sole concurrency permit; "b" (an independent chain's root, unrelated to "a") is still
    /// queued on the semaphore when FailFast cancels — its wait throws OperationCanceledException and
    /// it is recorded Skipped directly (not via CascadeSkip, since "b" is not a descendant of "a"), but
    /// its own descendants "c" and "d" are never dispatched at all unless the dispatcher sweeps every
    /// not-yet-terminal effective-set node at wind-down. Every node in the effective set must still get
    /// exactly one NodeResult (dbt semantics).</summary>
    [Fact]
    public async Task Deep_chain_under_failfast_records_every_effective_node()
    {
        DagNode N(string id, string name, params string[] deps) => new(new NodeId(id.PadLeft(16, '0')),
            NodeKind.Pipeline, name, [.. deps.Select(d => new NodeId(d.PadLeft(16, '0')))], null, name);
        var dag = new CompiledDag([N("a", "a"), N("b", "b"), N("c", "c", "b"), N("d", "d", "c")]);

        var fake = new FakeExecutor { FailNodes = { "a" } };
        fake.Delays["a"] = TimeSpan.FromMilliseconds(10);

        var result = await new RunOrchestrator(fake, Ctx())
            .ExecuteAsync(dag, new RunOptions(MaxConcurrency: 1, FailFast: true), default);

        Assert.Equal(4, result.Nodes.Count);
        var byName = result.Nodes.ToDictionary(r => r.Name);
        Assert.Equal(NodeStatus.Failed, byName["a"].Status);
        Assert.Equal(NodeStatus.Skipped, byName["b"].Status);
        Assert.Equal(NodeStatus.Skipped, byName["c"].Status);
        Assert.Equal(NodeStatus.Skipped, byName["d"].Status);
    }

    /// <summary>IRunEvents no-throw hardening. A throwing NodeCompleted handler
    /// must not prevent the dispatcher from cascading Skip to descendants of a failed node.</summary>
    [Fact]
    public async Task Throwing_event_handler_does_not_orphan_descendants()
    {
        DagNode N(string id, string name, params string[] deps) => new(new NodeId(id.PadLeft(16, '0')),
            NodeKind.Pipeline, name, [.. deps.Select(d => new NodeId(d.PadLeft(16, '0')))], null, name);
        var dag = new CompiledDag([N("a", "a"), N("b", "b", "a")]);

        var fake = new FakeExecutor { FailNodes = { "a" } };
        var recorder = new RecordingEvents(throwForNode: "a");

        var result = await new RunOrchestrator(fake, Ctx(recorder)).ExecuteAsync(dag, new RunOptions(), default);

        Assert.Equal(RunStatus.CompletedWithFailures, result.Status);
        Assert.Equal(2, result.Nodes.Count);
        var byName = result.Nodes.ToDictionary(r => r.Name);
        Assert.Equal(NodeStatus.Failed, byName["a"].Status);
        Assert.Equal(NodeStatus.Skipped, byName["b"].Status);
    }

    /// <summary>MaxConcurrency &lt;= 0 must be guarded before the semaphore ctor. 0 is
    /// otherwise a silent, permanent deadlock (SemaphoreSlim(0) grants no permits, ever), not a
    /// same-shape exception from the ctor like a negative value already produces.</summary>
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task MaxConcurrency_le_zero_throws(int maxConcurrency)
    {
        var fake = new FakeExecutor();
        var orchestrator = new RunOrchestrator(fake, Ctx());

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => orchestrator.ExecuteAsync(Dag(), new RunOptions(MaxConcurrency: maxConcurrency), default));
    }

    /// <summary>An INodeExecutor that throws directly, bypassing KindDispatchingExecutor entirely --
    /// exercises RunOrchestrator's OWN defensive catch (Exception ex) safety net (its doc comment: "a
    /// non-conforming executor can't hang the dispatcher's completion latch"), distinct from
    /// KindDispatchingExecutor's own PZ0501 wrap.</summary>
    private sealed class ThrowingExecutor(string message) : INodeExecutor
    {
        public Task<NodeResult> ExecuteAsync(DagNode node, RunContext ctx, CancellationToken ct) =>
            throw new InvalidOperationException(message);
    }

    /// <summary>RunOrchestrator's own defensive NodeFailed catch must
    /// redact a raw engine-error-shaped exception message exactly like KindDispatchingExecutor's wrap
    /// does -- both ultimately feed the same NodeResult.Error.Message that run_results.json/NDJSON
    /// surface.</summary>
    [Fact]
    public async Task Fatal_message_is_redacted()
    {
        const string engineEcho =
            "Binder Error: syntax error at or near \"CREATE\"\n" +
            "LINE 1: CREATE SECRET s (TYPE s3, KEY_ID 'AKID', SECRET 'SECRET_VALUE')\n" +
            "                                                        ^";
        var dag = new CompiledDag([new DagNode(new NodeId("1111111111111111"), NodeKind.Pipeline, "a", [], null, "a")]);

        var result = await new RunOrchestrator(new ThrowingExecutor(engineEcho), Ctx())
            .ExecuteAsync(dag, new RunOptions(), default);

        var node = Assert.Single(result.Nodes);
        Assert.Equal(NodeStatus.Failed, node.Status);
        Assert.NotNull(node.Error);
        Assert.DoesNotContain("SECRET_VALUE", node.Error!.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("LINE 1", node.Error.Message, StringComparison.Ordinal);
        Assert.Contains("Binder Error", node.Error.Message, StringComparison.Ordinal);
    }
}
