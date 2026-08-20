using System.Collections.Frozen;
using System.Diagnostics;
using Pz.Core.Dag;
using Pz.Core.Validation;
using Pz.Diagnostics.Otel;
using Pz.Engine.Execution;

namespace Pz.Engine.Dispatch;

/// <summary>
/// Topological, max-concurrency-gated scheduler over a <see cref="CompiledDag"/>. Every node
/// tracked by this run (the "effective set" — see <see cref="ExecuteAsync"/>) transitions exactly
/// once into a terminal <see cref="NodeStatus"/> (Success, Failed, or Skipped); each transition
/// appends one <see cref="NodeResult"/> to the run's result list and fires
/// <see cref="IRunEvents.NodeCompleted"/> exactly once. Concurrency is bounded by a
/// <see cref="SemaphoreSlim"/> sized to <see cref="RunOptions.MaxConcurrency"/>: nodes become
/// eligible to run the instant all of their parents have succeeded, but only acquire the
/// semaphore (and thus actually call the executor) once a permit is free, so bursts of
/// simultaneously-ready nodes never exceed the configured cap. A second, narrower gate applies on
/// top: a source/sink instance that declares <c>max_concurrency</c> gets its own
/// <see cref="SemaphoreSlim"/> (see <see cref="Pz.Engine.Execution.InstanceKey"/>), acquired BEFORE the
/// global permit in <c>RunNodeAsync</c> so that instance's queued nodes never occupy a global slot while
/// waiting on their own turn.
/// </summary>
public sealed class RunOrchestrator(INodeExecutor executor, RunContext ctx)
{
    /// <summary>"node.&lt;kind&gt;" span names are constant per
    /// <see cref="NodeKind"/> — caching them avoids a per-dispatch string allocation
    /// (<c>$"node.{node.Kind}"</c> interpolation, which also calls the enum's <c>ToString()</c>) on
    /// every node dispatch. Zero allocation is the target both WITH and WITHOUT an
    /// <see cref="Activity"/> listener registered, not just on the no-listener
    /// <c>StartActivity</c> no-op path.</summary>
    internal static readonly FrozenDictionary<NodeKind, string> SpanNames = new Dictionary<NodeKind, string>
    {
        [NodeKind.SourceLoad] = "node.SourceLoad",
        [NodeKind.Pipeline] = "node.Pipeline",
        [NodeKind.Check] = "node.Check",
        [NodeKind.SinkWrite] = "node.SinkWrite",
    }.ToFrozenDictionary();

    /// <summary>The <c>pz.node.kind</c> metric tag <see cref="KeyValuePair{TKey,TValue}"/>
    /// is likewise constant per <see cref="NodeKind"/> — caching it avoids both the enum
    /// <c>ToString()</c> allocation and a fresh boxed tag value on every
    /// <c>PzMeters.NodeDuration.Record</c> call.</summary>
    internal static readonly FrozenDictionary<NodeKind, KeyValuePair<string, object?>> NodeKindTags =
        new Dictionary<NodeKind, KeyValuePair<string, object?>>
        {
            [NodeKind.SourceLoad] = new("pz.node.kind", "SourceLoad"),
            [NodeKind.Pipeline] = new("pz.node.kind", "Pipeline"),
            [NodeKind.Check] = new("pz.node.kind", "Check"),
            [NodeKind.SinkWrite] = new("pz.node.kind", "SinkWrite"),
        }.ToFrozenDictionary();


    /// <summary>Executes the dag topologically. Selection (when non-null) is expanded to include all
    /// required ancestors (run-time semantics — differs deliberately from compile's selection-exact).
    /// A failed node marks all descendants Skipped; independent branches continue unless FailFast.
    /// A NodeResult is reported (and Events.NodeCompleted fired) for every executed/skipped node,
    /// in completion order. OperationCanceledException from user cancellation → RunStatus.Fatal
    /// only if no node failed; cancellation triggered by FailFast keeps CompletedWithFailures.</summary>
    public async Task<RunResult> ExecuteAsync(CompiledDag dag, RunOptions options, CancellationToken ct)
    {
        // 0 is not caught by SemaphoreSlim's own ctor guard (0 is a legal initial
        // count — it just means zero permits, ever, i.e. a silent, permanent deadlock here since nothing
        // else releases a first permit). Guard explicitly so the failure is immediate and diagnosable
        // instead of a hang. Negative values already throw from the SemaphoreSlim ctor below, but this
        // guard covers both with one clear message ahead of it.
        if (options.MaxConcurrency <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options), options.MaxConcurrency,
                "RunOptions.MaxConcurrency must be >= 1.");
        }

        var effective = ComputeEffectiveSet(dag, options.Selection);

        // Precondition guard, same style/placement as the MaxConcurrency check above (outside the try
        // below, so it propagates as a real ArgumentException instead of being swallowed by the outer
        // catch and reported as a Fatal RunResult): RunOptions.Seeded results must be outside the run's
        // effective set. The planner guarantees this today, but RunOrchestrator is a public API, and
        // "no silent failures" is binding -- left unchecked, a colliding id either wastes/ignores real
        // execution while driving child-readiness from the seeded (possibly divergent) outcome, or, if
        // the seeded status isn't Success, silently sweeps downstream children to Skipped. Checked
        // before any event fires so a precondition violation never emits a run_started for a run that
        // never legally started. Built only when there's a Seeded list to check (no HashSet allocation
        // on the common no-seeding path).
        if (options.Seeded is { Count: > 0 } seededResults)
        {
            var effectiveIds = effective.Select(n => n.Id).ToHashSet();
            var collisions = seededResults.Select(r => r.Id).Where(effectiveIds.Contains).ToList();
            if (collisions.Count > 0)
            {
                throw new ArgumentException(
                    "RunOptions.Seeded results must be outside the run's effective set; " +
                    $"found seeded id(s) also present in the effective set: {string.Join(", ", collisions.Select(id => id.Value))}.",
                    nameof(options));
            }
        }

        // Root span for the whole run, wrapping everything below via
        // this using-declaration's scope (which is the rest of the method, both the happy path and the
        // outer catch's Fatal-report path) so it always closes exactly once, on every exit. With no
        // listener registered anywhere (the common case — see PzActivitySource), StartActivity returns
        // null and every access below is a null-conditional no-op.
        using var runActivity = PzActivitySource.Instance.StartActivity("run");
        runActivity?.SetTag("pz.run.id", ctx.Paths.RunId);
        runActivity?.SetTag("pz.run.project", options.ProjectName);

        var runStopwatch = Stopwatch.StartNew();
        var gate = new object();
        var resultsOrdered = new List<NodeResult>();
        var terminal = new HashSet<NodeId>();
        var anyFailed = false;

        var semaphore = new SemaphoreSlim(options.MaxConcurrency);
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        // Sentinel-counted "outstanding work" latch: starts at 1 (the initial-dispatch phase
        // itself) so that spawning zero nodes (e.g. an empty effective set) still completes the
        // latch instead of hanging forever; released once the initial dispatch loop finishes.
        var outstanding = 1;
        var idle = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        void Finish()
        {
            if (Interlocked.Decrement(ref outstanding) == 0)
            {
                idle.TrySetResult();
            }
        }

        void RecordTerminal(NodeResult result)
        {
            bool recorded;
            lock (gate)
            {
                recorded = terminal.Add(result.Id);
                if (recorded)
                {
                    resultsOrdered.Add(result);
                    if (result.Status == NodeStatus.Failed)
                    {
                        anyFailed = true;
                    }
                }
            }

            if (recorded)
            {
                // Best-effort: a throwing subscriber must not break scheduler control flow (in
                // particular, must not prevent the Skip-cascade the caller runs right after this).
                ctx.Events.SafeNodeCompleted(result);
            }
        }

        void RecordSkip(DagNode node) => RecordTerminal(NodeResult.Skipped(node));

        try
        {
            ctx.Events.SafeRunStarted(ctx.Paths.RunId, options.ProjectName, effective.Count);

            // Carried-forward results enter the run's record first —
            // after run_started (events.md ordering: exactly one run_started opens the stream) and
            // before any node is dispatched. RecordTerminal gives them the same one-NodeResult-one-
            // NodeCompleted treatment as executed nodes; their ids are outside the effective set, so
            // the completeness sweep and the skip-cascade never touch them.
            foreach (var seeded in options.Seeded ?? [])
            {
                RecordTerminal(seeded);
            }

            var byId = effective.ToDictionary(n => n.Id);
            var children = BuildChildAdjacency(effective, byId);
            var remaining = effective.ToDictionary(n => n.Id, n => n.DependsOn.Count(byId.ContainsKey));

            // One SemaphoreSlim per capped instance PRESENT IN THIS RUN's effective set (never one per
            // declared source/sink in the project -- an instance with no node in this run needs no
            // permit pool). Initial count = the declared cap; a node whose instance isn't in this
            // dictionary (uncapped, or a Pipeline/Check node with no instance at all) skips instance
            // gating entirely in RunNodeAsync below.
            var instanceSemaphores = BuildInstanceSemaphores(effective);

            void CascadeSkip(DagNode failedNode)
            {
                foreach (var descendant in dag.Descendants(failedNode.Id))
                {
                    if (byId.ContainsKey(descendant.Id))
                    {
                        RecordSkip(descendant);
                    }
                }
            }

            async Task RunNodeAsync(DagNode node)
            {
                try
                {
                    // Instance permit BEFORE the global one: a capped instance's queued nodes must not
                    // hold a global slot while waiting their turn (that would let one hot instance
                    // starve every other node in the run). Acquired/released strictly outside the global
                    // permit's own try/finally below, so release order is the exact reverse of acquire
                    // order (global released first, instance released last -- LIFO). Cancellation while
                    // waiting here takes the same RecordSkip path the global wait already uses -- no
                    // permit was granted, so there is nothing to release.
                    var instanceKey = InstanceKey.For(node);
                    var instanceSemaphore = instanceKey is null ? null : instanceSemaphores.GetValueOrDefault(instanceKey);
                    if (instanceSemaphore is not null)
                    {
                        try
                        {
                            await instanceSemaphore.WaitAsync(linkedCts.Token).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            RecordSkip(node);
                            return;
                        }
                    }

                    try
                    {
                        try
                        {
                            await semaphore.WaitAsync(linkedCts.Token).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException)
                        {
                            RecordSkip(node);
                            return;
                        }

                        try
                        {
                            // A failure observed on another node between this node's readiness check
                            // and its permit acquisition is caught here so a just-cancelled run
                            // doesn't start pointless new work; a node already past this check when
                            // cancellation fires still runs (cooperative — it observes ct itself).
                            if (linkedCts.IsCancellationRequested)
                            {
                                RecordSkip(node);
                                return;
                            }

                            // The node span wraps ONLY the executor invocation —
                            // a node skipped via either check above never gets one, which is deliberate
                            // (no work was ever dispatched for it). Status is tagged from the NodeResult
                            // (or "skipped" on cancellation) rather than by catching exceptions here: the
                            // span itself must never alter the no-throw contract.
                            //
                            // Parent is the RUN span's context explicitly (never ambient Activity.Current):
                            // a node whose completion makes a child ready dispatches that child's
                            // RunNodeAsync synchronously, from inside OnChildMaybeReady, which runs BEFORE
                            // this node's own span (below) is disposed — ambient Activity.Current at that
                            // point would still be this node's span, wrongly nesting the child's node span
                            // under it instead of making it a sibling under run.
                            using var nodeActivity = PzActivitySource.Instance.StartActivity(
                                SpanNames[node.Kind], ActivityKind.Internal, runActivity?.Context ?? default);
                            nodeActivity?.SetTag("pz.node.id", node.Id.Value);
                            nodeActivity?.SetTag("pz.node.name", node.Name);

                            NodeResult result;
                            try
                            {
                                result = await executor.ExecuteAsync(node, ctx, linkedCts.Token).ConfigureAwait(false);
                            }
                            catch (OperationCanceledException)
                            {
                                nodeActivity?.SetTag("pz.node.status", "skipped");
                                RecordSkip(node);
                                return;
                            }
                            catch (Exception ex)
                            {
                                // Defensive: INodeExecutor implementations are expected to wrap their
                                // own exceptions into a Failed NodeResult (KindDispatchingExecutor
                                // does); this is a safety net so a non-conforming executor can't hang
                                // the dispatcher's completion latch.
                                // Same raw-message risk as KindDispatchingExecutor's own terminal wrap --
                                // redact a foreign exception before it reaches run_results.json/NDJSON via
                                // this NodeResult's Error.Message; a Pz-family exception passes through
                                // unredacted (see MessageRedaction.Redact(Exception)'s trust boundary doc).
                                result = new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0,
                                    TimeSpan.Zero,
                                    new PzError(PzErrorCode.NodeFailed, MessageRedaction.Redact(ex), null, null, null));
                            }

                            nodeActivity?.SetTag("pz.node.status", StatusTag(result.Status));
                            if (result.Status == NodeStatus.Failed)
                            {
                                nodeActivity?.SetStatus(ActivityStatusCode.Error);
                            }

                            if (result.Status != NodeStatus.Skipped)
                            {
                                PzMeters.NodeDuration.Record(result.Duration.TotalMilliseconds, NodeKindTags[result.Kind]);
                            }

                            RecordTerminal(result);

                            if (result.Status == NodeStatus.Failed)
                            {
                                CascadeSkip(node);
                                if (options.FailFast)
                                {
                                    linkedCts.Cancel();
                                }
                            }
                            else
                            {
                                foreach (var childId in children.GetValueOrDefault(node.Id, []))
                                {
                                    OnChildMaybeReady(childId);
                                }
                            }
                        }
                        finally
                        {
                            semaphore.Release();
                        }
                    }
                    finally
                    {
                        instanceSemaphore?.Release();
                    }
                }
                finally
                {
                    Finish();
                }
            }

            void OnReady(DagNode node)
            {
                if (linkedCts.IsCancellationRequested)
                {
                    RecordSkip(node);
                    return;
                }

                Interlocked.Increment(ref outstanding);
                _ = RunNodeAsync(node);
            }

            void OnChildMaybeReady(NodeId childId)
            {
                var ready = false;
                lock (gate)
                {
                    if (!terminal.Contains(childId) && --remaining[childId] == 0)
                    {
                        ready = true;
                    }
                }

                if (ready)
                {
                    OnReady(byId[childId]);
                }
            }

            var initial = effective.Where(n => remaining[n.Id] == 0)
                .OrderBy(n => KindRank(n.Kind))
                .ThenBy(n => n.Name, StringComparer.Ordinal);
            foreach (var node in initial)
            {
                OnReady(node);
            }

            Finish(); // release the initial-dispatch sentinel
            await idle.Task.ConfigureAwait(false);

            // FailFast completeness: a node that is skipped via a cancelled
            // semaphore wait (RunNodeAsync's OperationCanceledException branch, or OnReady's early
            // "already cancelled" check) never dispatches its own children — those children can be
            // left in "remaining > 0, never became ready" limbo forever, with no NodeResult of their
            // own, even though CascadeSkip already covers every descendant of a node that failed
            // outright. This sweep is the completeness backstop dbt semantics require: every node in
            // the effective set gets exactly one NodeResult by the time the run winds down, however it
            // got there.
            foreach (var node in effective)
            {
                if (!terminal.Contains(node.Id))
                {
                    RecordSkip(node);
                }
            }

            var status = ct.IsCancellationRequested && !anyFailed ? RunStatus.Fatal
                : anyFailed ? RunStatus.CompletedWithFailures
                : RunStatus.Success;

            int succeeded, failed, skipped;
            lock (gate)
            {
                succeeded = resultsOrdered.Count(r => r.Status == NodeStatus.Success);
                failed = resultsOrdered.Count(r => r.Status == NodeStatus.Failed);
                skipped = resultsOrdered.Count(r => r.Status == NodeStatus.Skipped);
            }

            ctx.Events.SafeRunCompleted(ctx.Paths.RunId, status, succeeded, failed, skipped, runStopwatch.Elapsed);

            // Happy path only: by the time idle.Task has completed, every dispatched RunNodeAsync has
            // released its semaphore permit(s) and returned (the outstanding-latch guarantees this), so
            // nothing can still be waiting on `semaphore`, any entry of `instanceSemaphores`, or
            // observing `linkedCts`. The outer catch below deliberately skips this disposal — an
            // orchestrator-level bug escaping mid-flight means some spawned RunNodeAsync tasks may not
            // have unwound yet, and disposing here could race a task still holding/awaiting these
            // objects.
            semaphore.Dispose();
            foreach (var instanceSemaphore in instanceSemaphores.Values)
            {
                instanceSemaphore.Dispose();
            }

            linkedCts.Dispose();

            return new RunResult(ctx.Paths.RunId, resultsOrdered, status);
        }
        catch (Exception)
        {
            // Unhandled orchestrator-level bug (not a node failure, not cancellation): report
            // Fatal with whatever partial results were recorded so far rather than rethrowing.
            List<NodeResult> partial;
            lock (gate)
            {
                partial = [.. resultsOrdered];
            }

            var partialSucceeded = partial.Count(r => r.Status == NodeStatus.Success);
            var partialFailed = partial.Count(r => r.Status == NodeStatus.Failed);
            var partialSkipped = partial.Count(r => r.Status == NodeStatus.Skipped);
            ctx.Events.SafeRunCompleted(ctx.Paths.RunId, RunStatus.Fatal, partialSucceeded, partialFailed,
                partialSkipped, runStopwatch.Elapsed);

            return new RunResult(ctx.Paths.RunId, partial, RunStatus.Fatal);
        }
    }

    /// <summary>Selection (when non-null) expanded to include every required ancestor — run-time
    /// semantics, deliberately different from compile's selection-exact. Order preserved from
    /// <see cref="CompiledDag.TopologicalOrder"/>.</summary>
    private static IReadOnlyList<DagNode> ComputeEffectiveSet(CompiledDag dag, IReadOnlySet<NodeId>? selection)
    {
        if (selection is null)
        {
            return [.. dag.TopologicalOrder()];
        }

        var byId = dag.TopologicalOrder().ToDictionary(n => n.Id);
        var include = new HashSet<NodeId>(selection);
        var queue = new Queue<NodeId>(selection);
        while (queue.Count > 0)
        {
            var id = queue.Dequeue();
            if (!byId.TryGetValue(id, out var node))
            {
                continue;
            }

            foreach (var dep in node.DependsOn)
            {
                if (include.Add(dep))
                {
                    queue.Enqueue(dep);
                }
            }
        }

        return [.. dag.TopologicalOrder().Where(n => include.Contains(n.Id))];
    }

    /// <summary>One <see cref="SemaphoreSlim"/> per distinct
    /// <see cref="InstanceKey"/> in <paramref name="effective"/> whose source/sink declares
    /// <c>max_concurrency</c>, sized to that cap. An instance with multiple nodes in the effective set
    /// (e.g. several datasets of one source) shares a single pool -- looked up again from the first
    /// node's Definition since instance-level fields are identical across every one of that instance's
    /// nodes. Instances absent from this dictionary (uncapped, or Pipeline/Check with no instance at
    /// all) are never gated in <c>RunNodeAsync</c>.</summary>
    private static Dictionary<string, SemaphoreSlim> BuildInstanceSemaphores(IReadOnlyList<DagNode> effective)
    {
        var semaphores = new Dictionary<string, SemaphoreSlim>();
        foreach (var node in effective)
        {
            var key = InstanceKey.For(node);
            if (key is null || semaphores.ContainsKey(key))
            {
                continue;
            }

            int? cap = node.Definition switch
            {
                SourceDatasetDef def => def.Source.MaxConcurrency,
                SinkOutputDef def => def.Sink.MaxConcurrency,
                _ => null,
            };

            if (cap is int c)
            {
                semaphores[key] = new SemaphoreSlim(c, c);
            }
        }

        return semaphores;
    }

    private static Dictionary<NodeId, List<NodeId>> BuildChildAdjacency(
        IReadOnlyList<DagNode> nodes, Dictionary<NodeId, DagNode> byId)
    {
        var map = new Dictionary<NodeId, List<NodeId>>();
        foreach (var node in nodes)
        {
            foreach (var dep in node.DependsOn)
            {
                if (!byId.ContainsKey(dep))
                {
                    continue;
                }

                if (!map.TryGetValue(dep, out var list))
                {
                    list = [];
                    map[dep] = list;
                }

                list.Add(node.Id);
            }
        }

        return map;
    }

    /// <summary>Lowercase primitive status value for the "node.&lt;kind&gt;" span's status tag —
    /// mirrors the vocabulary <c>https://pipelinez.dev/events/</c> already uses for NDJSON's status field.</summary>
    private static string StatusTag(NodeStatus status) => status switch
    {
        NodeStatus.Success => "success",
        NodeStatus.Failed => "failed",
        NodeStatus.Skipped => "skipped",
        _ => throw new ArgumentOutOfRangeException(nameof(status), status, "unknown node status"),
    };

    /// <summary>Matches <c>DagCompiler</c>'s (kindRank, Name ordinal) tie-break so simultaneously-
    /// ready nodes are dispatched in the same deterministic order compile-time ordering uses.</summary>
    private static int KindRank(NodeKind kind) => kind switch
    {
        NodeKind.SourceLoad => 0,
        NodeKind.Pipeline => 1,
        NodeKind.Check => 2,
        NodeKind.SinkWrite => 3,
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };
}
