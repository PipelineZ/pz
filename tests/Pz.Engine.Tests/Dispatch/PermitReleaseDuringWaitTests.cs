using System.Collections.Concurrent;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Engine.Dispatch;
using Pz.Engine.Execution;
using Pz.Engine.Resilience;
using Pz.Engine.State;
using Pz.Engine.Tests.Execution;
using Pz.Engine.Tests.Resilience;

namespace Pz.Engine.Tests.Dispatch;

/// <summary>A node waiting out a retry backoff or an open breaker holds no connector session, so it hands its
/// dispatch permits back for the length of the wait. Every wait here is an injected delay the test parks on a
/// <see cref="TaskCompletionSource"/>; no clock is involved.</summary>
public sealed class PermitReleaseDuringWaitTests
{
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(30);

    /// <summary>Exact backoff: a 1s Retry-After, capped at 30s, jitter pinned to zero.</summary>
    private static readonly RetryPolicy Policy = new(MaxAttempts: 3, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30));

    private static ConnectionDef Conn(string name, int? cap = null) =>
        new(name, "test", new Dictionary<string, object?>(), [], $"sources/{name}.yml", MaxConcurrency: cap);

    private static DagNode Load(string id, string name, ConnectionDef conn) =>
        new(new NodeId(id.PadLeft(16, '0')), NodeKind.SourceLoad, name, [], null,
            new SourceDatasetDef(conn, new DatasetDef(name, new Dictionary<string, object?>(), null)));

    private static RunContext Ctx(IRunEvents? events = null) => new(null!, new ConnectorRegistry(),
        new RunPaths(Path.GetTempPath(), "t"), events ?? NullRunEvents.Instance);

    private static NodeResult Success(DagNode node) =>
        new(node.Id, node.Kind, node.Name, NodeStatus.Success, 1, TimeSpan.Zero, null);

    private static PzConnectorException Throttled() =>
        new("throttled", isTransient: true, retryAfter: TimeSpan.FromSeconds(1));

    private static TaskCompletionSource Tcs() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static KindDispatchingExecutor Executor(
        Func<DagNode, int, CancellationToken, Task<NodeResult>> run, Func<TimeSpan, CancellationToken, Task> delay) =>
        new(Policy, new FixedRandom(0.5), delay) { ExecutorOverride = _ => new Scripted(run) };

    /// <summary>Hands the per-kind call to <c>run</c> with the attempt number the dispatching executor published.</summary>
    private sealed class Scripted(Func<DagNode, int, CancellationToken, Task<NodeResult>> run) : INodeExecutor
    {
        public Task<NodeResult> ExecuteAsync(DagNode node, RunContext ctx, CancellationToken ct) =>
            run(node, ctx.Attempts[node.Id], ct);
    }

    /// <summary>"a" fails transiently on attempt 1 and succeeds after; every other node runs <c>other</c>.</summary>
    private static Func<DagNode, int, CancellationToken, Task<NodeResult>> AThenOther(
        Func<DagNode, CancellationToken, Task<NodeResult>> other, ConcurrentQueue<string>? log = null) =>
        (node, attempt, ct) =>
        {
            if (node.Name != "a")
            {
                return other(node, ct);
            }

            log?.Enqueue($"a-{attempt}");
            return attempt == 1 ? Task.FromException<NodeResult>(Throttled()) : Task.FromResult(Success(node));
        };

    private sealed class RecordingEvents : IRunEvents
    {
        private readonly ConcurrentDictionary<string, TaskCompletionSource<NodeResult>> _completed = new();
        public readonly ConcurrentQueue<string> Retries = new();

        public Task<NodeResult> Completed(string name) =>
            _completed.GetOrAdd(name, _ => new(TaskCreationOptions.RunContinuationsAsynchronously)).Task;

        public void RunStarted(string runId, string projectName, int nodeCount) { }
        public void NodeStarted(DagNode node) { }
        public void NodeProgress(DagNode node, long rowsSoFar, long bytesSoFar, long batchesSoFar) { }
        public void RetryScheduled(DagNode node, int attempt, int maxAttempts, TimeSpan delay, string reason) =>
            Retries.Enqueue(node.Name);
        public void BreakerStateChanged(string instance, string oldState, string newState, string trigger, TimeSpan coolDown) { }
        public void SourceDriftDetected(DagNode node, string connection, string entity, string policy,
            IReadOnlyList<SchemaDriftDiffer.Change> changes, IReadOnlyList<SchemaColumn> observed, string hintsHash) { }
        public void MergeKeyDuplicatesDetected(DagNode node, string output, IReadOnlyList<string> keys,
            long duplicateGroups, long extraRows) { }
        public void LossyIntegerInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns) { }
        public void AmbiguousDateInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns, string format) { }
        public void NodeCompleted(NodeResult result) =>
            _completed.GetOrAdd(result.Name, _ => new(TaskCreationOptions.RunContinuationsAsynchronously)).TrySetResult(result);
        public void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped, TimeSpan duration) { }
        public void ConnectorLog(string connection, string level, string message) { }
    }

    [Fact]
    public async Task Unrelated_node_runs_while_a_node_waits_out_its_retry_backoff()
    {
        var backoffEntered = Tcs();
        var releaseBackoff = Tcs();
        var bDone = Tcs();
        var bStartedDuringBackoff = false;
        var executor = Executor(AThenOther((node, _) =>
            {
                bStartedDuringBackoff = backoffEntered.Task.IsCompleted && !releaseBackoff.Task.IsCompleted;
                bDone.TrySetResult();
                return Task.FromResult(Success(node));
            }),
            (_, ct) =>
            {
                backoffEntered.TrySetResult();
                return releaseBackoff.Task.WaitAsync(ct);
            });
        var dag = new CompiledDag([Load("1", "a", Conn("src-a")), Load("2", "b", Conn("src-b"))]);

        var run = new RunOrchestrator(executor, Ctx()).ExecuteAsync(dag, new RunOptions(MaxConcurrency: 1), default);
        try
        {
            await backoffEntered.Task.WaitAsync(Guard);
            await bDone.Task.WaitAsync(Guard);
            Assert.True(bStartedDuringBackoff);
        }
        finally
        {
            releaseBackoff.TrySetResult();
        }

        var result = await run.WaitAsync(Guard);

        Assert.Equal(RunStatus.Success, result.Status);
        Assert.Equal(2, result.Nodes.Single(r => r.Name == "a").Facts!.Attempts);
    }

    [Fact]
    public async Task Same_instance_node_runs_while_a_node_waits_out_its_retry_backoff()
    {
        var backoffEntered = Tcs();
        var releaseBackoff = Tcs();
        var bDone = Tcs();
        var bStartedDuringBackoff = false;
        var executor = Executor(AThenOther((node, _) =>
            {
                bStartedDuringBackoff = backoffEntered.Task.IsCompleted && !releaseBackoff.Task.IsCompleted;
                bDone.TrySetResult();
                return Task.FromResult(Success(node));
            }),
            (_, ct) =>
            {
                backoffEntered.TrySetResult();
                return releaseBackoff.Task.WaitAsync(ct);
            });
        var shared = Conn("shared", cap: 1);
        var dag = new CompiledDag([Load("1", "a", shared), Load("2", "b", shared)]);

        var run = new RunOrchestrator(executor, Ctx()).ExecuteAsync(dag, new RunOptions(MaxConcurrency: 8), default);
        try
        {
            await backoffEntered.Task.WaitAsync(Guard);
            await bDone.Task.WaitAsync(Guard);
            Assert.True(bStartedDuringBackoff);
        }
        finally
        {
            releaseBackoff.TrySetResult();
        }

        var result = await run.WaitAsync(Guard);

        Assert.Equal(RunStatus.Success, result.Status);
        Assert.Equal(2, result.Nodes.Single(r => r.Name == "a").Facts!.Attempts);
    }

    [Fact]
    public async Task Unrelated_node_runs_while_a_node_waits_for_an_open_breaker()
    {
        var time = new ManualTimeProvider();
        var breakers = new BreakerRegistry(new BreakerConfig(1, TimeSpan.FromMinutes(1)), time);
        var a = Load("1", "a", Conn("src-a"));
        var b = Load("2", "b", Conn("src-b"));
        var aBreaker = breakers.For(a)!;
        Assert.True(aBreaker.TryEnter(out _, out var ticket));
        aBreaker.RecordTransientFailure(ticket, null); // threshold 1: open for a minute

        var waitEntered = Tcs();
        var releaseWait = Tcs();
        var bDone = Tcs();
        var bStartedDuringWait = false;
        var executor = Executor((node, _, _) =>
            {
                if (node.Name == "b")
                {
                    bStartedDuringWait = waitEntered.Task.IsCompleted && !releaseWait.Task.IsCompleted;
                    bDone.TrySetResult();
                }

                return Task.FromResult(Success(node));
            },
            (_, ct) =>
            {
                waitEntered.TrySetResult();
                return releaseWait.Task.WaitAsync(ct);
            });

        var run = new RunOrchestrator(executor, Ctx() with { Breakers = breakers })
            .ExecuteAsync(new CompiledDag([a, b]), new RunOptions(MaxConcurrency: 1), default);
        try
        {
            await waitEntered.Task.WaitAsync(Guard);
            await bDone.Task.WaitAsync(Guard);
            Assert.True(bStartedDuringWait);
        }
        finally
        {
            time.Advance(TimeSpan.FromMinutes(1)); // the cool-down is over: a's probe is let through
            releaseWait.TrySetResult();
        }

        var result = await run.WaitAsync(Guard);

        Assert.Equal(RunStatus.Success, result.Status);
        Assert.Equal(1, result.Nodes.Single(r => r.Name == "a").Facts!.Attempts);
    }

    [Fact]
    public async Task Run_cancelled_during_backoff_skips_the_node_and_releases_nothing_twice()
    {
        var backoffEntered = Tcs();
        var executor = Executor(AThenOther((node, _) => Task.FromResult(Success(node))),
            (_, ct) =>
            {
                backoffEntered.TrySetResult();
                return Task.Delay(Timeout.Infinite, ct);
            });
        using var cts = new CancellationTokenSource();

        var run = new RunOrchestrator(executor, Ctx())
            .ExecuteAsync(new CompiledDag([Load("1", "a", Conn("src-a", cap: 1))]), new RunOptions(MaxConcurrency: 1), cts.Token);
        await backoffEntered.Task.WaitAsync(Guard);
        cts.Cancel();

        var result = await run.WaitAsync(Guard);

        Assert.Equal(NodeStatus.Skipped, Assert.Single(result.Nodes).Status);
    }

    [Fact]
    public async Task Backoff_ending_while_permits_are_taken_waits_then_retries()
    {
        var backoffEntered = Tcs();
        // Synchronous continuations: SetResult returns only once "a" is queued for its permits again.
        var releaseBackoff = new TaskCompletionSource();
        var bEntered = Tcs();
        var bGate = Tcs();
        var log = new ConcurrentQueue<string>();
        var events = new RecordingEvents();
        var executor = Executor(AThenOther(async (node, _) =>
            {
                bEntered.TrySetResult();
                await bGate.Task;
                log.Enqueue("b-end");
                return Success(node);
            }, log),
            (_, _) =>
            {
                backoffEntered.TrySetResult();
                return releaseBackoff.Task;
            });
        var dag = new CompiledDag([Load("1", "a", Conn("src-a")), Load("2", "b", Conn("src-b"))]);

        var run = new RunOrchestrator(executor, Ctx(events)).ExecuteAsync(dag, new RunOptions(MaxConcurrency: 1), default);
        try
        {
            await backoffEntered.Task.WaitAsync(Guard);
            await bEntered.Task.WaitAsync(Guard);
            releaseBackoff.SetResult();
            Assert.DoesNotContain("a-2", log);
        }
        finally
        {
            releaseBackoff.TrySetResult();
            bGate.TrySetResult();
        }

        var result = await run.WaitAsync(Guard);

        Assert.Equal(RunStatus.Success, result.Status);
        Assert.Equal(["a-1", "b-end", "a-2"], log.ToArray());
        Assert.Equal(2, result.Nodes.Single(r => r.Name == "a").Facts!.Attempts);
        Assert.Equal(["a"], events.Retries.ToArray());
    }

    [Fact]
    public async Task Run_cancelled_while_reacquiring_after_backoff_skips_the_node()
    {
        var backoffEntered = Tcs();
        var releaseBackoff = new TaskCompletionSource();
        var bEntered = Tcs();
        var bGate = Tcs();
        var log = new ConcurrentQueue<string>();
        var events = new RecordingEvents();
        var executor = Executor(AThenOther(async (node, _) =>
            {
                bEntered.TrySetResult();
                await bGate.Task; // deliberately ignores the run token: b is mid-work and finishes
                return Success(node);
            }, log),
            (_, _) =>
            {
                backoffEntered.TrySetResult();
                return releaseBackoff.Task;
            });
        var dag = new CompiledDag([Load("1", "a", Conn("src-a")), Load("2", "b", Conn("src-b"))]);
        using var cts = new CancellationTokenSource();

        var run = new RunOrchestrator(executor, Ctx(events)).ExecuteAsync(dag, new RunOptions(MaxConcurrency: 1), cts.Token);
        NodeResult aResult;
        try
        {
            await backoffEntered.Task.WaitAsync(Guard);
            await bEntered.Task.WaitAsync(Guard);
            releaseBackoff.SetResult();
            cts.Cancel();
            aResult = await events.Completed("a").WaitAsync(Guard);
        }
        finally
        {
            releaseBackoff.TrySetResult();
            bGate.TrySetResult();
        }

        var result = await run.WaitAsync(Guard);

        Assert.Equal(NodeStatus.Skipped, aResult.Status);
        Assert.Equal(NodeStatus.Success, result.Nodes.Single(r => r.Name == "b").Status);
        Assert.Equal(["a-1"], log.ToArray());
        Assert.Equal(2, result.Nodes.Count);
    }

    [Fact]
    public async Task Executor_without_a_lease_waits_as_before()
    {
        var delays = new ConcurrentQueue<TimeSpan>();
        var a = Load("1", "a", Conn("src-a"));
        var executor = Executor(AThenOther((node, _) => Task.FromResult(Success(node))),
            (d, _) =>
            {
                delays.Enqueue(d);
                return Task.CompletedTask;
            });

        var result = await executor.ExecuteAsync(a, Ctx(), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(2, result.Facts!.Attempts);
        Assert.Equal([Policy.ComputeDelay(1, TimeSpan.FromSeconds(1), new FixedRandom(0.5))], delays.ToArray());
    }

    /// <summary>Capped semaphores: a release past the initial count throws <see cref="SemaphoreFullException"/>.</summary>
    [Fact]
    public async Task Lease_cancelled_on_the_global_permit_gives_back_the_instance_permit()
    {
        using var global = new SemaphoreSlim(0, 1);
        using var instance = new SemaphoreSlim(1, 1);
        using var cts = new CancellationTokenSource();
        var lease = new DispatchLease(global, instance);

        var acquiring = lease.AcquireAsync(cts.Token);
        Assert.Equal(0, instance.CurrentCount);
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => acquiring);
        Assert.Equal(1, instance.CurrentCount);
        lease.Release();
        Assert.Equal(1, instance.CurrentCount);
    }

    [Fact]
    public async Task Lease_cancelled_while_released_holds_nothing_and_releases_nothing_twice()
    {
        using var global = new SemaphoreSlim(1, 1);
        using var instance = new SemaphoreSlim(1, 1);
        using var cts = new CancellationTokenSource();
        var lease = new DispatchLease(global, instance);
        await lease.AcquireAsync(default);

        var waiting = lease.WhileReleasedAsync(t => Task.Delay(Timeout.Infinite, t), cts.Token);
        Assert.Equal((1, 1), (global.CurrentCount, instance.CurrentCount));
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        lease.Release();
        lease.Release();
        Assert.Equal((1, 1), (global.CurrentCount, instance.CurrentCount));
    }

    [Fact]
    public async Task Lease_is_current_only_inside_its_scope()
    {
        using var global = new SemaphoreSlim(1, 1);
        var lease = new DispatchLease(global, null);
        Assert.Null(DispatchLease.Current);

        using (DispatchLease.Use(lease))
        {
            await Task.Yield();
            Assert.Same(lease, DispatchLease.Current);
        }

        Assert.Null(DispatchLease.Current);
    }
}
