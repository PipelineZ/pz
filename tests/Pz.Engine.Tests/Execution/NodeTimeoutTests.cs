using Pz.Core.Dag;
using Pz.Core.Validation;
using Pz.Engine.Dispatch;
using Pz.Engine.Execution;
using Pz.Engine.Resilience;

namespace Pz.Engine.Tests.Execution;

/// <summary><c>engine.node_timeout</c> bounds one attempt of one node. No test here waits on a clock:
/// the executor's delay seam decides whether the timeout "fires", and the fake node decides whether
/// it honours the cancellation that follows.</summary>
public sealed class NodeTimeoutTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromMinutes(30);

    private static readonly DagNode Node = new(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.Pipeline,
        "stg_orders", [], null, "stg_orders");

    private static RunContext Ctx(TimeSpan? timeout) => new(null!, new ConnectorRegistry(),
        new RunPaths(Path.GetTempPath(), "t"), NullRunEvents.Instance) { NodeTimeout = timeout };

    /// <summary>Every delay elapses at once: the timeout fires and the grace period after it runs out.</summary>
    private static Task Elapsed(TimeSpan _, CancellationToken __) => Task.CompletedTask;

    /// <summary>The timeout fires at once, but the grace period that follows it never runs out — a node
    /// that honours cancellation always gets to unwind.</summary>
    private static Task TimeoutElapses(TimeSpan duration, CancellationToken ct) =>
        duration == Timeout ? Task.CompletedTask : Never(duration, ct);

    /// <summary>No delay ever elapses on its own, so a timeout never fires.</summary>
    private static Task Never(TimeSpan _, CancellationToken ct) => Task.Delay(System.Threading.Timeout.Infinite, ct);

    private static KindDispatchingExecutor Executor(
        INodeExecutor inner, Func<TimeSpan, CancellationToken, Task> delay, RetryPolicy? policy = null) =>
        new(policy, delay: delay) { ExecutorOverride = _ => inner };

    private sealed class Scripted(Func<DagNode, CancellationToken, Task<NodeResult>> run) : INodeExecutor
    {
        public int Calls;

        public Task<NodeResult> ExecuteAsync(DagNode node, RunContext ctx, CancellationToken ct)
        {
            Interlocked.Increment(ref Calls);
            return run(node, ct);
        }
    }

    private static NodeResult Success(DagNode node) =>
        new(node.Id, node.Kind, node.Name, NodeStatus.Success, 1, TimeSpan.Zero, null);

    [Fact]
    public async Task A_node_that_outlives_the_timeout_is_cancelled_and_fails_with_PZ0525()
    {
        var sawCancel = false;
        var inner = new Scripted(async (node, ct) =>
        {
            try
            {
                await Task.Delay(System.Threading.Timeout.Infinite, ct);
            }
            catch (OperationCanceledException)
            {
                sawCancel = true;
                throw;
            }

            return Success(node);
        });

        var result = await Executor(inner, TimeoutElapses).ExecuteAsync(Node, Ctx(Timeout), default);

        Assert.True(sawCancel);
        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal(PzErrorCode.NodeTimedOut, result.Error!.Code);
        Assert.Contains("stg_orders", result.Error.Message);
        Assert.Contains("30m", result.Error.Message);
        Assert.Contains("engine.node_timeout", result.Error.Hint);
    }

    /// <summary>A cancelled attempt can leave half-built staging behind that a second attempt in the
    /// same run would collide with, so a timeout ends the node; `pz retry` reruns it cleanly.</summary>
    [Fact]
    public async Task A_timed_out_node_is_not_retried_in_the_same_run()
    {
        var inner = new Scripted(async (node, ct) =>
        {
            await Task.Delay(System.Threading.Timeout.Infinite, ct);
            return Success(node);
        });
        var policy = new RetryPolicy(MaxAttempts: 3, TimeSpan.Zero, TimeSpan.Zero);

        var result = await Executor(inner, TimeoutElapses, policy).ExecuteAsync(Node, Ctx(Timeout), default);

        Assert.Equal(PzErrorCode.NodeTimedOut, result.Error!.Code);
        Assert.Equal(1, inner.Calls);
    }

    [Fact]
    public async Task A_node_that_finishes_in_time_is_untouched()
    {
        var inner = new Scripted((node, _) => Task.FromResult(Success(node)));

        var result = await Executor(inner, Never).ExecuteAsync(Node, Ctx(Timeout), default);

        Assert.Equal(NodeStatus.Success, result.Status);
    }

    [Fact]
    public async Task With_no_timeout_configured_the_node_runs_on_the_run_token_itself()
    {
        using var run = new CancellationTokenSource();
        CancellationToken seen = default;
        var inner = new Scripted((node, ct) =>
        {
            seen = ct;
            return Task.FromResult(Success(node));
        });

        await Executor(inner, Elapsed).ExecuteAsync(Node, Ctx(timeout: null), run.Token);

        Assert.Equal(run.Token, seen);
    }

    /// <summary>Work that ignores cancellation may still hold the run's one DuckDB connection or a
    /// connector handle; nothing else in the run can be trusted to make progress, so this is not an
    /// ordinary node failure.</summary>
    [Fact]
    public async Task A_node_that_ignores_cancellation_is_PZ0526_unresponsive()
    {
        var never = new TaskCompletionSource<NodeResult>();
        var inner = new Scripted((_, _) => never.Task);

        var ex = await Assert.ThrowsAsync<NodeUnresponsiveException>(
            () => Executor(inner, Elapsed).ExecuteAsync(Node, Ctx(Timeout), default));

        Assert.Equal(PzErrorCode.NodeUnresponsive, ex.Error.Code);
        Assert.Contains("stg_orders", ex.Error.Message);
    }

    [Fact]
    public async Task Run_cancellation_while_a_node_is_in_flight_stays_a_cancellation()
    {
        using var run = new CancellationTokenSource();
        var entered = new TaskCompletionSource();
        var inner = new Scripted(async (node, ct) =>
        {
            entered.SetResult();
            await Task.Delay(System.Threading.Timeout.Infinite, ct);
            return Success(node);
        });

        var pending = Executor(inner, Never).ExecuteAsync(Node, Ctx(Timeout), run.Token);
        await entered.Task;
        run.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
    }
}
