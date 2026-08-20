using Pz.Connectors.Abstractions;
using Pz.Core.Model;
using Pz.Engine.Resilience;
using Pz.Engine.Tests.Execution;

namespace Pz.Engine.Tests.Resilience;

/// <summary><see cref="OperationGate"/>'s op-level retry loop,
/// pacing integration, and <see cref="OpStats"/> accounting. Every non-pacing test uses <see cref="Gate"/>
/// — a zero-delay seam like <c>RetryingExecutionTests</c>' node-level equivalent — so retry
/// correctness is proven via attempt/delay/stat counts, never wall-clock timing. Pacing tests build
/// the gate directly against a <see cref="ManualTimeProvider"/>-backed <see cref="InstancePacingState"/>,
/// whose delay func ALSO advances the manual clock by the requested wait: <see cref="OperationGate"/>
/// loops on <see cref="InstancePacingState.TryAcquire"/> inside <c>PaceAsync</c>, so a delay func that
/// does not advance time would spin forever once a positive wait is returned.</summary>
public class OperationGateTests
{
    private static OperationGate Gate(RetryPolicy? p = null, InstancePacingState? pacing = null,
        List<TimeSpan>? delays = null)
    {
        var recorded = delays ?? [];
        return new OperationGate(p ?? RetryPolicy.Default, pacing, new FixedRandom(0.5),
            (d, _) =>
            {
                recorded.Add(d);
                return Task.CompletedTask;
            });
    }

    private static PzConnectorException Transient(TimeSpan? retryAfter = null) =>
        new("op boom", isTransient: true, retryAfter);

    [Fact]
    public async Task Idempotent_transient_retries_under_policy()
    {
        var policy = new RetryPolicy(3, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30));
        var delays = new List<TimeSpan>();
        var gate = Gate(policy, delays: delays);
        var calls = 0;

        var result = await gate.ExecuteAsync("op", true, _ =>
        {
            calls++;
            return calls <= 2 ? throw Transient() : Task.FromResult(42);
        }, default);

        Assert.Equal(42, result);
        Assert.Equal([TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2)], delays);
        Assert.Equal(new OpStats(3, 2, 0), gate.Snapshot());
    }

    [Fact]
    public async Task RetryAfter_replaces_backoff()
    {
        var delays = new List<TimeSpan>();
        var gate = Gate(delays: delays);
        var calls = 0;

        var result = await gate.ExecuteAsync("op", true, _ =>
        {
            calls++;
            return calls == 1 ? throw Transient(TimeSpan.FromSeconds(7)) : Task.FromResult(1);
        }, default);

        Assert.Equal(1, result);
        Assert.Equal(TimeSpan.FromSeconds(7), delays[0]);
    }

    [Fact]
    public async Task Exhaustion_rethrows_last_transient()
    {
        var policy = new RetryPolicy(3, TimeSpan.Zero, TimeSpan.Zero);
        var gate = Gate(policy);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            gate.ExecuteAsync<int>("op", true, _ => throw Transient(), default));

        Assert.True(ex.IsTransient);
        Assert.Equal(3, gate.Snapshot().Executed);
    }

    [Fact]
    public async Task Non_idempotent_executes_once()
    {
        var gate = Gate();

        await Assert.ThrowsAsync<PzConnectorException>(() =>
            gate.ExecuteAsync<int>("op", false, _ => throw Transient(), default));

        Assert.Equal(new OpStats(1, 0, 0), gate.Snapshot());
    }

    [Fact]
    public async Task Non_transient_never_retries()
    {
        var gate = Gate();

        await Assert.ThrowsAsync<PzConnectorException>(() =>
            gate.ExecuteAsync<int>("op", true, _ => throw new PzConnectorException("op boom", isTransient: false), default));

        Assert.Equal(1, gate.Snapshot().Executed);
    }

    [Fact]
    public async Task Cancellation_propagates_unretried()
    {
        var gate = Gate();

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            gate.ExecuteAsync<int>("op", true, _ => throw new OperationCanceledException(), default));

        Assert.Equal(0, gate.Snapshot().Retried);
    }

    [Fact]
    public async Task Pacing_waits_accumulate()
    {
        var time = new ManualTimeProvider();
        var pacing = new InstancePacingState(new RateLimitDef(60, 1), time);
        var delays = new List<TimeSpan>();
        var gate = new OperationGate(RetryPolicy.Default, pacing, new FixedRandom(0.5), (d, _) =>
        {
            delays.Add(d);
            time.Advance(d);
            return Task.CompletedTask;
        });

        await gate.ExecuteAsync("op1", true, _ => Task.FromResult(1), default); // drains the one token
        await gate.ExecuteAsync("op2", true, _ => Task.FromResult(2), default); // must wait for refill

        Assert.Equal(TimeSpan.FromSeconds(1), Assert.Single(delays));
        Assert.Equal(1000, gate.Snapshot().ThrottleWaitMs);
    }

    [Fact]
    public async Task Retry_consumes_fresh_token()
    {
        var time = new ManualTimeProvider();
        var pacing = new InstancePacingState(new RateLimitDef(60, 1), time);
        var delays = new List<TimeSpan>();
        var policy = new RetryPolicy(3, TimeSpan.Zero, TimeSpan.Zero); // zero backoff isolates the pacing wait
        var gate = new OperationGate(policy, pacing, new FixedRandom(0.5), (d, _) =>
        {
            delays.Add(d);
            time.Advance(d);
            return Task.CompletedTask;
        });
        var calls = 0;

        var result = await gate.ExecuteAsync("op", true, _ =>
        {
            calls++;
            return calls == 1 ? throw Transient() : Task.FromResult(9);
        }, default);

        Assert.Equal(9, result);
        Assert.Equal(2, gate.Snapshot().Executed);
        Assert.Contains(TimeSpan.FromSeconds(1), delays); // the retry attempt paid a fresh pacing wait
        Assert.Equal(1000, gate.Snapshot().ThrottleWaitMs);
    }

    [Fact]
    public async Task ReportBudget_forwards_to_pacing()
    {
        var time = new ManualTimeProvider();
        var pacing = new InstancePacingState(null, time);
        var delays = new List<TimeSpan>();
        var gate = new OperationGate(RetryPolicy.Default, pacing, new FixedRandom(0.5), (d, _) =>
        {
            delays.Add(d);
            time.Advance(d);
            return Task.CompletedTask;
        });

        gate.ReportBudget(0, time.GetUtcNow() + TimeSpan.FromSeconds(5));
        var result = await gate.ExecuteAsync("op", true, _ => Task.FromResult(1), default);

        Assert.Equal(1, result);
        Assert.Equal(TimeSpan.FromSeconds(5), Assert.Single(delays));
        Assert.Equal(5000, gate.Snapshot().ThrottleWaitMs);
    }

    [Fact]
    public async Task ReportBudget_without_pacing_is_noop()
    {
        var gate = Gate();

        gate.ReportBudget(0, DateTimeOffset.UtcNow.AddSeconds(5)); // must not throw

        var result = await gate.ExecuteAsync("op", true, _ => Task.FromResult(1), default);

        Assert.Equal(1, result);
        Assert.Equal(new OpStats(1, 0, 0), gate.Snapshot());
    }
}
