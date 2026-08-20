using Pz.Connectors.Abstractions;

namespace Pz.Engine.Resilience;

/// <summary>Aggregate per-node-attempt operation stats. ThrottleWaitMs counts pacing
/// waits only (bucket + budget hint), never retry backoff.</summary>
public sealed record OpStats(long Executed, long Retried, long ThrottleWaitMs);

/// <summary>The engine's IOperationGate. One instance per node execution,
/// created by SourceLoadExecutor/SinkWriteExecutor for an IOperationGateAware connector. Reuses
/// the node's resolved RetryPolicy verbatim for op-level delays (RetryAfter replaces backoff,
/// ±25% jitter, cap — identical semantics to the node loop). Op exhaustion rethrows the last
/// transient exception, surfacing as ONE transient node failure — the breaker never sees
/// op-level failures, and the multiplicative worst case is bounded at
/// MaxAttempts (ops) × MaxAttempts (node). Thread-safe: partitions may call concurrently.</summary>
public sealed class OperationGate(RetryPolicy policy, InstancePacingState? pacing,
    Random jitter, Func<TimeSpan, CancellationToken, Task> delay) : IOperationGate
{
    private long _executed;
    private long _retried;
    private long _throttleWaitMs;

    public OpStats Snapshot() => new(
        Interlocked.Read(ref _executed), Interlocked.Read(ref _retried), Interlocked.Read(ref _throttleWaitMs));

    public void ReportBudget(int remaining, DateTimeOffset resetAt) => pacing?.ReportBudget(remaining, resetAt);

    public async Task<T> ExecuteAsync<T>(string opLabel, bool idempotent,
        Func<CancellationToken, Task<T>> op, CancellationToken ct)
    {
        var attempt = 1;
        while (true)
        {
            await PaceAsync(ct).ConfigureAwait(false);
            Interlocked.Increment(ref _executed);
            try
            {
                return await op(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw; // never retried, never counted as a failure — mirrors the node loop.
            }
            catch (PzConnectorException ex) when (ex.IsTransient && idempotent && attempt < policy.MaxAttempts)
            {
                var backoff = policy.ComputeDelay(attempt, ex.RetryAfter, jitter);
                Interlocked.Increment(ref _retried);
                await delay(backoff, ct).ConfigureAwait(false);
                attempt++;
            }
        }
    }

    private async Task PaceAsync(CancellationToken ct)
    {
        if (pacing is null)
        {
            return;
        }

        while (true)
        {
            var wait = pacing.TryAcquire();
            if (wait <= TimeSpan.Zero)
            {
                return;
            }

            Interlocked.Add(ref _throttleWaitMs, (long)wait.TotalMilliseconds);
            await delay(wait, ct).ConfigureAwait(false);
        }
    }
}
