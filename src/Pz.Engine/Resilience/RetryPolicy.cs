namespace Pz.Engine.Resilience;

/// <summary>Retry policy fed to <see cref="Pz.Engine.Execution.KindDispatchingExecutor"/>
/// — the only place a <see cref="Pz.Connectors.Abstractions.PzConnectorException"/> is still an exception with
/// <c>IsTransient</c>/<c>RetryAfter</c> intact (any outer decorator only sees the already-wrapped PZ0501
/// <c>NodeResult</c>). <see cref="Default"/> is 3 attempts, 1s base, 30s cap.</summary>
public sealed record RetryPolicy(int MaxAttempts, TimeSpan BaseDelay, TimeSpan MaxDelay)
{
    public static readonly RetryPolicy Default = new(3, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30));

    /// <summary>Delay before the retry attempt that follows the <paramref name="attempt"/>-th failure
    /// (1-based: <paramref name="attempt"/> = 1 is the delay after the FIRST failure, before the second
    /// try). When <paramref name="retryAfter"/> is present it replaces the exponential backoff entirely
    /// (still subject to the cap and jitter below) — a connector's own guidance always wins over the
    /// engine's guess. Exponential backoff is <c>BaseDelay * 2^(attempt-1)</c>, capped at
    /// <see cref="MaxDelay"/>, then jittered by ±25% using <paramref name="jitter"/> so tests can inject
    /// a deterministic source (a stub whose <c>NextDouble()</c> returns 0.5 produces exactly zero jitter,
    /// i.e. the capped/backoff value verbatim) — production code passes a real <see cref="Random"/>.</summary>
    public TimeSpan ComputeDelay(int attempt, TimeSpan? retryAfter, Random jitter)
    {
        var baseline = retryAfter ?? TimeSpan.FromMilliseconds(BaseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1));
        var capped = baseline > MaxDelay ? MaxDelay : baseline;

        // NextDouble() in [0,1) -> jitter factor in [0.75, 1.25); 0.5 (the stub-friendly midpoint) maps
        // to exactly 1.0, i.e. no jitter at all.
        var jitterFactor = 1.0 + ((jitter.NextDouble() - 0.5) * 0.5);
        var jitteredMs = capped.TotalMilliseconds * jitterFactor;
        return TimeSpan.FromMilliseconds(Math.Max(0, jitteredMs));
    }
}
