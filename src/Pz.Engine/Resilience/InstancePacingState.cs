using Pz.Core.Model;

namespace Pz.Engine.Resilience;

/// <summary>One instance's shared pacing state — a token bucket (only when
/// the instance declares rate_limit:) plus a proactive budget hint (always available, so
/// ReportBudget works with no rate_limit: configured). Shared across nodes, partitions, and
/// attempts for the whole run via RateLimiterRegistry. Thread-safe via a plain lock; all time
/// through TimeProvider (GetTimestamp for bucket refill, GetUtcNow for the hint's resetAt).
/// TryAcquire never blocks — it returns the wait the CALLER must apply (through its injectable
/// delay) before trying again, so pacing waits stay testable and cancellable at the gate.</summary>
public sealed class InstancePacingState(RateLimitDef? rateLimit, TimeProvider time)
{
    private readonly Lock _gate = new();
    private readonly double _capacity = rateLimit?.EffectiveBurst ?? 0;
    private readonly double _refillPerSecond = rateLimit is null ? 0 : rateLimit.RequestsPerMinute / 60.0;
    private double _tokens = rateLimit?.EffectiveBurst ?? 0; // bucket starts full
    private long _lastRefillAt = time.GetTimestamp();
    private (int Remaining, DateTimeOffset ResetAt)? _budget;

    /// <summary>Bounds a misbehaving server's budget hint: a
    /// resetAt hours/days out — or DateTimeOffset.MaxValue — would otherwise be honored uncapped,
    /// silently wedging every node of this instance with no event and no breaker trip. Clamping the
    /// stored resetAt here means a wedged instance stalls at most 15 minutes per hint, and it also
    /// makes the caller's Task.Delay(TryAcquire()) overflow-impossible.</summary>
    private static readonly TimeSpan MaxBudgetWait = TimeSpan.FromMinutes(15);

    public bool HasBucket { get; } = rateLimit is not null;

    /// <summary>Zero: an operation may proceed (one token consumed when a bucket exists).
    /// Positive: wait this long, then call again (budget hint first, then bucket deficit).</summary>
    public TimeSpan TryAcquire()
    {
        lock (_gate)
        {
            if (_budget is { Remaining: 0 } hint)
            {
                var now = time.GetUtcNow();
                if (now < hint.ResetAt)
                {
                    return hint.ResetAt - now;
                }

                _budget = null; // resetAt passed: hint cleared.
            }

            if (!HasBucket)
            {
                return TimeSpan.Zero;
            }

            var timestamp = time.GetTimestamp();
            var elapsed = time.GetElapsedTime(_lastRefillAt, timestamp);
            _tokens = Math.Min(_capacity, _tokens + (elapsed.TotalSeconds * _refillPerSecond));
            _lastRefillAt = timestamp;

            if (_tokens >= 1.0)
            {
                _tokens -= 1.0;
                return TimeSpan.Zero;
            }

            return TimeSpan.FromSeconds((1.0 - _tokens) / _refillPerSecond);
        }
    }

    /// <summary>Last writer wins; remaining &gt; 0 is recorded but has no pacing effect.
    /// resetAt is clamped to at most <see cref="MaxBudgetWait"/> from now (the engine bounds a
    /// single hint's wait to a sanity cap) — a provider's hint can request an early reset, never a
    /// later one than the cap.</summary>
    public void ReportBudget(int remaining, DateTimeOffset resetAt)
    {
        lock (_gate)
        {
            var now = time.GetUtcNow();
            var clampedResetAt = resetAt - now > MaxBudgetWait ? now + MaxBudgetWait : resetAt;
            _budget = (remaining, clampedResetAt);
        }
    }
}
