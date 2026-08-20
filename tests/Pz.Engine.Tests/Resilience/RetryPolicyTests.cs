using Pz.Engine.Resilience;

namespace Pz.Engine.Tests.Resilience;

public sealed class RetryPolicyTests
{
    /// <summary>Jitter-free stub: <c>NextDouble()</c> always returns 0.5, the exact midpoint that
    /// <see cref="RetryPolicy.ComputeDelay"/> maps to a jitter factor of 1.0 (no jitter at all) — lets
    /// backoff/cap tests assert exact millisecond values.</summary>
    private sealed class NoJitterRandom : Random
    {
        public override double NextDouble() => 0.5;
    }

    [Theory]
    [InlineData(1, 1_000)]
    [InlineData(2, 2_000)]
    [InlineData(3, 4_000)]
    [InlineData(4, 8_000)]
    [InlineData(5, 16_000)]
    [InlineData(6, 30_000)] // 32s would double past BaseDelay*2^5, capped at MaxDelay (30s)
    [InlineData(7, 30_000)]
    public void Backoff_doubles_and_caps(int attempt, int expectedMs)
    {
        var delay = RetryPolicy.Default.ComputeDelay(attempt, retryAfter: null, jitter: new NoJitterRandom());
        Assert.Equal(expectedMs, delay.TotalMilliseconds);
    }

    [Fact]
    public void RetryAfter_overrides_backoff()
    {
        // Attempt 1's plain backoff would be 1s; RetryAfter of 7s must win outright.
        var delay = RetryPolicy.Default.ComputeDelay(1, TimeSpan.FromSeconds(7), new NoJitterRandom());
        Assert.Equal(7_000, delay.TotalMilliseconds);
    }

    [Fact]
    public void RetryAfter_is_still_capped_at_MaxDelay()
    {
        var delay = RetryPolicy.Default.ComputeDelay(1, TimeSpan.FromSeconds(90), new NoJitterRandom());
        Assert.Equal(30_000, delay.TotalMilliseconds);
    }

    [Theory]
    [InlineData(0.0)]
    [InlineData(1.0)]
    public void Jitter_stays_within_25pct(double extremeNextDouble)
    {
        var stub = new FixedRandom(extremeNextDouble);
        var delay = RetryPolicy.Default.ComputeDelay(1, retryAfter: null, jitter: stub);

        // Attempt 1's uncapped/unjittered backoff is exactly 1000ms; ±25% bounds are [750, 1250].
        Assert.InRange(delay.TotalMilliseconds, 750, 1250);
    }

    private sealed class FixedRandom(double value) : Random
    {
        public override double NextDouble() => value;
    }
}
