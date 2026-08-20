namespace Pz.Engine.Tests.Resilience;

/// <summary>A settable fake clock for <see cref="TimeProvider.GetTimestamp"/>/<see cref="TimeProvider.GetElapsedTime(long)"/>
/// AND <see cref="TimeProvider.GetUtcNow"/>. Ticks are tracked 1:1 with <see cref="TimeSpan.Ticks"/>
/// (frequency = <see cref="TimeSpan.TicksPerSecond"/>), so <see cref="Advance"/> maps directly onto
/// elapsed time with no unit conversion to reason about in tests, and both clocks advance together.
/// Used by <c>CircuitBreakerTests</c> and by <c>InstancePacingStateTests</c>, whose budget hint compares
/// <c>GetUtcNow()</c> against <c>resetAt</c>; no other test-tree fake exposes a settable
/// <c>GetTimestamp()</c>/<c>TimestampFrequency</c> (<c>RunEventPublisherTests</c>'
/// <c>FixedTimeProvider</c> only overrides <c>GetUtcNow()</c>).</summary>
internal sealed class ManualTimeProvider : TimeProvider
{
    private long _ticks;
    private DateTimeOffset _utcNow = DateTimeOffset.UnixEpoch;

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public override long GetTimestamp() => _ticks;

    public override DateTimeOffset GetUtcNow() => _utcNow;

    public void Advance(TimeSpan by)
    {
        _ticks += by.Ticks;
        _utcNow += by;
    }
}
