namespace Pz.Connector.Http.Tests;

/// <summary>A settable fake clock; <see cref="GetUtcNow"/> only moves through <see cref="Advance"/>.</summary>
internal sealed class ManualClock : TimeProvider
{
    private DateTimeOffset _utcNow = DateTimeOffset.UnixEpoch;

    public override DateTimeOffset GetUtcNow() => _utcNow;

    public void Advance(TimeSpan by) => _utcNow += by;
}
