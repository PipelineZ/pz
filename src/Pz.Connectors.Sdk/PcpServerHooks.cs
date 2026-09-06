namespace Pz.Connectors.Sdk;

/// <summary>Test seams, reachable only through the internal RunAsync overload. Each one stages a
/// misbehaving connector the host must survive: a handshake that never answers, a Cancel that is
/// neither acknowledged nor honored, a Shutdown that is acknowledged and then ignored.</summary>
internal sealed class PcpServerHooks
{
    public static readonly PcpServerHooks None = new();

    public Func<CancellationToken, Task>? HangHandshake { get; init; }
    public bool IgnoreCancel { get; init; }
    public bool IgnoreShutdown { get; init; }
}
