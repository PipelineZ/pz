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

    /// <summary>Invoked inside the Configure RPC, after the config is stored and before the
    /// "connector configured" log -- i.e. at the one place a real config value crosses into the
    /// connector. A decorator around the connector object cannot reach this moment (Configure is the
    /// SDK's own RPC handler, not a connector method), which is why this needs a hook rather than a
    /// staged connector wrapper like every other switch in this class.</summary>
    public Action? OnConfigure { get; init; }
}
