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

    /// <summary>Awaited inside Configure's critical section, after the "already configured" check has
    /// passed but before <c>_config</c> is set. Lets a test hold one Configure call there while a
    /// second, concurrent one proves it is blocked on the same gate (not racing the first to see
    /// <c>_config</c> as null too) until this releases.</summary>
    public Func<CancellationToken, Task>? PauseInsideConfigure { get; init; }

    /// <summary>GetReadStats answers with an INTERNAL error, so a test can prove the host fails closed when it
    /// cannot learn whether a read was skipped.</summary>
    public bool FailReadStats { get; init; }

    /// <summary>GetReadStats answers UNIMPLEMENTED, as a connector built before the call existed would.</summary>
    public bool UnimplementedReadStats { get; init; }

    /// <summary>The process exits inside GetReadStats, so a test can prove the host reports the exit, not the
    /// failed call.</summary>
    public bool ExitOnReadStats { get; init; }
}
