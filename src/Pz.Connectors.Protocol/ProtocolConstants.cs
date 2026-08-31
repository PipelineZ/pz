namespace Pz.Connectors.Protocol;

/// <summary>Fixed v1 protocol constants (spec: fixed in v1, manifest overrides deferred).</summary>
public static class ProtocolConstants
{
    public static readonly TimeSpan HandshakeTimeout = TimeSpan.FromSeconds(15);
    public static readonly TimeSpan ShutdownGrace = TimeSpan.FromSeconds(10);
    public static readonly TimeSpan CancelGrace = TimeSpan.FromSeconds(5);
    public const int TicketLength = 16;
    public const string TransportPipe = "pipe";
    /// <summary>Reserved transport name for the future remote/Flight case. Never offered in v1.</summary>
    public const string TransportFlightReserved = "flight";
    public const string ErrorDetailTrailerKey = "pz-error-bin";
    /// <summary>Data-plane socket path = control socket path + this suffix.</summary>
    public const string DataSocketSuffix = ".data";
}
