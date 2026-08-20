namespace Pz.Connectors.Abstractions;

/// <summary>The connector protocol version this host/ABI assembly speaks.</summary>
public static class ProtocolVersion
{
    /// <summary>Incremented only on breaking ABI changes. Connectors declare their major in <see cref="ConnectorInfo.ProtocolMajor"/>.</summary>
    public const int Major = 1;
}
