using System.Reflection;

namespace Pz.Connectors.Sdk;

/// <summary>This SDK's own name and version -- what a connector built against it reports in its
/// handshake (<c>Hello.sdk</c>) and its packaged manifest (<c>pz.connector.json</c>'s <c>sdk</c>
/// property), so a host can name which SDK a hosted connector runs, not just the connector's own
/// declared identity. Distinct from the connector's own <see cref="Pz.Connectors.Abstractions.ConnectorInfo.Version"/>.</summary>
internal static class SdkInfo
{
    public const string Name = "Pz.Connectors.Sdk";

    /// <summary>MinVer-derived, the same version number this repo's <c>pz</c> CLI itself carries --
    /// both are built from one git tag stream.</summary>
    public static readonly string Version =
        typeof(SdkInfo).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
        ?? "unknown";
}
