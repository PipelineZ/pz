using System.Text.Json;
using System.Text.Json.Serialization;
using Pz.Connectors.Abstractions;

namespace Pz.Connectors.Sdk;

/// <summary>Renders <c>pz.connector.json</c> from the connector object itself, so the manifest and
/// the handshake's Hello can never disagree: both read <see cref="IConnector.Info"/> and
/// <see cref="IConnector.Capabilities"/>, and both spell capabilities through
/// <see cref="CapabilityNames"/>. Byte-stable: LF, two-space indent, fixed key order, entrypoints
/// sorted ordinal by RID, final newline.</summary>
internal static class ManifestWriter
{
    /// <summary>The source-generated contract bound to this writer's formatting. Serializing
    /// through the generated JsonTypeInfo rather than the reflection overload is what keeps the SDK
    /// trim- and AOT-clean.</summary>
    private static readonly SdkJsonContext Contract = new(new JsonSerializerOptions
    {
        WriteIndented = true,
        NewLine = "\n",
    });

    public static string Render(
        IConnector connector, IReadOnlyDictionary<string, string> entrypoints, bool projectDirectoryAnchor = false)
    {
        var document = new ManifestDocument(
            connector.Info.Name,
            ProtocolVersion.Major,
            ProtocolVersion.Major,
            CapabilityNames(DeclaredCapabilities(connector)),
            projectDirectoryAnchor,
            "process",
            new SortedDictionary<string, string>(
                entrypoints.ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal),
                StringComparer.Ordinal),
            new SdkDocument(SdkInfo.Name, SdkInfo.Version));
        return JsonSerializer.Serialize(document, Contract.ManifestDocument) + "\n";
    }

    /// <summary>What the connector declares, plus what only its .NET type says. A marker interface
    /// does not cross a process boundary, so a source that is <see cref="INativeOnlySource"/> is given
    /// <see cref="ConnectorCapabilities.NativeOnlyRead"/> here -- once, for the manifest and the
    /// handshake alike, because the host refuses a Hello whose capabilities differ from the manifest's.</summary>
    public static ConnectorCapabilities DeclaredCapabilities(IConnector connector) =>
        connector is INativeOnlySource
            ? connector.Capabilities | ConnectorCapabilities.NativeOnlyRead
            : connector.Capabilities;

    /// <summary>Every bit this build's <see cref="ConnectorCapabilities"/> defines, OR'd together.
    /// Masking a value against this before decomposing it is what keeps one undefined bit from making
    /// <see cref="Enum.ToString()"/> fall back to the raw decimal number for the whole value instead of
    /// a name list -- the same hazard <c>PcpClient.CapabilityNames</c> guards against on the host side.
    /// A connector never sets a bit outside its own build's enum, but a manifest is read by other pz
    /// builds too, so this stays the one place capability names are ever rendered.</summary>
    private static readonly ConnectorCapabilities KnownCapabilities =
        Enum.GetValues<ConnectorCapabilities>().Aggregate(ConnectorCapabilities.None, (acc, v) => acc | v);

    /// <summary>Member names of the set flags, in ascending flag order -- the order the flags
    /// enum's own ToString yields, which is also how the host spells a Hello's capabilities when it
    /// compares them to a manifest.</summary>
    public static IReadOnlyList<string> CapabilityNames(ConnectorCapabilities capabilities)
    {
        var known = capabilities & KnownCapabilities;
        return known == ConnectorCapabilities.None
            ? []
            : known.ToString().Split(", ", StringSplitOptions.RemoveEmptyEntries);
    }
}

internal sealed record ManifestDocument(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("protocolMajorMin")] int ProtocolMajorMin,
    [property: JsonPropertyName("protocolMajorMax")] int ProtocolMajorMax,
    [property: JsonPropertyName("capabilities")] IReadOnlyList<string> Capabilities,
    // Omit-when-false: a manifest that says nothing about the anchor must serialize identically to one
    // written before this field existed, and the host defaults ConnectorManifest.ProjectDirectoryAnchor
    // to false for exactly that reason. Placed between capabilities and runtime -- the same slot the
    // host's own ManifestDto declares it in.
    [property: JsonPropertyName("projectDirectoryAnchor"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    bool ProjectDirectoryAnchor,
    [property: JsonPropertyName("runtime")] string Runtime,
    [property: JsonPropertyName("entrypoints")] SortedDictionary<string, string> Entrypoints,
    // Last, always present: the newest field, and unlike projectDirectoryAnchor there is no
    // "unset" shape worth omitting -- every manifest this writer produces names its own SDK.
    [property: JsonPropertyName("sdk")] SdkDocument Sdk);

/// <summary>Which SDK produced the manifest and at what version -- distinct from
/// <see cref="ManifestDocument.Name"/> (the CONNECTOR's own identity). Mirrors
/// <c>Hello.sdk</c>, so the manifest and the handshake never disagree about which SDK built this
/// connector, the same guarantee <see cref="ManifestWriter"/>'s own doc comment already makes for
/// name and capabilities.</summary>
internal sealed record SdkDocument(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("version")] string Version);

[JsonSerializable(typeof(ManifestDocument))]
internal sealed partial class SdkJsonContext : JsonSerializerContext;
