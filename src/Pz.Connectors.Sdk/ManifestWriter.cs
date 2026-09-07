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
            CapabilityNames(connector.Capabilities),
            projectDirectoryAnchor,
            "process",
            new SortedDictionary<string, string>(
                entrypoints.ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal),
                StringComparer.Ordinal));
        return JsonSerializer.Serialize(document, Contract.ManifestDocument) + "\n";
    }

    /// <summary>Member names of the set flags, in ascending flag order -- the order the flags
    /// enum's own ToString yields, which is also how the host spells a Hello's capabilities when it
    /// compares them to a manifest.</summary>
    public static IReadOnlyList<string> CapabilityNames(ConnectorCapabilities capabilities) =>
        capabilities == ConnectorCapabilities.None
            ? []
            : capabilities.ToString().Split(", ", StringSplitOptions.RemoveEmptyEntries);
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
    [property: JsonPropertyName("entrypoints")] SortedDictionary<string, string> Entrypoints);

[JsonSerializable(typeof(ManifestDocument))]
internal sealed partial class SdkJsonContext : JsonSerializerContext;
