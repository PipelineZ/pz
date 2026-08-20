using System.Text.Json;

namespace Pz.PackageManagement.Hosting;

/// <summary>A connector's declared protocol-compatibility manifest, read from
/// <c>pz.connector.json</c> at the root of a materialized package directory.</summary>
public sealed record ConnectorManifest(
    string? Name, int ProtocolMajorMin, int ProtocolMajorMax, IReadOnlyList<string> Capabilities);

/// <summary>Reads a connector package's <c>pz.connector.json</c> manifest, if any, WITHOUT loading any
/// assembly — this is what lets <see cref="ConnectorHost.LoadFromDirectory"/> reject an
/// incompatible-protocol package before creating an <see cref="ConnectorLoadContext"/> at all.</summary>
public static class ManifestReader
{
    private static readonly JsonSerializerOptions Options = new() { PropertyNameCaseInsensitive = true };

    /// <summary>Reads <c>&lt;packageDir&gt;/pz.connector.json</c>. Returns null when the file is absent
    /// (the warn-and-attempt seam in <see cref="ConnectorHost"/> handles that case). A present-but-broken
    /// manifest — malformed JSON, or a <c>protocolMajorMin</c> greater than <c>protocolMajorMax</c> —
    /// signals a broken package and throws <see cref="ConnectorHostException"/> PZ0306 rather than
    /// silently ignoring it.</summary>
    public static ConnectorManifest? TryRead(string packageDir)
    {
        var path = Path.Combine(packageDir, "pz.connector.json");
        if (!File.Exists(path))
        {
            return null;
        }

        ManifestDto? dto;
        try
        {
            var bytes = File.ReadAllBytes(path);
            dto = JsonSerializer.Deserialize<ManifestDto>(bytes, Options);
        }
        catch (JsonException ex)
        {
            throw new ConnectorHostException(
                "PZ0306",
                $"pz.connector.json at '{path}' is malformed: {ex.Message}",
                "fix the manifest JSON, or remove the file to fall back to the no-manifest warn-and-load path");
        }

        if (dto is null)
        {
            throw new ConnectorHostException(
                "PZ0306",
                $"pz.connector.json at '{path}' is malformed: empty or 'null' JSON document",
                "fix the manifest JSON, or remove the file to fall back to the no-manifest warn-and-load path");
        }

        if (dto.ProtocolMajorMin > dto.ProtocolMajorMax)
        {
            throw new ConnectorHostException(
                "PZ0306",
                $"pz.connector.json at '{path}' declares an inverted protocol range (protocolMajorMin {dto.ProtocolMajorMin} > protocolMajorMax {dto.ProtocolMajorMax})",
                "fix the manifest's protocolMajorMin/protocolMajorMax ordering");
        }

        return new ConnectorManifest(dto.Name, dto.ProtocolMajorMin, dto.ProtocolMajorMax, dto.Capabilities ?? []);
    }

    private sealed class ManifestDto
    {
        public string? Name { get; set; }
        public int ProtocolMajorMin { get; set; }
        public int ProtocolMajorMax { get; set; }
        public List<string>? Capabilities { get; set; }
    }
}
