using System.Text.Json;
using Pz.PackageManagement.Restore;

namespace Pz.PackageManagement.Hosting;

/// <summary>A connector's declared protocol-compatibility manifest, read from
/// <c>pz.connector.json</c> at the root of a materialized package directory.
///
/// <para><paramref name="ProjectDirectoryAnchor"/> is the connector asking the host to supply the
/// project directory as a <c>base_dir</c> connection option, so a relative path in its config resolves
/// against the project rather than against the process working directory. Opt-in and defaulted false: a
/// manifest that says nothing receives nothing, so the option can never collide with a
/// <c>ConnectionConfigSchema</c> that does not declare it. A connector that opts in must declare
/// <c>base_dir</c> in that schema.</para>
///
/// <para><paramref name="Runtime"/> selects how the connector is hosted: null or <c>"dotnet"</c> means
/// the existing in-process <c>ConnectorLoadContext</c> path (byte-identical behavior); <c>"process"</c>
/// means an out-of-process host, spawned from the RID-selected entry in <paramref name="Entrypoints"/>
/// (package-relative paths, resolved via <see cref="ManifestReader.ResolveEntrypoint"/>). Any other
/// value is a runtime this host does not understand and is rejected at read time (PZ0354, "upgrade
/// pz").</para></summary>
public sealed record ConnectorManifest(
    string? Name, int ProtocolMajorMin, int ProtocolMajorMax, IReadOnlyList<string> Capabilities,
    bool ProjectDirectoryAnchor = false, string? Runtime = null,
    IReadOnlyDictionary<string, string>? Entrypoints = null)
{
    /// <summary>RID → package-relative entrypoint path. Never null, even when <see cref="Runtime"/> is
    /// null/<c>"dotnet"</c> (empty in that case) — callers never null-check it.</summary>
    public IReadOnlyDictionary<string, string> Entrypoints { get; init; } = Entrypoints ?? EmptyEntrypoints;

    private static readonly IReadOnlyDictionary<string, string> EmptyEntrypoints =
        new Dictionary<string, string>();
}

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

        if (dto.Runtime is not (null or "dotnet" or "process"))
        {
            throw new ConnectorHostException(
                "PZ0354",
                $"pz.connector.json at '{path}' declares unknown runtime '{dto.Runtime}'",
                "upgrade pz to a version that understands this connector's runtime, or pin an older connector version");
        }

        if (dto.Runtime == "process" && (dto.Entrypoints is null || dto.Entrypoints.Count == 0))
        {
            throw new ConnectorHostException(
                "PZ0354",
                $"pz.connector.json at '{path}' declares runtime 'process' but no entrypoints",
                "fix the manifest's entrypoints map (RID -> package-relative binary path), or rebuild the connector package");
        }

        return new ConnectorManifest(
            dto.Name, dto.ProtocolMajorMin, dto.ProtocolMajorMax, dto.Capabilities ?? [],
            dto.ProjectDirectoryAnchor, dto.Runtime, dto.Entrypoints);
    }

    /// <summary>Resolves <paramref name="rid"/> against <paramref name="manifest"/>'s <c>entrypoints</c>
    /// map to an absolute binary path, walking <see cref="RuntimeIdentifierGraph"/>'s fallback ancestry
    /// when there is no exact match (so a package shipping only <c>linux-x64</c> is still reachable from
    /// <c>linux-musl-x64</c>). Throws <see cref="ConnectorHostException"/> PZ0354 when nothing in the
    /// fallback chain has an entry.
    ///
    /// <para>Every caller that resolves an entrypoint needs it to actually be spawnable — the process
    /// host, before its first <c>OpenAsync</c>, and <c>pz connector test</c>'s own target resolution —
    /// so this is the one place both paths go through, and the one place that guarantees it, rather than
    /// leaving each caller to remember.</para></summary>
    public static string ResolveEntrypoint(ConnectorManifest manifest, string packageDir, string rid)
    {
        foreach (var candidate in RuntimeIdentifierGraph.Expand(rid))
        {
            if (manifest.Entrypoints.TryGetValue(candidate, out var relativePath))
            {
                var entrypoint = Path.Combine(packageDir, relativePath);
                EnsureExecutable(entrypoint);
                return entrypoint;
            }
        }

        throw new ConnectorHostException(
            "PZ0354",
            $"connector package '{manifest.Name ?? packageDir}' ships no binary for RID '{rid}'",
            $"this connector ships no binary for {rid}; add an entrypoints entry for it, or restore a build that supports this platform");
    }

    /// <summary>A restored package's entrypoint often reaches disk with the Unix executable bit
    /// missing: a <c>.nupkg</c> is a zip archive, and neither <c>NuGet.Packaging.PackageBuilder</c> nor
    /// a plain zip writer sets the Unix executable permission in an entry's external attributes unless a
    /// packer goes out of its way to (the same reason <c>dotnet tool install</c> has always had to chmod
    /// its own tool binaries after extraction). Silent no-op when the path does not exist (yet) or on
    /// Windows (no such concept) — callers still own reporting a missing entrypoint as PZ0354 themselves,
    /// this only ever ACTS on a file that is actually there.
    ///
    /// <para>Owner-execute only: the cache entry a restored package's <c>native/</c> asset lives in
    /// (<c>PackageMaterializer</c>'s content-addressed cache) is shared across every project that
    /// restores the same package, but nothing needs group/other execute to run it — the pz process tree
    /// always runs as the one user that did the restore.</para></summary>
    private static void EnsureExecutable(string entrypoint)
    {
        if (OperatingSystem.IsWindows() || !File.Exists(entrypoint))
        {
            return;
        }

        var mode = File.GetUnixFileMode(entrypoint);
        var executable = mode | UnixFileMode.UserExecute;
        if (executable != mode)
        {
            File.SetUnixFileMode(entrypoint, executable);
        }
    }

    private sealed class ManifestDto
    {
        public string? Name { get; set; }
        public int ProtocolMajorMin { get; set; }
        public int ProtocolMajorMax { get; set; }
        public List<string>? Capabilities { get; set; }
        public bool ProjectDirectoryAnchor { get; set; }
        public string? Runtime { get; set; }
        public Dictionary<string, string>? Entrypoints { get; set; }
    }
}
