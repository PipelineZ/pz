namespace Pz.PackageManagement.Hosting;

/// <summary>Answers questions about materialized connector packages straight off disk, without loading
/// any assembly — the same reason <see cref="ManifestReader"/> exists, applied to the whole
/// <c>.pz/packages</c> tree rather than one package.</summary>
public static class PackageManifests
{
    /// <summary>Connector names whose package manifest declares
    /// <see cref="ConnectorManifest.ProjectDirectoryAnchor"/>. Empty when
    /// <paramref name="packagesDir"/> does not exist — a project whose packages are not restored yet
    /// simply finds no manifests, and nothing here fails without them.
    ///
    /// <para>A broken manifest is skipped rather than thrown on: it IS an error, but the connector
    /// host owns reporting it with its own code and hint when it loads the package. Failing here would replace that message with this one, raised earlier and explaining
    /// less.</para></summary>
    public static IReadOnlySet<string> AnchoredConnectorNames(string packagesDir)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        if (!Directory.Exists(packagesDir))
        {
            return names;
        }

        // .pz/packages/<id>/<version>. Enumerated rather than derived from the lock file so this stays a
        // pure filesystem read with no restore-state coupling; a version directory that is a symlink into
        // the content-addressed cache resolves transparently.
        foreach (var idDir in SafeEnumerate(packagesDir))
        {
            foreach (var versionDir in SafeEnumerate(idDir))
            {
                ConnectorManifest? manifest;
                try
                {
                    manifest = ManifestReader.TryRead(versionDir);
                }
                catch (ConnectorHostException)
                {
                    continue;
                }

                if (manifest is { ProjectDirectoryAnchor: true, Name: { Length: > 0 } name })
                {
                    names.Add(name);
                }
            }
        }

        return names;
    }

    private static IEnumerable<string> SafeEnumerate(string dir)
    {
        try
        {
            return Directory.EnumerateDirectories(dir);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return [];
        }
    }
}
