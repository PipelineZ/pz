using NuGet.Packaging;

namespace Pz.PackageManagement.Restore;

/// <summary>Populates a content-addressed cache (<c>&lt;cacheRoot&gt;/&lt;sha512&gt;/</c>) from resolved
/// packages' downloaded .nupkg files, then links or copies each into <c>&lt;packagesDir&gt;/&lt;id&gt;/
/// &lt;version&gt;</c> in the <c>ConnectorHost</c> layout (<c>lib/</c>, <c>native/</c>,
/// <c>pz.connector.json</c> when present).
///
/// <para><b>Cache entry population is atomic</b>: extracted into <c>&lt;entry&gt;.tmp-&lt;guid&gt;</c>,
/// then <see cref="Directory.Move(string, string)"/>d into place — a reader never observes a
/// partially-written entry. The last two files written into the temp dir are <c>files.txt</c> (sorted,
/// every path the entry contains, relative to the entry root) and an empty <c>.ok</c> marker; an entry
/// is only trusted if BOTH exist and every path <c>files.txt</c> lists is actually present, so deleting
/// or truncating anything inside a cache entry (out-of-band tampering, disk corruption, ...) is
/// detected on the next <see cref="Materialize"/> call and the entry is wiped and re-extracted from the
/// resolved .nupkg (which <see cref="ResolveResult.NupkgPaths"/> still points at).</para>
///
/// <para><b>Concurrent populates of the same entry converge</b>: two overlapping <c>pz restore</c>
/// processes (or, within one process, two threads) racing to populate the SAME content-addressed entry
/// both extract independently into their own temp dir, but only one <see cref="Directory.Move(string,
/// string)"/> can win; the loser's Move throws because the destination now exists. That is treated as a
/// cache hit rather than an error — the winner's entry is exactly as valid as the loser's would have
/// been (same content address) — so the loser cleans up its now-redundant temp dir and returns
/// successfully. Only a Move failure where the destination is still absent/invalid propagates.</para>
///
/// <para><b>Root ("required") vs. library packages</b> (see <see cref="LockedPackage.Requested"/>): the
/// ALC (<c>ConnectorHost</c>/<c>ConnectorLoadContext</c>) resolves a connector's private dependencies
/// only from the connector package's OWN <c>lib/</c> directory — it never looks at a sibling package's
/// directory. So a root package that has any transitive (non-root) dependencies in the resolved lock is
/// always MATERIALIZED BY COPY (never symlinked), with every library package's <c>lib/*</c> AND
/// <c>native/*</c> files also copied alongside its own — flattening the whole private dependency
/// closure into the one directory the ALC actually probes (it resolves managed assemblies from that
/// <c>lib/</c> and unmanaged ones from its sibling <c>native/</c>, never from another package's
/// directory). Two packages contributing the same file name is refused with PZ0325 rather than letting
/// one silently overwrite the other. This is a deliberate v0 simplification: <see cref="LockFile"/> does not
/// track per-root dependency edges, so a root with ANY transitive deps in the lock gets ALL of them
/// copied in, even ones a different root actually owns; harmless in practice because
/// <c>ConnectorLoadContext.Load</c> only ever loads an assembly that is actually referenced by name.
/// A root with zero transitive deps in the whole lock is instead materialized by directory symlink
/// (falling back to copy if symlink creation fails — untrusted/unsupported filesystem, no permission,
/// etc.). Library (non-root) packages are always symlink-with-copy-fallback, since nothing needs to be
/// flattened into them.</para></summary>
public static class PackageMaterializer
{
    internal static readonly Func<string, string, bool> DefaultTrySymlink = TryCreateDirectorySymlink;

    /// <summary>Test seam only (mirrors <c>ConnectorHost.OnContextCreatedForTests</c>): swap in a
    /// delegate that always fails to exercise the copy fallback deterministically, regardless of what
    /// the current OS/filesystem actually supports. Reset to <see cref="DefaultTrySymlink"/> after use.</summary>
    internal static Func<string, string, bool> TrySymlink = DefaultTrySymlink;

    /// <summary>Extracts each resolved package into the content-addressed cache (idempotent; atomic
    /// temp+move population; corrupted entries re-extracted) and links/copies it to
    /// <c>&lt;packagesDir&gt;/&lt;id&gt;/&lt;version&gt;</c> in the ConnectorHost layout (lib/, native/,
    /// pz.connector.json when present). Returns per-package "cache hit" flags (true = the cache entry
    /// already existed and was valid; false = it was downloaded/extracted or re-extracted this call).
    /// Idempotent against a pre-existing <paramref name="packagesDir"/> entry: if
    /// <c>&lt;packagesDir&gt;/&lt;id&gt;/&lt;version&gt;</c> already exists, it is left untouched.</summary>
    public static IReadOnlyDictionary<string, bool> Materialize(
        ResolveResult resolved, string cacheRoot, string packagesDir)
    {
        Directory.CreateDirectory(cacheRoot);
        Directory.CreateDirectory(packagesDir);

        var hits = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
        var entryDirs = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var package in resolved.Lock.Packages)
        {
            var (entryDir, hit) = EnsureCacheEntry(package, resolved.NupkgPaths[package.Id], cacheRoot);
            entryDirs[package.Id] = entryDir;
            hits[package.Id] = hit;
        }

        var libraryPackages = resolved.Lock.Packages.Where(p => !p.Requested).ToArray();
        var rootPackages = resolved.Lock.Packages.Where(p => p.Requested).ToArray();

        foreach (var package in libraryPackages)
        {
            MaterializeVersionDir(entryDirs[package.Id], Path.Combine(packagesDir, package.Id, package.Version), null);
        }

        foreach (var package in rootPackages)
        {
            var versionDir = Path.Combine(packagesDir, package.Id, package.Version);
            if (libraryPackages.Length == 0)
            {
                MaterializeVersionDir(entryDirs[package.Id], versionDir, null);
                continue;
            }

            var flattened = new FlattenedAssets(
                CollectTransitive(package, libraryPackages, entryDirs, "lib", a => a.Assets.Lib),
                CollectTransitive(package, libraryPackages, entryDirs, "native", a => a.Assets.Native));
            MaterializeVersionDir(entryDirs[package.Id], versionDir, flattened);
        }

        return hits;
    }

    /// <summary>The absolute cache-entry paths of every library package's assets of one role, to be
    /// flattened into <paramref name="root"/>'s own directory.
    ///
    /// <para>Two packages contributing the same file name is refused rather than resolved: the
    /// flattening is a plain copy into one directory, so "resolving" it means one build of an assembly
    /// (or one native library) silently overwriting another chosen only by enumeration order, and the
    /// connector then loads whichever won. The root package itself counts as a contributor — a root
    /// shipping a file its own dependency also ships is the same ambiguity.</para></summary>
    private static IReadOnlyList<string> CollectTransitive(
        LockedPackage root, IReadOnlyList<LockedPackage> libraryPackages,
        IReadOnlyDictionary<string, string> entryDirs, string role,
        Func<LockedPackage, IReadOnlyList<LockedAsset>> assetsOf)
    {
        var ownerByFile = assetsOf(root)
            .ToDictionary(asset => asset.File, _ => root.Id, StringComparer.OrdinalIgnoreCase);
        var paths = new List<string>();

        foreach (var library in libraryPackages)
        {
            foreach (var asset in assetsOf(library))
            {
                if (ownerByFile.TryGetValue(asset.File, out var owner))
                {
                    throw new RestoreException(
                        "PZ0325",
                        $"packages '{owner}' and '{library.Id}' both provide {role}/{asset.File}, which the " +
                        $"connector package '{root.Id}' needs flattened into one directory",
                        "remove one of the two packages, or pin versions so only one of them is resolved");
                }

                ownerByFile[asset.File] = library.Id;
                paths.Add(Path.Combine(entryDirs[library.Id], role, asset.File));
            }
        }

        return paths;
    }

    /// <summary>Internal (not private) so <c>MaterializerTests</c> can drive it directly with several
    /// concurrent calls for the same package — the scenario the publish protocol below exists for.</summary>
    internal static (string EntryDir, bool Hit) EnsureCacheEntry(LockedPackage package, string nupkgPath, string cacheRoot)
    {
        var entryDir = Path.Combine(cacheRoot, package.Sha512);
        if (IsValidEntry(entryDir))
        {
            return (entryDir, true);
        }

        var tmpDir = entryDir + ".tmp-" + Guid.NewGuid().ToString("N");
        Directory.CreateDirectory(tmpDir);
        try
        {
            ExtractInto(nupkgPath, package, tmpDir);
            return (entryDir, Publish(tmpDir, entryDir));
        }
        catch
        {
            TryDelete(tmpDir);
            throw;
        }
    }

    /// <summary>Publishes an extracted temp directory as THE cache entry: false when this call
    /// populated it, true when a valid entry was already there (a concurrent racer won, or an earlier
    /// run had populated it between our check and now — redundant extraction, not a failure, since the
    /// entry is content-addressed and therefore byte-identical to ours).
    ///
    /// Publishing is a rename and the entry directory is NEVER deleted in place. Pre-deleting an
    /// existing entryDir before extracting loses a race: a caller that evaluated
    /// <see cref="IsValidEntry"/> a moment before another caller's rename landed would then delete that
    /// just-published, perfectly valid entry and repopulate it — in production, one restore deleting
    /// the very directory another restore is materializing files out of. A torn entry left behind by a
    /// crashed run is instead swapped aside under a unique name — that rename is itself atomic, so
    /// exactly one caller takes ownership of the replacement and the rest re-converge on the result.</summary>
    private static bool Publish(string tmpDir, string entryDir)
    {
        if (TryMove(tmpDir, entryDir, out var failure))
        {
            return false;
        }

        if (IsValidEntry(entryDir))
        {
            TryDelete(tmpDir);
            return true;
        }

        // entryDir exists but is not a valid entry: a previous run died mid-populate. Quarantine it
        // rather than deleting it where it stands, then publish over the vacated name.
        var deadDir = entryDir + ".dead-" + Guid.NewGuid().ToString("N");
        if (TryMove(entryDir, deadDir, out _))
        {
            TryDelete(deadDir);
        }

        if (TryMove(tmpDir, entryDir, out _))
        {
            return false;
        }

        if (IsValidEntry(entryDir))
        {
            // Another caller replaced the torn entry while we were quarantining it.
            TryDelete(tmpDir);
            return true;
        }

        throw new IOException(
            $"could not publish the package cache entry '{entryDir}': it exists but is not a valid " +
            "cache entry and could not be replaced (check permissions and free disk space)", failure);
    }

    private static bool TryMove(string from, string to, out Exception? failure)
    {
        try
        {
            Directory.Move(from, to);
            failure = null;
            return true;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            // Destination already exists, or the source was renamed away by a concurrent caller.
            failure = ex;
            return false;
        }
    }

    private static void TryDelete(string dir)
    {
        try { Directory.Delete(dir, recursive: true); } catch { /* best effort cleanup */ }
    }

    /// <summary>An entry is trusted only if BOTH bookkeeping files exist AND every path <c>files.txt</c>
    /// lists is actually present — a tampered/corrupted entry (missing dll, truncated files.txt, ...)
    /// fails at least one of these and is treated as a cache miss.</summary>
    private static bool IsValidEntry(string entryDir)
    {
        var okMarker = Path.Combine(entryDir, ".ok");
        var manifestPath = Path.Combine(entryDir, "files.txt");
        if (!File.Exists(okMarker) || !File.Exists(manifestPath))
        {
            return false;
        }

        return File.ReadAllLines(manifestPath)
            .Where(line => line.Length > 0)
            .All(relative => File.Exists(Path.Combine(entryDir, relative)));
    }

    private static void ExtractInto(string nupkgPath, LockedPackage package, string entryDir)
    {
        using var stream = File.OpenRead(nupkgPath);
        using var reader = new PackageArchiveReader(stream);
        var allFiles = reader.GetFiles().ToArray();

        var relativePaths = new List<string>();

        ExtractRole(reader, allFiles, package.Id, package.Assets.Lib, entryDir, "lib", relativePaths);
        ExtractRole(reader, allFiles, package.Id, package.Assets.Native, entryDir, "native", relativePaths);

        var connectorJsonPath = allFiles.FirstOrDefault(f => string.Equals(f, "pz.connector.json", StringComparison.OrdinalIgnoreCase));
        if (connectorJsonPath is not null)
        {
            ExtractFile(reader, connectorJsonPath, Path.Combine(entryDir, "pz.connector.json"));
            relativePaths.Add("pz.connector.json");
        }

        relativePaths.Sort(StringComparer.Ordinal);
        File.WriteAllLines(Path.Combine(entryDir, "files.txt"), relativePaths);
        File.WriteAllBytes(Path.Combine(entryDir, ".ok"), []); // written last: marks the entry complete
    }

    /// <summary>Extracts one role's assets by the EXACT archive path the resolver recorded, flattening
    /// them into <c>&lt;entryDir&gt;/&lt;role&gt;/</c>. The path is never re-derived from the file name:
    /// a name alone matches every framework and every RID the package ships, and the resolver already
    /// chose which one this host needs.</summary>
    private static void ExtractRole(
        PackageArchiveReader reader, IReadOnlyList<string> allFiles, string packageId,
        IReadOnlyList<LockedAsset> assets, string entryDir, string role, List<string> relativePaths)
    {
        if (assets.Count == 0)
        {
            return;
        }

        Directory.CreateDirectory(Path.Combine(entryDir, role));
        foreach (var asset in assets)
        {
            var archivePath = allFiles.FirstOrDefault(
                f => string.Equals(f.Replace('\\', '/'), asset.ArchivePath, StringComparison.OrdinalIgnoreCase));
            if (archivePath is null)
            {
                throw new RestoreException(
                    "PZ0321",
                    $"pz.lock.json expects '{asset.ArchivePath}' inside package '{packageId}', but that " +
                    "package does not contain it",
                    "run 'pz restore' to regenerate pz.lock.json");
            }

            ExtractFile(reader, archivePath, Path.Combine(entryDir, role, asset.File));
            relativePaths.Add($"{role}/{asset.File}");
        }
    }

    private static void ExtractFile(PackageArchiveReader reader, string archivePath, string destination)
    {
        using var entryStream = reader.GetStream(archivePath);
        using var fileStream = File.Create(destination);
        entryStream.CopyTo(fileStream);
    }

    /// <summary>The transitive assets to flatten into a root package's own directory, by role. Native
    /// assets are flattened for the same reason lib assets are: <c>ConnectorLoadContext</c> probes
    /// <c>&lt;lib&gt;/../native</c> and nowhere else, so a native library owned by a dependency is
    /// unreachable from the connector's load context unless it is placed there.</summary>
    private sealed record FlattenedAssets(IReadOnlyList<string> Lib, IReadOnlyList<string> Native);

    /// <summary><paramref name="flattened"/> non-null means "always copy, then flatten these transitive
    /// files in too" (root-with-transitive-deps rule); null means "prefer a symlink."</summary>
    private static void MaterializeVersionDir(string entryDir, string versionDir, FlattenedAssets? flattened)
    {
        if (Directory.Exists(versionDir))
        {
            return; // idempotent: a prior restore into this exact packagesDir already materialized it
        }

        Directory.CreateDirectory(Path.GetDirectoryName(versionDir)!);

        if (flattened is null && TrySymlink(versionDir, entryDir))
        {
            return;
        }

        CopyDirectory(entryDir, versionDir);

        if (flattened is null)
        {
            return;
        }

        CopyInto(flattened.Lib, Path.Combine(versionDir, "lib"));
        CopyInto(flattened.Native, Path.Combine(versionDir, "native"));
    }

    private static void CopyInto(IReadOnlyList<string> sources, string destinationDir)
    {
        if (sources.Count == 0)
        {
            return;
        }

        Directory.CreateDirectory(destinationDir);
        foreach (var source in sources)
        {
            File.Copy(source, Path.Combine(destinationDir, Path.GetFileName(source)), overwrite: true);
        }
    }

    private static void CopyDirectory(string sourceDir, string destDir)
    {
        Directory.CreateDirectory(destDir);
        foreach (var file in Directory.EnumerateFiles(sourceDir, "*", SearchOption.AllDirectories))
        {
            var relative = Path.GetRelativePath(sourceDir, file);
            if (relative is "files.txt" or ".ok")
            {
                continue; // cache bookkeeping only — not part of the ConnectorHost layout
            }

            var dest = Path.Combine(destDir, relative);
            Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
            File.Copy(file, dest, overwrite: true);
        }
    }

    private static bool TryCreateDirectorySymlink(string path, string pathToTarget)
    {
        try
        {
            Directory.CreateSymbolicLink(path, pathToTarget);
            return true;
        }
        catch
        {
            return false;
        }
    }
}
