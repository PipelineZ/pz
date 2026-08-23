using NuGet.Common;
using NuGet.Frameworks;
using NuGet.Packaging;
using NuGet.Protocol;
using NuGet.Protocol.Core.Types;
using NuGet.Versioning;
using Pz.PackageManagement.Hosting;

namespace Pz.PackageManagement.Restore;

/// <summary>Resolves connector packages and their transitive dependencies against configured NuGet feeds
/// (local folders in every test) using NuGet.Protocol directly, in-process (no `dotnet
/// restore`/MSBuild involved).</summary>
public static class NuGetResolver
{
    private static readonly NuGetFramework TargetFramework = NuGetFramework.Parse("net10.0");
    private static readonly FrameworkReducer Reducer = new();

    /// <summary>Resolves <paramref name="requirements"/> (id + version RANGE in
    /// <see cref="ConnectorPackageRef.Version"/>) plus transitive dependencies against
    /// <paramref name="feeds"/> (URLs or local folder paths). Downloads every resolved .nupkg into
    /// <paramref name="workDir"/>. Deterministic: feeds probed in declared order, first feed carrying
    /// any satisfying version wins the package; highest satisfying version within that feed.
    /// <paramref name="warn"/> receives diagnostics that must not stop the restore — today, a package
    /// that ships native assets for no RID compatible with <paramref name="rid"/>.</summary>
    public static async Task<ResolveResult> ResolveAsync(
        IReadOnlyList<ConnectorPackageRef> requirements, IReadOnlyList<string> feeds,
        string rid, string workDir, CancellationToken ct = default, Action<string>? warn = null)
    {
        Directory.CreateDirectory(workDir);

        var ridChain = RuntimeIdentifierGraph.Expand(rid);

        var repositories = feeds.Select(feed => Repository.Factory.GetCoreV3(feed)).ToArray();
        using var cache = new SourceCacheContext { NoCache = true };

        var resolved = new Dictionary<string, ResolvedPackage>(StringComparer.OrdinalIgnoreCase);
        var queue = new Queue<(string Id, VersionRange Range)>();

        // Packages named directly in `requirements` are "root/required" — everything else reached only
        // via transitive dependency traversal below is
        // not. Recorded per-package in the lock so downstream consumers don't need to re-derive it.
        var rootIds = new HashSet<string>(requirements.Select(r => r.PackageId), StringComparer.OrdinalIgnoreCase);

        foreach (var requirement in requirements)
        {
            queue.Enqueue((requirement.PackageId, ParseRequirementRange(requirement.PackageId, requirement.Version)));
        }

        while (queue.Count > 0)
        {
            var (id, range) = queue.Dequeue();

            var (bestVersion, byId) = await FindBestAsync(id, range, feeds, repositories, cache, ct);
            if (bestVersion is null)
            {
                throw new RestoreException(
                    "PZ0320",
                    $"package '{id}' {range} not found in any feed ({string.Join(", ", feeds)})",
                    "check the package id/version and your feeds (--feeds / PZ_FEEDS)");
            }

            // Highest-version-wins (v0-simplistic): once an id is resolved to a version, a
            // later edge requesting that same id only re-resolves if it demands something HIGHER — the
            // retained version is never re-verified against a later, NARROWER range for the same id
            // (e.g. an earlier wide edge picks 2.0.0, a later edge constrains to [1.0.0,1.5.0) — 2.0.0
            // does not satisfy that range, but it is kept anyway; no re-resolution or conflict error is
            // raised). See NuGetResolverTests.Highest_version_wins_even_when_later_range_is_narrower_v0_behavior
            // for the behavior this documents. A real conflict-resolution pass (à la NuGet's own
            // dependency graph resolver) is out of scope for v0.
            if (resolved.TryGetValue(id, out var existing) && existing.Version >= bestVersion)
            {
                continue; // already have an equal-or-higher version resolved for this id (highest-version-wins)
            }

            var nupkgPath = Path.Combine(workDir, $"{id}.{bestVersion.ToNormalizedString()}.nupkg");
            bool downloaded;
            await using (var fileStream = File.Create(nupkgPath))
            {
                downloaded = await byId!.CopyNupkgToStreamAsync(id, bestVersion, fileStream, cache, NullLogger.Instance, ct);
            }

            if (!downloaded)
            {
                File.Delete(nupkgPath);
                throw new RestoreException(
                    "PZ0320",
                    $"package '{id}' {bestVersion} could not be downloaded from its feed",
                    "check the package id/version and your feeds (--feeds / PZ_FEEDS)");
            }

            var nupkgBytes = await File.ReadAllBytesAsync(nupkgPath, ct);
            var sha512 = Convert.ToHexStringLower(System.Security.Cryptography.SHA512.HashData(nupkgBytes));

            using var readerStream = File.OpenRead(nupkgPath);
            using var reader = new PackageArchiveReader(readerStream);

            var lib = SelectNearestFrameworkAssets(reader.GetLibItems());
            var native = SelectNativeAssets(reader.GetFiles().ToArray(), ridChain, id, rid, warn);

            resolved[id] = new ResolvedPackage(id, bestVersion, sha512, nupkgPath, lib, native);

            var dependencyGroups = reader.NuspecReader.GetDependencyGroups().ToArray();
            var nearestGroupFramework = Reducer.GetNearest(TargetFramework, dependencyGroups.Select(g => g.TargetFramework));
            var dependencies = nearestGroupFramework is null
                ? []
                : dependencyGroups.First(g => g.TargetFramework.Equals(nearestGroupFramework)).Packages;

            foreach (var dependency in dependencies)
            {
                if (IsHostProvidedOrPlatform(dependency.Id))
                {
                    continue;
                }

                queue.Enqueue((dependency.Id, dependency.VersionRange));
            }
        }

        var packages = resolved.Values
            .OrderBy(p => p.Id, StringComparer.Ordinal)
            .Select(p => new LockedPackage(
                p.Id,
                p.Version.ToNormalizedString(),
                p.Sha512,
                new LockedAssets(p.Lib, p.Native),
                rootIds.Contains(p.Id)))
            .ToArray();

        var nupkgPaths = resolved.Values.ToDictionary(p => p.Id, p => p.NupkgPath, StringComparer.OrdinalIgnoreCase);

        return new ResolveResult(new LockFile(LockFileWriter.CurrentVersion, rid, packages), nupkgPaths);
    }

    private static async Task<(NuGetVersion? Version, FindPackageByIdResource? Resource)> FindBestAsync(
        string id, VersionRange range, IReadOnlyList<string> feeds, SourceRepository[] repositories,
        SourceCacheContext cache, CancellationToken ct)
    {
        for (var i = 0; i < feeds.Count; i++)
        {
            var byId = await repositories[i].GetResourceAsync<FindPackageByIdResource>(ct);
            var versions = await byId.GetAllVersionsAsync(id, cache, NullLogger.Instance, ct);

            // API adaptation: VersionRange.FindBestMatch does NOT return the highest satisfying
            // version — verified empirically, it favors the version closest to the range's floor (its
            // documented use is floating-version resolution). This resolver's contract is "highest
            // satisfying version within the feed," so it selects one explicitly instead.
            //
            // NuGet convention: VersionRange.Satisfies does NOT gate on
            // prerelease — a plain range like "[1.0.0,2.0.0)" happily matches "1.5.0-beta" too. Real
            // NuGet only considers prerelease versions eligible when the requirement ITSELF opts in,
            // i.e. its range's floor or ceiling version carries a prerelease label (an exact pin like
            // "[1.5.0-beta]" or a floor like ">=1.0.0-rc"). We mirror that: prerelease candidates are
            // filtered out unless the range's MinVersion or MaxVersion is itself prerelease.
            var allowPrerelease = (range.MinVersion?.IsPrerelease ?? false) || (range.MaxVersion?.IsPrerelease ?? false);
            var best = versions
                .Where(v => range.Satisfies(v))
                .Where(v => allowPrerelease || !v.IsPrerelease)
                .OrderByDescending(v => v, VersionComparer.Default)
                .FirstOrDefault();
            if (best is not null)
            {
                return (best, byId);
            }
        }

        return (null, null);
    }

    /// <summary>Lib assets from the nearest-net10.0 framework group. The materializer places them
    /// straight into the package's own flat lib/ dir, so each asset carries both the name it lands under
    /// and the <c>lib/&lt;tfm&gt;/</c> path it must be extracted from — the chosen framework is part of
    /// that path, and dropping it is what let archive order substitute a net472 build for a net9.0
    /// one.</summary>
    private static IReadOnlyList<LockedAsset> SelectNearestFrameworkAssets(IEnumerable<FrameworkSpecificGroup> groups)
    {
        var groupList = groups.ToArray();
        var nearest = Reducer.GetNearest(TargetFramework, groupList.Select(g => g.TargetFramework));
        if (nearest is null)
        {
            return [];
        }

        return groupList
            .First(g => g.TargetFramework.Equals(nearest))
            .Items
            .Where(item => Path.GetFileName(item) is { Length: > 0 })
            .Select(item => new LockedAsset(Path.GetFileName(item), NormalizeArchivePath(item)))
            .OrderBy(asset => asset.File, StringComparer.Ordinal)
            .ToArray();
    }

    /// <summary>Native assets from the MOST SPECIFIC RID in <paramref name="ridChain"/> the package
    /// actually ships — never a union across the chain, mirroring NuGet's own single-RID selection. A
    /// package shipping a <c>runtimes/</c> tree that matches nothing in the chain is reported through
    /// <paramref name="warn"/> naming the RIDs it does ship, rather than resolving zero assets in
    /// silence and surfacing later as a DllNotFoundException.</summary>
    private static IReadOnlyList<LockedAsset> SelectNativeAssets(
        IReadOnlyList<string> allFiles, IReadOnlyList<string> ridChain, string packageId, string hostRid,
        Action<string>? warn)
    {
        foreach (var candidateRid in ridChain)
        {
            var prefix = $"runtimes/{candidateRid}/native/";
            var assets = allFiles
                .Where(f => f.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                .Where(f => Path.GetFileName(f) is { Length: > 0 })
                .Select(f => new LockedAsset(Path.GetFileName(f), NormalizeArchivePath(f)))
                .OrderBy(asset => asset.File, StringComparer.Ordinal)
                .ToArray();
            if (assets.Length > 0)
            {
                return assets;
            }
        }

        var shippedRids = allFiles
            .Where(f => f.StartsWith("runtimes/", StringComparison.OrdinalIgnoreCase))
            .Select(f => f.Split('/'))
            .Where(segments => segments.Length > 3 && segments[2].Equals("native", StringComparison.OrdinalIgnoreCase))
            .Select(segments => segments[1])
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(r => r, StringComparer.Ordinal)
            .ToArray();

        if (shippedRids.Length > 0)
        {
            warn?.Invoke(
                $"package '{packageId}' ships native assets for {string.Join(", ", shippedRids)} but none " +
                $"is compatible with this host's runtime identifier '{hostRid}'; no native assets were " +
                "installed for it");
        }

        return [];
    }

    /// <summary>Archive entry paths are '/'-separated by the zip format; a
    /// <see cref="PackageArchiveReader"/> on Windows can hand back '\' separators for the same entry, and
    /// the path is used verbatim as a lookup key at extraction time.</summary>
    private static string NormalizeArchivePath(string archivePath) => archivePath.Replace('\\', '/');

    /// <summary>A bare version in a requirement (e.g. <c>1.2.3</c>) is an EXACT pin — the
    /// range <c>[1.2.3]</c> — not NuGet's own "minimum version" convention for bare dependency versions.
    /// A requirement already written as a bracketed/parenthesized range passes through unchanged.
    /// Transitive dependency ranges (parsed straight from the nuspec) are never passed through this
    /// method — they keep NuGet's normal minimum-version semantics.
    /// <para>v0 supports exact pins and bracket/parenthesis ranges only — floating ranges
    /// (<c>1.0.*</c>, <c>1.*</c>) are rejected here with <see cref="RestoreException"/> PZ0323, because
    /// <see cref="VersionRange.Satisfies"/> only checks the lower bound and would otherwise silently
    /// accept ANY higher version for a "floating" requirement, which is not v0's contract.</para></summary>
    private static VersionRange ParseRequirementRange(string packageId, string versionText)
    {
        // A bare '*' (e.g. "1.0.*") is NuGet's OWN floating-version syntax, understood natively by
        // VersionRange.Parse — it must NOT be routed through the exact-pin wrap below (wrapping it as
        // "[1.0.*]" would mangle it into an invalid version literal instead of a recognizable floating
        // range), so it takes the pass-through branch like bracket/parenthesis ranges do.
        var range = versionText.Contains('[') || versionText.Contains('(') || versionText.Contains('*')
            ? VersionRange.Parse(versionText)
            : VersionRange.Parse($"[{versionText}]");

        if (range.IsFloating)
        {
            throw new RestoreException(
                "PZ0323",
                $"requirement '{packageId}' '{versionText}' uses a floating version range, which v0 does not support",
                "use an exact pin (e.g. '1.2.3') or a bracket/parenthesis range (e.g. '[1.0.0,2.0.0)')");
        }

        return range;
    }

    /// <summary>Dependency ids the ALC unifies to the host copy (<see cref="SharedAssemblies.Names"/>)
    /// plus platform ids (System.*, Microsoft.NETCore.*, NETStandard.*) are never resolved or materialized.</summary>
    private static bool IsHostProvidedOrPlatform(string id) =>
        SharedAssemblies.Names.Contains(id) ||
        id.StartsWith("System.", StringComparison.OrdinalIgnoreCase) ||
        id.StartsWith("Microsoft.NETCore.", StringComparison.OrdinalIgnoreCase) ||
        id.StartsWith("NETStandard.", StringComparison.OrdinalIgnoreCase);

    private sealed record ResolvedPackage(
        string Id, NuGetVersion Version, string Sha512, string NupkgPath,
        IReadOnlyList<LockedAsset> Lib, IReadOnlyList<LockedAsset> Native);
}
