namespace Pz.PackageManagement.Restore;

/// <summary>The materialized files one locked package contributes, split by role. File names are
/// relative to the package's <c>lib/</c> (resp. <c>native/</c>) directory, sorted ordinal.</summary>
public sealed record LockedAssets(IReadOnlyList<string> Lib, IReadOnlyList<string> Native);

/// <summary>One resolved package pinned to an exact version, its content hash, and the assets the
/// materializer must extract. <paramref name="Requested"/> is true for packages that were themselves
/// one of the requirements passed to
/// <see cref="NuGetResolver.ResolveAsync"/> (i.e. a connector declared in project.yml's
/// <c>connectors:</c>), false for packages pulled in only transitively. The materializer uses it to
/// decide which package directories get transitive lib dlls copied in (and are therefore always
/// copied, never symlinked); <see cref="DriftChecker"/> uses it to tell "a declared connector" apart
/// from "an incidental dependency" when checking for stale/missing lock entries. Defaults to true so
/// positional constructions need not supply it.</summary>
public sealed record LockedPackage(string Id, string Version, string Sha512, LockedAssets Assets, bool Requested = true);

/// <summary>The full resolved dependency closure for one restore, pinned to a specific RID. Committed
/// as <c>pz.lock.json</c> at the project root; see <see cref="LockFileWriter"/> for the byte-stable
/// serialization.</summary>
public sealed record LockFile(int Version, string Rid, IReadOnlyList<LockedPackage> Packages);

/// <summary>Resolution output: the lock plus, per package id, the downloaded .nupkg path in
/// <c>workDir</c> for the materializer to extract (avoids a second download).</summary>
public sealed record ResolveResult(LockFile Lock, IReadOnlyDictionary<string, string> NupkgPaths);
