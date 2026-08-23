namespace Pz.PackageManagement.Restore;

/// <summary>One file a locked package contributes, recorded as BOTH the name it is materialized under
/// (relative to the package's <c>lib/</c> resp. <c>native/</c> directory) and the exact
/// <paramref name="ArchivePath"/> inside the .nupkg it must be extracted from.
///
/// <para>The archive path is the load-bearing half. The resolver picks one target framework and one
/// RID for every package; recording only the file name would discard both, leaving the materializer to
/// re-find the name by prefix scan — where archive order, not the resolver, decides whether a
/// <c>net472</c> or a <c>net9.0</c> build (and an <c>arm64</c> or an <c>x64</c> native library) lands
/// on disk. Extracting <paramref name="ArchivePath"/> verbatim is what makes the asset the resolver
/// chose the asset that is installed.</para></summary>
public sealed record LockedAsset(string File, string ArchivePath);

/// <summary>The materialized files one locked package contributes, split by role, sorted ordinal by
/// <see cref="LockedAsset.File"/>.</summary>
public sealed record LockedAssets(IReadOnlyList<LockedAsset> Lib, IReadOnlyList<LockedAsset> Native);

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
/// serialization and for the <see cref="LockFileWriter.CurrentVersion"/> a lock must declare to be
/// read back.</summary>
public sealed record LockFile(int Version, string Rid, IReadOnlyList<LockedPackage> Packages);

/// <summary>Resolution output: the lock plus, per package id, the downloaded .nupkg path in
/// <c>workDir</c> for the materializer to extract (avoids a second download).</summary>
public sealed record ResolveResult(LockFile Lock, IReadOnlyDictionary<string, string> NupkgPaths);
