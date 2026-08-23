using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.Restore;

namespace Pz.PackageManagement.Tests.Restore;

/// <summary>The asset the resolver chose must be the asset that lands on disk, and a native library a
/// connector depends on must be reachable from the connector's load context.
///
/// <para>Every fact here reads the MATERIALIZED CONTENT rather than checking for presence: a
/// wrong-architecture <c>.so</c> and a .NET Framework build of the right assembly are both perfectly
/// valid files with the right names, which is what made the defect these cover invisible.</para></summary>
public sealed class AssetSelectionTests
{
    private static string NewDir(string prefix) =>
        Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), "pz-tests", prefix + Guid.NewGuid().ToString("N"))).FullName;

    private static Task<ResolveResult> Resolve(
        string feedDir, string packageId, string rid = "linux-x64", Action<string>? warn = null) =>
        NuGetResolver.ResolveAsync(
            [new ConnectorPackageRef(packageId, "1.0.0")], [feedDir], rid, NewDir("wd-"), warn: warn);

    private static string Materialize(ResolveResult resolved)
    {
        var packagesDir = NewDir("packages-");
        PackageMaterializer.Materialize(resolved, NewDir("cache-"), packagesDir);
        return packagesDir;
    }

    /// <summary>The archive lists net472 first. A file-name lookup takes it; the resolver's own
    /// nearest-framework choice is net9.0, and that is what must be installed on a net10.0 host.</summary>
    [Fact]
    public async Task Nearest_target_framework_is_installed_not_the_first_in_the_archive()
    {
        var resolved = await Resolve(SyntheticNupkg.CreateFeedWhereWrongBuildsSortFirst(), "FakeMultiTarget");

        var lib = Assert.Single(resolved.Lock.Packages.Single().Assets.Lib);
        Assert.Equal("lib/net9.0/FakeMultiTarget.dll", lib.ArchivePath);

        var packagesDir = Materialize(resolved);
        Assert.Equal(
            "lib/net9.0/FakeMultiTarget.dll",
            SyntheticNupkg.OriginOf(Path.Combine(packagesDir, "FakeMultiTarget", "1.0.0", "lib", "FakeMultiTarget.dll")));
    }

    /// <summary>Same archive, same defect one prefix over: linux-arm64 sorts before linux-x64, and both
    /// ship a file called libfake.so.</summary>
    [Fact]
    public async Task Host_architecture_native_asset_is_installed_not_the_first_in_the_archive()
    {
        var resolved = await Resolve(SyntheticNupkg.CreateFeedWhereWrongBuildsSortFirst(), "FakeMultiTarget");

        var native = Assert.Single(resolved.Lock.Packages.Single().Assets.Native);
        Assert.Equal("runtimes/linux-x64/native/libfake.so", native.ArchivePath);

        var packagesDir = Materialize(resolved);
        Assert.Equal(
            "runtimes/linux-x64/native/libfake.so",
            SyntheticNupkg.OriginOf(Path.Combine(packagesDir, "FakeMultiTarget", "1.0.0", "native", "libfake.so")));
    }

    /// <summary>A connector's load context probes its own package's <c>native/</c> and nowhere else, so
    /// a native library owned by a dependency is only reachable if materialization flattens it in the way
    /// it already flattens managed assemblies.</summary>
    [Fact]
    public async Task Dependency_owned_native_assets_are_flattened_into_the_connector_package()
    {
        var resolved = await Resolve(
            SyntheticNupkg.CreateFeedWhereDependencyOwnsTheNativeLibrary(), "FakeNativeConsumer");

        var packagesDir = Materialize(resolved);
        var connectorDir = Path.Combine(packagesDir, "FakeNativeConsumer", "1.0.0");

        Assert.True(File.Exists(Path.Combine(connectorDir, "lib", "FakeNativeOwner.dll")));
        Assert.Equal(
            "runtimes/linux-x64/native/libowned.so",
            SyntheticNupkg.OriginOf(Path.Combine(connectorDir, "native", "libowned.so")));
    }

    /// <summary>Two dependencies shipping one file name cannot both survive a flattening copy, and
    /// whichever enumeration reaches last is not an answer. Refusing names both packages and the
    /// file.</summary>
    [Fact]
    public async Task Colliding_dependency_assets_fail_loudly_rather_than_overwrite()
    {
        var resolved = await Resolve(
            SyntheticNupkg.CreateFeedWithCollidingDependencies(), "FakeCollisionConsumer");

        var ex = Assert.Throws<RestoreException>(() => Materialize(resolved));

        Assert.Equal("PZ0325", ex.Code);
        Assert.Contains("Shared.dll", ex.Message);
        Assert.Contains("FakeCollidingA", ex.Message);
        Assert.Contains("FakeCollidingB", ex.Message);
    }

    /// <summary>A package shipping only linux-x64 must be selected for a musl host, which NuGet's RID
    /// graph makes a descendant of linux-x64 — the exact-prefix match resolved zero assets there.</summary>
    [Fact]
    public async Task Native_assets_are_reached_through_the_rid_graph()
    {
        var resolved = await Resolve(
            SyntheticNupkg.CreateFeedWithLinuxOnlyNativePackage(), "FakeLinuxOnlyNative", rid: "linux-musl-x64");

        var native = Assert.Single(resolved.Lock.Packages.Single().Assets.Native);
        Assert.Equal("runtimes/linux-x64/native/libfake.so", native.ArchivePath);
    }

    /// <summary>Resolving nothing must not be silent. macOS is not a descendant of linux in the RID
    /// graph, so the honest outcome is "no native assets, and here is what the package does ship" rather
    /// than a DllNotFoundException much later.</summary>
    [Fact]
    public async Task Incompatible_rid_is_reported_naming_the_rids_the_package_ships()
    {
        var warnings = new List<string>();
        var resolved = await Resolve(
            SyntheticNupkg.CreateFeedWithLinuxOnlyNativePackage(), "FakeLinuxOnlyNative",
            rid: "osx-arm64", warn: warnings.Add);

        Assert.Empty(resolved.Lock.Packages.Single().Assets.Native);
        var warning = Assert.Single(warnings);
        Assert.Contains("FakeLinuxOnlyNative", warning);
        Assert.Contains("linux-x64", warning);
        Assert.Contains("osx-arm64", warning);
    }

    /// <summary>A package with no <c>runtimes/</c> tree at all is not a diagnosis — every managed-only
    /// connector would otherwise warn on every restore.</summary>
    [Fact]
    public async Task A_package_with_no_native_assets_is_not_reported()
    {
        var warnings = new List<string>();
        await Resolve(
            SyntheticNupkg.CreateFeedWithCollidingDependencies(), "FakeCollidingA",
            rid: "osx-arm64", warn: warnings.Add);

        Assert.Empty(warnings);
    }
}
