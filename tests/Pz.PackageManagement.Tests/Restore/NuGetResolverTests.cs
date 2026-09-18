using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.Restore;

namespace Pz.PackageManagement.Tests.Restore;

[Collection("local-feed")]
public sealed class NuGetResolverTests(FeedFixture feed)
{
    private static string NewWorkDir() =>
        Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), "pz-tests", "wd-" + Guid.NewGuid().ToString("N"))).FullName;

    private Task<ResolveResult> Resolve(string versionRange) =>
        NuGetResolver.ResolveAsync(
            [new ConnectorPackageRef("FakeSourceConnector", versionRange)],
            [feed.FeedDir], "linux-x64", NewWorkDir());

    [Fact]
    public async Task Exact_version_pin_is_honored()
    {
        var result = await Resolve("1.0.0");
        var root = Assert.Single(result.Lock.Packages, p => p.Id == "FakeSourceConnector");
        Assert.Equal("1.0.0", root.Version);
    }

    [Fact]
    public async Task Range_resolves_to_highest_and_lock_pins_exact()
    {
        var result = await Resolve("[1.0.0,2.0.0)");
        var root = Assert.Single(result.Lock.Packages, p => p.Id == "FakeSourceConnector");
        Assert.Equal("1.2.3", root.Version);
    }

    [Fact]
    public async Task Transitive_closure_is_resolved_and_locked()
    {
        var result = await Resolve("1.2.3");
        var dep = Assert.Single(result.Lock.Packages, p => p.Id == "FakeTransitiveDep");
        Assert.Equal("2.0.0", dep.Version);
        Assert.Contains(dep.Assets.Lib, a => a.File == "FakeTransitiveDep.dll");
    }

    [Fact]
    public async Task Host_provided_dependencies_are_excluded()
    {
        var result = await Resolve("1.2.3");
        Assert.DoesNotContain(result.Lock.Packages, p => p.Id == "Pz.Connectors.Abstractions");
        Assert.DoesNotContain(result.Lock.Packages, p => p.Id.StartsWith("System.", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Every_asset_records_the_hash_of_its_own_content()
    {
        var result = await Resolve("1.2.3");
        var root = result.Lock.Packages.Single(p => p.Id == "FakeSourceConnector");
        var dll = Assert.Single(root.Assets.Lib, a => a.File == "FakeSourceConnector.dll");

        using var reader = new NuGet.Packaging.PackageArchiveReader(result.NupkgPaths["FakeSourceConnector"]);
        using var entry = reader.GetStream(dll.ArchivePath);
        using var buffer = new MemoryStream();
        await entry.CopyToAsync(buffer);
        var expected = Convert.ToHexStringLower(System.Security.Cryptography.SHA512.HashData(buffer.ToArray()));

        Assert.Equal(expected, dll.Sha512);
    }

    [Fact]
    public async Task A_lock_pins_the_locked_version_even_when_the_range_admits_a_newer_one()
    {
        var pinned = await Resolve("1.0.0");

        var result = await NuGetResolver.ResolveAsync(
            [new ConnectorPackageRef("FakeSourceConnector", "[1.0.0,2.0.0)")],
            [feed.FeedDir], "linux-x64", NewWorkDir(), pins: pinned.Lock);

        var root = Assert.Single(result.Lock.Packages, p => p.Id == "FakeSourceConnector");
        Assert.Equal("1.0.0", root.Version);
    }

    [Fact]
    public async Task A_pinned_package_whose_feed_content_changed_is_PZ0327()
    {
        var pinned = await Resolve("1.2.3");
        var tampered = new LockFile(pinned.Lock.Version, pinned.Lock.Rid, pinned.Lock.Packages
            .Select(p => p.Id == "FakeSourceConnector" ? p with { Sha512 = new string('0', 128) } : p)
            .ToArray());

        var ex = await Assert.ThrowsAsync<RestoreException>(() => NuGetResolver.ResolveAsync(
            [new ConnectorPackageRef("FakeSourceConnector", "1.2.3")],
            [feed.FeedDir], "linux-x64", NewWorkDir(), pins: tampered));

        Assert.Equal("PZ0327", ex.Code);
        Assert.Contains("FakeSourceConnector", ex.Message);
        Assert.Contains("--update", ex.Hint);
    }

    [Fact]
    public async Task Sha512_recorded_matches_nupkg_bytes()
    {
        var result = await Resolve("1.2.3");
        var root = result.Lock.Packages.Single(p => p.Id == "FakeSourceConnector");
        var bytes = await File.ReadAllBytesAsync(result.NupkgPaths["FakeSourceConnector"]);
        var expected = Convert.ToHexStringLower(System.Security.Cryptography.SHA512.HashData(bytes));
        Assert.Equal(expected, root.Sha512);
    }

    [Fact]
    public async Task Missing_package_is_error_PZ0320_naming_feeds()
    {
        var ex = await Assert.ThrowsAsync<RestoreException>(() =>
            NuGetResolver.ResolveAsync([new ConnectorPackageRef("NoSuchPackage", "1.0.0")],
                [feed.FeedDir], "linux-x64", NewWorkDir()));
        Assert.Equal("PZ0320", ex.Code);
        Assert.Contains("NoSuchPackage", ex.Message);
        Assert.Contains(feed.FeedDir, ex.Message);
    }

    [Fact]
    public async Task Rid_specific_native_assets_are_selected()
    {
        // Synthetic nupkg with a fake runtimes/ tree (no fixture project ships natives).
        var syntheticFeed = SyntheticNupkg.CreateFeedWithNativePackage(); // helper below
        var result = await NuGetResolver.ResolveAsync(
            [new ConnectorPackageRef("FakeNative", "1.0.0")],
            [syntheticFeed], "linux-x64", NewWorkDir());
        var pkg = result.Lock.Packages.Single(p => p.Id == "FakeNative");
        var native = Assert.Single(pkg.Assets.Native);
        Assert.Equal("libfake.so", native.File);
        Assert.Equal("runtimes/linux-x64/native/libfake.so", native.ArchivePath);
    }

    [Fact]
    public async Task Prerelease_excluded_without_opt_in()
    {
        // Feed carries stable 1.0.0/1.2.3 plus prerelease 1.5.0-beta.1. A plain range does not opt into
        // prerelease (its floor/ceiling are both stable), so 1.5.0-beta.1 must never win even though it
        // is numerically higher than 1.2.3 and VersionRange.Satisfies alone does not exclude it.
        var result = await Resolve("[1.0.0,2.0.0)");
        var root = Assert.Single(result.Lock.Packages, p => p.Id == "FakeSourceConnector");
        Assert.Equal("1.2.3", root.Version);
    }

    [Fact]
    public async Task Prerelease_selected_with_explicit_opt_in()
    {
        // An exact pin on the prerelease version itself is the opt-in: its own range floor/ceiling carry
        // the prerelease label, so it is eligible and (being the only candidate) wins.
        var result = await Resolve("[1.5.0-beta.1]");
        var root = Assert.Single(result.Lock.Packages, p => p.Id == "FakeSourceConnector");
        Assert.Equal("1.5.0-beta.1", root.Version);
    }

    [Fact]
    public async Task Floating_range_is_rejected_with_clear_error()
    {
        var ex = await Assert.ThrowsAsync<RestoreException>(() =>
            NuGetResolver.ResolveAsync([new ConnectorPackageRef("FakeSourceConnector", "1.0.*")],
                [feed.FeedDir], "linux-x64", NewWorkDir()));
        Assert.Equal("PZ0323", ex.Code);
        Assert.Contains("FakeSourceConnector", ex.Message);
        Assert.Contains("1.0.*", ex.Message);
    }

    [Fact]
    public async Task Highest_version_wins_even_when_later_range_is_narrower_v0_behavior()
    {
        // v0 highest-version-wins: once FakeTransitiveDep resolves to 2.0.0 via the
        // first (wide) edge, a second edge for the SAME id with a narrower range that 2.0.0 does not
        // even satisfy is not re-verified against it — 2.0.0 is retained rather than re-resolving down
        // to whatever the narrower edge's own best match would have been (1.0.0). This pins a known
        // tradeoff rather than asserting it is ideal behavior.
        var result = await NuGetResolver.ResolveAsync(
            [
                new ConnectorPackageRef("FakeTransitiveDep", "[1.0.0,3.0.0)"), // wide: resolves to 2.0.0
                new ConnectorPackageRef("FakeTransitiveDep", "[1.0.0,1.5.0)"), // narrower: excludes 2.0.0
            ],
            [feed.FeedDir], "linux-x64", NewWorkDir());

        var dep = Assert.Single(result.Lock.Packages, p => p.Id == "FakeTransitiveDep");
        Assert.Equal("2.0.0", dep.Version);
    }
}
