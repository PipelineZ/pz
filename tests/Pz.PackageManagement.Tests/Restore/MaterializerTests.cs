using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.Restore;

namespace Pz.PackageManagement.Tests.Restore;

[Collection("local-feed")]
public sealed class MaterializerTests(FeedFixture feed)
{
    private string NewCacheRoot() =>
        Directory.CreateDirectory(Path.Combine(feed.WorkRoot, "cache-" + Guid.NewGuid().ToString("N"))).FullName;

    private static string NewPackagesDir() =>
        Path.Combine(Path.GetTempPath(), "pz-tests", "packages-" + Guid.NewGuid().ToString("N"));

    private static string NewWorkDir() =>
        Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), "pz-tests", "wd-" + Guid.NewGuid().ToString("N"))).FullName;

    private Task<ResolveResult> ResolveFakeSourceConnector(string version = "1.2.3") =>
        NuGetResolver.ResolveAsync(
            [new ConnectorPackageRef("FakeSourceConnector", version)], [feed.FeedDir], "linux-x64", NewWorkDir());

    private Task<ResolveResult> ResolveFakeTransitiveDepAlone() =>
        NuGetResolver.ResolveAsync(
            [new ConnectorPackageRef("FakeTransitiveDep", "2.0.0")], [feed.FeedDir], "linux-x64", NewWorkDir());

    [Fact]
    public async Task Materializes_package_layout()
    {
        var resolved = await ResolveFakeSourceConnector();
        var packagesDir = NewPackagesDir();

        PackageMaterializer.Materialize(resolved, NewCacheRoot(), packagesDir);

        Assert.True(File.Exists(Path.Combine(packagesDir, "FakeSourceConnector", "1.2.3", "lib", "FakeSourceConnector.dll")));
        Assert.True(File.Exists(Path.Combine(packagesDir, "FakeSourceConnector", "1.2.3", "pz.connector.json")));
        Assert.True(File.Exists(Path.Combine(packagesDir, "FakeTransitiveDep", "2.0.0", "lib", "FakeTransitiveDep.dll")));

        // The transitive dll must ALSO be materialized into the root package's own lib/ — a connector
        // package is self-contained, nothing at runtime probes a sibling package's directory.
        Assert.True(File.Exists(Path.Combine(packagesDir, "FakeSourceConnector", "1.2.3", "lib", "FakeTransitiveDep.dll")));
    }

    [Fact]
    public async Task Second_materialize_is_cache_hit()
    {
        var resolved = await ResolveFakeSourceConnector();
        var cacheRoot = NewCacheRoot();

        var first = PackageMaterializer.Materialize(resolved, cacheRoot, NewPackagesDir());
        Assert.All(first.Values, hit => Assert.False(hit));

        var second = PackageMaterializer.Materialize(resolved, cacheRoot, NewPackagesDir());
        Assert.All(second.Values, hit => Assert.True(hit));
    }

    [Fact]
    public async Task Corrupted_cache_entry_is_refetched()
    {
        var resolved = await ResolveFakeSourceConnector();
        var cacheRoot = NewCacheRoot();
        PackageMaterializer.Materialize(resolved, cacheRoot, NewPackagesDir());

        var rootSha = resolved.Lock.Packages.Single(p => p.Id == "FakeSourceConnector").Sha512;
        var entryDll = Path.Combine(cacheRoot, rootSha, "lib", "FakeSourceConnector.dll");
        Assert.True(File.Exists(entryDll));
        File.Delete(entryDll); // violates files.txt for this entry

        var hits = PackageMaterializer.Materialize(resolved, cacheRoot, NewPackagesDir());
        Assert.False(hits["FakeSourceConnector"]);
        Assert.True(File.Exists(entryDll));
    }

    [Fact]
    public async Task Link_or_copy_produces_loadable_layout()
    {
        // FakeTransitiveDep alone has zero transitive deps of its own — the one case eligible for the
        // symlink optimization. On this dev machine (Linux, ordinary permissions) the default
        // TrySymlink delegate is expected to actually succeed; either way the layout must be loadable.
        var resolved = await ResolveFakeTransitiveDepAlone();
        var packagesDir = NewPackagesDir();
        PackageMaterializer.Materialize(resolved, NewCacheRoot(), packagesDir);

        var versionDir = Path.Combine(packagesDir, "FakeTransitiveDep", "2.0.0");
        if ((File.GetAttributes(versionDir) & FileAttributes.ReparsePoint) != 0)
        {
            var target = Directory.ResolveLinkTarget(versionDir, returnFinalTarget: true);
            Assert.NotNull(target);
            Assert.True(Directory.Exists(target!.FullName));
        }

        Assert.True(File.Exists(Path.Combine(versionDir, "lib", "FakeTransitiveDep.dll")));
    }

    /// <summary>The copy-fallback branch of the symlink-eligible path must be exercised deterministically,
    /// not left to whatever this machine happens to support. Forces the
    /// symlink attempt to fail via the test seam and asserts the layout is still fully loadable (a plain
    /// directory copy, no reparse point).</summary>
    [Fact]
    public async Task Symlink_failure_falls_back_to_copy()
    {
        var resolved = await ResolveFakeTransitiveDepAlone();
        var packagesDir = NewPackagesDir();

        PackageMaterializer.TrySymlink = (_, _) => false;
        try
        {
            PackageMaterializer.Materialize(resolved, NewCacheRoot(), packagesDir);
        }
        finally
        {
            PackageMaterializer.TrySymlink = PackageMaterializer.DefaultTrySymlink;
        }

        var versionDir = Path.Combine(packagesDir, "FakeTransitiveDep", "2.0.0");
        Assert.False((File.GetAttributes(versionDir) & FileAttributes.ReparsePoint) != 0);
        Assert.True(File.Exists(Path.Combine(versionDir, "lib", "FakeTransitiveDep.dll")));
    }

    [Fact]
    public async Task Root_with_transitive_deps_is_always_copied_never_symlinked()
    {
        var resolved = await ResolveFakeSourceConnector();
        var packagesDir = NewPackagesDir();
        PackageMaterializer.Materialize(resolved, NewCacheRoot(), packagesDir);

        var versionDir = Path.Combine(packagesDir, "FakeSourceConnector", "1.2.3");
        Assert.False((File.GetAttributes(versionDir) & FileAttributes.ReparsePoint) != 0);
    }

    /// <summary>8 concurrent restores populating the SAME content-addressed cache entry
    /// must all converge on one valid entry rather than have 7 of them fail with an unhandled
    /// Directory.Move exception -- the loser of the atomic-rename race must treat it as a cache hit,
    /// not rethrow. Real concurrency, not simulated: 8 real Task.Run calls racing
    /// EnsureCacheEntry for the identical package against a shared, empty cache root.</summary>
    [Fact]
    public async Task Concurrent_cache_populates_converge()
    {
        var resolved = await ResolveFakeSourceConnector();
        var cacheRoot = NewCacheRoot();
        var package = resolved.Lock.Packages.Single(p => p.Id == "FakeSourceConnector");
        var nupkgPath = resolved.NupkgPaths["FakeSourceConnector"];

        var tasks = Enumerable.Range(0, 8)
            .Select(_ => Task.Run(() => PackageMaterializer.EnsureCacheEntry(package, nupkgPath, cacheRoot)))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        var entryDir = Path.Combine(cacheRoot, package.Sha512);
        Assert.All(results, r => Assert.Equal(entryDir, r.EntryDir));

        // Exactly one valid entry — no stray ".tmp-<guid>" leftovers from losing racers.
        var siblings = Directory.GetDirectories(cacheRoot);
        var entry = Assert.Single(siblings);
        Assert.Equal(entryDir, entry);
        Assert.True(File.Exists(Path.Combine(entryDir, ".ok")));
        Assert.True(File.Exists(Path.Combine(entryDir, "files.txt")));
        Assert.True(File.Exists(Path.Combine(entryDir, "lib", "FakeSourceConnector.dll")));

        // Exactly one racer actually populated the entry (won the Move); the other 7 converged on it as
        // a cache hit.
        Assert.Single(results, r => !r.Hit);
        Assert.Equal(7, results.Count(r => r.Hit));
    }

    /// <summary>A torn entry from a crashed run (directory present, `.ok` marker absent) must be
    /// replaced, not rethrown at the caller — and replaced by swapping it aside, leaving no `.dead-`
    /// or `.tmp-` litter behind. This is the case the old unconditional pre-delete covered before it
    /// was removed for racing with a concurrent publisher.</summary>
    [Fact]
    public async Task Torn_cache_entry_is_replaced_rather_than_failing_the_restore()
    {
        var resolved = await ResolveFakeSourceConnector();
        var cacheRoot = NewCacheRoot();
        var package = resolved.Lock.Packages.Single(p => p.Id == "FakeSourceConnector");
        var nupkgPath = resolved.NupkgPaths["FakeSourceConnector"];

        // A half-populated entry: real directory, some content, no `.ok`.
        var entryDir = Directory.CreateDirectory(Path.Combine(cacheRoot, package.Sha512)).FullName;
        File.WriteAllText(Path.Combine(entryDir, "files.txt"), "lib/FakeSourceConnector.dll\n");

        var result = PackageMaterializer.EnsureCacheEntry(package, nupkgPath, cacheRoot);

        Assert.False(result.Hit); // this call populated it
        Assert.True(File.Exists(Path.Combine(entryDir, ".ok")));
        Assert.True(File.Exists(Path.Combine(entryDir, "lib", "FakeSourceConnector.dll")));
        Assert.Equal([entryDir], Directory.GetDirectories(cacheRoot));
    }

    /// <summary>An entry that is already valid is returned as a hit and left byte-for-byte alone. The
    /// sentinel is the guard: a re-populate would wipe it, which is exactly what the pre-delete used
    /// to do to a concurrent publisher's freshly written entry.</summary>
    [Fact]
    public async Task Valid_cache_entry_is_reused_and_never_repopulated()
    {
        var resolved = await ResolveFakeSourceConnector();
        var cacheRoot = NewCacheRoot();
        var package = resolved.Lock.Packages.Single(p => p.Id == "FakeSourceConnector");
        var nupkgPath = resolved.NupkgPaths["FakeSourceConnector"];

        var first = PackageMaterializer.EnsureCacheEntry(package, nupkgPath, cacheRoot);
        Assert.False(first.Hit);

        var sentinel = Path.Combine(first.EntryDir, "sentinel.marker");
        File.WriteAllText(sentinel, "still here");

        var second = PackageMaterializer.EnsureCacheEntry(package, nupkgPath, cacheRoot);

        Assert.True(second.Hit);
        Assert.Equal(first.EntryDir, second.EntryDir);
        Assert.True(File.Exists(sentinel), "a valid entry must be reused in place, never deleted and rebuilt");
    }
}
