using System.Text;
using NuGet.Packaging;
using NuGet.Packaging.Core;
using NuGet.Versioning;

namespace Pz.PackageManagement.Tests.Restore;

/// <summary>Builds synthetic .nupkg fixtures for asset selection, which no real fixture project can
/// express: multi-RID <c>runtimes/</c> trees, multi-TFM <c>lib/</c> trees whose ARCHIVE ORDER puts the
/// wrong build first, a dependency owning the native library, and two dependencies shipping the same
/// assembly name.
///
/// <para>Every file's content is its own archive path, so a test can assert WHICH build was extracted
/// by reading the materialized file — a bare presence check cannot tell a <c>net472</c> build from a
/// <c>net9.0</c> one.</para></summary>
public static class SyntheticNupkg
{
    /// <summary>What a materialized file's content reveals about where it came from — see the class
    /// summary. Extraction preserves content, so this is the archive path the asset was extracted
    /// from.</summary>
    public static string OriginOf(string materializedFilePath) => File.ReadAllText(materializedFilePath);

    private static string NewFeedDir() =>
        Directory.CreateDirectory(
            Path.Combine(Path.GetTempPath(), "pz-tests", "synthetic-feed-" + Guid.NewGuid().ToString("N"))).FullName;

    /// <summary>A package carrying a fake <c>runtimes/</c> tree, so RID native-asset selection can be
    /// tested without a real native-bearing fixture project.</summary>
    public static string CreateFeedWithNativePackage()
    {
        var feedDir = NewFeedDir();
        Build(feedDir, "FakeNative", "1.0.0", [],
            "lib/net10.0/FakeNative.dll",
            "runtimes/linux-x64/native/libfake.so",
            "runtimes/win-x64/native/fake.dll");
        return feedDir;
    }

    /// <summary>A package whose <c>runtimes/</c> tree ships ONLY <c>linux-x64</c> — the shape a
    /// <c>linux-musl-x64</c> host reaches through the RID graph and an <c>osx-arm64</c> host does
    /// not.</summary>
    public static string CreateFeedWithLinuxOnlyNativePackage()
    {
        var feedDir = NewFeedDir();
        Build(feedDir, "FakeLinuxOnlyNative", "1.0.0", [],
            "lib/net10.0/FakeLinuxOnlyNative.dll",
            "runtimes/linux-x64/native/libfake.so");
        return feedDir;
    }

    /// <summary>A package whose archive lists <c>lib/net472</c> and <c>runtimes/linux-arm64</c> BEFORE
    /// the builds a net10.0 linux-x64 host needs. Selecting by file name and taking the first archive
    /// match yields the net472 assembly and the arm64 native library; selecting by the resolver's own
    /// choice yields net9.0 and linux-x64.</summary>
    public static string CreateFeedWhereWrongBuildsSortFirst()
    {
        var feedDir = NewFeedDir();
        Build(feedDir, "FakeMultiTarget", "1.0.0", [],
            "lib/net472/FakeMultiTarget.dll",
            "lib/net8.0/FakeMultiTarget.dll",
            "lib/net9.0/FakeMultiTarget.dll",
            "runtimes/linux-arm64/native/libfake.so",
            "runtimes/linux-x64/native/libfake.so",
            "runtimes/win-x64/native/libfake.so");
        return feedDir;
    }

    /// <summary>A connector package with no native assets of its own that depends on a package owning
    /// them — the shape whose native library the connector's load context can only reach if
    /// materialization flattens it in.</summary>
    public static string CreateFeedWhereDependencyOwnsTheNativeLibrary()
    {
        var feedDir = NewFeedDir();
        Build(feedDir, "FakeNativeOwner", "1.0.0", [],
            "lib/net10.0/FakeNativeOwner.dll",
            "runtimes/linux-x64/native/libowned.so");
        Build(feedDir, "FakeNativeConsumer", "1.0.0", [("FakeNativeOwner", "1.0.0")],
            "lib/net10.0/FakeNativeConsumer.dll");
        return feedDir;
    }

    /// <summary>Two dependencies of one connector shipping an assembly of the SAME name — the collision
    /// that a copy-into-one-directory flattening can only resolve by overwriting.</summary>
    public static string CreateFeedWithCollidingDependencies()
    {
        var feedDir = NewFeedDir();
        Build(feedDir, "FakeCollidingA", "1.0.0", [], "lib/net10.0/Shared.dll");
        Build(feedDir, "FakeCollidingB", "1.0.0", [], "lib/net10.0/Shared.dll");
        Build(feedDir, "FakeCollisionConsumer", "1.0.0",
            [("FakeCollidingA", "1.0.0"), ("FakeCollidingB", "1.0.0")],
            "lib/net10.0/FakeCollisionConsumer.dll");
        return feedDir;
    }

    private static void Build(
        string feedDir, string id, string version,
        IReadOnlyList<(string Id, string Version)> dependencies, params string[] targetPaths)
    {
        var builder = new PackageBuilder { Id = id, Version = new NuGetVersion(version) };
        builder.Authors.Add("pz-tests");
        builder.Description = "synthetic asset-selection fixture";

        // Staged OUTSIDE feedDir: a local feed is a flat scan for *.nupkg, and stray files under it
        // are not worth betting the fixture on.
        var stagingDir = Directory.CreateDirectory(
            Path.Combine(Path.GetTempPath(), "pz-tests", "synthetic-staging-" + Guid.NewGuid().ToString("N"))).FullName;
        foreach (var targetPath in targetPaths)
        {
            // Content = archive path, so a test can tell which build was extracted. Staged under a
            // path-derived name because PackageBuilder reads each file from disk at Save() time.
            var sourcePath = Path.Combine(stagingDir, targetPath.Replace('/', '_'));
            File.WriteAllText(sourcePath, targetPath, Encoding.UTF8);
            builder.Files.Add(new PhysicalPackageFile { SourcePath = sourcePath, TargetPath = targetPath });
        }

        if (dependencies.Count > 0)
        {
            builder.DependencyGroups.Add(new PackageDependencyGroup(
                NuGet.Frameworks.NuGetFramework.Parse("net10.0"),
                dependencies.Select(d => new PackageDependency(d.Id, VersionRange.Parse($"[{d.Version}]"))).ToArray()));
        }

        using var stream = File.Create(Path.Combine(feedDir, $"{id}.{version}.nupkg"));
        builder.Save(stream);
    }
}
