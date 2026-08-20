using Pz.TestSupport;

namespace Pz.PackageManagement.Tests.Restore;

public sealed class FeedFixture : IDisposable
{
    public string FeedDir { get; }
    public string WorkRoot { get; }

    public FeedFixture()
    {
        WorkRoot = Path.Combine(Path.GetTempPath(), "pz-tests", "restore-" + Guid.NewGuid().ToString("N"));
        FeedDir = Path.Combine(WorkRoot, "feed");
        Directory.CreateDirectory(FeedDir);
        // Pack FakeTransitiveDep on its own, using its own .csproj-declared <PackageVersion> (2.0.0) —
        // no command-line override involved, so there is nothing to leak into it. Fix 4 (fixture debt):
        // FakeSourceConnector's version is now supplied via a project-local MSBuild property
        // (FakeSourceConnectorVersion, see its .csproj) instead of the global "PackageVersion" property,
        // so packing FakeSourceConnector at any version no longer affects what FakeTransitiveDep's own
        // build sees for its PackageVersion — its true 2.0.0 is what ends up recorded as the dependency
        // version in FakeSourceConnector's nuspec.
        LocalFeed.Pack(FeedDir, "FakeTransitiveDep", version: null);
        // An older FakeTransitiveDep version too (packed directly, no ProjectReference involved, so no
        // leak concern) — gives Highest_version_wins_even_when_later_range_is_narrower_v0_behavior two
        // real versions of the same id to pick between.
        LocalFeed.Pack(FeedDir, "FakeTransitiveDep", "1.0.0");
        LocalFeed.Pack(FeedDir, "FakeSourceConnector", "1.0.0", versionProperty: "FakeSourceConnectorVersion");
        LocalFeed.Pack(FeedDir, "FakeSourceConnector", "1.2.3", versionProperty: "FakeSourceConnectorVersion");
        LocalFeed.Pack(FeedDir, "FakeSourceConnector", "1.5.0-beta.1", versionProperty: "FakeSourceConnectorVersion");
    }

    public void Dispose()
    {
        try { Directory.Delete(WorkRoot, recursive: true); } catch { /* best effort */ }
    }
}

[CollectionDefinition("local-feed")]
public class LocalFeedCollection : ICollectionFixture<FeedFixture>;
