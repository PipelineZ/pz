using Pz.TestSupport;

namespace Pz.PackageManagement.Tests.Restore;

/// <summary>Regression net for the fixture-pack file-lock flake: three test assemblies
/// (`FeedFixture`, Pz.Cli.Tests' `CliLocalFeedFixture`, Pz.EndToEnd.Tests' `M2LocalFeedFixture`) pack
/// the SAME fixture csprojs concurrently during a parallel `dotnet test` run; the feed output dirs are
/// unique but the projects' obj/bin intermediates are shared, so unserialized packs race on MSBuild
/// intermediate files. Every `HermeticDotnet.Run` child holds a machine-wide named mutex for exactly
/// this reason — this test hammers the same-project-concurrent-pack shape (hotter than the natural race) and must
/// stay green deterministically. It cannot be a deterministic RED pre-fix (the collision is a real
/// race), so the fix's evidence standard is this test plus the stress gate, not a red/green flip.</summary>
public sealed class LocalFeedConcurrencyTests
{
    [Fact]
    public async Task Concurrent_packs_of_the_same_fixture_project_all_succeed()
    {
        var workRoot = Path.Combine(Path.GetTempPath(), "pz-tests", "pack-race-" + Guid.NewGuid().ToString("N"));
        try
        {
            var feedDirs = Enumerable.Range(0, 3)
                .Select(i => Path.Combine(workRoot, $"feed{i}"))
                .ToArray();
            foreach (var dir in feedDirs)
            {
                Directory.CreateDirectory(dir);
            }

            await Task.WhenAll(feedDirs.Select(dir =>
                Task.Run(() => LocalFeed.Pack(dir, "FakeTransitiveDep", version: null))));

            foreach (var dir in feedDirs)
            {
                Assert.Single(Directory.GetFiles(dir, "*.nupkg"));
            }
        }
        finally
        {
            try { Directory.Delete(workRoot, recursive: true); } catch { /* best effort */ }
        }
    }
}
