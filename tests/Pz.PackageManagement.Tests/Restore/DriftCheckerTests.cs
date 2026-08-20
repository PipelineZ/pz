using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.Restore;

namespace Pz.PackageManagement.Tests.Restore;

public class DriftCheckerTests
{
    private static LockedPackage Locked(string id, string version, bool requested = true) =>
        new(id, version, new string('a', 128), new LockedAssets([$"{id}.dll"], []), requested);

    private static string PackagesDirWith(params (string Id, string Version)[] installed)
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-tests", "packages-" + Guid.NewGuid().ToString("N"));
        foreach (var (id, version) in installed)
        {
            Directory.CreateDirectory(Path.Combine(dir, id, version));
        }

        return dir;
    }

    [Fact]
    public void Requirement_missing_from_lock_is_reported()
    {
        var lockFile = new LockFile(1, "linux-x64", []);
        var findings = DriftChecker.Verify(
            [new ConnectorPackageRef("FakeSourceConnector", "1.2.3")], lockFile, PackagesDirWith());

        Assert.Contains(findings, f => f.Contains("FakeSourceConnector") && f.Contains("no entry in pz.lock.json"));
    }

    [Fact]
    public void Locked_version_outside_declared_range_is_reported()
    {
        var lockFile = new LockFile(1, "linux-x64", [Locked("FakeSourceConnector", "1.0.0")]);
        var packagesDir = PackagesDirWith(("FakeSourceConnector", "1.0.0"));

        var findings = DriftChecker.Verify(
            [new ConnectorPackageRef("FakeSourceConnector", "[1.2.0,2.0.0)")], lockFile, packagesDir);

        Assert.Contains(findings, f => f.Contains("does not admit"));
    }

    [Fact]
    public void Stale_lock_entry_with_no_requirement_is_reported()
    {
        var lockFile = new LockFile(1, "linux-x64",
            [Locked("FakeSourceConnector", "1.2.3"), Locked("OldConnector", "1.0.0")]);
        var packagesDir = PackagesDirWith(("FakeSourceConnector", "1.2.3"), ("OldConnector", "1.0.0"));

        var findings = DriftChecker.Verify(
            [new ConnectorPackageRef("FakeSourceConnector", "1.2.3")], lockFile, packagesDir);

        Assert.Contains(findings, f => f.Contains("OldConnector") && f.Contains("no requirement declares it"));
    }

    [Fact]
    public void Missing_package_directory_is_reported()
    {
        var lockFile = new LockFile(1, "linux-x64", [Locked("FakeSourceConnector", "1.2.3")]);
        var packagesDir = PackagesDirWith(); // nothing materialized

        var findings = DriftChecker.Verify(
            [new ConnectorPackageRef("FakeSourceConnector", "1.2.3")], lockFile, packagesDir);

        Assert.Contains(findings, f => f.Contains("missing under"));
    }

    [Fact]
    public void Clean_case_reports_no_drift()
    {
        var lockFile = new LockFile(1, "linux-x64", [Locked("FakeSourceConnector", "1.2.3")]);
        var packagesDir = PackagesDirWith(("FakeSourceConnector", "1.2.3"));

        var findings = DriftChecker.Verify(
            [new ConnectorPackageRef("FakeSourceConnector", "1.2.3")], lockFile, packagesDir);

        Assert.Empty(findings);
    }

    [Fact]
    public void Transitive_only_lock_entries_are_never_reported_as_stale()
    {
        // FakeTransitiveDep is Requested=false (pulled in only transitively) — it must never be
        // reported as a stale/orphaned entry just because it isn't itself a declared connector.
        var lockFile = new LockFile(1, "linux-x64",
            [Locked("FakeSourceConnector", "1.2.3"), Locked("FakeTransitiveDep", "2.0.0", requested: false)]);
        var packagesDir = PackagesDirWith(("FakeSourceConnector", "1.2.3"), ("FakeTransitiveDep", "2.0.0"));

        var findings = DriftChecker.Verify(
            [new ConnectorPackageRef("FakeSourceConnector", "1.2.3")], lockFile, packagesDir);

        Assert.Empty(findings);
    }
}
