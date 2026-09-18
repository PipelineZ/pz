using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.Restore;

namespace Pz.PackageManagement.Tests.Restore;

public class DriftCheckerTests
{
    private static LockedPackage Locked(string id, string version, bool requested = true) =>
        new(id, version, new string('a', 128),
            new LockedAssets([new LockedAsset($"{id}.dll", $"lib/net10.0/{id}.dll")], []), requested);

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
        var lockFile = new LockFile(LockFileWriter.CurrentVersion, "linux-x64", []);
        var findings = DriftChecker.Verify(
            [new ConnectorPackageRef("FakeSourceConnector", "1.2.3")], lockFile, PackagesDirWith(), "linux-x64");

        Assert.Contains(findings, f => f.Message.Contains("FakeSourceConnector") && f.Message.Contains("no entry in pz.lock.json"));
    }

    [Fact]
    public void Locked_version_outside_declared_range_is_reported()
    {
        var lockFile = new LockFile(LockFileWriter.CurrentVersion, "linux-x64", [Locked("FakeSourceConnector", "1.0.0")]);
        var packagesDir = PackagesDirWith(("FakeSourceConnector", "1.0.0"));

        var findings = DriftChecker.Verify(
            [new ConnectorPackageRef("FakeSourceConnector", "[1.2.0,2.0.0)")], lockFile, packagesDir, "linux-x64");

        Assert.Contains(findings, f => f.Message.Contains("does not admit"));
    }

    [Fact]
    public void Stale_lock_entry_with_no_requirement_is_reported()
    {
        var lockFile = new LockFile(LockFileWriter.CurrentVersion, "linux-x64",
            [Locked("FakeSourceConnector", "1.2.3"), Locked("OldConnector", "1.0.0")]);
        var packagesDir = PackagesDirWith(("FakeSourceConnector", "1.2.3"), ("OldConnector", "1.0.0"));

        var findings = DriftChecker.Verify(
            [new ConnectorPackageRef("FakeSourceConnector", "1.2.3")], lockFile, packagesDir, "linux-x64");

        Assert.Contains(findings, f => f.Message.Contains("OldConnector") && f.Message.Contains("no requirement declares it"));
    }

    [Fact]
    public void Missing_package_directory_is_reported()
    {
        var lockFile = new LockFile(LockFileWriter.CurrentVersion, "linux-x64", [Locked("FakeSourceConnector", "1.2.3")]);
        var packagesDir = PackagesDirWith(); // nothing materialized

        var findings = DriftChecker.Verify(
            [new ConnectorPackageRef("FakeSourceConnector", "1.2.3")], lockFile, packagesDir, "linux-x64");

        Assert.Contains(findings, f => f.Message.Contains("missing under"));
    }

    [Fact]
    public void A_lock_restored_for_another_rid_is_reported()
    {
        var lockFile = new LockFile(LockFileWriter.CurrentVersion, "osx-arm64", [Locked("FakeSourceConnector", "1.2.3")]);
        var packagesDir = PackagesDirWith(("FakeSourceConnector", "1.2.3"));

        var findings = DriftChecker.Verify(
            [new ConnectorPackageRef("FakeSourceConnector", "1.2.3")], lockFile, packagesDir, "linux-x64");

        var finding = Assert.Single(findings);
        Assert.Equal("PZ0321", finding.Code);
        Assert.Contains("osx-arm64", finding.Message);
        Assert.Contains("linux-x64", finding.Message);
    }

    [Fact]
    public void A_materialized_asset_whose_content_differs_from_the_lock_is_PZ0326()
    {
        var packagesDir = PackagesDirWith(("FakeSourceConnector", "1.2.3"));
        var native = Path.Combine(packagesDir, "FakeSourceConnector", "1.2.3", "native");
        Directory.CreateDirectory(native);
        File.WriteAllText(Path.Combine(native, "connector"), "modified after restore");
        var recorded = Convert.ToHexStringLower(System.Security.Cryptography.SHA512.HashData("as restored"u8));
        var lockFile = new LockFile(LockFileWriter.CurrentVersion, "linux-x64", [
            new LockedPackage("FakeSourceConnector", "1.2.3", new string('a', 128),
                new LockedAssets([], [new LockedAsset("connector", "runtimes/linux-x64/native/connector", recorded)])),
        ]);

        var findings = DriftChecker.Verify(
            [new ConnectorPackageRef("FakeSourceConnector", "1.2.3")], lockFile, packagesDir, "linux-x64");

        var finding = Assert.Single(findings);
        Assert.Equal("PZ0326", finding.Code);
        Assert.Contains("native/connector", finding.Message);
        Assert.Contains("pz restore", finding.Hint);
    }

    [Fact]
    public void An_asset_the_lock_records_no_hash_for_is_not_checked()
    {
        var packagesDir = PackagesDirWith(("FakeSourceConnector", "1.2.3"));
        var lib = Path.Combine(packagesDir, "FakeSourceConnector", "1.2.3", "lib");
        Directory.CreateDirectory(lib);
        File.WriteAllText(Path.Combine(lib, "FakeSourceConnector.dll"), "whatever");
        var lockFile = new LockFile(LockFileWriter.CurrentVersion, "linux-x64", [Locked("FakeSourceConnector", "1.2.3")]);

        var findings = DriftChecker.Verify(
            [new ConnectorPackageRef("FakeSourceConnector", "1.2.3")], lockFile, packagesDir, "linux-x64");

        Assert.Empty(findings);
    }

    [Fact]
    public void Clean_case_reports_no_drift()
    {
        var lockFile = new LockFile(LockFileWriter.CurrentVersion, "linux-x64", [Locked("FakeSourceConnector", "1.2.3")]);
        var packagesDir = PackagesDirWith(("FakeSourceConnector", "1.2.3"));

        var findings = DriftChecker.Verify(
            [new ConnectorPackageRef("FakeSourceConnector", "1.2.3")], lockFile, packagesDir, "linux-x64");

        Assert.Empty(findings);
    }

    [Fact]
    public void Transitive_only_lock_entries_are_never_reported_as_stale()
    {
        // FakeTransitiveDep is Requested=false (pulled in only transitively) — it must never be
        // reported as a stale/orphaned entry just because it isn't itself a declared connector.
        var lockFile = new LockFile(LockFileWriter.CurrentVersion, "linux-x64",
            [Locked("FakeSourceConnector", "1.2.3"), Locked("FakeTransitiveDep", "2.0.0", requested: false)]);
        var packagesDir = PackagesDirWith(("FakeSourceConnector", "1.2.3"), ("FakeTransitiveDep", "2.0.0"));

        var findings = DriftChecker.Verify(
            [new ConnectorPackageRef("FakeSourceConnector", "1.2.3")], lockFile, packagesDir, "linux-x64");

        Assert.Empty(findings);
    }
}
