using Pz.PackageManagement.Hosting;

namespace Pz.PackageManagement.Tests.Hosting;

/// <summary>The package half of the project-directory anchor: a connector loaded from a package asks
/// for one in its own <c>pz.connector.json</c>, and the host reads that off disk without loading any
/// assembly (it must — the anchor is applied to the project before the connector registry exists).</summary>
public sealed class PackageManifestsTests
{
    private static string NewPackagesDir() =>
        Directory.CreateDirectory(
            Path.Combine(Path.GetTempPath(), "pz-tests", "pkgs-" + Guid.NewGuid().ToString("N"))).FullName;

    private static void WriteManifest(string packagesDir, string id, string manifestJson)
    {
        var versionDir = Directory.CreateDirectory(Path.Combine(packagesDir, id, "1.0.0")).FullName;
        File.WriteAllText(Path.Combine(versionDir, "pz.connector.json"), manifestJson);
    }

    [Fact]
    public void A_manifest_declaring_the_anchor_contributes_its_connector_name()
    {
        var packagesDir = NewPackagesDir();
        WriteManifest(packagesDir, "Pz.Connector.DeltaLake",
            """{ "name": "deltalake", "protocolMajorMin": 1, "protocolMajorMax": 1, "projectDirectoryAnchor": true }""");

        Assert.Equal(["deltalake"], PackageManifests.AnchoredConnectorNames(packagesDir));
    }

    /// <summary>Opt-in: a manifest that says nothing gets nothing. Every connector package shipped
    /// before the field existed is exactly this shape, and injecting into one would trip its
    /// ConnectionConfigSchema's additionalProperties: false.</summary>
    [Fact]
    public void A_manifest_that_says_nothing_contributes_nothing()
    {
        var packagesDir = NewPackagesDir();
        WriteManifest(packagesDir, "Pz.Connector.Whatever",
            """{ "name": "whatever", "protocolMajorMin": 1, "protocolMajorMax": 1, "capabilities": ["source"] }""");

        Assert.Empty(PackageManifests.AnchoredConnectorNames(packagesDir));
    }

    /// <summary>A project whose packages are not restored yet finds no manifests. Nothing fails —
    /// restore is what makes them appear, and the anchor is applied on every verb including ones that
    /// run before it.</summary>
    [Fact]
    public void A_missing_packages_dir_is_empty_not_an_error() =>
        Assert.Empty(PackageManifests.AnchoredConnectorNames(
            Path.Combine(Path.GetTempPath(), "pz-tests", "absent-" + Guid.NewGuid().ToString("N"))));

    /// <summary>A broken manifest IS an error, but the connector host owns reporting it with its own code and
    /// hint when it loads the package. Throwing here would replace that message with one raised earlier
    /// that explains less — and would fail every verb's project load, including verbs that never touch
    /// the package.</summary>
    [Fact]
    public void A_broken_manifest_is_skipped_rather_than_thrown_on()
    {
        var packagesDir = NewPackagesDir();
        WriteManifest(packagesDir, "Pz.Connector.Broken", "{ not json");
        WriteManifest(packagesDir, "Pz.Connector.Good",
            """{ "name": "good", "protocolMajorMin": 1, "protocolMajorMax": 1, "projectDirectoryAnchor": true }""");

        Assert.Equal(["good"], PackageManifests.AnchoredConnectorNames(packagesDir));
    }
}
