using Pz.Connectors.Abstractions;
using Pz.PackageManagement.Hosting;

namespace Pz.PackageManagement.Tests.Hosting;

/// <summary>Exercises the pre-load protocol handshake: <see cref="ManifestReader"/> parsing
/// <c>pz.connector.json</c> and <see cref="ConnectorHost.LoadFromDirectory"/>'s pre-assembly-load check.
/// Manifests are written directly into <see cref="FakeConnectorFixture"/>'s already-built package
/// directories (no repack needed) and removed again in a finally block, since the fixture and its
/// on-disk packages are shared with <see cref="ConnectorHostTests"/> via the "fake-connectors"
/// collection.</summary>
[Collection("fake-connectors")]
public class ManifestTests(FakeConnectorFixture fixture)
{
    private static readonly ConnectorPackageRef A = new("FakeConnectorA", "1.0.0");

    private string ManifestPathFor(ConnectorPackageRef packageRef) =>
        Path.Combine(fixture.PackagesRoot, packageRef.PackageId, packageRef.Version, "pz.connector.json");

    private string PackageDirFor(ConnectorPackageRef packageRef) =>
        Path.Combine(fixture.PackagesRoot, packageRef.PackageId, packageRef.Version);

    [Fact]
    public async Task Compatible_manifest_loads()
    {
        var path = ManifestPathFor(A);
        File.WriteAllText(path, """{"protocolMajorMin":1,"protocolMajorMax":1}""");
        try
        {
            await using var host = ConnectorHost.LoadFromDirectory(fixture.PackagesRoot, [A]);
            Assert.Equal("fakeA", host.Get("fakeA").Info.Name);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Incompatible_manifest_is_PZ0306_before_load()
    {
        var path = ManifestPathFor(A);
        File.WriteAllText(path, """{"protocolMajorMin":99,"protocolMajorMax":99}""");

        var contextsCreated = 0;
        ConnectorHost.OnContextCreatedForTests = _ => contextsCreated++;
        try
        {
            var ex = Assert.Throws<ConnectorHostException>(
                () => ConnectorHost.LoadFromDirectory(fixture.PackagesRoot, [A]));

            Assert.Equal("PZ0306", ex.Code);
            Assert.Contains("99", ex.Message);
            Assert.Contains(ProtocolVersion.Major.ToString(), ex.Message);
            Assert.NotNull(ex.Hint);

            // The seam-based proof: if the host created even one ConnectorLoadContext before raising
            // PZ0306, this would be nonzero. A WeakReference/GC check could not distinguish "never
            // created" from "created and already collected" - only the seam can.
            Assert.Equal(0, contextsCreated);
        }
        finally
        {
            ConnectorHost.OnContextCreatedForTests = null;
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Missing_manifest_warns_and_loads()
    {
        var warnings = new List<string>();
        await using var host = ConnectorHost.LoadFromDirectory(fixture.PackagesRoot, [A], warnings.Add);

        Assert.Single(warnings);
        Assert.Contains("FakeConnectorA", warnings[0]);
        Assert.Equal("fakeA", host.Get("fakeA").Info.Name);
    }

    [Fact]
    public void Malformed_manifest_is_PZ0306()
    {
        var path = ManifestPathFor(A);
        File.WriteAllText(path, "{ not json");
        try
        {
            var ex = Assert.Throws<ConnectorHostException>(
                () => ConnectorHost.LoadFromDirectory(fixture.PackagesRoot, [A]));

            Assert.Equal("PZ0306", ex.Code);
            Assert.Contains("pz.connector.json", ex.Message);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Inverted_range_is_PZ0306()
    {
        var path = ManifestPathFor(A);
        File.WriteAllText(path, """{"protocolMajorMin":2,"protocolMajorMax":1}""");
        try
        {
            var ex = Assert.Throws<ConnectorHostException>(
                () => ConnectorHost.LoadFromDirectory(fixture.PackagesRoot, [A]));

            Assert.Equal("PZ0306", ex.Code);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Runtime_process_with_entrypoints_parses()
    {
        var path = ManifestPathFor(A);
        File.WriteAllText(path, """
            {
              "name": "deltalake",
              "protocolMajorMin": 1,
              "protocolMajorMax": 1,
              "capabilities": ["Merge", "Transactional", "ColumnPartitionedWrites", "NativeCopy"],
              "runtime": "process",
              "entrypoints": {
                "linux-x64":  "runtimes/linux-x64/native/pz-deltalake",
                "linux-arm64":"runtimes/linux-arm64/native/pz-deltalake",
                "osx-arm64":  "runtimes/osx-arm64/native/pz-deltalake",
                "win-x64":    "runtimes/win-x64/native/pz-deltalake.exe"
              }
            }
            """);
        try
        {
            var manifest = ManifestReader.TryRead(PackageDirFor(A));

            Assert.NotNull(manifest);
            Assert.Equal("process", manifest!.Runtime);
            Assert.Equal("runtimes/linux-x64/native/pz-deltalake", manifest.Entrypoints["linux-x64"]);
            Assert.Equal("runtimes/win-x64/native/pz-deltalake.exe", manifest.Entrypoints["win-x64"]);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Runtime_absent_or_dotnet_means_alc()
    {
        var path = ManifestPathFor(A);
        try
        {
            File.WriteAllText(path, """{"protocolMajorMin":1,"protocolMajorMax":1}""");
            var absent = ManifestReader.TryRead(PackageDirFor(A));
            Assert.NotNull(absent);
            Assert.Null(absent!.Runtime);
            Assert.Empty(absent.Entrypoints);

            File.WriteAllText(path, """{"protocolMajorMin":1,"protocolMajorMax":1,"runtime":"dotnet"}""");
            var explicitDotnet = ManifestReader.TryRead(PackageDirFor(A));
            Assert.NotNull(explicitDotnet);
            Assert.Equal("dotnet", explicitDotnet!.Runtime);
            Assert.Empty(explicitDotnet.Entrypoints);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Unknown_runtime_is_PZ0354_upgrade_pz()
    {
        var path = ManifestPathFor(A);
        File.WriteAllText(path, """{"protocolMajorMin":1,"protocolMajorMax":1,"runtime":"python"}""");
        try
        {
            var ex = Assert.Throws<ConnectorHostException>(() => ManifestReader.TryRead(PackageDirFor(A)));

            Assert.Equal("PZ0354", ex.Code);
            Assert.NotNull(ex.Hint);
            Assert.Contains("upgrade pz", ex.Hint);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Process_runtime_without_entrypoints_is_PZ0354()
    {
        var path = ManifestPathFor(A);
        File.WriteAllText(path, """{"protocolMajorMin":1,"protocolMajorMax":1,"runtime":"process"}""");
        try
        {
            var ex = Assert.Throws<ConnectorHostException>(() => ManifestReader.TryRead(PackageDirFor(A)));

            Assert.Equal("PZ0354", ex.Code);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void ResolveEntrypoint_exact_rid()
    {
        var manifest = new ConnectorManifest(
            "deltalake", 1, 1, [], Runtime: "process",
            Entrypoints: new Dictionary<string, string>
            {
                ["linux-x64"] = "runtimes/linux-x64/native/pz-deltalake",
            });

        var resolved = ManifestReader.ResolveEntrypoint(manifest, "/packages/deltalake/1.0.0", "linux-x64");

        Assert.Equal(
            Path.Combine("/packages/deltalake/1.0.0", "runtimes/linux-x64/native/pz-deltalake"), resolved);
    }

    [Fact]
    public void ResolveEntrypoint_falls_back_through_rid_graph()
    {
        var manifest = new ConnectorManifest(
            "deltalake", 1, 1, [], Runtime: "process",
            Entrypoints: new Dictionary<string, string>
            {
                ["linux-x64"] = "runtimes/linux-x64/native/pz-deltalake",
            });

        var resolved = ManifestReader.ResolveEntrypoint(manifest, "/packages/deltalake/1.0.0", "linux-musl-x64");

        Assert.Equal(
            Path.Combine("/packages/deltalake/1.0.0", "runtimes/linux-x64/native/pz-deltalake"), resolved);
    }

    [Fact]
    public void ResolveEntrypoint_missing_rid_is_PZ0354()
    {
        var manifest = new ConnectorManifest(
            "deltalake", 1, 1, [], Runtime: "process",
            Entrypoints: new Dictionary<string, string>
            {
                ["linux-x64"] = "runtimes/linux-x64/native/pz-deltalake",
            });

        var ex = Assert.Throws<ConnectorHostException>(
            () => ManifestReader.ResolveEntrypoint(manifest, "/packages/deltalake/1.0.0", "win-arm64"));

        Assert.Equal("PZ0354", ex.Code);
        Assert.Contains("deltalake", ex.Message);
        Assert.Contains("win-arm64", ex.Message);
        Assert.NotNull(ex.Hint);
    }
}
