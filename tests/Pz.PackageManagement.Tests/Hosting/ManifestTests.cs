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
}
