using Pz.PackageManagement.Hosting;

namespace Pz.PackageManagement.Tests.Hosting;

/// <summary>Exercises <see cref="ManifestReader"/>: <c>pz.connector.json</c> parsing and
/// entrypoint resolution. Manifests are written into a per-test temp package directory — no built
/// connector package is involved, the reader never loads an assembly.</summary>
public sealed class ManifestTests : IDisposable
{
    private readonly string _packageDir = Path.Combine(
        Path.GetTempPath(), "pz-manifest-tests", Guid.NewGuid().ToString("N"));

    public ManifestTests() => Directory.CreateDirectory(_packageDir);

    public void Dispose()
    {
        try { Directory.Delete(_packageDir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private void WriteManifest(string json) =>
        File.WriteAllText(Path.Combine(_packageDir, "pz.connector.json"), json);

    [Fact]
    public void Missing_manifest_reads_as_null()
    {
        Assert.Null(ManifestReader.TryRead(_packageDir));
    }

    [Fact]
    public void Malformed_manifest_is_PZ0306()
    {
        WriteManifest("{ not json");

        var ex = Assert.Throws<ConnectorHostException>(() => ManifestReader.TryRead(_packageDir));

        Assert.Equal("PZ0306", ex.Code);
        Assert.Contains("pz.connector.json", ex.Message);
    }

    [Fact]
    public void Inverted_range_is_PZ0306()
    {
        WriteManifest("""{"protocolMajorMin":2,"protocolMajorMax":1}""");

        var ex = Assert.Throws<ConnectorHostException>(() => ManifestReader.TryRead(_packageDir));

        Assert.Equal("PZ0306", ex.Code);
    }

    [Fact]
    public void Runtime_process_with_entrypoints_parses()
    {
        WriteManifest("""
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

        var manifest = ManifestReader.TryRead(_packageDir);

        Assert.NotNull(manifest);
        Assert.Equal("process", manifest!.Runtime);
        Assert.Equal("runtimes/linux-x64/native/pz-deltalake", manifest.Entrypoints["linux-x64"]);
        Assert.Equal("runtimes/win-x64/native/pz-deltalake.exe", manifest.Entrypoints["win-x64"]);
    }

    // An absent runtime field and an explicit "dotnet" parse identically — the reader reports what the
    // manifest said (null vs "dotnet"); refusing them for an external package (PZ0360) is the
    // registry's decision, not the reader's.
    [Fact]
    public void Runtime_absent_or_dotnet_parses()
    {
        WriteManifest("""{"protocolMajorMin":1,"protocolMajorMax":1}""");
        var absent = ManifestReader.TryRead(_packageDir);
        Assert.NotNull(absent);
        Assert.Null(absent!.Runtime);
        Assert.Empty(absent.Entrypoints);

        WriteManifest("""{"protocolMajorMin":1,"protocolMajorMax":1,"runtime":"dotnet"}""");
        var explicitDotnet = ManifestReader.TryRead(_packageDir);
        Assert.NotNull(explicitDotnet);
        Assert.Equal("dotnet", explicitDotnet!.Runtime);
        Assert.Empty(explicitDotnet.Entrypoints);
    }

    [Fact]
    public void Unknown_runtime_is_PZ0354_upgrade_pz()
    {
        WriteManifest("""{"protocolMajorMin":1,"protocolMajorMax":1,"runtime":"python"}""");

        var ex = Assert.Throws<ConnectorHostException>(() => ManifestReader.TryRead(_packageDir));

        Assert.Equal("PZ0354", ex.Code);
        Assert.NotNull(ex.Hint);
        Assert.Contains("upgrade pz", ex.Hint);
    }

    [Fact]
    public void Process_runtime_without_entrypoints_is_PZ0354()
    {
        WriteManifest("""{"protocolMajorMin":1,"protocolMajorMax":1,"runtime":"process"}""");

        var ex = Assert.Throws<ConnectorHostException>(() => ManifestReader.TryRead(_packageDir));

        Assert.Equal("PZ0354", ex.Code);
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

    [Fact]
    public void ResolveEntrypoint_path_escaping_the_package_directory_is_PZ0354()
    {
        var manifest = new ConnectorManifest(
            "hostile", 1, 1, [], Runtime: "process",
            Entrypoints: new Dictionary<string, string>
            {
                ["linux-x64"] = "../../../etc/passwd",
            });

        var ex = Assert.Throws<ConnectorHostException>(
            () => ManifestReader.ResolveEntrypoint(manifest, "/packages/hostile/1.0.0", "linux-x64"));

        Assert.Equal("PZ0354", ex.Code);
        Assert.Contains("hostile", ex.Message);
        Assert.Contains("linux-x64", ex.Message);
        Assert.NotNull(ex.Hint);
    }
}
