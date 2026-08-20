using Pz.TestSupport;

namespace Pz.PackageManagement.Tests;

public sealed class FakeConnectorFixture : IDisposable
{
    public string PackagesRoot { get; }

    public FakeConnectorFixture()
    {
        PackagesRoot = Path.Combine(Path.GetTempPath(), "pz-tests", "packages-" + Guid.NewGuid().ToString("N"));
        var repoRoot = FindRepoRoot();
        Build(repoRoot, "FakeConnectorA", "1.0.0");
        Build(repoRoot, "FakeConnectorB", "1.0.0");
        Build(repoRoot, "FakeConnectorOld", "1.0.0");
    }

    private void Build(string repoRoot, string project, string version)
    {
        var lib = Path.Combine(PackagesRoot, project, version, "lib");
        Directory.CreateDirectory(lib);
        var csproj = Path.Combine(repoRoot, "tests", "fixtures", "connector-host", project, project + ".csproj");
        // HermeticDotnet (not a raw Process.Start): this exact site hung the whole assembly for 2+
        // minutes when its dotnet build spawned persistent MSBuild workers that inherited the redirected
        // pipes — see HermeticDotnet's doc for the mechanism and the live stack-trace evidence.
        HermeticDotnet.Run($"build \"{csproj}\" -c Debug -o \"{lib}\" --nologo -v q" + HermeticDotnet.BuildArgs,
            $"fixture build for {project}");
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Pz.slnx"))) dir = dir.Parent;
        return dir?.FullName ?? throw new InvalidOperationException("Pz.slnx not found above test base dir");
    }

    public void Dispose()
    {
        try { Directory.Delete(PackagesRoot, recursive: true); } catch { /* best effort */ }
    }
}

[CollectionDefinition("fake-connectors")]
public class FakeConnectorCollection : ICollectionFixture<FakeConnectorFixture>;
