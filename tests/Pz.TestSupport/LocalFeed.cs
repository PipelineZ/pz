namespace Pz.TestSupport;

/// <summary>Shared `dotnet pack` helper for the connector-host fixture projects. A static packing helper
/// rather than a fixture class: each consumer keeps its own thin <see cref="IDisposable"/> fixture wrapping
/// these calls, because lifetime/collection wiring (temp dir ownership, xunit collection fixtures) differs
/// per assembly.</summary>
public static class LocalFeed
{
    /// <summary>Packs a fixture project from tests/fixtures/connector-host into feedDir.
    /// versionProperty: "PackageVersion" for standalone projects, "FakeSourceConnectorVersion"
    /// for FakeSourceConnector (project-local property; avoids the PackageVersion leak into
    /// ProjectReference'd FakeTransitiveDep — see that csproj's comment). Serialization against
    /// concurrent fixture builds/packs in other test processes lives in
    /// <see cref="HermeticDotnet.Run"/>'s machine-wide lock, shared by every fixture child.</summary>
    public static void Pack(string feedDir, string project, string? version, string versionProperty = "PackageVersion")
    {
        var repoRoot = FindRepoRoot();
        var csproj = Path.Combine(repoRoot, "tests", "fixtures", "connector-host", project, project + ".csproj");
        var versionArg = version is null ? string.Empty : $" -p:{versionProperty}={version}";
        HermeticDotnet.Run($"pack \"{csproj}\" -c Debug -o \"{feedDir}\"{versionArg} --nologo -v q" + HermeticDotnet.BuildArgs,
            $"fixture pack for {project}");
    }

    /// <summary>Walks up from <see cref="AppContext.BaseDirectory"/> to the directory containing
    /// Pz.slnx.</summary>
    public static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Pz.slnx")))
        {
            dir = dir.Parent;
        }

        return dir?.FullName ?? throw new InvalidOperationException("Pz.slnx not found above test base dir");
    }
}
