using System.Diagnostics;
using System.IO.Compression;
using System.Reflection;
using System.Text.RegularExpressions;
using Pz.Cli;
using Pz.TestSupport;

namespace Pz.Cli.Tests;

/// <summary>Proves MinVer's version actually flows into the running tool, without a
/// pack roundtrip (that's <c>scripts/verify-tool-install.sh</c>'s job).
/// System.CommandLine's <c>RootCommand</c> ships a built-in "--version" option (see
/// <c>System.CommandLine.VersionOption.VersionOptionAction.GetExecutableVersion</c>) that prints
/// <c>Assembly.GetEntryAssembly()</c>'s <see cref="AssemblyInformationalVersionAttribute"/> — the exact
/// property MinVer's MSBuild target sets from git height/tags.
///
/// <see cref="Version_verb_prints_the_assembly_informational_version"/> runs Pz.Cli.dll as a real
/// subprocess (via `dotnet exec`) rather than calling <c>CliApp.Build().Parse(...).Invoke()</c> in-proc:
/// in-proc, <c>Assembly.GetEntryAssembly()</c> is the xunit test host, not Pz.Cli, so "--version" would
/// print the test host's own version, not Pz.Cli's — proving nothing about the shipped tool.</summary>
public class PackagingSmokeTests
{
    // MinVer-shaped: MAJOR.MINOR.PATCH, optional "-prerelease" (e.g. "-alpha.0.80" pre-tag), optional
    // "+buildmetadata" (MinVer includes the commit sha in InformationalVersion, stripped for
    // PackageVersion). Matches both the pre-tag height build ("0.0.0-alpha.0.80+<sha>") and a real
    // tagged release ("0.1.0").
    private static readonly Regex MinVerShaped =
        new(@"^\d+\.\d+\.\d+(-[0-9A-Za-z-.]+)?(\+[0-9A-Za-z-.]+)?$", RegexOptions.Compiled);

    [Fact]
    public void Assembly_informational_version_is_minver_shaped()
    {
        var attribute = typeof(CliApp).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>();

        Assert.NotNull(attribute);
        Assert.False(string.IsNullOrWhiteSpace(attribute!.InformationalVersion));
        Assert.Matches(MinVerShaped, attribute.InformationalVersion);
    }

    [Fact]
    public void Version_verb_prints_the_assembly_informational_version()
    {
        var expected = typeof(CliApp).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()!
            .InformationalVersion;

        // Pz.Cli.Tests references Pz.Cli via ProjectReference, so its build output is copied alongside
        // the test assembly in the same output directory.
        var cliDll = Path.Combine(AppContext.BaseDirectory, "Pz.Cli.dll");
        Assert.True(File.Exists(cliDll), $"expected {cliDll} to exist (Pz.Cli ProjectReference output)");

        var psi = new ProcessStartInfo("dotnet", $"exec \"{cliDll}\" --version")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using var process = Process.Start(psi)!;
        var stdout = process.StandardOutput.ReadToEnd();
        var stderr = process.StandardError.ReadToEnd();
        process.WaitForExit();

        Assert.Equal(0, process.ExitCode);
        Assert.Equal(expected, stdout.Trim());
        Assert.Empty(stderr);
    }

    /// <summary>Pack-inspect regression coverage for
    /// https://pipelinez.dev/how-to/author-a-connector/'s manifest contract. Each connector's <c>.csproj</c> packs
    /// <c>pz.connector.json</c> at the nupkg root via <c>Pack="true" PackagePath=""</c> (see that file's
    /// comment) -- this test proves the packed BYTES actually contain the entry, at the root, not just
    /// that the source file exists on disk. Without this, a future edit that changes `PackagePath` (e.g.
    /// to a subfolder) or swaps `<None Include>` for a `<Content Include>` that isn't `Pack="true"` would
    /// silently ship a connector nupkg <see cref="Pz.PackageManagement.Hosting.ConnectorHost"/> can no
    /// longer manifest-check (PZ0306 handshake) -- nothing else in the suite packs these projects for
    /// real and opens the resulting archive.
    ///
    /// Builtin connectors set <c>IsPackable=false</c> (they ship inside the Pz.Cli tool package, so a
    /// standalone nupkg would be one nobody could install and use), which is why the pack below passes
    /// <c>-p:IsPackable=true</c>: a command-line global property outranks the project's own. That keeps
    /// this test meaningful rather than vacuous. The manifest Pack items were deliberately left in place
    /// so that flipping <c>IsPackable</c> back on yields a publishable package, which is the path if a
    /// builtin is ever unbundled and restored like a third-party connector -- and this test is precisely
    /// what proves that promise still holds.</summary>
    [Theory]
    [InlineData("Pz.Connector.LocalFiles")]
    [InlineData("Pz.Connector.Postgres")]
    [InlineData("Pz.Connector.S3")]
    public void Connector_nupkg_contains_manifest_at_root(string projectName)
    {
        var repoRoot = FindRepoRoot();
        var csprojPath = Path.Combine(repoRoot, "connectors", projectName, projectName + ".csproj");
        Assert.True(File.Exists(csprojPath), $"expected {csprojPath} to exist");

        var outDir = Path.Combine(Path.GetTempPath(), "pz-pack-inspect", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(outDir);
        try
        {
            // HermeticDotnet (not a raw Process.Start): a redirected dotnet pack that spawns persistent
            // MSBuild/Roslyn children hangs ReadToEnd() after the pack exits — see HermeticDotnet's doc.
            // It throws (with the child's output) on a nonzero exit, replacing the old ExitCode assert.
            HermeticDotnet.Run($"pack \"{csprojPath}\" -c Release -o \"{outDir}\" --nologo -v quiet -p:IsPackable=true" + HermeticDotnet.BuildArgs,
                $"dotnet pack for {projectName}");

            var nupkg = Directory.GetFiles(outDir, "*.nupkg").SingleOrDefault();
            Assert.True(nupkg is not null, $"expected exactly one .nupkg in {outDir}");

            using var archive = ZipFile.OpenRead(nupkg!);
            var entry = archive.GetEntry("pz.connector.json");
            Assert.True(entry is not null, $"{projectName}'s nupkg is missing pz.connector.json at its root");
        }
        finally
        {
            try { Directory.Delete(outDir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Pz.slnx"))) dir = dir.Parent;
        return dir?.FullName ?? throw new InvalidOperationException("Pz.slnx not found above test base dir");
    }
}
