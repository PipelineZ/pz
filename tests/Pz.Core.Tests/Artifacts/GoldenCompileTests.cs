using System.Runtime.CompilerServices;
using Pz.Core.Artifacts;
using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Templating;

namespace Pz.Core.Tests.Artifacts;

public class GoldenCompileTests
{
    private static readonly IReadOnlyDictionary<string, string> Env = new Dictionary<string, string>
    {
        ["DATA_DIR"] = "/tmp/pz-data",
        ["OUT_DIR"] = "/tmp/pz-out",
    };

    private static string SourceTreeDir([CallerFilePath] string thisFile = "") =>
        Path.GetDirectoryName(Path.GetDirectoryName(thisFile))!; // tests/Pz.Core.Tests

    private static string CompileToTemp()
    {
        var projectDir = Path.Combine(AppContext.BaseDirectory, "Fixtures", "hello-pz");
        var project = ProjectLoader.Load(projectDir, Env);
        var ctx = new RenderContext(project, "test-run",
            new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)) { Env = Env };
        var dag = DagCompiler.Compile(project, ctx);
        var target = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
        ManifestWriter.Write(dag, project, target);
        return target;
    }

    [Fact]
    public void Compile_of_hello_pz_matches_golden_snapshots()
    {
        var actualDir = CompileToTemp();
        try
        {
            var goldenSource = Path.Combine(SourceTreeDir(), "Golden", "hello-pz");
            if (Environment.GetEnvironmentVariable("PZ_UPDATE_GOLDEN") == "1")
            {
                if (Directory.Exists(goldenSource)) Directory.Delete(goldenSource, recursive: true);
                CopyTree(actualDir, goldenSource);
                return; // snapshots refreshed; commit them
            }
            var goldenDir = Path.Combine(AppContext.BaseDirectory, "Golden", "hello-pz");
            var expected = RelativeFiles(goldenDir);
            var actual = RelativeFiles(actualDir);
            Assert.Equal(expected, actual);
            foreach (var rel in expected)
            {
                var goldenBytes = File.ReadAllBytes(Path.Combine(goldenDir, rel));
                var actualBytes = File.ReadAllBytes(Path.Combine(actualDir, rel));
                Assert.True(
                    goldenBytes.SequenceEqual(actualBytes),
                    $"golden mismatch for {rel}; run with PZ_UPDATE_GOLDEN=1 to regenerate after reviewing the diff");
            }
        }
        finally { Directory.Delete(actualDir, recursive: true); }
    }

    [Fact]
    public void Compile_is_byte_stable_across_two_runs()
    {
        var first = CompileToTemp();
        var second = CompileToTemp();
        try
        {
            foreach (var rel in RelativeFiles(first))
                Assert.Equal(
                    File.ReadAllBytes(Path.Combine(first, rel)),
                    File.ReadAllBytes(Path.Combine(second, rel)));
        }
        finally
        {
            Directory.Delete(first, recursive: true);
            Directory.Delete(second, recursive: true);
        }
    }

    private static List<string> RelativeFiles(string root) =>
        Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories)
            .Select(f => Path.GetRelativePath(root, f).Replace('\\', '/'))
            .Where(f => f != ".gitkeep")
            .Order(StringComparer.Ordinal).ToList();

    private static void CopyTree(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (var file in Directory.EnumerateFiles(from, "*", SearchOption.AllDirectories))
        {
            var dest = Path.Combine(to, Path.GetRelativePath(from, file));
            Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
            File.Copy(file, dest, overwrite: true);
        }
    }
}
