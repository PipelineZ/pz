using System.Text.Json;
using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary>project.yml's <c>extensions:</c> through a real <c>pz run</c>, using an extension statically
/// linked into DuckDB.NET so nothing is downloaded.</summary>
[Collection("console-and-env-serialized")]
public sealed class ExtensionsRunTests : IDisposable
{
    private readonly string _work =
        Path.Combine(Path.GetTempPath(), "pz-extensions-run-tests", Guid.NewGuid().ToString("N"));

    public ExtensionsRunTests()
    {
        CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", "two-flows"), _work);
        File.AppendAllText(Path.Combine(_work, "project.yml"), "\nextensions:\n  - parquet\n");
    }

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static void CopyTree(string from, string to)
    {
        foreach (var dir in Directory.GetDirectories(from, "*", SearchOption.AllDirectories))
        {
            Directory.CreateDirectory(dir.Replace(from, to));
        }

        Directory.CreateDirectory(to);
        foreach (var file in Directory.GetFiles(from, "*", SearchOption.AllDirectories))
        {
            File.Copy(file, file.Replace(from, to));
        }
    }

    [Fact]
    public void Run_records_loaded_extensions()
    {
        var exit = CliApp.Build().Parse(["run", "--all", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);

        var runResults = Assert.Single(Directory.GetFiles(Path.Combine(_work, ".pz", "runs"), "run_results.json",
            SearchOption.AllDirectories));
        using var doc = JsonDocument.Parse(File.ReadAllText(runResults));
        var names = doc.RootElement.GetProperty("extensions").EnumerateArray()
            .Select(e => e.GetProperty("name").GetString());
        Assert.Contains("parquet", names);
    }
}
