using System.Text.Json;
using Pz.Cli;

namespace Pz.Cli.Tests;

// This class, PlanCommandTests, and RunCommandTests all mutate the process-global DATA_DIR/OUT_DIR
// environment variables (one test here even clears DATA_DIR entirely to exercise the "undeclared env
// var" error), so they must serialize against each other. See the "console-and-env-serialized"
// collection definition in RestoreCommandTests.cs for the full rationale -- this class joins it for
// DATA_DIR/OUT_DIR, not Console (it doesn't redirect Console.Out/Error).
[Collection("console-and-env-serialized")]
public class CompileCommandTests : IDisposable
{
    private readonly string _work =
        Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));

    public CompileCommandTests()
    {
        Environment.SetEnvironmentVariable("DATA_DIR", "/tmp/pz-data");
        Environment.SetEnvironmentVariable("OUT_DIR", "/tmp/pz-out");
        CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", "hello-pz"), _work);
    }

    public void Dispose() => Directory.Delete(_work, recursive: true);

    [Fact]
    public void Compile_succeeds_and_writes_target_artifacts()
    {
        var exit = CliApp.Build().Parse(["compile", "--project", _work]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);
        Assert.True(File.Exists(Path.Combine(_work, ".pz", "target", "manifest.json")));
        Assert.True(File.Exists(Path.Combine(_work, ".pz", "target", "compiled", "stg_orders.sql")));
        Assert.True(File.Exists(Path.Combine(_work, ".pz", "target", "compiled", "orders_enriched.sql")));
    }

    [Fact]
    public void Compile_with_select_filters_emitted_artifacts()
    {
        var exit = CliApp.Build().Parse(["compile", "--project", _work, "--select", "stg_orders"]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);
        Assert.True(File.Exists(Path.Combine(_work, ".pz", "target", "manifest.json")));
        Assert.True(File.Exists(Path.Combine(_work, ".pz", "target", "compiled", "stg_orders.sql")));
        Assert.False(File.Exists(Path.Combine(_work, ".pz", "target", "compiled", "orders_enriched.sql")));
    }

    [Fact]
    public void Selecting_isolated_check_node_compiles_cleanly()
    {
        var exit = CliApp.Build().Parse(["compile", "--project", _work, "--select", "check_orders_enriched_unique_id"]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);

        var manifestPath = Path.Combine(_work, ".pz", "target", "manifest.json");
        Assert.True(File.Exists(manifestPath));

        using var manifest = JsonDocument.Parse(File.ReadAllBytes(manifestPath));
        var nodes = manifest.RootElement.GetProperty("nodes");
        Assert.Equal(1, nodes.GetArrayLength());

        var node = nodes[0];
        Assert.Equal("check", node.GetProperty("kind").GetString());
        Assert.Equal("check_orders_enriched_unique_id", node.GetProperty("name").GetString());
        Assert.Equal("pipelines/orders_enriched.sql", node.GetProperty("file").GetString());
    }

    [Fact]
    public void Compile_on_broken_project_returns_config_error()
    {
        Environment.SetEnvironmentVariable("DATA_DIR", null); // makes ${DATA_DIR} undeclared
        try
        {
            var exit = CliApp.Build().Parse(["compile", "--project", _work]).Invoke();
            Assert.Equal(ExitCodes.ConfigError, exit);
        }
        finally { Environment.SetEnvironmentVariable("DATA_DIR", "/tmp/pz-data"); }
    }

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
