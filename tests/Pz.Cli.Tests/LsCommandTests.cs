using Pz.Cli;

namespace Pz.Cli.Tests;

/// <summary>`pz ls`: compiles the project and prints one row per node in topological order —
/// `kind  name  tags`. Uses the same `hello-pz` fixture as
/// <see cref="CompileCommandTests"/>. Joins "console-and-env-serialized" (see its definition in
/// RestoreCommandTests.cs) because it redirects Console.Out/Error and mutates the process-global
/// DATA_DIR/OUT_DIR environment variables.</summary>
[Collection("console-and-env-serialized")]
public sealed class LsCommandTests : IDisposable
{
    private readonly string _work =
        Path.Combine(Path.GetTempPath(), "pz-ls-tests", Guid.NewGuid().ToString("N"));

    public LsCommandTests()
    {
        Environment.SetEnvironmentVariable("DATA_DIR", "/tmp/pz-data");
        Environment.SetEnvironmentVariable("OUT_DIR", "/tmp/pz-out");
        CopyTree(Path.Combine(AppContext.BaseDirectory, "Fixtures", "hello-pz"), _work);
    }

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void Ls_lists_nodes_in_topological_order()
    {
        var output = RunAndCaptureStdout(["ls", "--project", _work]);

        var lines = output.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal($"{"kind",-10} {"name",-40} {"tags"}", lines[0]);

        var dataLines = lines.Skip(1).ToArray();
        Assert.StartsWith("source_load", dataLines[0]);
        Assert.Contains($"{"source_load",-10} {"src_crm__customers",-40} {"-"}", dataLines);

        // No artifacts are written by `pz ls` -- it is a read-only reporting verb.
        Assert.False(Directory.Exists(Path.Combine(_work, ".pz")));
    }

    [Fact]
    public void Ls_honors_select()
    {
        var output = RunAndCaptureStdout(["ls", "--project", _work, "--select", "orders_enriched"]);

        var lines = output.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        var dataLines = lines.Skip(1).ToArray();

        Assert.Single(dataLines);
        Assert.Contains("orders_enriched", dataLines[0]);
    }

    [Fact]
    public void Ls_errors_cleanly_outside_project()
    {
        var emptyDir = Path.Combine(Path.GetTempPath(), "pz-ls-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(emptyDir);
        try
        {
            var stderr = RunAndCaptureStderr(["ls", "--project", emptyDir], out var exit);
            Assert.Equal(ExitCodes.ConfigError, exit);
            Assert.Contains("project.yml", stderr);
        }
        finally
        {
            Directory.Delete(emptyDir, recursive: true);
        }
    }

    private static string RunAndCaptureStdout(string[] args)
    {
        var stdout = new StringWriter();
        var original = Console.Out;
        Console.SetOut(stdout);
        int exit;
        try { exit = CliApp.Build().Parse(args).Invoke(); }
        finally { Console.SetOut(original); }
        Assert.Equal(ExitCodes.Ok, exit);
        return stdout.ToString();
    }

    private static string RunAndCaptureStderr(string[] args, out int exit)
    {
        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        try { exit = CliApp.Build().Parse(args).Invoke(); }
        finally { Console.SetError(original); }
        return stderr.ToString();
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
