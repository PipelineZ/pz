using System.Text.Json;
using Pz.Cli;
using Pz.Cli.Commands;
using Pz.Core.Loading;

namespace Pz.Cli.Tests;

/// <summary>`pz init` scaffolds the embedded <c>Templates/init/**</c> starter project
/// (see <see cref="InitCommand"/>) into a target directory, substituting <c>{{PROJECT_NAME}}</c>
/// (sanitized) into project.yml's <c>name:</c>. Every target directory used here is a unique absolute
/// temp path passed directly as the `name` argument — never a bare relative name — so these tests never
/// depend on (or mutate) the test process's current directory.</summary>
// This class redirects Console.Out/Error in several facts, so it must join the collection that
// serializes them -- xunit only serializes classes within the SAME collection. See the
// "console-and-env-serialized" collection definition in RestoreCommandTests.cs.
[Collection("console-and-env-serialized")]
public class InitCommandTests : IDisposable
{
    private readonly string _work = Path.Combine(Path.GetTempPath(), "pz-init-tests", Guid.NewGuid().ToString("N"));

    public InitCommandTests() => Directory.CreateDirectory(_work);

    public void Dispose()
    {
        try { Directory.Delete(_work, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void Init_scaffolds_runnable_project()
    {
        var targetDir = Path.Combine(_work, "demo");

        var initExit = CliApp.Build().Parse(["init", targetDir, "--sample"]).Invoke();
        Assert.Equal(ExitCodes.Ok, initExit);

        Assert.True(File.Exists(Path.Combine(targetDir, "project.yml")));
        Assert.True(File.Exists(Path.Combine(targetDir, "connections.yml")));
        Assert.True(File.Exists(Path.Combine(targetDir, "pipelines", "stg_orders.sql")));
        Assert.True(File.Exists(Path.Combine(targetDir, "pipelines", "orders_enriched.sql")));
        Assert.True(File.Exists(Path.Combine(targetDir, "pipelines", "order_totals.sql")));
        Assert.True(File.Exists(Path.Combine(targetDir, "pipelines", "product_catalog.sql")));
        Assert.True(File.Exists(Path.Combine(targetDir, "data", "products.csv")));
        Assert.True(File.Exists(Path.Combine(targetDir, "pipelines", "configs", "orders_enriched.yml")));
        Assert.True(File.Exists(Path.Combine(targetDir, "data", "customers.csv")));
        Assert.True(File.Exists(Path.Combine(targetDir, "data", "orders.csv")));
        Assert.True(File.Exists(Path.Combine(targetDir, "README.md")));

        var projectYml = File.ReadAllText(Path.Combine(targetDir, "project.yml"));
        Assert.Contains("name: demo", projectYml);
        Assert.DoesNotContain("{{PROJECT_NAME}}", projectYml);

        var runExit = CliApp.Build().Parse(["run", "--all", "--project", targetDir]).Invoke();
        Assert.Equal(ExitCodes.Ok, runExit);

        var curated = Path.Combine(targetDir, "out", "orders_curated", "orders_curated.parquet");
        var totals = Path.Combine(targetDir, "out", "order_totals", "order_totals.csv");
        Assert.True(File.Exists(curated) && new FileInfo(curated).Length > 0);
        Assert.True(File.Exists(totals) && new FileInfo(totals).Length > 0);

        var catalog = Path.Combine(targetDir, "out", "product_catalog", "product_catalog.csv");
        Assert.True(File.Exists(catalog) && new FileInfo(catalog).Length > 0);

        var runsDir = Path.Combine(targetDir, ".pz", "runs");
        var runDir = Directory.GetDirectories(runsDir).Single();
        var runResults = JsonDocument.Parse(File.ReadAllBytes(Path.Combine(runDir, "run_results.json")));
        Assert.Equal("success", runResults.RootElement.GetProperty("status").GetString());
        foreach (var node in runResults.RootElement.GetProperty("nodes").EnumerateArray())
        {
            Assert.Equal("success", node.GetProperty("status").GetString());
        }
    }

    /// <summary>The bare verb scaffolds the MINIMAL project — two files, nothing to delete before
    /// authoring. The sample's files compile, so shipping them by default meant a first `pz run --all`
    /// moved demo data the author never wrote; it is now opt-in via <c>--sample</c>.</summary>
    [Fact]
    public void Init_without_sample_scaffolds_the_minimal_project()
    {
        var targetDir = Path.Combine(_work, "minimal");

        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["init", targetDir]).Invoke());

        Assert.Equal(
            ["connections.yml", "project.yml"],
            Directory.GetFiles(targetDir, "*", SearchOption.AllDirectories)
                .Select(f => Path.GetRelativePath(targetDir, f).Replace(Path.DirectorySeparatorChar, '/'))
                .OrderBy(f => f, StringComparer.Ordinal)
                .ToArray());
    }

    /// <summary>The minimal scaffold must still be a project pz can load and compile -- it is the
    /// starting point for every hand-authored project, so a typo in the template would strand every
    /// one of them at their first command.</summary>
    [Fact]
    public void Minimal_scaffold_loads_and_compiles_clean()
    {
        var targetDir = Path.Combine(_work, "minimal-loads");
        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["init", targetDir]).Invoke());

        var project = ProjectLoader.Load(targetDir, new Dictionary<string, string>());

        Assert.Equal("minimal_loads", project.Name);
        Assert.Empty(project.Connections);
        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["compile", "--project", targetDir]).Invoke());
    }

    [Fact]
    public void Init_rejects_nonempty_target()
    {
        var targetDir = Path.Combine(_work, "occupied");
        Directory.CreateDirectory(targetDir);
        File.WriteAllText(Path.Combine(targetDir, "existing.txt"), "already here\n");

        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        int exit;
        try
        {
            exit = CliApp.Build().Parse(["init", targetDir]).Invoke();
        }
        finally
        {
            Console.SetError(original);
        }

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0130", stderr.ToString());
        // The pre-existing file must survive untouched -- init must not have written into the directory.
        Assert.True(File.Exists(Path.Combine(targetDir, "existing.txt")));
        Assert.False(File.Exists(Path.Combine(targetDir, "project.yml")));
    }

    /// <summary>A target path that already exists as a FILE (not a directory) passes the
    /// `Directory.Exists` check (false for a file) and would reach `Directory.CreateDirectory(targetDir)`,
    /// which throws a raw <see cref="IOException"/> -- an unhandled, non-PZ-coded crash instead of a clean
    /// user-facing error. Asserts the same PZ0130 family used for the non-empty-directory case, and that
    /// the pre-existing file survives untouched.</summary>
    [Fact]
    public void Init_rejects_target_that_is_a_file()
    {
        var targetPath = Path.Combine(_work, "already-a-file");
        File.WriteAllText(targetPath, "not a directory\n");

        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        int exit;
        try
        {
            exit = CliApp.Build().Parse(["init", targetPath]).Invoke();
        }
        finally
        {
            Console.SetError(original);
        }

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0130", stderr.ToString());
        // The pre-existing file must survive untouched, and init must not have somehow created a
        // same-named directory alongside/instead of it.
        Assert.True(File.Exists(targetPath));
        Assert.Equal("not a directory\n", File.ReadAllText(targetPath));
    }

    /// <summary>The next-steps hint must echo the SANITIZED project
    /// name (matching project.yml's `name:`), not the raw target-directory argument -- so the printed
    /// suggestion is always consistent with the scaffolded project's actual name and stays copy-paste-safe
    /// even when the raw argument needed sanitizing (e.g. shell-hazardous characters like `!`).</summary>
    [Fact]
    public void Init_next_steps_hint_echoes_sanitized_name()
    {
        var targetDir = Path.Combine(_work, "My-Proj!");

        var stdout = new StringWriter();
        var original = Console.Out;
        Console.SetOut(stdout);
        int exit;
        try
        {
            exit = CliApp.Build().Parse(["init", targetDir, "--sample"]).Invoke();
        }
        finally
        {
            Console.SetOut(original);
        }

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("cd my_proj && pz run orders_enriched", stdout.ToString(), StringComparison.Ordinal);
    }

    /// <summary>The template ships TWO independent flows, so
    /// the scaffold itself demonstrates the PZ0215 bare-run gate from minute one.</summary>
    [Fact]
    public void Init_scaffold_bare_run_requires_flow_or_all()
    {
        var targetDir = Path.Combine(_work, "gate");
        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["init", targetDir, "--sample"]).Invoke());

        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        int exit;
        try
        {
            exit = CliApp.Build().Parse(["run", "--project", targetDir]).Invoke();
        }
        finally
        {
            Console.SetError(original);
        }

        Assert.Equal(ExitCodes.ConfigError, exit);
        Assert.Contains("PZ0215", stderr.ToString());
        Assert.Contains("--all", stderr.ToString());
    }

    [Fact]
    public void Init_scaffold_named_flow_runs_end_to_end()
    {
        var targetDir = Path.Combine(_work, "named");
        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["init", targetDir, "--sample"]).Invoke());

        var exit = CliApp.Build().Parse(["run", "product_catalog", "--project", targetDir]).Invoke();
        Assert.Equal(ExitCodes.Ok, exit);

        var catalog = Path.Combine(targetDir, "out", "product_catalog", "product_catalog.csv");
        Assert.True(File.Exists(catalog) && new FileInfo(catalog).Length > 0);
        // The orders flow did not run.
        Assert.False(File.Exists(Path.Combine(targetDir, "out", "orders_curated", "orders_curated.parquet")));
    }

    [Fact]
    public void Init_sanitizes_project_name()
    {
        var targetDir = Path.Combine(_work, "My-Proj!");

        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        int exit;
        try
        {
            exit = CliApp.Build().Parse(["init", targetDir]).Invoke();
        }
        finally
        {
            Console.SetError(original);
        }

        Assert.Equal(ExitCodes.Ok, exit);
        Assert.Contains("warning", stderr.ToString(), StringComparison.OrdinalIgnoreCase);

        var projectYml = File.ReadAllText(Path.Combine(targetDir, "project.yml"));
        Assert.Contains("name: my_proj", projectYml);
    }

    [Theory]
    [InlineData("My-Proj!", "my_proj")]
    [InlineData("hello_pz", "hello_pz")]
    [InlineData("123abc", "p_123abc")]
    [InlineData("___", "p_")]
    [InlineData("Multi   Word", "multi_word")]
    public void SanitizeProjectName_matches_expected(string raw, string expected) =>
        Assert.Equal(expected, InitCommand.SanitizeProjectName(raw));

    /// <summary><see cref="Template_matches_no_platform_paths"/> only
    /// scans the on-disk TEMPLATE SOURCE, which proves the embedded-resource inputs are clean but not that
    /// the actual write path (<see cref="InitCommand.Execute"/>'s <c>StreamReader</c>/<c>File.WriteAllText</c>
    /// round trip through a real, installed-tool-style scaffold) preserves that on every OS -- a
    /// platform-default <see cref="Encoding"/>/newline setting regressing in that code path would slip
    /// past a source-only scan. This test instead runs a REAL `pz init` and byte-scans the SCAFFOLDED
    /// OUTPUT on disk, closing that gap.</summary>
    [Fact]
    public void Init_scaffolded_output_is_lf_only()
    {
        var targetDir = Path.Combine(_work, "lf-check");
        var initExit = CliApp.Build().Parse(["init", targetDir, "--sample"]).Invoke();
        Assert.Equal(ExitCodes.Ok, initExit);

        var files = Directory.GetFiles(targetDir, "*", SearchOption.AllDirectories);
        Assert.NotEmpty(files);

        foreach (var file in files)
        {
            var bytes = File.ReadAllBytes(file);
            Assert.True(Array.IndexOf(bytes, (byte)'\r') < 0, $"expected no CR bytes in scaffolded {file}");

            var text = System.Text.Encoding.UTF8.GetString(bytes);
            Assert.DoesNotContain('\\', text);
        }
    }

    [Fact]
    public void Scaffolded_project_declares_the_retention_default()
    {
        var targetDir = Path.Combine(_work, "retention");
        Assert.Equal(ExitCodes.Ok, CliApp.Build().Parse(["init", targetDir]).Invoke());

        var projectYml = File.ReadAllText(Path.Combine(targetDir, "project.yml"));
        Assert.Contains("retention:", projectYml, StringComparison.Ordinal);
        Assert.Contains("keep_last: 10", projectYml, StringComparison.Ordinal);

        // The template's literal `keep_last: 10` and ProjectLoader's
        // `DefaultKeepLast` (used when `retention:` is absent) are two independent literals with
        // nothing binding them -- bumping one alone would leave the string assertion above green
        // while the scaffolded and implicit defaults silently diverge. Load the actual scaffolded
        // project (no connector/env-var resolution happens in ProjectLoader.Load itself, so this is
        // cheap) and a bare project with no `retention:` key at all, and prove they agree.
        var scaffolded = ProjectLoader.Load(targetDir, new Dictionary<string, string>());

        var bareDir = Path.Combine(_work, "retention-bare");
        Directory.CreateDirectory(bareDir);
        File.WriteAllText(Path.Combine(bareDir, "project.yml"), "name: bare\nversion: 0.1.0\n");
        var bare = ProjectLoader.Load(bareDir, new Dictionary<string, string>());

        Assert.NotNull(scaffolded.Retention);
        Assert.NotNull(bare.Retention);
        Assert.Equal(bare.Retention!.KeepLast, scaffolded.Retention!.KeepLast);
    }

    /// <summary>Byte scan over the on-disk template source (not the built assembly) -- the embedded
    /// resources are compiled FROM these files, so proving the source is `/`-separated-content and
    /// LF-only is equivalent to proving the scaffolded output is, on every OS. Guards the Windows
    /// byte-contract for the one CLI feature that ships literal file content.</summary>
    [Fact]
    public void Template_matches_no_platform_paths()
    {
        var templateDir = FindTemplateDir();
        var files = Directory.GetFiles(templateDir, "*", SearchOption.AllDirectories);
        Assert.NotEmpty(files);

        foreach (var file in files)
        {
            var bytes = File.ReadAllBytes(file);
            Assert.DoesNotContain((byte)'\r', bytes);

            var text = System.Text.Encoding.UTF8.GetString(bytes);
            // Path-shaped references inside the templates (project.yml's `path:` values, the README's
            // relative pointers) must use '/'  -- a literal backslash would be a Windows-path leak.
            Assert.DoesNotContain('\\', text);
        }
    }

    private static string FindTemplateDir()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Pz.slnx"))) dir = dir.Parent;
        var repoRoot = dir?.FullName ?? throw new InvalidOperationException("Pz.slnx not found above test base dir");
        // The whole Templates/ tree, not one template: `minimal` ships alongside `init` and is what
        // the bare verb scaffolds, so scoping this to a single directory would leave the DEFAULT
        // output unguarded -- and any template added later silently unguarded too.
        return Path.Combine(repoRoot, "src", "Pz.Cli", "Templates");
    }
}
