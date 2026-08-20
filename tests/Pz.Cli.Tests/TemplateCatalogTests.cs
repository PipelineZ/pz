using System.Reflection;
using Pz.Cli.Commands;

namespace Pz.Cli.Tests;

/// <summary>Binds the on-disk `templates/` tree to what actually ships. Every template is a real
/// project a stranger starts from, so a missing README strands them and a missing .gitignore is
/// invisible until they commit their own run artifacts -- neither failure surfaces from a scaffold
/// that otherwise succeeds, which is why they are asserted structurally here.</summary>
public class TemplateCatalogTests
{
    internal static readonly string[] RequiredFiles =
        ["project.yml", "connections.yml", "README.md", ".gitignore"];

    [Fact]
    public void Every_template_ships_the_required_files()
    {
        var templates = Directory.GetDirectories(TemplatesDir());
        Assert.NotEmpty(templates);

        foreach (var dir in templates)
        {
            foreach (var required in RequiredFiles)
            {
                Assert.True(File.Exists(Path.Combine(dir, required)),
                    $"template '{Path.GetFileName(dir)}' is missing {required}");
            }
        }
    }

    [Fact]
    public void Every_template_gitignores_run_state_and_output()
    {
        foreach (var dir in Directory.GetDirectories(TemplatesDir()))
        {
            var ignored = File.ReadAllText(Path.Combine(dir, ".gitignore"));
            Assert.Contains(".pz/", ignored, StringComparison.Ordinal);
            Assert.Contains("out/", ignored, StringComparison.Ordinal);
        }
    }

    /// <summary>The .gitignore travels through the MSBuild glob, the embed, and the copy-out, and a
    /// dotfile lost at any of those steps fails silently -- the scaffold still succeeds, just without
    /// the file. Asserted against the embedded resources rather than the source tree, because the
    /// source tree is the one place it cannot go missing.</summary>
    [Fact]
    public void Every_template_embeds_its_gitignore()
    {
        var embedded = typeof(InitCommand).Assembly.GetManifestResourceNames()
            .Select(n => n.Replace('\\', '/'))
            .Where(n => n.StartsWith("Templates/", StringComparison.Ordinal))
            .ToArray();

        foreach (var dir in Directory.GetDirectories(TemplatesDir()))
        {
            var id = Path.GetFileName(dir);
            Assert.Contains($"Templates/{id}/.gitignore", embedded);
        }
    }

    /// <summary>The embed and the tree must hold exactly the same files. A file present on disk but
    /// not embedded ships a broken template; a file embedded but not on disk is a run artifact that
    /// leaked into the tool. Neither fails the build on its own, and the second is easy to cause --
    /// these directories are runnable in place. This compares against files ON DISK (minus `.pz`/`out`),
    /// not against what git tracks -- an untracked stray file under <c>templates/</c> would be embedded
    /// and would ship, and this test would not catch it.</summary>
    [Fact]
    public void Embedded_resources_match_the_template_tree()
    {
        var root = TemplatesDir();
        var onDisk = Directory.GetFiles(root, "*", SearchOption.AllDirectories)
            .Select(p => "Templates/" + Path.GetRelativePath(root, p).Replace('\\', '/'))
            .Where(p => !p.Contains("/.pz/", StringComparison.Ordinal)
                     && !p.Contains("/out/", StringComparison.Ordinal))
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToArray();

        var embedded = typeof(InitCommand).Assembly.GetManifestResourceNames()
            .Select(n => n.Replace('\\', '/'))
            .Where(n => n.StartsWith("Templates/", StringComparison.Ordinal))
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(onDisk, embedded);
    }

    /// <summary>Catalog and tree are bound in BOTH directions on purpose: a directory with no entry
    /// is a template nobody can select, and an entry with no directory is a `--template` id that
    /// scaffolds an empty project. Neither shows up as a build or scaffold failure on its own.</summary>
    [Fact]
    public void Catalog_ids_and_template_directories_are_the_same_set()
    {
        var onDisk = Directory.GetDirectories(TemplatesDir())
            .Select(Path.GetFileName)
            .OrderBy(id => id, StringComparer.Ordinal)
            .ToArray();
        var catalog = TemplateCatalog.All
            .Select(t => t.Id)
            .OrderBy(id => id, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(onDisk, catalog);
    }

    [Fact]
    public void Default_template_is_in_the_catalog() =>
        Assert.NotNull(TemplateCatalog.Find(TemplateCatalog.DefaultId));

    internal static string TemplatesDir()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Pz.slnx")))
        {
            dir = dir.Parent;
        }

        var root = dir?.FullName ?? throw new InvalidOperationException("Pz.slnx not found above test base dir");
        return Path.Combine(root, "templates");
    }
}
