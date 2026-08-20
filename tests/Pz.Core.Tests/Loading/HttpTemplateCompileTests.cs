using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Templating;

namespace Pz.Core.Tests.Loading;

/// <summary>Offline compile guard (tiers 1-2: load + compile only, no DuckDB, no network) for
/// `templates/http/` -- the shipped `http` template (GitHub issues -> parquet delta log). Mirrors
/// <see cref="MssqlMartSampleCompileTests"/>, the pattern authority for locating and compiling a
/// real project tree in this suite. Deliberately passes NO env vars: the template ships with its
/// `auth:` line commented out precisely so it loads (and runs, rate-limited) with zero setup --
/// this test is what keeps that promise from rotting.</summary>
public sealed class HttpTemplateCompileTests
{
    [Fact]
    public void Http_template_compiles_clean()
    {
        var dir = Path.Combine(FindRepoRoot(), "templates", "http");
        var env = new Dictionary<string, string>();

        var project = ProjectLoader.Load(dir, env);
        var ctx = new RenderContext(project, "test-run", DateTimeOffset.UnixEpoch) { Env = env };
        var dag = DagCompiler.Compile(project, ctx);

        Assert.Empty(dag.Warnings);
        Assert.Contains(dag.Nodes, n => n.Kind == NodeKind.SinkWrite);
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Pz.slnx")))
        {
            dir = dir.Parent;
        }

        return dir?.FullName ?? throw new InvalidOperationException("Pz.slnx not found above test base dir");
    }
}
