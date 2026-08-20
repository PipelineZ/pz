using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Templating;
using Pz.DuckDb;

namespace Pz.Engine.Tests.Validation;

/// <summary>Offline compile guard for the `incremental` template. Its read is declared entirely in
/// SQL via watermark(), with no `sync:` block to fall back on, so a regression in that derivation
/// path would strand every project scaffolded from it at compile time.
///
/// Lives here rather than in Pz.Core.Tests because folding the watermark() comparison needs the
/// real DuckDB-backed <see cref="DuckDbSqlAstReader"/> -- a stub would parse nothing and assure
/// nothing. Mirrors <c>MssqlMartSampleCompileTests</c>.</summary>
public sealed class IncrementalTemplateCompileTests
{
    [Fact]
    public void Incremental_template_compiles_clean()
    {
        var dir = Path.Combine(FindRepoRoot(), "templates", "incremental");
        var env = new Dictionary<string, string>();

        var project = ProjectLoader.Load(dir, env);
        var ctx = new RenderContext(project, "test-run", DateTimeOffset.UnixEpoch) { Env = env };
        var dag = DagCompiler.Compile(project, ctx, sqlAst: new DuckDbSqlAstReader());

        Assert.Empty(dag.Warnings);
        Assert.Contains(dag.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Contains(dag.Nodes, n => n.Kind == NodeKind.SourceLoad);
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
