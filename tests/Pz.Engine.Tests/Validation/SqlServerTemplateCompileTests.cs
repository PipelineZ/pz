using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Templating;
using Pz.DuckDb;

namespace Pz.Engine.Tests.Validation;

/// <summary>Offline compile guard for the `sqlserver` template -- the one starting point that
/// cannot be run in CI, so compiling it is the only proof it is not broken. Passes placeholder
/// credentials because loading resolves ${VAR} references eagerly; nothing here connects.</summary>
public sealed class SqlServerTemplateCompileTests
{
    private static readonly Dictionary<string, string> Env = new()
    {
        ["ERP_DB_HOST"] = "erp.example.invalid",
        ["ERP_DB_NAME"] = "erp",
        ["ERP_DB_USER"] = "sa",
        ["ERP_DB_PASSWORD"] = "placeholder",
        ["MART_DB_HOST"] = "mart.example.invalid",
        ["MART_DB_NAME"] = "mart",
        ["MART_DB_USER"] = "sa",
        ["MART_DB_PASSWORD"] = "placeholder",
    };

    [Fact]
    public void SqlServer_template_compiles_clean()
    {
        var dir = Path.Combine(FindRepoRoot(), "templates", "sqlserver");

        var project = ProjectLoader.Load(dir, Env);
        var ctx = new RenderContext(project, "test-run", DateTimeOffset.UnixEpoch) { Env = Env };
        var dag = DagCompiler.Compile(project, ctx, sqlAst: new DuckDbSqlAstReader());

        Assert.Empty(dag.Warnings);
        Assert.Contains(dag.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Contains(dag.Nodes, n => n.Kind == NodeKind.Check);
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
