using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.DuckDb;

namespace Pz.Engine.Tests.Validation;

/// <summary>Offline compile guard (tiers 1-2: load + compile only, no network) for
/// `samples/mssql-mart/` -- the template the real (private-repo) production mart is cloned from.
/// Its README is that deployment's production checklist; this test is what keeps the template from
/// rotting out from under that checklist. Mirrors <c>SqlDryCompilerTests</c>'s samples-loading
/// arrangement (<c>CompileHelloPz</c>/<c>FindRepoRoot</c>), the pattern authority for how a real
/// `samples/` project tree is located and compiled in this suite.
///
/// Lives here rather than in Pz.Core.Tests because the sample declares its incremental in SQL
/// via <c>watermark()</c>, and folding that comparison needs the real DuckDB-backed
/// <see cref="DuckDbSqlAstReader"/> -- a stub would parse nothing and assure nothing.</summary>
public sealed class MssqlMartSampleCompileTests
{
    private static readonly Dictionary<string, string> Env = new()
    {
        ["ERP_DB_HOST"] = "erp.example.invalid",
        ["ERP_DB_NAME"] = "erp",
        ["MART_DB_HOST"] = "mart.example.invalid",
        ["MART_DB_NAME"] = "mart",
    };

    private static (PzProject Project, CompiledDag Dag) Compile()
    {
        var project = ProjectLoader.Load(Path.Combine(FindRepoRoot(), "samples", "mssql-mart"), Env);
        var ctx = new RenderContext(project, "test-run", DateTimeOffset.UnixEpoch) { Env = Env };
        return (project, DagCompiler.Compile(project, ctx, sqlAst: new DuckDbSqlAstReader()));
    }

    [Fact]
    public void Mssql_mart_sample_compiles_clean()
    {
        var (_, dag) = Compile();

        Assert.Empty(dag.Warnings);
        Assert.Contains(dag.Nodes, n => n.Kind == NodeKind.SinkWrite);
    }

    // The sample's whole point: the connection is credentials
    // only, and everything about WHAT to read -- the entity, its partitioning, its incremental -- is
    // written in the pipeline SQL. If a future edit moves any of it back into YAML, this fails.
    [Fact]
    public void Everything_about_the_read_is_declared_in_sql()
    {
        var (project, dag) = Compile();

        Assert.All(project.Connections, c => Assert.Empty(c.Datasets));

        var read = (SourceDatasetDef)Assert.Single(dag.Nodes, n => n.Kind == NodeKind.SourceLoad).Definition!;

        Assert.Equal("dbo.orders", read.Dataset.Name);
        Assert.Equal("updated_at", read.Dataset.SyncMode!.Incremental!.Cursor);
        Assert.Null(read.Dataset.Columns);
        Assert.Equal("order_id", read.Dataset.Options["partition_column"]);
        Assert.Equal(3, read.Dataset.Retry!.MaxAttempts);
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
