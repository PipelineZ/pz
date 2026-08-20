using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.DuckDb;

namespace Pz.Engine.Tests.Validation;

/// <summary>Offline compile guard (tiers 1-2: load + compile only, no network) for
/// `samples/mysql-native/` -- the template README's runnable claims rot silently without it.
/// Mirrors <see cref="MssqlMartSampleCompileTests"/>, the pattern authority for locating and
/// compiling a real `samples/` tree in this suite, and lives here for the same reason: the
/// sample declares its incremental in SQL via <c>watermark()</c>, and folding that
/// comparison needs the real DuckDB-backed <see cref="DuckDbSqlAstReader"/>.</summary>
public sealed class MySqlNativeSampleCompileTests
{
    private static readonly Dictionary<string, string> Env = new()
    {
        ["SHOP_DB_HOST"] = "shop.example.invalid",
        ["SHOP_DB_NAME"] = "shop",
        ["SHOP_DB_USER"] = "pz",
        ["SHOP_DB_PASSWORD"] = "unused-offline",
        ["MART_DB_HOST"] = "mart.example.invalid",
        ["MART_DB_NAME"] = "mart",
        ["MART_DB_USER"] = "pz",
        ["MART_DB_PASSWORD"] = "unused-offline",
    };

    private static (PzProject Project, CompiledDag Dag) Compile()
    {
        var project = ProjectLoader.Load(Path.Combine(FindRepoRoot(), "samples", "mysql-native"), Env);
        var ctx = new RenderContext(project, "test-run", DateTimeOffset.UnixEpoch) { Env = Env };
        return (project, DagCompiler.Compile(project, ctx, sqlAst: new DuckDbSqlAstReader()));
    }

    [Fact]
    public void Mysql_native_sample_compiles_clean()
    {
        var (_, dag) = Compile();

        Assert.Empty(dag.Warnings);
        Assert.Equal(2, dag.Nodes.Count(n => n.Kind == NodeKind.SinkWrite));
    }

    /// <summary>The sample's two pipelines ARE the connector's two delivery shapes: an
    /// incremental (SQL-declared cursor) read into append-with-consent (PZ0214's happy path),
    /// and a full read into replace. If an edit changes either pairing, the README's delivery
    /// story is wrong -- fail here, not in a user's project.</summary>
    [Fact]
    public void The_two_delivery_shapes_survive_compilation()
    {
        var (project, dag) = Compile();

        Assert.All(project.Connections, c => Assert.Empty(c.Datasets));

        var reads = dag.Nodes.Where(n => n.Kind == NodeKind.SourceLoad)
            .Select(n => (SourceDatasetDef)n.Definition!).ToDictionary(d => d.Dataset.Name);
        Assert.Equal("updated_at", reads["orders"].Dataset.SyncMode!.Incremental!.Cursor);
        Assert.Null(reads["orders"].Dataset.Columns);
        Assert.Null(reads["products"].Dataset.SyncMode);

        var writes = dag.Nodes.Where(n => n.Kind == NodeKind.SinkWrite)
            .Select(n => (SinkOutputDef)n.Definition!).ToDictionary(d => d.Output.Name);
        Assert.Equal("append", writes["orders_log"].Output.Mode);
        Assert.True(writes["orders_log"].Output.AcceptDuplicates);
        Assert.Equal("replace", writes["products_snapshot"].Output.Mode);
        Assert.False(writes["products_snapshot"].Output.AcceptDuplicates);
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
