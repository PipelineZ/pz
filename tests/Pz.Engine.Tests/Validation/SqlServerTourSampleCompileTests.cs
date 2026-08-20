using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.DuckDb;

namespace Pz.Engine.Tests.Validation;

/// <summary>Offline compile guard (tiers 1-2: load + compile only, no docker, no network) for the five
/// projects under `samples/sqlserver-tour/`. Each project's README teaches one path through the engine
/// and tells the reader which PZ#### a deliberate break produces; this test is what keeps those five
/// trees compiling as the authoring surface moves under them. Mirrors
/// <see cref="MssqlMartSampleCompileTests"/>'s arrangement, and lives here for the same reason: two of
/// the five declare their incremental in SQL via <c>watermark()</c>, and folding that comparison
/// needs the real DuckDB-backed <see cref="DuckDbSqlAstReader"/> — a stub would parse nothing and
/// assure nothing.
///
/// The per-project assertions below are deliberately about the *lesson* each project exists to teach
/// (which surface declares the read, which strategy the write uses), not about node counts: a sample
/// that still compiles but has quietly stopped demonstrating its own README is the failure mode worth
/// catching.</summary>
public sealed class SqlServerTourSampleCompileTests
{
    // The tour's connections.yml files interpolate these; `port:` is a literal 14333 in each file
    // (the schema wants an integer and env interpolation yields strings).
    private static readonly Dictionary<string, string> Env = new()
    {
        ["PZ_MSSQL_HOST"] = "localhost",
        ["PZ_MSSQL_PORT"] = "14333",
        ["PZ_MSSQL_DB"] = "pz",
        ["PZ_MSSQL_USER"] = "sa",
        ["PZ_MSSQL_PASSWORD"] = "not-a-real-password",
    };

    private static (PzProject Project, CompiledDag Dag) Compile(string project)
    {
        var loaded = ProjectLoader.Load(
            Path.Combine(FindRepoRoot(), "samples", "sqlserver-tour", project), Env);
        var ctx = new RenderContext(loaded, "test-run", DateTimeOffset.UnixEpoch) { Env = Env };
        return (loaded, DagCompiler.Compile(loaded, ctx, sqlAst: new DuckDbSqlAstReader()));
    }

    [Theory]
    [InlineData("01-full-refresh-replace")]
    [InlineData("02-incremental-merge")]
    [InlineData("03-windowed-backfill")]
    [InlineData("04-query-and-procedure")]
    [InlineData("05-checks-and-retry")]
    [InlineData("06-remote-state")]
    public void Tour_project_compiles_clean(string project)
    {
        var (_, dag) = Compile(project);

        Assert.Empty(dag.Warnings);
        Assert.Contains(dag.Nodes, n => n.Kind == NodeKind.SourceLoad);
        Assert.Contains(dag.Nodes, n => n.Kind == NodeKind.SinkWrite);
    }

    // 01's lesson: the read is declared in YAML (with a contract) while the write is declared at the
    // sink() call. Both sides in one place would be the thing PZ0341 refuses.
    [Fact]
    public void Full_refresh_declares_its_read_in_yaml_and_its_write_at_the_call_site()
    {
        var (_, dag) = Compile("01-full-refresh-replace");

        var read = (SourceDatasetDef)Assert.Single(dag.Nodes, n => n.Kind == NodeKind.SourceLoad).Definition!;
        Assert.Equal("dbo.customers", read.Dataset.Name);
        Assert.NotNull(read.Dataset.Columns);
        Assert.Null(read.Dataset.SyncMode?.Incremental);
    }

    // 02's lesson: connections.yml carries credentials only — the `where` is the whole incremental
    // declaration, and no columns: contract is needed to type the cursor.
    [Fact]
    public void Incremental_merge_declares_everything_about_the_read_in_sql()
    {
        var (project, dag) = Compile("02-incremental-merge");

        Assert.All(project.Connections, c => Assert.Empty(c.Datasets));

        var read = (SourceDatasetDef)Assert.Single(dag.Nodes, n => n.Kind == NodeKind.SourceLoad).Definition!;
        Assert.Equal("dbo.orders", read.Dataset.Name);
        Assert.Equal("updated_at", read.Dataset.SyncMode!.Incremental!.Cursor);
        Assert.True(read.Dataset.SyncMode.Incremental.DeclaredInSql);
        Assert.Null(read.Dataset.Columns);
    }

    // 03's lesson: initial/max_window/until written as SQL bounds — a floor and a ceiling, the ceiling
    // folded around the watermark() call. A standalone constant comparison would be an ordinary filter
    // and would NOT show up here, which is exactly what the README's callout warns about.
    [Fact]
    public void Windowed_backfill_declares_a_floor_and_a_ceiling_in_sql()
    {
        var (_, dag) = Compile("03-windowed-backfill");

        var read = (SourceDatasetDef)Assert.Single(dag.Nodes, n => n.Kind == NodeKind.SourceLoad).Definition!;
        var incremental = read.Dataset.SyncMode!.Incremental!;

        Assert.True(incremental.DeclaredInSql);
        Assert.Equal("id", incremental.Cursor);
        Assert.Contains(incremental.SqlBounds!, b => !b.IsUpper);
        Assert.Contains(incremental.SqlBounds!, b => b.IsUpper);
        Assert.Equal("4", read.Dataset.Options["partitions"]?.ToString());
    }

    // 04's lesson: two independent flows in one project (which is what makes bare `pz run` PZ0215),
    // one reading a query: entity and one a procedure: entity whose $watermark parameter is the pushdown.
    [Fact]
    public void Query_and_procedure_are_two_independent_flows()
    {
        var (_, dag) = Compile("04-query-and-procedure");

        var reads = dag.Nodes.Where(n => n.Kind == NodeKind.SourceLoad)
            .Select(n => (SourceDatasetDef)n.Definition!).ToList();

        Assert.Equal(2, reads.Count);
        Assert.Contains(reads, r => r.Dataset.Options.ContainsKey("query"));

        var proc = Assert.Single(reads, r => r.Dataset.Options.ContainsKey("procedure"));
        Assert.Equal("order_id", proc.Dataset.SyncMode!.Incremental!.Cursor);
        Assert.False(proc.Dataset.SyncMode.Incremental.DeclaredInSql);
    }

    // 05's lesson: checks and the sink write are siblings under the pipeline, with no edge between
    // them — a failing check fails the run but never blocks the load.
    [Fact]
    public void Checks_and_the_sink_write_are_both_children_of_the_pipeline()
    {
        var (_, dag) = Compile("05-checks-and-retry");

        var checks = dag.Nodes.Where(n => n.Kind == NodeKind.Check).ToList();
        Assert.Equal(5, checks.Count);

        var sink = Assert.Single(dag.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.DoesNotContain(checks, c => sink.DependsOn.Contains(c.Id));
    }

    // 06's lesson: the `state:` block moved pz's own memory into SQL Server -- the backend resolves
    // from project.yml (never the ambient environment for a project that pins it), the `ops`
    // connection exists purely for state.connection to name, artifacts defaults to true on a
    // non-local backend, and events is the explicit opt-in. Everything here validates offline: the
    // first network call is at run time, which keeps this suite docker-free.
    [Fact]
    public void Remote_state_pins_its_backend_in_project_yml()
    {
        var (project, dag) = Compile("06-remote-state");

        Assert.Equal(StateConfig.SqlServer, project.State.Backend);
        Assert.Equal("project.yml", project.State.BackendSource);
        Assert.Equal("ops", project.State.Connection);
        Assert.Equal("pzstate", project.State.Schema);
        Assert.True(project.State.Artifacts);
        Assert.True(project.State.Events);

        // The state connection is credentials-only: no pipeline reads or writes through it.
        var ops = Assert.Single(project.Connections, c => c.Name == "ops");
        Assert.Equal("sqlserver", ops.Connector);
        Assert.Empty(ops.Datasets);

        // The pipeline itself is 02's shape on purpose: the delta a dead host must not lose.
        var read = (SourceDatasetDef)Assert.Single(dag.Nodes, n => n.Kind == NodeKind.SourceLoad).Definition!;
        Assert.True(read.Dataset.SyncMode!.Incremental!.DeclaredInSql);
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
