using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.DuckDb;
using Pz.Engine.Validation;

namespace Pz.Engine.Tests.Validation;

/// <summary>End-to-end (through <see cref="DagCompiler.Compile"/> with the real
/// <see cref="DuckDbSqlAstReader"/>, then <see cref="SqlDryCompiler"/> proving the assembled SQL is
/// actually valid, executable DuckDB SQL) for the three ephemeral-CTE-inlining shapes a purely textual
/// splice gets wrong: a consumer that opens with a comment before its WITH, a consumer using
/// WITH RECURSIVE, and an ephemeral body ending in its own trailing `;`. Lives here rather than in
/// Pz.Core.Tests because it needs the real DuckDB-backed reader -- a stub would parse nothing and
/// assure nothing (mirrors <see cref="MssqlMartSampleCompileTests"/>'s rationale).</summary>
public sealed class DagCompilerEphemeralInliningTests
{
    private static PipelineDef Pipe(string name, string sql, string materialization = "table") =>
        new(name, sql, materialization, [], [], $"pipelines/{name}.sql");

    private static async Task<(CompiledDag Dag, DryCompileResult DryCompile)> CompileAndDryCompile(
        params PipelineDef[] pipelines)
    {
        var project = new PzProject("t", "0.0.0", new EngineConfig(),
            new Dictionary<string, object?>(), [], [], [.. pipelines]);
        var ctx = new RenderContext(project, "run-1", new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var dag = DagCompiler.Compile(project, ctx, sqlAst: new DuckDbSqlAstReader());
        var dryCompile = await SqlDryCompiler.RunAsync(dag, default);
        return (dag, dryCompile);
    }

    [Fact]
    public async Task Consumer_beginning_with_a_comment_before_with_compiles_and_dry_compiles_clean()
    {
        var eph = Pipe("eph", "select 1 as n", "ephemeral");
        var consumer = Pipe("consumer",
            "-- notes about this pipeline\n" +
            "with recent as (select 2 as z) select recent.z, e.n from recent, {{ ref('eph') }} e");

        var (dag, dryCompile) = await CompileAndDryCompile(eph, consumer);

        var node = dag.Nodes.Single(n => n.Name == "consumer");
        Assert.Contains("__pz_cte__eph", node.RenderedSql, StringComparison.Ordinal);
        Assert.Empty(dryCompile.Errors);
    }

    [Fact]
    public async Task Consumer_using_with_recursive_compiles_and_dry_compiles_clean()
    {
        var eph = Pipe("eph", "select 100 as n", "ephemeral");
        var consumer = Pipe("consumer",
            "with recursive t(n) as (select 1 union all select n + 1 from t where n < 3) " +
            "select t.n, e.n as en from t, {{ ref('eph') }} e");

        var (dag, dryCompile) = await CompileAndDryCompile(eph, consumer);

        var node = dag.Nodes.Single(n => n.Name == "consumer");
        Assert.Contains("RECURSIVE", node.RenderedSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("__pz_cte__eph", node.RenderedSql, StringComparison.Ordinal);
        Assert.Empty(dryCompile.Errors);
    }

    [Fact]
    public async Task Ephemeral_body_ending_in_a_semicolon_compiles_and_dry_compiles_clean()
    {
        var eph = Pipe("eph", "select 1 as n;\n", "ephemeral");
        var consumer = Pipe("consumer", "select * from {{ ref('eph') }}");

        var (dag, dryCompile) = await CompileAndDryCompile(eph, consumer);

        var node = dag.Nodes.Single(n => n.Name == "consumer");
        Assert.Contains("__pz_cte__eph", node.RenderedSql, StringComparison.Ordinal);
        Assert.Empty(dryCompile.Errors);
    }

    [Fact]
    public void Watermark_inside_an_ephemeral_survives_the_ast_route_and_is_rewritten_on_the_consumer()
    {
        var dataset = new DatasetDef("orders",
            new Dictionary<string, object?> { ["path"] = "orders.csv", ["format"] = "csv" },
            new Dictionary<string, string> { ["updated_at"] = "timestamp", ["id"] = "bigint" });
        var crm = new ConnectionDef("crm", "localfiles",
            new Dictionary<string, object?> { ["root"] = "/data" }, [dataset], "connections.yml");
        var eph = Pipe("orders_filtered",
            "select * from {{ source('crm', 'orders') }} -- only what changed\n" +
            "where updated_at > {{ watermark('crm', 'orders') }}", "ephemeral");
        var consumer = Pipe("orders_curated",
            "with latest as (select max(id) as max_id from {{ ref('orders_filtered') }}) " +
            "select o.* from {{ ref('orders_filtered') }} o, latest");
        var project = new PzProject("t", "0.0.0", new EngineConfig(),
            new Dictionary<string, object?>(), [], [crm], [eph, consumer]);
        var ctx = new RenderContext(project, "run-1", new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));

        var dag = DagCompiler.Compile(project, ctx, sqlAst: new DuckDbSqlAstReader());

        var source = dag.Nodes.Single(n => n.Name == "src_crm__orders");
        var incremental = Assert.IsType<SourceDatasetDef>(source.Definition).Dataset.SyncMode?.Incremental;
        Assert.Equal("updated_at", incremental?.Cursor);
        var node = dag.Nodes.Single(n => n.Name == "orders_curated");
        Assert.NotEmpty(node.WatermarkSubstitutions);
        Assert.Contains("__pz_cte__orders_filtered", node.RenderedSql, StringComparison.Ordinal);
    }
}
