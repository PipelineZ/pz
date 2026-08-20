using Pz.Core.Dag;
using Pz.Core.Validation;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Dag;

/// <summary>Inline sink binding via `INSERT INTO {{ sink(...) }}` — renderer marker
/// extraction, XOR binding exclusivity (PZ0206/PZ0207), sink-name validation (PZ0201), and every
/// malformed sink() call shape (PZ0208). See DagCompilerTests for the pre-existing YAML-only paths
/// this is additive to.</summary>
public class InlineSinkBindingTests
{
    [Fact]
    public void Inline_binding_compiles_to_sink_write_edge()
    {
        var p = Project(
            [Pipe("totals", "INSERT INTO {{ sink('lake', 'totals', strategy: 'replace', format: 'parquet') }} select 1 as x")],
            sinks: [Sink()]);
        var dag = DagCompiler.Compile(p, Ctx(p));

        var sinkNode = dag.Nodes.Single(n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal("lake.totals", sinkNode.Name);

        var pipelineNode = dag.Nodes.Single(n => n.Kind == NodeKind.Pipeline);
        Assert.Equal("totals", pipelineNode.Name);
        Assert.Contains(pipelineNode.Id, sinkNode.DependsOn);

        var sql = pipelineNode.RenderedSql!;
        Assert.DoesNotContain("insert", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("__pz_sink__", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Leading_comments_and_whitespace_are_tolerated()
    {
        var p = Project(
            [Pipe("totals", "-- comment\n  INSERT INTO {{ sink('lake', 'totals', strategy: 'replace', format: 'parquet') }} select 1 as x")],
            sinks: [Sink()]);
        var dag = DagCompiler.Compile(p, Ctx(p));

        var pipelineNode = dag.Nodes.Single(n => n.Kind == NodeKind.Pipeline);
        Assert.Equal("select 1 as x", pipelineNode.RenderedSql);
    }

    [Fact]
    public void Cte_inside_query_is_preserved()
    {
        var p = Project(
            [Pipe("totals", "INSERT INTO {{ sink('lake', 'totals', strategy: 'replace', format: 'parquet') }} with x as (select 1) select * from x")],
            sinks: [Sink()]);
        var dag = DagCompiler.Compile(p, Ctx(p));

        var pipelineNode = dag.Nodes.Single(n => n.Kind == NodeKind.Pipeline);
        Assert.StartsWith("with x as", pipelineNode.RenderedSql);
    }

    [Fact]
    public void Two_pipelines_claiming_one_output_is_PZ0206_listing_both()
    {
        var p = Project(
            [Pipe("a", "INSERT INTO {{ sink('lake', 'totals', strategy: 'replace', format: 'parquet') }} select 1 as x"),
             Pipe("b", "INSERT INTO {{ sink('lake', 'totals', strategy: 'replace', format: 'parquet') }} select 2 as x")],
            sinks: [Sink()]);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.SinkBindingConflict);
        Assert.Contains("a", error.Message);
        Assert.Contains("b", error.Message);
        // Points at a claimant pipeline, not the sink file: the sink file declares only the
        // connection, so it is not where the conflicting claim was written.
        Assert.Equal("pipelines/a.sql", error.File);
    }

    [Fact]
    public void Two_pipelines_claiming_one_output_with_different_kwargs_is_reported_once()
    {
        // Synthesized outputs are keyed by (sink, output), so a disagreement produces ONE OutputDef
        // and therefore one PZ0206 -- not one per claimant.
        var p = Project(
            [Pipe("a", Into("totals", "replace") + "select 1 as x"),
             Pipe("b", Into("totals", "merge", ["x"]) + "select 2 as x")],
            sinks: [Sink()]);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));

        Assert.Single(ex.Errors, e => e.Code == PzErrorCode.SinkBindingConflict);
    }

    [Fact]
    public void Write_options_come_from_the_call_site_not_from_yaml()
    {
        var p = Project(
            [Pipe("a", Into("orders_current", "merge", ["order_id"], sink: "mart", format: null)
                + "select 1 as order_id")],
            sinks: [Sink("mart")]);

        var dag = DagCompiler.Compile(p, Ctx(p));

        var node = Assert.Single(dag.Nodes, n => n.Kind == NodeKind.SinkWrite);
        var def = Assert.IsType<SinkOutputDef>(node.Definition);
        Assert.Equal("mart.orders_current", node.Name);
        Assert.Equal("merge", def.Output.Mode);
        Assert.Equal(["order_id"], def.Output.Keys);
        Assert.Equal("a", def.Output.Input); // synthesized from the claimant, exactly as before
        Assert.Empty(def.Output.Options);
    }

    // PZ0207 (orphan output) is retired. An output exists
    // precisely because a sink() call site declared it, so a declared-but-unwritten output is no
    // longer representable -- a sink nothing writes to is simply an unused connection.
    [Fact]
    public void A_sink_no_pipeline_writes_to_produces_no_node_and_no_orphan_warning()
    {
        var p = Project(
            [Pipe("a", "select 1 as x")],
            sinks: [Sink()]);
        var dag = DagCompiler.Compile(p, Ctx(p)); // must NOT throw

        Assert.DoesNotContain(dag.Warnings, w => w.Code == PzErrorCode.SinkOutputUnbound);
        Assert.DoesNotContain(dag.Nodes, n => n.Kind == NodeKind.SinkWrite);
    }

    [Fact]
    public void Unknown_sink_or_output_in_sink_call_is_PZ0201()
    {
        var p = Project([Pipe("a", "INSERT INTO {{ sink('nope', 'totals', strategy: 'replace', format: 'parquet') }} select 1 as x")]);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.UnresolvedRef);
        Assert.Contains("nope", error.Message);
    }

    [Fact]
    public void Multiple_sink_calls_in_one_pipeline_is_PZ0208()
    {
        var p = Project(
            [Pipe("a", "INSERT INTO {{ sink('lake', 'totals', strategy: 'replace', format: 'parquet') }} select {{ sink('lake', 'totals2', strategy: 'replace', format: 'parquet') }} as x")],
            sinks: [Sink()]);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.InvalidSinkCall);
        Assert.Contains("a", error.Message);
    }

    [Fact]
    public void Sink_call_not_in_prefix_position_is_PZ0208()
    {
        var p = Project(
            [Pipe("a", "select * from {{ sink('lake', 'totals', strategy: 'replace', format: 'parquet') }}")],
            sinks: [Sink()]);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));

        Assert.Single(ex.Errors, e => e.Code == PzErrorCode.InvalidSinkCall);
    }

    [Fact]
    public void Ephemeral_pipeline_with_sink_call_is_PZ0208()
    {
        var p = Project(
            [Pipe("a", "INSERT INTO {{ sink('lake', 'totals', strategy: 'replace', format: 'parquet') }} select 1 as x", materialization: "ephemeral")],
            sinks: [Sink()]);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));

        Assert.Single(ex.Errors, e => e.Code == PzErrorCode.InvalidSinkCall);
    }

    /// <summary>Direct source-drain (a sink draining a source dataset via YAML
    /// `input:`) is removed. The replacement for "move a source straight to a sink" is a pass-through
    /// pipeline — `INSERT INTO {{ sink(...) }} select * from {{ source(...) }}` — which lands the
    /// source and drains it. The SinkWrite depends on the pipeline; the SourceLoad is its transitive
    /// upstream.</summary>
    [Fact]
    public void Passthrough_pipeline_drains_a_source_to_a_sink()
    {
        var p = Project(
            [Pipe("raw_orders",
                "INSERT INTO {{ sink('lake', 'raw_orders', strategy: 'replace', format: 'parquet') }} select * from {{ source('crm', 'orders') }}")],
            sources: [Crm("orders")],
            sinks: [Sink()]);
        var dag = DagCompiler.Compile(p, Ctx(p));

        var sinkNode = dag.Nodes.Single(n => n.Kind == NodeKind.SinkWrite);
        var pipelineNode = dag.Nodes.Single(n => n.Kind == NodeKind.Pipeline);
        var sourceNode = dag.Nodes.Single(n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal("lake.raw_orders", sinkNode.Name);
        Assert.Contains(pipelineNode.Id, sinkNode.DependsOn);
        Assert.Contains(sourceNode.Id, pipelineNode.DependsOn);
    }

    [Fact]
    public void Insert_form_pipeline_remains_refable()
    {
        var p = Project(
            [Pipe("totals", "INSERT INTO {{ sink('lake', 'totals', strategy: 'replace', format: 'parquet') }} select 1 as x"),
             Pipe("consumer", "select * from {{ ref('totals') }}")],
            sinks: [Sink()]);
        var dag = DagCompiler.Compile(p, Ctx(p));

        var consumer = dag.Nodes.Single(n => n.Name == "consumer");
        var totalsNode = dag.Nodes.Single(n => n.Name == "totals");
        Assert.Contains("staging.totals", consumer.RenderedSql);
        Assert.Contains(totalsNode.Id, consumer.DependsOn);
    }
}
