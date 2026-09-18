using Pz.Core.Dag;
using Pz.Core.Validation;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Dag;

public class CompileWarningsTests
{
    [Fact]
    public void Clean_project_has_no_warnings()
    {
        var p = Project(
            [Pipe("totals", "INSERT INTO {{ sink('lake', 'totals', strategy: 'replace', format: 'parquet') }} select 1 as x")],
            sinks: [Sink()]);
        var dag = DagCompiler.Compile(p, Ctx(p));
        Assert.Empty(dag.Warnings);
    }

    [Fact]
    public void Load_time_warnings_on_the_project_surface_in_the_compiled_dag()
    {
        // ConnectionsLoader's "${VAR} inside entities: is never interpolated" warning (and any other
        // load-time finding) is on PzProject, not CompiledDag -- this pins that DagCompiler merges it
        // into the one channel every caller (plan/run/validate) already renders warnings through,
        // rather than that finding being silently dropped between loading and compiling.
        var loadTimeWarning = new PzWarning(PzErrorCode.EnvRefNotInterpolatedInEntity,
            "connections.yml: something", "connections.yml", null, "fix it");
        var p = Project(
            [Pipe("totals", "INSERT INTO {{ sink('lake', 'totals', strategy: 'replace', format: 'parquet') }} select 1 as x")],
            sinks: [Sink()]) with
        {
            Warnings = [loadTimeWarning],
        };

        var dag = DagCompiler.Compile(p, Ctx(p));

        Assert.Contains(dag.Warnings, w => w.Code == PzErrorCode.EnvRefNotInterpolatedInEntity);
    }

    [Fact]
    public void A_sink_no_pipeline_writes_to_produces_no_node_and_no_warning()
    {
        // An output exists precisely because a sink() call site declared it, so one can no
        // longer be declared without a writer. A sink nothing writes to is just an unused connection.
        var p = Project(
            [Pipe("a", "INSERT INTO {{ sink('lake', 'used', strategy: 'replace', format: 'parquet') }} select 1 as x")],
            sinks: [Sink(), Sink("unused")]);
        var dag = DagCompiler.Compile(p, Ctx(p)); // must NOT throw
        Assert.DoesNotContain(dag.Warnings, x => x.Code == PzErrorCode.SinkOutputUnbound);
        Assert.Single(dag.Nodes, n => n.Kind == NodeKind.SinkWrite);
        Assert.Equal("lake.used", Assert.Single(dag.Nodes, n => n.Kind == NodeKind.SinkWrite).Name);
    }

    [Theory]
    [InlineData("strategyy")]
    [InlineData("stratgy")]
    [InlineData("Strategy")]
    [InlineData("keyz")]
    public void A_kwarg_one_edit_from_a_pz_key_is_named_not_silently_sent_to_the_connector(string typo)
    {
        // pz cannot REFUSE an unrecognized kwarg -- no connector publishes a write-option vocabulary,
        // so `keyz` may genuinely be one. But a typo of `strategy` silently defaulting the write to
        // append is the failure this surface can least afford, so a near miss is said out loud.
        var p = Project(
            [Pipe("a", $"INSERT INTO {{{{ sink('lake', 'out', {typo}: 'merge') }}}} select 1 as x")],
            sinks: [Sink()]);

        var dag = DagCompiler.Compile(p, Ctx(p)); // a warning, never an error

        var warning = Assert.Single(dag.Warnings, w => w.Code == PzErrorCode.InvalidSinkCall);
        Assert.Contains(typo, warning.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("format")]
    [InlineData("path")]
    [InlineData("bucket")]
    [InlineData("compression")]
    public void A_genuine_connector_option_draws_no_near_miss_warning(string option)
    {
        var p = Project(
            [Pipe("a", $"INSERT INTO {{{{ sink('lake', 'out', {option}: 'v') }}}} select 1 as x")],
            sinks: [Sink()]);

        var dag = DagCompiler.Compile(p, Ctx(p));

        Assert.DoesNotContain(dag.Warnings, w => w.Code == PzErrorCode.InvalidSinkCall);
    }

    // The read-side twin of the two facts above -- SourceFunction.NearMissKwarg existed but was never
    // wired into DagCompiler, so `source(..., synk: {...})` used to ride along as a connector option
    // with no warning at all.
    [Theory]
    [InlineData("colums")]
    [InlineData("Sync")]
    [InlineData("retri")]
    public void A_source_kwarg_one_edit_from_a_pz_key_is_named_not_silently_sent_to_the_connector(string typo)
    {
        // `crm` declares no datasets in YAML -- the call site's kwargs are the whole story, so this
        // never trips PZ0341 (declared in both places).
        var p = Project(
            [Pipe("a", $"select * from {{{{ source('crm', 'orders', {typo}: 'x') }}}}")],
            sinks: [Sink("crm")]);

        var dag = DagCompiler.Compile(p, Ctx(p)); // a warning, never an error

        var warning = Assert.Single(dag.Warnings, w => w.Code == PzErrorCode.UnresolvedRef);
        Assert.Contains(typo, warning.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("partitions")] // a genuine, pz-documented source() kwarg -- not a near miss of anything
    [InlineData("connect_timeout")]
    public void A_genuine_source_connector_option_draws_no_near_miss_warning(string option)
    {
        var p = Project(
            [Pipe("a", $"select * from {{{{ source('crm', 'orders', {option}: 4) }}}}")],
            sinks: [Sink("crm")]);

        var dag = DagCompiler.Compile(p, Ctx(p));

        Assert.DoesNotContain(dag.Warnings, w => w.Code == PzErrorCode.UnresolvedRef);
    }

    [Fact]
    public void Dead_leaf_pipeline_is_PZ0223_warning()
    {
        var p = Project(
            [Pipe("used", "INSERT INTO {{ sink('lake', 'out', strategy: 'replace', format: 'parquet') }} select 1 as x from {{ ref('lonely') }}"),
             Pipe("lonely", "select 1 as x"),          // consumed by 'used' via ref → NOT dead
             Pipe("dangling", "select 1 as x")],        // no sink, no ref consumer → dead leaf
            sinks: [Sink()]);
        var dag = DagCompiler.Compile(p, Ctx(p));
        var w = Assert.Single(dag.Warnings, x => x.Code == PzErrorCode.DeadLeafPipeline);
        Assert.Contains("dangling", w.Message);
    }

    [Fact]
    public void Intermediate_with_no_sink_but_a_ref_consumer_is_silent()
    {
        var p = Project(
            [Pipe("stg", "select 1 as x"),
             Pipe("top", "INSERT INTO {{ sink('lake', 'out', strategy: 'replace', format: 'parquet') }} select * from {{ ref('stg') }}")],
            sinks: [Sink()]);
        var dag = DagCompiler.Compile(p, Ctx(p));
        Assert.Empty(dag.Warnings);
    }
}
