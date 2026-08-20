using Pz.Core.Dag;
using Pz.Core.Validation;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Dag;

/// <summary>Array INSERT INTO fan-out: one materialized SELECT drained to N sink outputs. Scalar form
/// stays the 1:1 case; PZ0208 still guards ephemeral and non-leading sink() calls.</summary>
public class SinkFanOutTests
{
    [Fact]
    public void Array_form_binds_two_outputs_to_one_pipeline()
    {
        var p = Project(
            [Pipe("stg", "INSERT INTO [{{ sink('ok', 'stg', strategy: 'replace', format: 'parquet') }}, {{ sink('flaky', 'stg', strategy: 'replace', format: 'parquet') }}] select 1 as x")],
            sinks: [Sink("ok"), Sink("flaky")]);
        var dag = DagCompiler.Compile(p, Ctx(p));

        var sinks = dag.Nodes.Where(n => n.Kind == NodeKind.SinkWrite).OrderBy(n => n.Name).ToList();
        Assert.Equal(["flaky.stg", "ok.stg"], sinks.Select(n => n.Name));

        var pipe = dag.Nodes.Single(n => n.Kind == NodeKind.Pipeline);
        Assert.All(sinks, s => Assert.Contains(pipe.Id, s.DependsOn));
        Assert.Equal("select 1 as x", pipe.RenderedSql);
        Assert.DoesNotContain("__pz_sink__", pipe.RenderedSql!, StringComparison.Ordinal);
    }

    [Fact]
    public void Scalar_form_still_works()
    {
        var p = Project(
            [Pipe("totals", "INSERT INTO {{ sink('lake', 'totals', strategy: 'replace', format: 'parquet') }} select 1 as x")],
            sinks: [Sink()]);
        var dag = DagCompiler.Compile(p, Ctx(p));
        var pipe = dag.Nodes.Single(n => n.Kind == NodeKind.Pipeline);
        Assert.Equal("select 1 as x", pipe.RenderedSql);
        Assert.Single(dag.Nodes, n => n.Kind == NodeKind.SinkWrite);
    }

    [Fact]
    public void Array_binding_the_same_output_twice_is_PZ0208()
    {
        var p = Project(
            [Pipe("stg", "INSERT INTO [{{ sink('ok', 'stg', strategy: 'replace', format: 'parquet') }}, {{ sink('ok', 'stg', strategy: 'replace', format: 'parquet') }}] select 1 as x")],
            sinks: [Sink("ok")]);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        Assert.Single(ex.Errors, e => e.Code == PzErrorCode.InvalidSinkCall);
    }

    [Fact]
    public void Array_that_is_not_the_leading_statement_is_PZ0208()
    {
        var p = Project(
            [Pipe("stg", "select 1 as x -- INSERT INTO [{{ sink('ok', 'stg', strategy: 'replace', format: 'parquet') }}]")],
            sinks: [Sink("ok")]);
        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));
        Assert.Single(ex.Errors, e => e.Code == PzErrorCode.InvalidSinkCall);
    }
}
