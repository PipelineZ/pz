using Pz.Cli.Commands;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Cli.Tests;

/// <summary>RunSelection's invocation resolver — positional
/// names, --select, and --all are mutually exclusive (PZ0216); names are exact-match `+name+`
/// closures; bare `pz run` on a 2+-flow project is PZ0215 (plan is ungated).</summary>
public class RunSelectionTests
{
    private static readonly NodeId SrcAId = new("aaaaaaaaaaaaaaa1");
    private static readonly NodeId PipeAId = new("aaaaaaaaaaaaaaa2");
    private static readonly NodeId SinkAId = new("aaaaaaaaaaaaaaa3");
    private static readonly NodeId SrcBId = new("bbbbbbbbbbbbbbb1");
    private static readonly NodeId PipeBId = new("bbbbbbbbbbbbbbb2");
    private static readonly NodeId SinkBId = new("bbbbbbbbbbbbbbb3");

    private static DagNode Source(NodeId id, string name) =>
        new(id, NodeKind.SourceLoad, name, [], null,
            new SourceDatasetDef(
                new ConnectionDef("mem", "inmemory", new Dictionary<string, object?>(),
                    [new DatasetDef("foo", new Dictionary<string, object?>(), null)],
                    "sources/mem.yml"),
                new DatasetDef("foo", new Dictionary<string, object?>(), null)));

    private static DagNode Pipe(NodeId id, string name, NodeId dependsOn) =>
        new(id, NodeKind.Pipeline, name, [dependsOn], "select 1",
            new PipelineDef(name, "select 1", "table", [], [], $"pipelines/{name}.sql"));

    private static DagNode Sink(NodeId id, string name, NodeId dependsOn) =>
        new(id, NodeKind.SinkWrite, $"{name}.out", [dependsOn], null,
            new SinkOutputDef(
                new ConnectionDef(name, "inmemory", new Dictionary<string, object?>(), [],
                    $"sinks/{name}.yml") { Outputs = [new OutputDef("out", "pipe", "replace", "fail_on_change",
                        new Dictionary<string, object?>())] },
                new OutputDef("out", "pipe", "replace", "fail_on_change",
                    new Dictionary<string, object?>())));

    private static CompiledDag OneFlow() => new([
        Source(SrcAId, "src_a"), Pipe(PipeAId, "pipe_a", SrcAId), Sink(SinkAId, "sink_a", PipeAId),
    ]);

    private static CompiledDag TwoFlows() => new([
        Source(SrcAId, "src_a"), Pipe(PipeAId, "pipe_a", SrcAId), Sink(SinkAId, "sink_a", PipeAId),
        Source(SrcBId, "src_b"), Pipe(PipeBId, "pipe_b", SrcBId), Sink(SinkBId, "sink_b", PipeBId),
    ]);

    [Fact]
    public void Bare_run_on_single_flow_returns_null_selection() =>
        Assert.Null(RunSelection.Resolve(OneFlow(), [], null, all: false, gateBareMultiFlow: true));

    [Fact]
    public void Bare_run_on_multi_flow_is_PZ0215_naming_the_flows()
    {
        var ex = Assert.Throws<PzValidationException>(() =>
            RunSelection.Resolve(TwoFlows(), [], null, all: false, gateBareMultiFlow: true));
        var error = Assert.Single(ex.Errors);
        Assert.Equal(PzErrorCode.MultiFlowNeedsSelection, error.Code);
        Assert.Contains("sink_a.out", error.Message);
        Assert.Contains("sink_b.out", error.Message);
    }

    [Fact]
    public void Bare_plan_on_multi_flow_is_ungated() =>
        Assert.Null(RunSelection.Resolve(TwoFlows(), [], null, all: false, gateBareMultiFlow: false));

    [Fact]
    public void All_bypasses_the_gate() =>
        Assert.Null(RunSelection.Resolve(TwoFlows(), [], null, all: true, gateBareMultiFlow: true));

    [Fact]
    public void Name_selects_the_whole_flow_and_nothing_else()
    {
        var selection = RunSelection.Resolve(
            TwoFlows(), ["pipe_a"], null, all: false, gateBareMultiFlow: true);
        Assert.Equal(new HashSet<NodeId> { SrcAId, PipeAId, SinkAId }, selection);
    }

    [Fact]
    public void Two_names_union_both_flows()
    {
        var selection = RunSelection.Resolve(
            TwoFlows(), ["pipe_a", "sink_b.out"], null, all: false, gateBareMultiFlow: true);
        Assert.Equal(
            new HashSet<NodeId> { SrcAId, PipeAId, SinkAId, SrcBId, PipeBId, SinkBId }, selection);
    }

    [Fact]
    public void Select_still_resolves_through_the_selector()
    {
        var selection = RunSelection.Resolve(
            TwoFlows(), [], "+sink_a.out", all: false, gateBareMultiFlow: true);
        Assert.Equal(new HashSet<NodeId> { SrcAId, PipeAId, SinkAId }, selection);
    }

    [Fact]
    public void Unknown_names_aggregate_PZ0210_errors_listing_the_flows()
    {
        var ex = Assert.Throws<PzValidationException>(() =>
            RunSelection.Resolve(TwoFlows(), ["nope", "also_nope"], null, false, true));
        Assert.Equal(2, ex.Errors.Count);
        Assert.All(ex.Errors, e => Assert.Equal(PzErrorCode.SelectorNoMatch, e.Code));
        Assert.Contains("nope", ex.Errors[0].Message);
        Assert.Contains("sink_b.out", ex.Errors[0].Message);
    }

    [Theory]
    [InlineData(new[] { "pipe_a" }, "+pipe_b", false)] // names + --select
    [InlineData(new[] { "pipe_a" }, null, true)]       // names + --all
    [InlineData(new string[0], "+pipe_b", true)]       // --select + --all
    public void Mixing_selection_mechanisms_is_PZ0216(string[] names, string? select, bool all)
    {
        var ex = Assert.Throws<PzValidationException>(() =>
            RunSelection.Resolve(TwoFlows(), names, select, all, gateBareMultiFlow: true));
        var error = Assert.Single(ex.Errors);
        Assert.Equal(PzErrorCode.SelectionConflict, error.Code);
    }
}
