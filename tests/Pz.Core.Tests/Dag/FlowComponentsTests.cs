using Pz.Core.Dag;
using Pz.Core.Model;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Dag;

/// <summary>An "independent flow" is a connected component of
/// the compiled DAG (edges undirected), labeled by its SinkWrite node names — or, when nothing
/// drains it, its leaf non-Check node names.</summary>
public class FlowComponentsTests
{
    [Fact]
    public void Single_chain_is_one_component_labeled_by_its_sink()
    {
        var p = Project(
            [Pipe("stg", "select * from {{ source('crm', 'orders') }}"),
             Pipe("mart", "INSERT INTO {{ sink('lake', 'out', strategy: 'replace', format: 'parquet') }} select * from {{ ref('stg') }}")],
            sources: [Crm("orders")],
            sinks: [Sink()]);
        var flows = FlowComponents.Compute(DagCompiler.Compile(p, Ctx(p)));
        var flow = Assert.Single(flows);
        Assert.Equal("lake.out", flow.Label);
    }

    [Fact]
    public void Two_disjoint_chains_are_two_components()
    {
        var p = Project(
            [Pipe("a", "INSERT INTO {{ sink('lake_a', 'out', strategy: 'replace', format: 'parquet') }} select * from {{ source('crm', 'orders') }}"),
             Pipe("b", "INSERT INTO {{ sink('lake_b', 'out', strategy: 'replace', format: 'parquet') }} select * from {{ source('crm', 'customers') }}")],
            sources: [Crm("orders", "customers")],
            sinks: [Sink("lake_a"), Sink("lake_b")]);
        var flows = FlowComponents.Compute(DagCompiler.Compile(p, Ctx(p)));
        Assert.Equal(2, flows.Count);
        Assert.Equal(new HashSet<string> { "lake_a.out", "lake_b.out" },
            flows.Select(f => f.Label).ToHashSet());
    }

    [Fact]
    public void Diamond_shape_is_one_component()
    {
        var p = Project(
            [Pipe("stg", "select * from {{ source('crm', 'orders') }}"),
             Pipe("m1", "select * from {{ ref('stg') }}"),
             Pipe("m2", "select * from {{ ref('stg') }}"),
             Pipe("top", "INSERT INTO {{ sink('lake', 'out', strategy: 'replace', format: 'parquet') }} select * from {{ ref('m1') }} union all select * from {{ ref('m2') }}")],
            sources: [Crm("orders")],
            sinks: [Sink()]);
        Assert.Single(FlowComponents.Compute(DagCompiler.Compile(p, Ctx(p))));
    }

    [Fact]
    public void Flow_with_two_sinks_joins_terminal_names()
    {
        var p = Project(
            [Pipe("a", "INSERT INTO [{{ sink('lake_a', 'out', strategy: 'replace', format: 'parquet') }}, {{ sink('lake_b', 'out', strategy: 'replace', format: 'parquet') }}] select * from {{ source('crm', 'orders') }}")],
            sources: [Crm("orders")],
            sinks: [Sink("lake_a"), Sink("lake_b")]);
        var flow = Assert.Single(FlowComponents.Compute(DagCompiler.Compile(p, Ctx(p))));
        Assert.Equal("lake_a.out + lake_b.out", flow.Label);
    }

    [Fact]
    public void Component_with_no_sink_is_labeled_by_leaf_pipelines_not_checks()
    {
        var p = Project(
            [Pipe("stg", "select * from {{ source('crm', 'orders') }}"),
             Pipe("mart", "select * from {{ ref('stg') }}",
                 checks: [new CheckDef("not_null", ["id"], new Dictionary<string, object?>(), null)])],
            sources: [Crm("orders")]);
        var flow = Assert.Single(FlowComponents.Compute(DagCompiler.Compile(p, Ctx(p))));
        Assert.Equal("mart", flow.Label);
    }
}
