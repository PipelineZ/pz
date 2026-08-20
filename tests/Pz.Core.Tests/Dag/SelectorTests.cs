using Pz.Core.Dag;
using Pz.Core.Validation;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Dag;

public class SelectorTests
{
    private static CompiledDag Dag()
    {
        var p = Project(
            [Pipe("stg", "select * from {{ source('crm', 'orders') }}"),
             Pipe("mart", "INSERT INTO {{ sink('lake', 'out', strategy: 'replace', format: 'parquet') }} select * from {{ ref('stg') }}", tags: ["daily"])],
            sources: [Crm("orders", "customers")],
            sinks: [Sink()]);
        return DagCompiler.Compile(p, Ctx(p));
    }

    private static HashSet<string> Names(CompiledDag dag, string expr) =>
        Selector.Apply(dag, expr)
            .Select(id => dag.Nodes.Single(n => n.Id == id).Name).ToHashSet();

    [Fact]
    public void Name_selects_single_node() =>
        Assert.Equal(["stg"], Names(Dag(), "stg"));

    [Fact]
    public void Descendants_operator_selects_downstream()
    {
        var dag = Dag();
        Assert.Equal(["stg", "mart", "lake.out"], Names(dag, "stg+"));
    }

    [Fact]
    public void Ancestors_operator_selects_upstream()
    {
        var dag = Dag();
        Assert.Equal(["src_crm__orders", "stg", "mart"], Names(dag, "+mart"));
    }

    [Fact]
    public void Tag_selects_tagged_pipelines() =>
        Assert.Equal(["mart"], Names(Dag(), "tag:daily"));

    [Fact]
    public void Source_pattern_selects_source_loads() =>
        Assert.Equal(["src_crm__orders"], Names(Dag(), "source:crm.*"));

    [Fact]
    public void Space_is_union() =>
        Assert.Equal(["stg", "mart"], Names(Dag(), "stg mart"));

    [Fact]
    public void Comma_is_intersection() =>
        Assert.Equal(["mart"], Names(Dag(), "tag:daily,+mart"));

    [Fact]
    public void Wildcard_matches_names() =>
        Assert.Equal(["src_crm__orders"], Names(Dag(), "src_crm__*"));

    [Fact]
    public void Unknown_selector_is_error_PZ0210()
    {
        var ex = Assert.Throws<PzValidationException>(() => Selector.Apply(Dag(), "nonexistent"));
        var error = Assert.Single(ex.Errors);
        Assert.Equal(PzErrorCode.SelectorNoMatch, error.Code);
        Assert.Contains("nonexistent", error.Message);
    }

    /// <summary>FlowClosure is the structural `+name+` — seeds
    /// plus every ancestor AND descendant — used by `pz run &lt;name&gt;` (which resolves exact node
    /// names itself and deliberately bypasses the atom grammar).</summary>
    [Fact]
    public void FlowClosure_is_ancestors_plus_descendants()
    {
        var dag = Dag();
        var stg = dag.Nodes.Single(n => n.Name == "stg").Id;
        var closure = Selector.FlowClosure(dag, [stg]);
        Assert.Equal(new HashSet<string> { "src_crm__orders", "stg", "mart", "lake.out" },
            closure.Select(id => dag.Nodes.Single(n => n.Id == id).Name).ToHashSet());
    }

    /// <summary>An entity is spelled the way its own system spells it, so a SinkWrite node can be named
    /// `lake.orders-current` and a source `/v2/events` — the atom grammar must admit dashes and slashes,
    /// not just dots.</summary>
    [Theory]
    [InlineData("lake.orders-current")]
    [InlineData("lake./v2/events")]
    public void An_atom_may_name_a_path_or_dash_shaped_entity(string entity)
    {
        var p = Project(
            [Pipe("mart", $"INSERT INTO {{{{ sink('lake', '{entity["lake.".Length..]}', strategy: 'replace', format: 'parquet') }}}} select 1 as x")],
            sinks: [Sink()]);
        var dag = DagCompiler.Compile(p, Ctx(p));

        Assert.Equal([entity], Names(dag, entity));
    }

    [Fact]
    public void A_genuinely_malformed_atom_is_still_refused() =>
        Assert.Throws<PzValidationException>(() => Selector.Apply(Dag(), "st g!"));
}
