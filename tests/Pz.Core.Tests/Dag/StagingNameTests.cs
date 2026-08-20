using Pz.Core.Dag;
using Pz.Core.Validation;
using Pz.Core.Model;
using static Pz.Core.Tests.TestProjects;

namespace Pz.Core.Tests.Dag;

/// <summary>An entity is named the way its own system names it, so
/// a dataset name may carry characters no unquoted SQL identifier can. These pin the fold that keeps
/// the staging relation a single legal identifier, and the collision the fold makes possible.</summary>
public class StagingNameTests
{
    private static ConnectionDef Src(string name, params string[] datasets) =>
        new(name, "localfiles", new Dictionary<string, object?> { ["root"] = "/data" },
            [.. datasets.Select(d => new DatasetDef(d,
                new Dictionary<string, object?> { ["path"] = $"{d}.csv", ["format"] = "csv" }, null))],
            "connections.yml");

    [Theory]
    [InlineData("orders", "src_erp__orders")]
    [InlineData("dbo.orders", "src_erp__dbo_orders")]
    [InlineData("/v2/events", "src_erp___v2_events")]
    [InlineData("orders-current", "src_erp__orders_current")]
    public void Every_character_outside_the_identifier_class_folds(string dataset, string expected) =>
        Assert.Equal(expected, StagingName.ForSourceLoad("erp", dataset));

    // Interpolated raw, `staging.src_erp__dbo.orders` is a FOUR-part name and DuckDB looks for a
    // catalog called `staging`. This is the property the fold exists for.
    [Fact]
    public void A_dotted_entity_stages_as_one_identifier()
    {
        var p = Project(
            [Pipe("a", "INSERT INTO {{ sink('lake', 'out', strategy: 'replace', format: 'parquet') }} " +
                       "select 1 as x from {{ source('erp', 'dbo.orders') }}")],
            sources: [Src("erp", "dbo.orders")],
            sinks: [Sink()]);

        var dag = DagCompiler.Compile(p, Ctx(p));

        var node = Assert.Single(dag.Nodes, n => n.Kind == NodeKind.SourceLoad);
        Assert.Equal("src_erp__dbo_orders", node.Name);
        Assert.Contains("staging.src_erp__dbo_orders",
            Assert.Single(dag.Nodes, n => n.Kind == NodeKind.Pipeline).RenderedSql, StringComparison.Ordinal);
    }

    [Fact]
    public void Two_datasets_that_fold_together_are_PZ0110_not_a_silent_overwrite()
    {
        var p = Project(
            [Pipe("a", "INSERT INTO {{ sink('lake', 'out', strategy: 'replace', format: 'parquet') }} " +
                       "select 1 as x from {{ source('erp', 'dbo.orders') }} " +
                       "union all select 2 from {{ source('erp', 'dbo_orders') }}")],
            sources: [Src("erp", "dbo.orders", "dbo_orders")],
            sinks: [Sink()]);

        var ex = Assert.Throws<PzValidationException>(() => DagCompiler.Compile(p, Ctx(p)));

        var error = Assert.Single(ex.Errors, e => e.Code == PzErrorCode.DuplicateName);
        Assert.Contains("dbo.orders", error.Message, StringComparison.Ordinal);
        Assert.Contains("dbo_orders", error.Message, StringComparison.Ordinal);
    }
}
