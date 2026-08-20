using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

/// <summary>The compiler decides WHAT is pushable, the
/// executor decides WHETHER this connector gets it. ReadHints is best-effort, so an incapable
/// connector is simply handed nothing and extracts as it always did — the pipeline's own SQL still
/// runs in DuckDB over whatever landed, so results are identical either way.</summary>
public class ReadHintsGatingTests
{
    private static SourceDatasetDef DefWith(ReadHintPlan? plan) =>
        new(new ConnectionDef("crm", "postgres", new Dictionary<string, object?>(), [], "connections.yml"),
            new DatasetDef("orders", new Dictionary<string, object?>(), null),
            plan);

    [Fact]
    public void Hints_are_withheld_from_a_connector_without_the_capability()
    {
        var hints = SourceLoadExecutor.HintsFor(
            DefWith(new ReadHintPlan(["id"], "status = 'open'")), ConnectorCapabilities.None);

        Assert.Null(hints.Columns);
        Assert.Null(hints.PredicateSql);
    }

    [Fact]
    public void Column_pruning_and_predicate_pushdown_are_gated_independently()
    {
        var def = DefWith(new ReadHintPlan(["id"], "status = 'open'"));

        var columnsOnly = SourceLoadExecutor.HintsFor(def, ConnectorCapabilities.ColumnPruning);
        Assert.Equal(new[] { "id" }, columnsOnly.Columns!);
        Assert.Null(columnsOnly.PredicateSql);

        var predicateOnly = SourceLoadExecutor.HintsFor(def, ConnectorCapabilities.PredicatePushdown);
        Assert.Null(predicateOnly.Columns);
        Assert.Equal("status = 'open'", predicateOnly.PredicateSql);
    }

    [Fact]
    public void A_fully_capable_connector_gets_both_halves()
    {
        var hints = SourceLoadExecutor.HintsFor(
            DefWith(new ReadHintPlan(["id", "amount"], "status = 'open'")),
            ConnectorCapabilities.ColumnPruning | ConnectorCapabilities.PredicatePushdown);

        Assert.Equal(new[] { "id", "amount" }, hints.Columns!);
        Assert.Equal("status = 'open'", hints.PredicateSql);
    }

    [Fact]
    public void A_node_compiled_without_hints_pushes_nothing_however_capable_the_connector()
    {
        var hints = SourceLoadExecutor.HintsFor(DefWith(null),
            ConnectorCapabilities.ColumnPruning | ConnectorCapabilities.PredicatePushdown);

        Assert.Same(ReadHints.None, hints);
    }

    // -- schema reconciliation -----------------------------------------------------------------
    //
    // Ingest binds batch fields BY POSITION. GetSchemaAsync takes no hints, so it reports the dataset's
    // full shape while a pruning connector sends only the requested columns. Left unreconciled the
    // staged table keeps the full column list and every pruned batch lands one column to the left of
    // where it belongs -- an int64 id silently reading a float64 amount's bytes. These pin the fix.

    private static Schema FullSchema() => new(
        [
            new Field("id", Int64Type.Default, nullable: false),
            new Field("customer", StringType.Default, nullable: true),
            new Field("amount", DoubleType.Default, nullable: true),
        ],
        null);

    [Fact]
    public void The_staging_schema_is_narrowed_to_the_pruned_columns_in_the_sources_own_order()
    {
        // The compiler hands over an ordinally sorted set; the connector must be asked for them in the
        // order the source declares, so its SELECT list and the staged table line up field for field.
        var (hints, schema) = SourceLoadExecutor.ProjectToHints(new ReadHints(["amount", "id"], null), FullSchema());

        Assert.Equal(new[] { "id", "amount" }, hints.Columns!);
        Assert.Equal(new[] { "id", "amount" }, schema.FieldsList.Select(f => f.Name));
    }

    [Fact]
    public void A_hint_naming_a_column_the_source_lacks_is_dropped_whole()
    {
        // Extraction then proceeds unpruned and the pipeline's own SQL reports the unknown column with
        // its usual message, rather than the connector failing on a SELECT list pz built.
        var (hints, schema) = SourceLoadExecutor.ProjectToHints(
            new ReadHints(["id", "nope"], "amount > 1"), FullSchema());

        Assert.Null(hints.Columns);
        Assert.Equal("amount > 1", hints.PredicateSql);
        Assert.Equal(3, schema.FieldsList.Count);
    }

    [Fact]
    public void Nothing_is_narrowed_when_no_columns_are_pushed()
    {
        var (hints, schema) = SourceLoadExecutor.ProjectToHints(new ReadHints(null, "amount > 1"), FullSchema());

        Assert.Null(hints.Columns);
        Assert.Equal(3, schema.FieldsList.Count);
    }
}
