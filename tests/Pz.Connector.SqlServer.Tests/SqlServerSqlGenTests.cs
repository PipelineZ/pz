using Pz.Connectors.Abstractions;
using Pz.Connector.SqlServer;

namespace Pz.Connector.SqlServer.Tests;

public class SqlServerSqlGenTests
{
    private static DatasetSpec Spec(params (string Key, object? Value)[] options) =>
        new("ms", "ds", options.ToDictionary(o => o.Key, o => o.Value));

    // The dataset name IS the object name.
    private static DatasetSpec EntitySpec(string entity) => new("ms", entity, new Dictionary<string, object?>());

    [Fact]
    public void An_unqualified_entity_reads_from_the_default_schema()
    {
        Assert.Equal("select * from [dbo].[orders]",
            SqlServerSource.BuildSelect(EntitySpec("orders"), ReadHints.None));
    }

    [Fact]
    public void A_dotted_entity_reads_from_its_own_schema_with_bracket_escaping()
    {
        Assert.Equal("select * from [sales]]x].[t]",
            SqlServerSource.BuildSelect(EntitySpec("sales]x.t"), ReadHints.None));
    }

    // A cross-database name would otherwise be quoted as ONE identifier literally called [db.sales] --
    // a silent wrong read. Refused instead.
    [Fact]
    public void A_three_part_entity_is_refused_rather_than_quoted_wrong()
    {
        var ex = Assert.Throws<PzConnectorException>(
            () => SqlServerSource.BuildSelect(EntitySpec("db.sales.orders"), ReadHints.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("db.sales.orders", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Column_pruning_projects_quoted_hint_columns()
    {
        Assert.Equal("select [id], [name] from [dbo].[orders]",
            SqlServerSource.BuildSelect(EntitySpec("orders"), new ReadHints(Columns: ["id", "name"])));
    }

    [Fact]
    public void Predicate_watermark_and_window_join_one_and_chain_each_parenthesized()
    {
        var spec = EntitySpec("orders") with
        {
            WatermarkCursor = "updated_at",
            WatermarkValue = "2026-01-01T00:00:00.000000",
            WatermarkUpperBound = "2026-02-01T00:00:00.000000",
        };
        // Every term individually parenthesized: the disjunctive engine predicate gains one wrapping
        // pair so the watermark AND can never bind into the middle of its OR.
        Assert.Equal(
            "select * from [dbo].[orders] where ((id > 10 or id < 2)) " +
            "and ([updated_at] > '2026-01-01T00:00:00.000000') " +
            "and ([updated_at] <= '2026-02-01T00:00:00.000000')",
            SqlServerSource.BuildSelect(spec, new ReadHints(PredicateSql: "id > 10 or id < 2")));
    }

    [Fact]
    public void Watermark_value_is_quote_doubled_never_column_cast()
    {
        var spec = EntitySpec("t") with { WatermarkCursor = "c", WatermarkValue = "o'brien" };
        var sql = SqlServerSource.BuildSelect(spec, ReadHints.None);
        Assert.Contains("([c] > 'o''brien')", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("cast([c]", sql, StringComparison.Ordinal); // sargability: never cast the column
    }

    [Fact]
    public void Query_mode_returns_query_verbatim_and_ignores_hints()
    {
        var spec = Spec(("query", "select 1 as x"));
        Assert.Equal("select 1 as x", SqlServerSource.BuildSelect(spec, new ReadHints(Columns: ["x"])));
    }

    // Two-way, not three-way: `table:` is retired, so "neither" means "read the entity" rather than an
    // error, and only query/procedure can conflict.
    [Fact]
    public void Query_and_procedure_are_mutually_exclusive()
    {
        var ex = Assert.Throws<PzConnectorException>(() => SqlServerSource.BuildSelect(
            Spec(("query", "select 1"), ("procedure", "dbo.p")), ReadHints.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("mutually exclusive", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Watermark_lower_inclusive_true_uses_greater_equal()
    {
        var spec = EntitySpec("orders") with
        {
            WatermarkCursor = "updated_at",
            WatermarkValue = "2020-01-01T00:00:00.000000",
            WatermarkLowerInclusive = true,
        };
        var sql = SqlServerSource.BuildSelect(spec, ReadHints.None);
        Assert.Contains("([updated_at] >= '2020-01-01T00:00:00.000000')", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Watermark_lower_inclusive_false_uses_greater()
    {
        var spec = EntitySpec("orders") with
        {
            WatermarkCursor = "updated_at",
            WatermarkValue = "2020-01-01T00:00:00.000000",
            WatermarkLowerInclusive = false,
        };
        var sql = SqlServerSource.BuildSelect(spec, ReadHints.None);
        Assert.Contains("([updated_at] > '2020-01-01T00:00:00.000000')", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Connector_declares_inclusive_watermark_bound()
    {
        Assert.True(new SqlServerConnector().Capabilities.HasFlag(ConnectorCapabilities.InclusiveWatermarkBound));
    }

    // A cursor-set/value-null spec is the first-run shape SpecBuilder stamps for every incremental
    // dataset (DatasetSpec.WatermarkCursor's doc comment: the predicate applies only "when set
    // (alongside WatermarkValue)"). The gate must key
    // off WatermarkValue, not WatermarkCursor alone, or this throws NullReferenceException instead of
    // producing the same unbounded SELECT as a watermark-free spec.
    [Fact]
    public void Cursor_set_value_null_produces_same_select_as_no_watermark()
    {
        var cursorOnlySpec = EntitySpec("orders") with { WatermarkCursor = "updated_at" };
        var noWatermarkSpec = EntitySpec("orders");

        var cursorOnlySql = SqlServerSource.BuildSelect(cursorOnlySpec, ReadHints.None);
        var noWatermarkSql = SqlServerSource.BuildSelect(noWatermarkSpec, ReadHints.None);

        Assert.Equal(noWatermarkSql, cursorOnlySql);
        Assert.DoesNotContain("updated_at", cursorOnlySql, StringComparison.Ordinal);
    }

    // The default capture instance is SQL Server's own {schema}_{table} convention, with both halves
    // read off the entity name. The naming a separate `schema:`/`table:` dataset produced must be
    // reproduced verbatim, or an existing cdc deployment loses its slot.
    [Fact]
    public void The_default_capture_instance_still_derives_from_schema_and_table()
    {
        Assert.Equal("dbo_orders", SqlServerCdc.CaptureInstance(EntitySpec("dbo.orders")));
        Assert.Equal("dbo_orders", SqlServerCdc.CaptureInstance(EntitySpec("orders")));
    }
}
