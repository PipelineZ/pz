using Pz.Core.Dag;

namespace Pz.DuckDb.Tests;

/// <summary>What a pipeline's SQL lets pz ask the source connector for. Driven through the real DuckDB
/// parser, because every shape here is measured against <c>json_serialize_sql</c> rather than
/// assumed.</summary>
public sealed class DuckDbReadHintsTests
{
    private static ReadHintPlan Extract(string sql, string baseTable = "src_a", string? cursorColumn = null) =>
        new DuckDbSqlAstReader().ExtractReadHints(sql, baseTable, cursorColumn);

    // -- the hazard this whole feature is gated on ---------------------------------------------

    [Fact]
    public void Or_at_the_top_of_a_where_is_never_split_into_conjuncts()
    {
        // AND and OR both serialize as class CONJUNCTION and differ only in `type`. Splitting an OR's
        // children would push `a = 1` alone and silently drop every row where only `b = 2` holds --
        // wrong data, not a missed optimisation.
        var plan = Extract("select id from src_a where a = 1 or b = 2");

        // The whole disjunction is one conjunct, so it may be pushed entire -- but never halved.
        Assert.DoesNotContain("AND", plan.PredicateSql ?? "", StringComparison.OrdinalIgnoreCase);
        Assert.NotNull(plan.PredicateSql);
        Assert.Contains("OR", plan.PredicateSql!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void And_at_the_top_of_a_where_splits_into_conjuncts()
    {
        var plan = Extract("select id from src_a where a = 1 and b = 2");

        Assert.NotNull(plan.PredicateSql);
        Assert.Contains("a", plan.PredicateSql!, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("b", plan.PredicateSql!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Nested_ands_flatten_into_independent_conjuncts()
    {
        var plan = Extract("select id from src_a where (a = 1 and b = 2) and c = 3");

        Assert.NotNull(plan.PredicateSql);
        foreach (var col in new[] { "a", "b", "c" })
        {
            Assert.Contains(col, plan.PredicateSql!, StringComparison.OrdinalIgnoreCase);
        }
    }

    // -- projection ----------------------------------------------------------------------------

    [Fact]
    public void Columns_are_collected_from_the_select_list_and_the_where_clause()
    {
        var plan = Extract("select o.id, o.amount from src_a o where o.status = 'open'");

        Assert.NotNull(plan.Columns);
        Assert.Equal(new[] { "amount", "id", "status" }, plan.Columns!);
    }

    [Fact]
    public void Unqualified_columns_resolve_to_the_sole_base_table()
    {
        var plan = Extract("select id, amount from src_a where status = 'open'");

        Assert.NotNull(plan.Columns);
        Assert.Equal(new[] { "amount", "id", "status" }, plan.Columns!);
    }

    [Fact]
    public void Star_suppresses_the_column_list_entirely()
    {
        // Over-pushing is the one unsafe direction: prune a column the SQL references and the staged
        // table lacks it at run time. A star means every column is referenced.
        var plan = Extract("select * from src_a");

        Assert.Null(plan.Columns);
    }

    [Fact]
    public void Qualified_star_on_the_target_table_suppresses_the_column_list()
    {
        var plan = Extract("select a.*, b.id from src_a a join src_b b on a.id = b.id");

        Assert.Null(plan.Columns);
    }

    [Fact]
    public void Qualified_star_on_another_table_leaves_our_column_list_intact()
    {
        var plan = Extract("select b.*, a.id from src_a a join src_b b on a.id = b.id");

        Assert.NotNull(plan.Columns);
        Assert.Equal(new[] { "id" }, plan.Columns!);
    }

    [Fact]
    public void Columns_inside_function_arguments_are_collected()
    {
        var plan = Extract("select sum(amount) from src_a group by name");

        Assert.NotNull(plan.Columns);
        Assert.Equal(new[] { "amount", "name" }, plan.Columns!);
    }

    [Fact]
    public void A_subquery_anywhere_suppresses_the_column_list()
    {
        // A subquery has its own scope: an unqualified `x` inside it belongs to src_b, not src_a.
        // Collecting it would make pz ask the connector for a column the source table has not got --
        // a loud query error rather than silent loss, but a break either way.
        var plan = Extract("select id from src_a where id in (select x from src_b)");

        Assert.Null(plan.Columns);
    }

    [Fact]
    public void Sql_that_does_not_read_the_target_table_pushes_nothing()
    {
        var plan = Extract("select id from src_other", baseTable: "src_a");

        Assert.Null(plan.Columns);
        Assert.Null(plan.PredicateSql);
    }

    // -- predicate rejection -------------------------------------------------------------------

    [Fact]
    public void A_conjunct_touching_another_table_is_not_pushed()
    {
        var plan = Extract("select a.id from src_a a join src_b b on a.id = b.id where a.x = 1 and b.y = 2");

        Assert.NotNull(plan.PredicateSql);
        Assert.DoesNotContain("y", plan.PredicateSql!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void A_subquery_conjunct_is_not_pushed()
    {
        var plan = Extract("select id from src_a where id in (select id from src_b)");

        Assert.Null(plan.PredicateSql);
    }

    [Fact]
    public void A_cursor_column_comparison_is_not_pushed_as_a_predicate()
    {
        // Cursor bounds are load-bearing and route to DatasetSpec.WatermarkUpperBound, which REFUSES on
        // an incapable connector. ReadHints is best-effort, so a cursor bound must never ride it.
        var plan = Extract(
            "select id from src_a where updated_at > TIMESTAMP '2026-01-01' and status = 'open'",
            cursorColumn: "updated_at");

        Assert.NotNull(plan.PredicateSql);
        Assert.DoesNotContain("updated_at", plan.PredicateSql!, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("status", plan.PredicateSql!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void A_watermark_sentinel_conjunct_is_not_pushed()
    {
        // The sentinel is substituted per-run by PipelineExecutor, long after compile: pushing it would
        // send the literal placeholder text to the source.
        var plan = Extract(
            "select id from src_a where updated_at > '__pz_watermark__crm__orders__' and status = 'open'");

        Assert.NotNull(plan.PredicateSql);
        Assert.DoesNotContain("__pz_watermark__", plan.PredicateSql!, StringComparison.Ordinal);
        Assert.Contains("status", plan.PredicateSql!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void A_nondeterministic_conjunct_is_not_pushed()
    {
        var plan = Extract("select id from src_a where created_at > now()");

        Assert.Null(plan.PredicateSql);
    }

    [Fact]
    public void A_pushed_predicate_carries_no_table_qualifier()
    {
        // The connector builds `select … from "schema"."table" where (<predicate>)` with no alias in
        // scope, so `o.status` would name a relation its SQL never declares.
        var plan = Extract("select o.id from src_a o where o.status = 'open'");

        Assert.NotNull(plan.PredicateSql);
        Assert.DoesNotContain("o.", plan.PredicateSql!, StringComparison.Ordinal);
        Assert.Contains("status", plan.PredicateSql!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void No_where_clause_pushes_no_predicate()
    {
        var plan = Extract("select id from src_a");

        Assert.Null(plan.PredicateSql);
    }
}
