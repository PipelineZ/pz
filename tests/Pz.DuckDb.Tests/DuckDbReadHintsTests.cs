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

    // -- predicates across joins ---------------------------------------------------------------
    // A pushed predicate filters the source BEFORE the join. That equals filtering after it only
    // while the join cannot null-extend the target's rows; otherwise rows the WHERE was meant to see
    // as NULL never land, and the result is silently wrong.

    [Fact]
    public void An_anti_join_predicate_on_the_null_supplied_side_is_not_pushed()
    {
        // Pushing `id IS NULL` lands zero src_a rows, and the anti-join then returns every src_b row.
        var plan = Extract("select b.id from src_b b left join src_a a on a.id = b.id where a.id is null");

        Assert.Null(plan.PredicateSql);
    }

    [Theory]
    [InlineData("select a.id from src_b b left join src_a a on a.id = b.id where coalesce(a.x, 0) = 0")]
    [InlineData("select a.id from src_a a right join src_b b on a.id = b.id where a.x = 1")]
    [InlineData("select a.id from src_a a full join src_b b on a.id = b.id where a.x = 1")]
    [InlineData("select a.id from src_b b full join src_a a on a.id = b.id where a.x = 1")]
    [InlineData("select a.id from src_b b left join (src_a a join src_c c on a.id = c.id) on a.id = b.id where a.x = 1")]
    [InlineData("select a.id from src_b b asof join src_a a on b.t >= a.t where a.x = 1")]
    [InlineData("select a.id from src_a a positional join src_b b where a.x = 1")]
    public void A_predicate_is_not_pushed_through_a_join_that_does_not_preserve_it(string sql)
    {
        Assert.Null(Extract(sql).PredicateSql);
    }

    [Theory]
    [InlineData("select a.id from src_a a left join src_b b on a.id = b.id where a.x = 1")]
    [InlineData("select a.id from src_b b right join src_a a on a.id = b.id where a.x = 1")]
    [InlineData("select a.id from src_a a cross join src_b b where a.x = 1")]
    [InlineData("select a.id from src_a a semi join src_b b on a.id = b.id where a.x = 1")]
    [InlineData("select a.id from src_a a anti join src_b b on a.id = b.id where a.x = 1")]
    [InlineData("select a.id from src_b b join src_c c on b.id = c.id join src_a a on a.id = b.id where a.x = 1")]
    public void A_predicate_on_the_preserved_side_of_a_join_is_still_pushed(string sql)
    {
        var predicate = Extract(sql).PredicateSql;

        Assert.NotNull(predicate);
        Assert.Contains("x", predicate!, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>The contract behind every rule above, checked against results rather than AST shape:
    /// landing only the rows the pushed predicate keeps must not change what the pipeline returns.
    /// The data has unmatched rows on both sides and NULLs in the filtered column, which is what an
    /// unsafe push needs in order to show.</summary>
    [Theory]
    [InlineData("select b.id from src_b b left join src_a a on a.id = b.id where a.id is null")]
    [InlineData("select b.id, a.x from src_b b left join src_a a on a.id = b.id where coalesce(a.x, 0) = 0")]
    [InlineData("select a.id, b.id from src_a a right join src_b b on a.id = b.id where a.x is distinct from 1")]
    [InlineData("select a.id, b.id from src_a a full join src_b b on a.id = b.id where a.x is null")]
    [InlineData("select a.id, b.id from src_a a left join src_b b on a.id = b.id where a.x = 1")]
    [InlineData("select a.id, b.id from src_b b right join src_a a on a.id = b.id where a.x is null")]
    [InlineData("select a.id from src_a a semi join src_b b on a.id = b.id where a.x = 1")]
    [InlineData("select a.id from src_a a anti join src_b b on a.id = b.id where a.x is null")]
    [InlineData("select a.id, b.id from src_a a join src_b b on a.id = b.id where a.x = 1 and b.id > 1")]
    [InlineData("select a.id, b.id from src_a a join src_a b on a.parent_id = b.id where a.x = 1")]
    [InlineData("select id from src_a where x = 1 and id not in (select parent_id from src_a where parent_id is not null)")]
    public async Task Landing_only_the_pushed_rows_never_changes_the_result(string sql)
    {
        const string rows = "(values (1, 1, null), (2, null, 1), (3, 0, 1), (4, 1, 2), (5, 2, null)) t(id, x, parent_id)";
        var predicate = Extract(sql).PredicateSql;

        async Task<string> RunAsync(string? landedWhere)
        {
            await using var duck = DuckSession.Open(":memory:");
            await duck.ExecuteAsync(
                $"create table src_a as select * from {rows}{(landedWhere is null ? "" : $" where {landedWhere}")}");
            await duck.ExecuteAsync("create table src_b as select * from (values (2), (3), (4), (9)) t(id)");
            return await duck.ScalarAsync<string>(
                $"select coalesce(string_agg(r::varchar, '|' order by r::varchar), '') from ({sql}) r");
        }

        Assert.Equal(await RunAsync(null), await RunAsync(predicate));
    }

    [Fact]
    public void A_self_join_pushes_no_predicate()
    {
        // One SourceLoad feeds both aliases: a filter meant for `a` would starve `b` too.
        var plan = Extract("select a.id from src_a a join src_a b on a.parent_id = b.id where a.x = 1");

        Assert.Null(plan.PredicateSql);
        Assert.Equal(new[] { "id", "parent_id", "x" }, plan.Columns!);
    }

    [Theory]
    [InlineData("select id from src_a where x = 1 and id not in (select parent_id from src_a)")]
    [InlineData("with c as (select z.extra, z.id from src_a z) select o.id from src_a o join c on c.id = o.id where o.x = 1")]
    public void A_second_reference_outside_the_outer_from_pushes_nothing(string sql)
    {
        // The other reference reads the same staged table with needs this walk never sees.
        var plan = Extract(sql);

        Assert.Null(plan.Columns);
        Assert.Null(plan.PredicateSql);
    }

    [Fact]
    public void An_unqualified_predicate_beside_a_from_subquery_is_not_pushed()
    {
        // `x` may belong to the derived table; src_a is the only BASE table but not the only relation.
        var plan = Extract("select id from src_a, (select 1 as x) t where x = 1");

        Assert.Null(plan.PredicateSql);
        Assert.Null(plan.Columns);
    }

    [Fact]
    public void A_predicate_over_a_select_list_alias_is_not_pushed()
    {
        // DuckDB lets WHERE name a select-list alias; the source has no such column.
        var plan = Extract("select amount * 2 as doubled from src_a where doubled > 10 and status = 'open'");

        Assert.NotNull(plan.PredicateSql);
        Assert.DoesNotContain("doubled", plan.PredicateSql!, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("status", plan.PredicateSql!, StringComparison.OrdinalIgnoreCase);
    }

    // -- projection across joins ---------------------------------------------------------------
    // A hint that is too narrow is the unsafe direction: the staged table lacks a column the SQL
    // still binds. Any reference this walk cannot attribute means "read every column".

    [Theory]
    [InlineData("select order_id, amount from src_a o join src_c c using (customer_id) where o.x = 1")]
    [InlineData("select o.order_id from src_a o natural join src_c c")]
    [InlineData("select id, b.name from src_a a join src_b b on a.k = b.k")]
    [InlineData("select a.id from src_a a join src_b b on a.k = b.k order by created_at")]
    public void A_join_with_a_column_that_cannot_be_attributed_reads_every_column(string sql)
    {
        Assert.Null(Extract(sql).Columns);
    }

    [Fact]
    public void A_fully_qualified_join_keeps_its_column_list()
    {
        var plan = Extract("select a.id, b.name from src_a a join src_b b on a.k = b.k where a.x = 1");

        Assert.Equal(new[] { "id", "k", "x" }, plan.Columns!);
    }

    [Fact]
    public void A_struct_field_path_names_its_root_column()
    {
        var plan = Extract("select o.payload.kind, o.id from src_a o");

        Assert.Equal(new[] { "id", "payload" }, plan.Columns!);
    }

    [Fact]
    public void An_unattributable_qualifier_reads_every_column()
    {
        // `payload.kind` is a struct path here, not alias.column — its root cannot be told apart from
        // a qualifier, so nothing is pruned.
        var plan = Extract("select payload.kind, id from src_a");

        Assert.Null(plan.Columns);
    }
}
