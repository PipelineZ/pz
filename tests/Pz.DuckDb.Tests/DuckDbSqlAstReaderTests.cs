using Pz.Core.Dag;

namespace Pz.DuckDb.Tests;

public sealed class DuckDbSqlAstReaderTests
{
    private const string S = "__pz_watermark__crm__orders__";
    private static WatermarkAnalysis Analyze(string sql) => new DuckDbSqlAstReader().Analyze(sql, [S]);

    [Theory]
    [InlineData($"select * from staging.src_crm__orders o where o.updated_at > '{S}'", "updated_at", false)]
    [InlineData($"select * from staging.src_crm__orders o where o.updated_at >= '{S}'", "updated_at", true)]
    [InlineData($"select * from staging.src_crm__orders where updated_at > '{S}'", "updated_at", false)]        // unqualified, single table
    [InlineData($"select * from staging.src_crm__orders o where '{S}' < o.updated_at", "updated_at", false)]     // flipped spelling
    [InlineData($"select * from staging.src_crm__orders o where '{S}' <= o.updated_at", "updated_at", true)]
    [InlineData($"select * from staging.src_crm__orders o where o.updated_at > '{S}' - interval 2 hour", "updated_at", false)]
    [InlineData($"select * from staging.src_crm__orders o where o.updated_at >= date_trunc('day', '{S}')", "updated_at", true)]
    public void Recognized_shapes(string sql, string column, bool inclusive)
    {
        var analysis = Analyze(sql);
        Assert.Empty(analysis.Violations);
        var cmp = Assert.Single(analysis.Comparisons);
        Assert.Equal(column, cmp.Column);
        Assert.Equal("src_crm__orders", cmp.ColumnTable);
        Assert.Equal(inclusive, cmp.Inclusive);
        Assert.Contains(S, cmp.ValueExprSql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData($"select * from staging.src_crm__orders o where o.updated_at != '{S}'")]
    [InlineData($"select * from staging.src_crm__orders o where o.updated_at = '{S}'")]
    [InlineData($"select * from staging.src_crm__orders o where date_trunc('day', o.updated_at) > '{S}'")]       // cursor-side function
    [InlineData($"select * from staging.src_crm__orders o where o.updated_at > '{S}' - o.slack")]                // column on value side
    [InlineData($"select '{S}' from staging.src_crm__orders")]                                                   // outside a comparison
    [InlineData($"select * from staging.src_crm__orders o, staging.src_crm__customers c where updated_at > '{S}'")] // ambiguous unqualified
    public void Rejected_shapes_report_violation_never_guess(string sql)
    {
        var analysis = Analyze(sql);
        Assert.Empty(analysis.Comparisons);
        var violation = Assert.Single(analysis.Violations);
        Assert.Equal(S, violation.Sentinel);
        Assert.False(string.IsNullOrWhiteSpace(violation.Reason));
    }

    // -- Ceilings ------------------------------------------------------------------------------
    //
    // `max_window` and `until` are spelled in SQL as an UPPER bound on the cursor, so the reader must
    // recognize `<`/`<=` as a ceiling rather than normalising every comparison to a lower bound.

    [Theory]
    [InlineData($"select * from staging.src_crm__orders o where o.updated_at < '{S}'", false)]
    [InlineData($"select * from staging.src_crm__orders o where o.updated_at <= '{S}'", true)]
    [InlineData($"select * from staging.src_crm__orders o where '{S}' > o.updated_at", false)]   // flipped spelling
    [InlineData($"select * from staging.src_crm__orders o where '{S}' >= o.updated_at", true)]
    public void Upper_bound_shapes_are_recognized(string sql, bool inclusive)
    {
        var analysis = Analyze(sql);

        Assert.Empty(analysis.Violations);
        var cmp = Assert.Single(analysis.Comparisons);
        Assert.True(cmp.IsUpper);
        Assert.Equal("updated_at", cmp.Column);
        Assert.Equal("src_crm__orders", cmp.ColumnTable);
        Assert.Equal(inclusive, cmp.Inclusive);
    }

    [Fact]
    public void A_floor_and_a_ceiling_on_one_sentinel_are_both_recognized()
    {
        // The whole point of the trio: the resume floor plus a max_window ceiling, in one WHERE.
        var analysis = Analyze($"select * from staging.src_crm__orders o " +
                               $"where o.updated_at > '{S}' and o.updated_at <= '{S}' + interval 7 day");

        Assert.Empty(analysis.Violations);
        Assert.Equal(2, analysis.Comparisons.Count);
        Assert.Single(analysis.Comparisons, c => !c.IsUpper);
        var upper = Assert.Single(analysis.Comparisons, c => c.IsUpper);
        var lower = Assert.Single(analysis.Comparisons, c => !c.IsUpper);
        Assert.True(upper.Inclusive);
        // DuckDB regenerates `interval 7 day` as `to_day(7)`, so assert on the shape rather than the
        // spelling: the ceiling is anchored on the same sentinel but carries the offset the floor lacks.
        Assert.Contains(S, upper.ValueExprSql, StringComparison.Ordinal);
        Assert.Contains("to_day", upper.ValueExprSql, StringComparison.OrdinalIgnoreCase);
        Assert.NotEqual(lower.ValueExprSql, upper.ValueExprSql);
    }

    [Fact]
    public void A_constant_ceiling_carries_no_sentinel_so_the_reader_never_sees_it()
    {
        // `until` is an absolute stop unrelated to the resume point, so it has no watermark() call and
        // no sentinel. Analyze walks sentinels, so it reports only the floor -- pinned here so the
        // reader's sentinel-driven walk is not mistaken for the whole story.
        var analysis = Analyze($"select * from staging.src_crm__orders o " +
                               $"where o.updated_at > '{S}' and o.updated_at <= TIMESTAMP '2026-06-01'");

        Assert.Empty(analysis.Violations);
        Assert.False(Assert.Single(analysis.Comparisons).IsUpper);
    }

    [Fact]
    public void The_null_guard_still_wraps_a_recognized_ceiling()
    {
        // Same guard as the floor: a NULL bound (first run, bare watermark()) must not exclude every row.
        var analysis = Analyze($"select * from staging.src_crm__orders o where o.updated_at <= '{S}'");

        Assert.Empty(analysis.Violations);
        Assert.Contains("IS NULL", analysis.RewrittenSql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void A_floor_is_still_the_default_reading_of_a_recognized_comparison()
    {
        var analysis = Analyze($"select * from staging.src_crm__orders o where o.updated_at > '{S}'");

        Assert.False(Assert.Single(analysis.Comparisons).IsUpper);
    }

    [Fact]
    public void Rewritten_sql_carries_null_guard_and_round_trips_through_duckdb()
    {
        var analysis = Analyze($"select * from staging.src_crm__orders o where o.amount > 0 and o.updated_at > '{S}' - interval 2 hour");
        Assert.Contains("IS NULL", analysis.RewrittenSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(S, analysis.RewrittenSql, StringComparison.Ordinal);
        // Deterministic: same input, byte-identical output.
        var again = Analyze($"select * from staging.src_crm__orders o where o.amount > 0 and o.updated_at > '{S}' - interval 2 hour");
        Assert.Equal(analysis.RewrittenSql, again.RewrittenSql);
    }

    // DagCompiler.BuildInlinedSql assembles an ephemeral consumer as
    //   with __pz_cte__<name> as (<ephemeral body carrying the watermark filter>) <consumer body>
    // so the recognized comparison lives INSIDE the CTE body — its alias must resolve against the
    // CTE's own FROM clause (scope-aware), not the outer statement's FROM (which is the CTE ref).
    private const string CteOpen = "with __pz_cte__orders_recent as (";
    private const string CteClose = ") select * from __pz_cte__orders_recent";

    [Fact]
    public void Qualified_watermark_inside_ephemeral_cte_body_is_recognized()
    {
        var analysis = Analyze(
            $"{CteOpen}select o.id, o.updated_at from staging.src_crm__orders o where o.updated_at > '{S}'{CteClose}");
        Assert.Empty(analysis.Violations);
        var cmp = Assert.Single(analysis.Comparisons);
        Assert.Equal("updated_at", cmp.Column);
        Assert.Equal("src_crm__orders", cmp.ColumnTable);
        Assert.False(cmp.Inclusive);
        // The NULL-guard rewrite targets the comparison inside the CTE — the WITH must survive.
        Assert.Contains("IS NULL", analysis.RewrittenSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("WITH", analysis.RewrittenSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(S, analysis.RewrittenSql, StringComparison.Ordinal);
    }

    [Fact]
    public void Unqualified_watermark_inside_ephemeral_cte_body_resolves_to_cte_sole_base_table()
    {
        var analysis = Analyze(
            $"{CteOpen}select id, updated_at from staging.src_crm__orders where updated_at > '{S}'{CteClose}");
        Assert.Empty(analysis.Violations);
        var cmp = Assert.Single(analysis.Comparisons);
        Assert.Equal("updated_at", cmp.Column);
        Assert.Equal("src_crm__orders", cmp.ColumnTable);
    }

    [Fact]
    public void Watermark_inside_ephemeral_cte_body_rewrite_is_deterministic()
    {
        var sql = $"{CteOpen}select o.id, o.updated_at from staging.src_crm__orders o where o.updated_at >= '{S}' - interval 2 hour{CteClose}";
        var first = Analyze(sql);
        var second = Analyze(sql);
        Assert.Empty(first.Violations);
        Assert.Equal(first.RewrittenSql, second.RewrittenSql);
        Assert.Contains("WITH", first.RewrittenSql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Deterministic_value_side_functions_are_recognized()
    {
        // Arithmetic and pure scalar functions on the value side evaluate identically at extraction-
        // bound time and pipeline-predicate time — welcome; only volatile functions are refused.
        var analysis = Analyze(
            $"select * from staging.src_crm__orders o where o.updated_at > greatest('{S}', '{S}' - interval 2 hour)");
        Assert.Empty(analysis.Violations);
        Assert.Single(analysis.Comparisons);
    }

    [Fact]
    public void Volatile_value_side_function_is_rejected_naming_the_function()
    {
        // now() is CONSISTENT_WITHIN_QUERY: it evaluates differently at extraction-bound time vs
        // pipeline-predicate time, so rows landed in staging could be excluded by the predicate while
        // advancement (MAX(cursor)) moves past them — permanently skipped, breaking effectively-once.
        var analysis = Analyze(
            $"select * from staging.src_crm__orders o where o.updated_at > greatest('{S}', now() - interval 1 hour)");
        Assert.Empty(analysis.Comparisons);
        var violation = Assert.Single(analysis.Violations);
        Assert.Contains("now", violation.Reason, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("deterministic", violation.Reason, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Volatile_random_on_the_value_side_is_rejected()
    {
        var analysis = Analyze(
            $"select * from staging.src_crm__orders o where o.updated_at > '{S}' + random()");
        Assert.Empty(analysis.Comparisons);
        var violation = Assert.Single(analysis.Violations);
        Assert.Contains("random", violation.Reason, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Value_side_function_absent_from_the_catalog_is_rejected_naming_the_function()
    {
        // clock_timestamp() parses as a FUNCTION node (json_serialize_sql never validates function
        // existence — it's a pure parse) but is absent from duckdb_functions() entirely on the pinned
        // DuckDB. Catalog absence must not be silently treated as deterministic: an unrecognized name
        // is rejected outright, same as a VOLATILE/CONSISTENT_WITHIN_QUERY hit, because its
        // determinism cannot be verified against the catalog at all.
        var analysis = Analyze(
            $"select * from staging.src_crm__orders o where o.updated_at > greatest('{S}', clock_timestamp())");
        Assert.Empty(analysis.Comparisons);
        var violation = Assert.Single(analysis.Violations);
        Assert.Contains("clock_timestamp", violation.Reason, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("catalog", violation.Reason, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Join_resolves_each_alias_to_its_base_table()
    {
        var analysis = Analyze(
            $"select * from staging.src_crm__orders o join staging.src_crm__customers c on c.id = o.customer_id where o.updated_at > '{S}'");
        var cmp = Assert.Single(analysis.Comparisons);
        Assert.Equal("src_crm__orders", cmp.ColumnTable);
    }

    [Fact]
    public void Loose_occurrence_alongside_a_recognized_comparison_is_still_a_violation()
    {
        // The WHERE clause has a perfectly valid comparison, but the sentinel ALSO appears loose in
        // the select list. Total-or-error is occurrence-level, not sentinel-level: the recognized
        // comparison must not let the loose occurrence silently ride along into RewrittenSql.
        var sql = $"select '{S}' as x from staging.src_crm__orders o where o.updated_at > '{S}'";
        var analysis = Analyze(sql);

        var violation = Assert.Single(analysis.Violations);
        Assert.Equal(S, violation.Sentinel);
        Assert.Contains("comparison", violation.Reason, StringComparison.OrdinalIgnoreCase);

        // violations non-empty => no rewrite, exactly like every other rejected shape.
        Assert.Equal(sql, analysis.RewrittenSql);
    }
}
