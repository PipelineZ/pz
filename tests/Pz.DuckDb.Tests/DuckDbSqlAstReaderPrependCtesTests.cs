using Pz.Core.Dag;

namespace Pz.DuckDb.Tests;

/// <summary>DagCompiler.BuildInlinedSql's AST route: <see cref="DuckDbSqlAstReader.PrependCtes"/> folds
/// an ephemeral pipeline's SELECT into a consumer's own WITH clause by reading and re-emitting the
/// parsed AST -- never by sniffing consumer text for a "with" keyword. Pins the three shapes a purely
/// textual splice gets wrong: a consumer that opens with a comment before its WITH, a consumer using
/// WITH RECURSIVE, and an ephemeral body ending in its own trailing `;`.</summary>
public sealed class DuckDbSqlAstReaderPrependCtesTests
{
    private static readonly DuckDbSqlAstReader Reader = new();

    [Fact]
    public void Consumer_with_no_with_clause_gets_one_synthesized()
    {
        var merged = Reader.PrependCtes(
            "select * from __pz_cte__eph", [("__pz_cte__eph", "select 1 as n")]);

        Assert.NotNull(merged);
        Assert.Contains("WITH __pz_cte__eph AS (SELECT 1 AS n)", merged, StringComparison.Ordinal);
        Assert.Contains("SELECT * FROM __pz_cte__eph", merged, StringComparison.Ordinal);
    }

    [Fact]
    public void Consumer_beginning_with_a_comment_before_with_still_merges()
    {
        // A purely textual `TrimStart().StartsWith("with")` check misses this: the comment sits before
        // the keyword. The AST route doesn't care where the comment was -- the parser already dropped it.
        var merged = Reader.PrependCtes(
            "-- a note about this pipeline\nwith x as (select 2 as m) select * from __pz_cte__eph, x",
            [("__pz_cte__eph", "select 1 as n")]);

        Assert.NotNull(merged);
        // Both CTEs present, ours first, exactly one WITH keyword (no doubled/nested WITH).
        Assert.Equal(1, CountOccurrences(merged, "WITH "));
        Assert.Contains("__pz_cte__eph AS (SELECT 1 AS n)", merged, StringComparison.Ordinal);
        Assert.Contains("x AS (SELECT 2 AS m)", merged, StringComparison.Ordinal);
        Assert.True(merged.IndexOf("__pz_cte__eph", StringComparison.Ordinal)
            < merged.IndexOf("x AS", StringComparison.Ordinal));
    }

    [Fact]
    public void Consumer_using_with_recursive_keeps_the_recursive_keyword_and_merges()
    {
        // A textual splice that strips "with" and re-adds it ahead of RECURSIVE breaks DuckDB's grammar
        // (RECURSIVE must immediately follow WITH, before the first CTE name). The AST route never
        // touches the consumer's own recursive CTE entry -- DuckDB's own deserializer decides to emit
        // RECURSIVE for the whole merged clause because ONE of the entries needs it.
        var merged = Reader.PrependCtes(
            "with recursive x as (select 1 as n union all select n + 1 from x where n < 3) " +
            "select * from x, __pz_cte__eph",
            [("__pz_cte__eph", "select 100 as y")]);

        Assert.NotNull(merged);
        Assert.Contains("WITH RECURSIVE", merged, StringComparison.Ordinal);
        Assert.Contains("__pz_cte__eph AS (SELECT 100 AS y)", merged, StringComparison.Ordinal);
        Assert.Contains("x AS", merged, StringComparison.Ordinal);
    }

    [Fact]
    public void Ephemeral_body_ending_in_a_semicolon_is_accepted()
    {
        // A textual splice would embed the trailing `;` inside the parenthesized CTE body -- a second,
        // empty statement DuckDB refuses. The AST route re-parses the ephemeral body as one statement (a
        // trailing `;` alone never creates a second one) and never concatenates raw text.
        var merged = Reader.PrependCtes(
            "select * from __pz_cte__eph", [("__pz_cte__eph", "select 1 as n;")]);

        Assert.NotNull(merged);
        Assert.Contains("__pz_cte__eph AS (SELECT 1 AS n)", merged, StringComparison.Ordinal);
    }

    [Fact]
    public void Two_ephemeral_bodies_merge_in_the_given_order()
    {
        var merged = Reader.PrependCtes(
            "select * from __pz_cte__a, __pz_cte__b",
            [("__pz_cte__a", "select 1 as n"), ("__pz_cte__b", "select 2 as m")]);

        Assert.NotNull(merged);
        Assert.True(merged.IndexOf("__pz_cte__a", StringComparison.Ordinal)
            < merged.IndexOf("__pz_cte__b", StringComparison.Ordinal));
    }

    [Fact]
    public void Unparseable_consumer_returns_null_so_the_caller_falls_back()
    {
        var merged = Reader.PrependCtes("select * from (", [("__pz_cte__eph", "select 1 as n")]);
        Assert.Null(merged);
    }

    [Fact]
    public void Unparseable_cte_body_returns_null_so_the_caller_falls_back()
    {
        var merged = Reader.PrependCtes("select * from __pz_cte__eph", [("__pz_cte__eph", "select * from (")]);
        Assert.Null(merged);
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }
}
