using Pz.Core.Dag;

namespace Pz.DuckDb.Tests;

/// <summary>One reader owns one parse-only session: every call on it reuses that session, and a reused
/// reader answers exactly as a fresh one would.</summary>
public sealed class DuckDbSqlAstReaderReuseTests
{
    private const string S = "__pz_watermark__crm__orders__";
    private const string Recognized = $"select * from staging.src_crm__orders o where o.updated_at > '{S}'";
    private const string Shape = $"select * from staging.src_crm__orders o where o.updated_at = '{S}'";
    private const string Now =
        $"select * from staging.src_crm__orders o where o.updated_at > greatest('{S}', now() - interval 1 hour)";
    private const string Absent =
        $"select * from staging.src_crm__orders o where o.updated_at > greatest('{S}', clock_timestamp())";
    private const string Hints = "select id from src_a where a = 1 and b = 2";
    private static readonly (string Alias, string Sql)[] Ctes = [("__pz_cte__eph", "select 1 as n")];

    [Fact]
    public void One_session_serves_every_call_on_a_reader()
    {
        using var reader = new DuckDbSqlAstReader();

        reader.ExtractReadHints(Hints, "src_a", null);
        reader.ExtractReadHints(Hints, "src_a", null);
        reader.ExtractReadHints(Hints, "src_a", null);
        reader.Analyze(Recognized, [S]);
        reader.PrependCtes("select * from __pz_cte__eph", Ctes);

        Assert.Equal(1, reader.SessionsOpenedForTests);
    }

    [Fact]
    public void Reused_reader_matches_fresh_readers()
    {
        using var reused = new DuckDbSqlAstReader();

        AssertSameAnalysis(Fresh(r => r.Analyze(Recognized, [S])), reused.Analyze(Recognized, [S]));
        AssertSameAnalysis(Fresh(r => r.Analyze(Shape, [S])), reused.Analyze(Shape, [S]));
        AssertSameHints(
            Fresh(r => r.ExtractReadHints(Hints, "src_a", null)), reused.ExtractReadHints(Hints, "src_a", null));
        AssertSameHints(
            Fresh(r => r.ExtractReadHints("select from where", "t", null)),
            reused.ExtractReadHints("select from where", "t", null));
        Assert.Equal(
            Fresh(r => r.PrependCtes("select * from __pz_cte__eph", Ctes)),
            reused.PrependCtes("select * from __pz_cte__eph", Ctes));
    }

    [Fact]
    public void Reused_reader_recovers_after_a_failed_statement()
    {
        using var reused = new DuckDbSqlAstReader();

        AssertSameHints(ReadHintPlan.None, reused.ExtractReadHints("select from where", "t", null));

        AssertSameHints(
            Fresh(r => r.ExtractReadHints(Hints, "src_a", null)), reused.ExtractReadHints(Hints, "src_a", null));
    }

    [Fact]
    public void Non_deterministic_function_rules_are_unchanged()
    {
        using var reader = new DuckDbSqlAstReader();

        var nonConsistent = Assert.Single(reader.Analyze(Now, [S]).Violations);
        Assert.Contains("now", nonConsistent.Reason, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("deterministic", nonConsistent.Reason, StringComparison.OrdinalIgnoreCase);

        var absent = Assert.Single(reader.Analyze(Absent, [S]).Violations);
        Assert.Contains("clock_timestamp", absent.Reason, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("catalog", absent.Reason, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(Fresh(r => r.Analyze(Absent, [S])).Violations, reader.Analyze(Absent, [S]).Violations);
    }

    [Fact]
    public void Dispose_then_use_throws_ObjectDisposedException()
    {
        var reader = new DuckDbSqlAstReader();
        reader.ExtractReadHints(Hints, "src_a", null);
        reader.Dispose();

        Assert.Throws<ObjectDisposedException>(() => reader.ExtractReadHints(Hints, "src_a", null));
        Assert.Throws<ObjectDisposedException>(() => reader.Analyze(Recognized, [S]));
        Assert.Throws<ObjectDisposedException>(() => reader.PrependCtes("select 1", Ctes));
    }

    private static T Fresh<T>(Func<DuckDbSqlAstReader, T> call)
    {
        using var reader = new DuckDbSqlAstReader();
        return call(reader);
    }

    private static void AssertSameAnalysis(WatermarkAnalysis expected, WatermarkAnalysis actual)
    {
        Assert.Equal(expected.Comparisons, actual.Comparisons);
        Assert.Equal(expected.Violations, actual.Violations);
        Assert.Equal(expected.RewrittenSql, actual.RewrittenSql);
    }

    private static void AssertSameHints(ReadHintPlan expected, ReadHintPlan actual)
    {
        Assert.Equal(expected.Columns, actual.Columns);
        Assert.Equal(expected.PredicateSql, actual.PredicateSql);
    }
}
