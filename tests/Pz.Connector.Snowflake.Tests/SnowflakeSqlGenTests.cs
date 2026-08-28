using Pz.Connectors.Abstractions;

namespace Pz.Connector.Snowflake.Tests;

public class SnowflakeSqlGenTests
{
    private static DatasetSpec Spec(string dataset, params (string, object?)[] options) =>
        new("sf", dataset, options.ToDictionary(x => x.Item1, x => x.Item2));

    [Fact]
    public void Plain_table_selects_star() =>
        Assert.Equal("select * from \"RAW\".\"ORDERS\"",
            SnowflakeSource.BuildSelect(Spec("RAW.ORDERS"), ReadHints.None));

    [Fact]
    public void Column_hints_prune_projection() =>
        Assert.Equal("select \"id\", \"amount\" from \"PUBLIC\".\"ORDERS\"",
            SnowflakeSource.BuildSelect(Spec("ORDERS"), new ReadHints(Columns: ["id", "amount"])));

    [Fact]
    public void Predicate_and_watermark_terms_are_self_parenthesized()
    {
        var spec = Spec("ORDERS") with { WatermarkCursor = "updated_at", WatermarkValue = "2026-01-01T00:00:00.000000" };
        var sql = SnowflakeSource.BuildSelect(spec, new ReadHints(PredicateSql: "\"a\" = 1 or \"b\" = 2"));
        Assert.Equal(
            "select * from \"PUBLIC\".\"ORDERS\" where ((\"a\" = 1 or \"b\" = 2)) " +
            "and ((\"updated_at\" > '2026-01-01T00:00:00.000000'))", sql);
    }

    [Fact]
    public void Inclusive_lower_bound_uses_gte()
    {
        var spec = Spec("ORDERS") with
        {
            WatermarkCursor = "id", WatermarkValue = "100", WatermarkLowerInclusive = true,
        };
        Assert.Contains("\"id\" >= '100'", SnowflakeSource.BuildSelect(spec, ReadHints.None));
    }

    [Fact]
    public void Upper_bound_is_lte()
    {
        var spec = Spec("ORDERS") with
        {
            WatermarkCursor = "id", WatermarkValue = "100", WatermarkUpperBound = "200",
        };
        var sql = SnowflakeSource.BuildSelect(spec, ReadHints.None);
        Assert.Contains("\"id\" > '100'", sql);
        Assert.Contains("\"id\" <= '200'", sql);
    }

    [Fact]
    public void Cursor_without_value_is_an_unfiltered_select()
    {
        var spec = Spec("ORDERS") with { WatermarkCursor = "id" };
        Assert.Equal("select * from \"PUBLIC\".\"ORDERS\"", SnowflakeSource.BuildSelect(spec, ReadHints.None));
    }

    [Fact]
    public void Query_mode_is_verbatim_and_hints_are_ignored() =>
        Assert.Equal("select 1 as x",
            SnowflakeSource.BuildSelect(Spec("d", ("query", "select 1 as x")), new ReadHints(Columns: ["x"])));

    [Fact]
    public void Watermark_value_single_quotes_are_escaped()
    {
        var spec = Spec("ORDERS") with { WatermarkCursor = "c", WatermarkValue = "o'clock" };
        Assert.Contains("'o''clock'", SnowflakeSource.BuildSelect(spec, ReadHints.None));
    }
}
