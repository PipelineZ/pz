using System;
using Pz.Connectors.Abstractions.Paths;
using Xunit;

namespace Pz.Connectors.Abstractions.Tests.Paths;

public class PathTemplateRenderTests
{
    [Theory]
    [InlineData("2026-07-11", 2026, 7, 11, 0, 0)]
    [InlineData("2026-07-11T09:30:15.000000", 2026, 7, 11, 9, 30)]
    public void ParseCanonical_reads_both_forms(string s, int y, int mo, int d, int h, int mi)
    {
        var dt = PathTemplate.ParseCanonical(s);
        Assert.Equal(new DateTimeOffset(y, mo, d, h, mi, 0, TimeSpan.Zero), new DateTimeOffset(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, 0, TimeSpan.Zero));
    }

    [Fact]
    public void Render_substitutes_all_tokens()
    {
        var dt = new DateTimeOffset(2026, 7, 5, 9, 3, 0, TimeSpan.Zero);
        Assert.Equal("e/2026/07/05/09*.parquet", PathTemplate.Render("e/{yyyy}/{MM}/{dd}/{HH}*.parquet", dt));
        Assert.Equal("e/26/07/*.parquet", PathTemplate.Render("e/{yy}/{MM}/*.parquet", dt));
    }

    [Fact]
    public void Render_substitutes_minute_token()
    {
        var dt = new DateTimeOffset(2026, 7, 5, 9, 3, 0, TimeSpan.Zero);
        Assert.Equal("e/09/03/*.parquet", PathTemplate.Render("e/{HH}/{mm}/*.parquet", dt));
    }

    [Theory]
    [InlineData("e/{yyyy}/{MM}/*.parquet", "e/")]
    [InlineData("events/2026/07/12/*.parquet", "events/2026/07/12/")]
    [InlineData("events/2026/07/11/10*.parquet", "events/2026/07/11/10")]
    public void StaticPrefix_is_leading_literal(string pattern, string expected)
        => Assert.Equal(expected, PathTemplate.StaticPrefix(pattern));

    [Fact]
    public void StaticPrefix_all_literal_returns_whole_string()
        => Assert.Equal("events/raw/data.parquet", PathTemplate.StaticPrefix("events/raw/data.parquet"));
}
