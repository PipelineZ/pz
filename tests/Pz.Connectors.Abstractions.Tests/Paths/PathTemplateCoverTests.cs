using System;
using System.Linq;
using Pz.Connectors.Abstractions.Paths;
using Xunit;

namespace Pz.Connectors.Abstractions.Tests.Paths;

public class PathTemplateCoverTests
{
    private static DateTimeOffset T(string s) => PathTemplate.ParseCanonical(s);

    [Fact]
    public void Cover_matches_spec_worked_example()   // 2026-07-11 10:00 .. 2026-07-13 12:00, hour granularity
    {
        var cover = PathTemplate.WindowCover("events/{yyyy}/{MM}/{dd}/{HH}*.parquet",
            T("2026-07-11T10:00:00"), T("2026-07-13T12:00:00"));

        // 14 hour-prefixes for 07-11 (10..23), 1 whole-day for 07-12, 13 hour-prefixes for 07-13 (00..12).
        Assert.Equal(14 + 1 + 13, cover.Count);
        Assert.Equal("events/2026/07/11/10*.parquet", cover[0]);
        Assert.Contains("events/2026/07/12/*.parquet", cover);           // day collapsed
        Assert.Equal("events/2026/07/13/12*.parquet", cover[^1]);
    }

    [Fact]
    public void Cover_collapses_full_month_and_year()
    {
        var cover = PathTemplate.WindowCover("e/{yyyy}/{MM}/{dd}/*.parquet",
            T("2025-01-01"), T("2026-12-31"));
        // Whole year: {MM} and {dd} both coarsen to '*', but each remains its own path segment (a single
        // '*' only matches within one segment — see AzureSource.GlobToRegexPattern/GlobToRegexPattern-style
        // glob matchers used downstream): a whole year is "events/2026/*/*/*.parquet", not a
        // cross-segment collapse.
        Assert.Contains("e/2025/*/*/*.parquet", cover);   // whole year 2025
        Assert.Contains("e/2026/*/*/*.parquet", cover);   // whole year 2026
        Assert.Equal(2, cover.Count);
    }

    [Fact]
    public void Cover_single_bucket()
    {
        var cover = PathTemplate.WindowCover("e/{yyyy}/{MM}/{dd}/*.parquet", T("2026-07-11"), T("2026-07-11"));
        Assert.Equal(["e/2026/07/11/*.parquet"], cover);
    }

    [Fact]
    public void Cover_is_chronological_and_disjoint()
    {
        var cover = PathTemplate.WindowCover("e/{yyyy}/{MM}/{dd}/{HH}*.parquet",
            T("2026-02-27T22:00:00"), T("2026-03-01T02:00:00"));   // leap-year Feb boundary
        Assert.Equal(cover, cover.OrderBy(x => x, StringComparer.Ordinal).ToList()); // path order == chrono here
        Assert.Equal(cover.Distinct().Count(), cover.Count);
    }
}
