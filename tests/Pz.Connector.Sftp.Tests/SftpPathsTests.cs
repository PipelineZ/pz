using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connector.Sftp.Tests;

/// <summary>Pure path resolution, glob listing, and watermark-window cover narrowing — no network,
/// only <see cref="FakeSftpFileSystem"/>.</summary>
public class SftpPathsTests
{
    private static readonly IReadOnlyDictionary<string, object?> NoOptions = new Dictionary<string, object?>();

    // ---- ResolveReadPattern -------------------------------------------------------------------

    [Fact]
    public void ResolveReadPattern_defaults_to_entity_dot_format_under_root()
    {
        var spec = new DatasetSpec("sftp", "orders", NoOptions);

        var pattern = SftpPaths.ResolveReadPattern("/data", spec, "csv");

        Assert.Equal("/data/orders.csv", pattern);
    }

    [Fact]
    public void ResolveReadPattern_explicit_path_joins_under_root()
    {
        var spec = new DatasetSpec("sftp", "orders", new Dictionary<string, object?> { ["path"] = "in/orders.csv" });

        var pattern = SftpPaths.ResolveReadPattern("/data", spec, "csv");

        Assert.Equal("/data/in/orders.csv", pattern);
    }

    [Fact]
    public void ResolveReadPattern_with_no_root_leaves_the_explicit_path_unmangled()
    {
        var spec = new DatasetSpec("sftp", "orders", new Dictionary<string, object?> { ["path"] = "/abs/in/orders.csv" });

        var pattern = SftpPaths.ResolveReadPattern(null, spec, "csv");

        Assert.Equal("/abs/in/orders.csv", pattern);
    }

    // ---- ResolveOutputDir ----------------------------------------------------------------------

    [Fact]
    public void ResolveOutputDir_defaults_to_entity_under_root()
    {
        var spec = new OutputSpec("sftp", "orders", "replace", "strict", NoOptions);

        var dir = SftpPaths.ResolveOutputDir("/data", spec);

        Assert.Equal("/data/orders", dir);
    }

    // ---- GlobToRegexPattern ---------------------------------------------------------------------

    [Fact]
    public void GlobToRegexPattern_star_does_not_cross_a_slash()
    {
        var regex = new System.Text.RegularExpressions.Regex(SftpPaths.GlobToRegexPattern("in/*.csv"));

        Assert.Matches(regex, "in/a.csv");
        Assert.DoesNotMatch(regex, "in/sub/c.csv");
    }

    [Fact]
    public void GlobToRegexPattern_double_star_crosses_a_slash()
    {
        var regex = new System.Text.RegularExpressions.Regex(SftpPaths.GlobToRegexPattern("in/**/*.csv"));

        Assert.Matches(regex, "in/sub/c.csv");
    }

    [Fact]
    public void GlobToRegexPattern_question_mark_matches_exactly_one_char()
    {
        var regex = new System.Text.RegularExpressions.Regex(SftpPaths.GlobToRegexPattern("in/?.csv"));

        Assert.Matches(regex, "in/a.csv");
        Assert.DoesNotMatch(regex, "in/ab.csv");
    }

    [Fact]
    public void GlobToRegexPattern_escapes_regex_metacharacters_in_literal_names()
    {
        var regex = new System.Text.RegularExpressions.Regex(SftpPaths.GlobToRegexPattern("in/x+y.csv"));

        Assert.Matches(regex, "in/x+y.csv");
        Assert.DoesNotMatch(regex, "in/xxy.csv");   // '+' must be literal, not "one-or-more of the preceding"
    }

    // ---- ListMatches: literal path ---------------------------------------------------------------

    [Fact]
    public void ListMatches_literal_pattern_returns_a_single_element_when_the_file_exists()
    {
        var fake = new FakeSftpFileSystem();
        fake.Seed("in/a.csv", []);
        var spec = new DatasetSpec("sftp", "a", NoOptions);

        var matches = SftpPaths.ListMatches(fake, "in/a.csv", spec);

        Assert.Equal(["in/a.csv"], matches);
    }

    [Fact]
    public void ListMatches_literal_pattern_with_no_match_returns_empty()
    {
        var fake = new FakeSftpFileSystem();
        var spec = new DatasetSpec("sftp", "a", NoOptions);

        var matches = SftpPaths.ListMatches(fake, "in/missing.csv", spec);

        Assert.Empty(matches);
    }

    // ---- ListMatches: glob ------------------------------------------------------------------------

    [Fact]
    public void ListMatches_single_star_matches_only_the_immediate_directory_sorted_ordinally()
    {
        var fake = new FakeSftpFileSystem();
        fake.Seed("in/a.csv", []);
        fake.Seed("in/b.csv", []);
        fake.Seed("in/sub/c.csv", []);
        var spec = new DatasetSpec("sftp", "in", NoOptions);

        var matches = SftpPaths.ListMatches(fake, "in/*.csv", spec);

        Assert.Equal(["in/a.csv", "in/b.csv"], matches);
    }

    [Fact]
    public void ListMatches_double_star_recurses_into_subdirectories()
    {
        var fake = new FakeSftpFileSystem();
        fake.Seed("in/a.csv", []);
        fake.Seed("in/b.csv", []);
        fake.Seed("in/sub/c.csv", []);
        var spec = new DatasetSpec("sftp", "in", NoOptions);

        // "**" alone (no intervening literal '/' the way "in/**/*.csv" has one) matches zero-or-more
        // path segments, so it reaches both the immediate directory and its subdirectories — same
        // shape as AzureSourcePlanTests.MatchGlob_double_star_crosses_slash_boundaries's "in/**.parquet".
        var matches = SftpPaths.ListMatches(fake, "in/**.csv", spec);

        Assert.Equal(["in/a.csv", "in/b.csv", "in/sub/c.csv"], matches);
    }

    [Fact]
    public void ListMatches_double_star_then_literal_slash_requires_an_actual_subdirectory()
    {
        var fake = new FakeSftpFileSystem();
        fake.Seed("in/a.csv", []);
        fake.Seed("in/sub/c.csv", []);
        var spec = new DatasetSpec("sftp", "in", NoOptions);

        // Unlike "in/**.csv", "in/**/*.csv" has a literal '/' between the "**" and the final segment,
        // so it only matches names with a real path separator there — "in/a.csv" sits directly in
        // "in/" and has no such separator.
        var matches = SftpPaths.ListMatches(fake, "in/**/*.csv", spec);

        Assert.Equal(["in/sub/c.csv"], matches);
    }

    [Fact]
    public void ListMatches_regex_metacharacters_in_file_names_do_not_leak_into_the_glob()
    {
        var fake = new FakeSftpFileSystem();
        fake.Seed("in/x+y.csv", []);
        var spec = new DatasetSpec("sftp", "in", NoOptions);

        var matches = SftpPaths.ListMatches(fake, "in/x+y.csv", spec);

        Assert.Equal(["in/x+y.csv"], matches);
    }

    // ---- ListMatches: watermark window cover --------------------------------------------------

    [Fact]
    public void ListMatches_window_cover_narrows_to_the_watermark_bounds_inclusive()
    {
        var fake = new FakeSftpFileSystem();
        fake.Seed("daily/2026-08-26.csv", []);
        fake.Seed("daily/2026-08-27.csv", []);
        fake.Seed("daily/2026-08-28.csv", []);
        fake.Seed("daily/2026-08-29.csv", []);
        var spec = new DatasetSpec("sftp", "daily", NoOptions)
        {
            WatermarkCursor = "d",
            WatermarkValue = "2026-08-27",
            WatermarkUpperBound = "2026-08-29",
        };

        var matches = SftpPaths.ListMatches(fake, "daily/{yyyy}-{MM}-{dd}.csv", spec);

        Assert.Equal(["daily/2026-08-27.csv", "daily/2026-08-28.csv", "daily/2026-08-29.csv"], matches);
    }

    [Fact]
    public void ListMatches_without_both_watermark_bounds_does_not_apply_the_cover()
    {
        var fake = new FakeSftpFileSystem();
        fake.Seed("daily/2026-08-26.csv", []);
        fake.Seed("daily/2026-08-27.csv", []);
        var spec = new DatasetSpec("sftp", "daily", NoOptions) { WatermarkCursor = "d", WatermarkValue = "2026-08-27" };

        var matches = SftpPaths.ListMatches(fake, "daily/{yyyy}-{MM}-{dd}.csv", spec);

        // No upper bound: the pattern is treated as a literal path (still contains '{'/'}' tokens,
        // which never match a real listed file) — nothing is returned, not the whole directory.
        Assert.Empty(matches);
    }

    [Fact]
    public void ListMatches_without_date_tokens_does_not_apply_the_cover()
    {
        var fake = new FakeSftpFileSystem();
        fake.Seed("in/a.csv", []);
        fake.Seed("in/b.csv", []);
        var spec = new DatasetSpec("sftp", "in", NoOptions)
        {
            WatermarkCursor = "d",
            WatermarkValue = "2026-08-27",
            WatermarkUpperBound = "2026-08-29",
        };

        var matches = SftpPaths.ListMatches(fake, "in/*.csv", spec);

        Assert.Equal(["in/a.csv", "in/b.csv"], matches);
    }
}
