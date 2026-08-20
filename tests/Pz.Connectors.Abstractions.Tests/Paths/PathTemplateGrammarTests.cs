using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Paths;
using Xunit;

namespace Pz.Connectors.Abstractions.Tests.Paths;

public class PathTemplateGrammarTests
{
    [Fact]
    public void HasDateTokens_detects_presence()
    {
        Assert.True(PathTemplate.HasDateTokens("e/{yyyy}/{MM}/*.parquet"));
        Assert.False(PathTemplate.HasDateTokens("e/*.parquet"));
    }

    [Theory]
    [InlineData("e/{yyyy}/*.parquet", DateGranularity.Year)]
    [InlineData("e/{yyyy}/{MM}/*.parquet", DateGranularity.Month)]
    [InlineData("e/{yyyy}/{MM}/{dd}/*.parquet", DateGranularity.Day)]
    [InlineData("e/{yyyy}/{MM}/{dd}/{HH}*.parquet", DateGranularity.Hour)]
    [InlineData("e/{yyyy}/{MM}/{dd}/{HH}{mm}*.parquet", DateGranularity.Minute)]
    public void Validate_returns_finest_granularity(string pattern, DateGranularity expected)
        => Assert.Equal(expected, PathTemplate.Validate(pattern, "dataset 'x'"));

    [Fact]
    public void Key_token_is_reserved_and_never_counts_as_a_date_token()
    {
        // {key} is the http sink's merge-key substitution token (mode: merge requires it in
        // path:); calendar-token validation must never claim it.
        Assert.False(PathTemplate.HasDateTokens("/anything/comments/{key}"));
        Assert.Equal(DateGranularity.Year,
            PathTemplate.Validate("e/{yyyy}/{key}/*.parquet", "dataset 'x'"));
    }

    [Fact]
    public void Key_lookalike_tokens_still_reject()
    {
        Assert.True(PathTemplate.HasDateTokens("/items/{Key}"));
        Assert.True(PathTemplate.HasDateTokens("/items/{keys}"));
        Assert.Throws<PzConnectorException>(() => PathTemplate.Validate("/items/{keys}", "output 'x'"));
    }

    [Theory]
    [InlineData("e/{yyyy}/{dd}/*.parquet")]   // gap: dd without MM
    [InlineData("e/{MM}/{yyyy}/*.parquet")]   // out of order
    [InlineData("e/{yyyy}/{zz}/*.parquet")]   // unknown token
    public void Validate_rejects_malformed(string pattern)
    {
        var ex = Assert.Throws<PzConnectorException>(() => PathTemplate.Validate(pattern, "dataset 'x'"));
        Assert.False(ex.IsTransient);
        Assert.Contains("dataset 'x'", ex.Message);
    }
}
