using System.Globalization;
using Pz.Connectors.Toolkit.Http;

namespace Pz.Connectors.Toolkit.Tests.Http;

public class RateLimitHeadersTests
{
    [Fact]
    public void Parses_x_prefixed_pair()
    {
        var response = new System.Net.Http.HttpResponseMessage();
        response.Headers.TryAddWithoutValidation("X-RateLimit-Remaining", "42");
        response.Headers.TryAddWithoutValidation("X-RateLimit-Reset", "1789000000");
        var now = DateTimeOffset.UtcNow;

        var result = RateLimitHeaders.TryParse(response, now, out var remaining, out var resetAt);

        Assert.True(result);
        Assert.Equal(42, remaining);
        Assert.Equal(DateTimeOffset.FromUnixTimeSeconds(1789000000), resetAt);
    }

    [Fact]
    public void Parses_delta_seconds_reset()
    {
        var response = new System.Net.Http.HttpResponseMessage();
        response.Headers.TryAddWithoutValidation("X-RateLimit-Remaining", "10");
        response.Headers.TryAddWithoutValidation("X-RateLimit-Reset", "30");
        var now = new DateTimeOffset(2026, 7, 20, 0, 0, 0, TimeSpan.Zero);

        var result = RateLimitHeaders.TryParse(response, now, out var remaining, out var resetAt);

        Assert.True(result);
        Assert.Equal(10, remaining);
        Assert.Equal(now.AddSeconds(30), resetAt);
    }

    [Fact]
    public void Parses_standardized_names()
    {
        var response = new System.Net.Http.HttpResponseMessage();
        response.Headers.TryAddWithoutValidation("RateLimit-Remaining", "25");
        response.Headers.TryAddWithoutValidation("RateLimit-Reset", "1789000000");
        var now = DateTimeOffset.UtcNow;

        var result = RateLimitHeaders.TryParse(response, now, out var remaining, out var resetAt);

        Assert.True(result);
        Assert.Equal(25, remaining);
        Assert.Equal(DateTimeOffset.FromUnixTimeSeconds(1789000000), resetAt);
    }

    [Fact]
    public void X_prefixed_wins_when_both()
    {
        var response = new System.Net.Http.HttpResponseMessage();
        response.Headers.TryAddWithoutValidation("X-RateLimit-Remaining", "99");
        response.Headers.TryAddWithoutValidation("RateLimit-Remaining", "1");
        response.Headers.TryAddWithoutValidation("X-RateLimit-Reset", "1789000000");
        response.Headers.TryAddWithoutValidation("RateLimit-Reset", "1780000000");
        var now = DateTimeOffset.UtcNow;

        var result = RateLimitHeaders.TryParse(response, now, out var remaining, out var resetAt);

        Assert.True(result);
        Assert.Equal(99, remaining);
        Assert.Equal(DateTimeOffset.FromUnixTimeSeconds(1789000000), resetAt);
    }

    [Theory]
    [InlineData(true, false)]  // Only Remaining
    [InlineData(false, true)]  // Only Reset
    [InlineData(false, false)] // Neither
    public void Missing_either_header_false(bool hasRemaining, bool hasReset)
    {
        var response = new System.Net.Http.HttpResponseMessage();
        if (hasRemaining)
            response.Headers.TryAddWithoutValidation("X-RateLimit-Remaining", "42");
        if (hasReset)
            response.Headers.TryAddWithoutValidation("X-RateLimit-Reset", "1789000000");
        var now = DateTimeOffset.UtcNow;

        var result = RateLimitHeaders.TryParse(response, now, out var remaining, out var resetAt);

        Assert.False(result);
        Assert.Equal(0, remaining);
        Assert.Equal(default, resetAt);
    }

    [Fact]
    public void Garbage_values_false()
    {
        var response = new System.Net.Http.HttpResponseMessage();
        response.Headers.TryAddWithoutValidation("X-RateLimit-Remaining", "lots");
        response.Headers.TryAddWithoutValidation("X-RateLimit-Reset", "1789000000");
        var now = DateTimeOffset.UtcNow;

        var result = RateLimitHeaders.TryParse(response, now, out var remaining, out var resetAt);

        Assert.False(result);
    }

    [Fact]
    public void Negative_remaining_false()
    {
        var response = new System.Net.Http.HttpResponseMessage();
        response.Headers.TryAddWithoutValidation("X-RateLimit-Remaining", "-1");
        response.Headers.TryAddWithoutValidation("X-RateLimit-Reset", "1789000000");
        var now = DateTimeOffset.UtcNow;

        var result = RateLimitHeaders.TryParse(response, now, out var remaining, out var resetAt);

        Assert.False(result);
        Assert.Equal(0, remaining);
    }

    // A provider sending X-RateLimit-Reset in epoch MILLISECONDS (~1.79e12) passes both the
    // > 10_000_000 epoch heuristic and long.TryParse, but DateTimeOffset.FromUnixTimeSeconds only
    // accepts up to 253_402_300_799 (year 9999) and throws ArgumentOutOfRangeException -- on the
    // success path of every gated HTTP page fetch. TryParse must never throw.

    [Fact]
    public void Epoch_milliseconds_reset_false()
    {
        var response = new System.Net.Http.HttpResponseMessage();
        response.Headers.TryAddWithoutValidation("X-RateLimit-Remaining", "5");
        response.Headers.TryAddWithoutValidation("X-RateLimit-Reset", "1789000000000");
        var now = DateTimeOffset.UtcNow;

        var result = RateLimitHeaders.TryParse(response, now, out var remaining, out var resetAt);

        Assert.False(result);
        Assert.Equal(0, remaining);
        Assert.Equal(default, resetAt);
    }

    [Theory]
    [InlineData(253_402_300_799, true)]  // DateTimeOffset.FromUnixTimeSeconds' max representable value
    [InlineData(253_402_300_800, false)] // one second beyond it
    public void Epoch_reset_boundary(long resetValue, bool expected)
    {
        var response = new System.Net.Http.HttpResponseMessage();
        response.Headers.TryAddWithoutValidation("X-RateLimit-Remaining", "5");
        response.Headers.TryAddWithoutValidation("X-RateLimit-Reset", resetValue.ToString(CultureInfo.InvariantCulture));
        var now = DateTimeOffset.UtcNow;

        var result = RateLimitHeaders.TryParse(response, now, out var remaining, out var resetAt);

        Assert.Equal(expected, result);
        if (expected)
        {
            Assert.Equal(DateTimeOffset.FromUnixTimeSeconds(resetValue), resetAt);
        }
        else
        {
            Assert.Equal(0, remaining);
        }
    }
}
