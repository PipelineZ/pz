namespace Pz.Connectors.Toolkit.Http;

/// <summary>Parses proactive rate-limit metadata from a response: X-RateLimit-Remaining/-Reset
/// (the de-facto pair) or the standardized RateLimit-Remaining/-Reset. Reset accepts both epoch
/// seconds and delta seconds — values > 10_000_000 (~1970-04-26 as an epoch; no sane delta) are
/// epoch, else delta from <paramref name="now"/>. Values beyond 253_402_300_799 (year 9999 UTC,
/// the largest instant <see cref="DateTimeOffset.FromUnixTimeSeconds"/> can represent — e.g. a
/// provider sending epoch MILLISECONDS instead of seconds) are rejected as invalid rather than
/// guessed at: TryParse returns false (no budget hint this response) instead of throwing — the
/// ms-vs-s ambiguity isn't worth resolving silently. Header values are provider metadata, not
/// secrets — safe to parse, never logged verbatim by callers.</summary>
public static class RateLimitHeaders
{
    private const long EpochThresholdSeconds = 10_000_000;
    private const long MaxEpochSeconds = 253_402_300_799;

    public static bool TryParse(HttpResponseMessage response, DateTimeOffset now,
        out int remaining, out DateTimeOffset resetAt)
    {
        remaining = 0;
        resetAt = default;

        var remainingRaw = First(response, "X-RateLimit-Remaining") ?? First(response, "RateLimit-Remaining");
        var resetRaw = First(response, "X-RateLimit-Reset") ?? First(response, "RateLimit-Reset");
        if (remainingRaw is null || resetRaw is null)
        {
            return false;
        }

        if (!int.TryParse(remainingRaw, out remaining) || remaining < 0)
        {
            remaining = 0;
            return false;
        }

        if (!long.TryParse(resetRaw, out var reset) || reset < 0)
        {
            remaining = 0;
            return false;
        }

        if (reset > EpochThresholdSeconds)
        {
            if (reset > MaxEpochSeconds)
            {
                // Out of DateTimeOffset.FromUnixTimeSeconds' representable range -- most likely a
                // provider sending epoch MILLISECONDS (~1.79e12), which also clears the epoch-vs-delta
                // heuristic above. Never guess ms-vs-s: false just means no budget hint from this
                // response, instead of throwing from inside TryParse.
                remaining = 0;
                return false;
            }

            resetAt = DateTimeOffset.FromUnixTimeSeconds(reset);
        }
        else
        {
            resetAt = now.AddSeconds(reset);
        }

        return true;
    }

    private static string? First(HttpResponseMessage response, string name)
        => response.Headers.TryGetValues(name, out var values) ? values.FirstOrDefault() : null;
}
