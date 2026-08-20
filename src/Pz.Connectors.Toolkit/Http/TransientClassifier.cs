using System.Globalization;
using System.Net.Sockets;

namespace Pz.Connectors.Toolkit.Http;

/// <summary>The canonical HTTP transient classification for connectors (generalizes
/// <c>AzureTransient</c>): the 408/429/5xx-retryable status set the DuckDB httpfs family also
/// retries, plus the network-level exception shapes a dropped/reset connection surfaces as. Pure
/// and offline-testable; produces inputs for <c>PzConnectorException(IsTransient, RetryAfter)</c> —
/// classification only, never retrying (single-retry-owner: the engine owns policy).</summary>
public static class TransientClassifier
{
    private static readonly HashSet<int> TransientStatusCodes = [408, 429, 500, 502, 503, 504];

    public static bool IsTransientStatus(int statusCode) => TransientStatusCodes.Contains(statusCode);

    public static bool IsTransientException(Exception ex) => ex switch
    {
        HttpRequestException { StatusCode: { } status } when IsTransientStatus((int)status) => true,
        HttpRequestException { InnerException: { } inner } => IsTransientException(inner),
        IOException => true,
        TimeoutException => true,
        SocketException => true,
        _ => false,
    };

    /// <summary>Parses a Retry-After header value: delta-seconds or RFC 7231 HTTP-date. A past date
    /// clamps to zero (retry immediately); unparsable/absent is null (no server hint).</summary>
    public static TimeSpan? ParseRetryAfter(string? headerValue, DateTimeOffset now)
    {
        if (string.IsNullOrWhiteSpace(headerValue))
        {
            return null;
        }

        if (long.TryParse(headerValue, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var seconds))
        {
            if (seconds < 0)
            {
                return TimeSpan.Zero;
            }

            if (seconds > (long)TimeSpan.MaxValue.TotalSeconds)
            {
                return null;
            }

            return TimeSpan.FromSeconds(seconds);
        }

        if (DateTimeOffset.TryParse(headerValue, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var when))
        {
            var delta = when - now;
            return delta > TimeSpan.Zero ? delta : TimeSpan.Zero;
        }

        return null;
    }
}
