using System.Globalization;
using Pz.Connectors.Toolkit.Http;

namespace Pz.Connector.Http;

/// <summary>How the configured header states the wait.</summary>
internal enum RetryAfterKind
{
    /// <summary>A delay in seconds.</summary>
    Seconds,

    /// <summary>A delay in milliseconds.</summary>
    Milliseconds,

    /// <summary>An absolute Unix time in seconds.</summary>
    Epoch,
}

/// <summary>The connection's <c>retry_after</c> option: a header that states how long to wait when a
/// throttled response (429/503, or an <c>on_status</c> transient) carries no usable <c>Retry-After</c>.
/// Classification input only; the engine does the waiting. Header values are provider metadata, never
/// secrets.</summary>
internal sealed record HttpRetryAfterHeader(string Header, RetryAfterKind Kind)
{
    /// <summary>The longest wait the configured header can impose, the same bound as the read's shared
    /// pause.</summary>
    internal static readonly TimeSpan MaxWait = TimeSpan.FromSeconds(600);

    /// <summary>A stated wait this far above <see cref="MaxWait"/> is almost surely a wrong <c>kind</c> (an epoch
    /// read as seconds, milliseconds read as an epoch), so the capped wait carries a note saying so.</summary>
    private static readonly TimeSpan Implausible = TimeSpan.FromDays(1);

    /// <summary>The wait the response's configured header states, capped at <see cref="MaxWait"/>, or null when
    /// the header is absent, unreadable, or states no wait (zero, or an epoch already past -- usual under clock
    /// skew): a zero hint would override every other source and make the engine retry with no delay at all,
    /// so it counts as no value and the caller falls through to the rule's <c>retry_after</c> or the engine's
    /// backoff. <paramref name="note"/> is empty, or a message suffix saying why the value was ignored or that
    /// an implausibly long wait was capped.</summary>
    public TimeSpan? Read(HttpResponseMessage response, DateTimeOffset now, out string note)
    {
        note = "";
        if (!response.Headers.TryGetValues(Header, out var values)
            && !response.Content.Headers.TryGetValues(Header, out values))
        {
            return null;
        }

        var raw = values.FirstOrDefault()?.Trim();
        if (!decimal.TryParse(raw, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out var number))
        {
            note = $" (retry_after header '{Header}' value is not a non-negative number; ignored)";
            return null;
        }

        var seconds = Kind switch
        {
            RetryAfterKind.Seconds => number,
            RetryAfterKind.Milliseconds => number / 1000m,
            _ => number - now.ToUnixTimeMilliseconds() / 1000m,
        };
        if (seconds <= 0)
        {
            note = $" (retry_after header '{Header}' states no wait; ignored)";
            return null;
        }

        if (seconds < (decimal)MaxWait.TotalSeconds)
        {
            return TimeSpan.FromMilliseconds((double)(seconds * 1000m));
        }

        if (seconds > (decimal)Implausible.TotalSeconds)
        {
            note = $" (retry_after header '{Header}' states {seconds.ToString("0.###", CultureInfo.InvariantCulture)} s; " +
                $"capped at {(int)MaxWait.TotalSeconds} s, check retry_after.kind)";
        }

        return MaxWait;
    }

    /// <summary>The wait a non-2xx response states: its own <c>Retry-After</c>; when that is missing or unusable
    /// and <paramref name="useConfigured"/>, the wait <paramref name="configured"/> (the connection's
    /// <c>retry_after</c> header) states. Every request the connector sends, the session login included, resolves
    /// its wait here. <paramref name="note"/> says when the configured header was present but unusable.</summary>
    public static TimeSpan? Resolve(HttpResponseMessage response, HttpRetryAfterHeader? configured, bool useConfigured,
        DateTimeOffset now, out string note)
    {
        note = "";
        var standard = TransientClassifier.ParseRetryAfter(
            response.Headers.TryGetValues("Retry-After", out var values) ? values.FirstOrDefault() : null, now);
        return standard is null && useConfigured && configured is not null
            ? configured.Read(response, now, out note)
            : standard;
    }

    /// <summary>Parses the <c>retry_after</c> map; null when absent. Problems go to <paramref name="errors"/>.</summary>
    public static HttpRetryAfterHeader? Parse(object? raw, List<string> errors)
    {
        if (raw is null)
        {
            return null;
        }

        if (raw is not IReadOnlyDictionary<string, object?> map)
        {
            errors.Add("'retry_after' must be a map { header, kind } (kind: seconds, ms or epoch)");
            return null;
        }

        var before = errors.Count;
        foreach (var key in map.Keys.Where(k => k is not ("header" or "kind")))
        {
            errors.Add($"retry_after: unknown key '{key}' (accepted: header, kind)");
        }

        var header = map.TryGetValue("header", out var h) ? YamlScalarText.Of(h) : null;
        if (string.IsNullOrEmpty(header) || YamlScalarText.NonScalarKind(h) is not null || !HttpHeaderNames.IsToken(header))
        {
            errors.Add($"retry_after.header must be a header name, got '{header}'");
        }

        var kindText = map.TryGetValue("kind", out var k) ? YamlScalarText.Of(k) : null;
        RetryAfterKind? kind = kindText switch
        {
            "seconds" => RetryAfterKind.Seconds,
            "ms" => RetryAfterKind.Milliseconds,
            "epoch" => RetryAfterKind.Epoch,
            _ => null,
        };
        if (kind is null)
        {
            errors.Add(kindText is null
                ? "retry_after.kind is required (seconds, ms or epoch)"
                : $"retry_after.kind must be seconds, ms or epoch, got '{kindText}'");
        }

        return errors.Count > before ? null : new HttpRetryAfterHeader(header!, kind!.Value);
    }
}
