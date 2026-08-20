using System.Text.RegularExpressions;

namespace Pz.Connectors.Abstractions.Paths;

/// <summary>Calendar-token path templating shared by every file connector (AzureBlob, LocalFiles, S3).
/// Pure and dependency-free (BCL only) so it lives in the ABI without touching the Apache.Arrow-only
/// dependency allowlist.</summary>
public static partial class PathTemplate
{
    private static readonly (string Token, DateGranularity Level)[] Tokens =
    [
        ("{yyyy}", DateGranularity.Year), ("{yy}", DateGranularity.Year),
        ("{MM}", DateGranularity.Month), ("{dd}", DateGranularity.Day),
        ("{HH}", DateGranularity.Hour), ("{mm}", DateGranularity.Minute),
    ];

    [GeneratedRegex(@"\{[A-Za-z]+\}")]
    private static partial Regex AnyToken();

    // The http sink's merge-key substitution token (`mode: merge` requires it in `path:`; the
    // row's key value is substituted per request). Reserved here so calendar-token validation
    // never claims it — exact-case `{key}` only; `{Key}`/`{keys}` still reject as unknown.
    private const string MergeKeyToken = "{key}";

    public static bool HasDateTokens(string pattern) =>
        AnyToken().Matches(pattern).Any(m => m.Value != MergeKeyToken);

    /// <summary>Validates that tokens appear coarse→fine, contiguous (no gap), and are all known.
    /// Returns the finest granularity present. Throws a permanent PzConnectorException otherwise.</summary>
    public static DateGranularity Validate(string pattern, string subject)
    {
        var seen = new List<DateGranularity>();
        foreach (Match m in AnyToken().Matches(pattern))
        {
            if (m.Value == MergeKeyToken)
            {
                continue;
            }

            var match = Tokens.FirstOrDefault(t => t.Token == m.Value);
            if (match.Token is null)
            {
                throw new PzConnectorException(
                    $"{subject}: unknown path token '{m.Value}' (allowed: {{yyyy}} {{yy}} {{MM}} {{dd}} {{HH}} {{mm}})",
                    isTransient: false);
            }
            seen.Add(match.Level);
        }

        if (seen.Count == 0)
        {
            throw new PzConnectorException($"{subject}: no calendar tokens in path", isTransient: false);
        }

        // Distinct levels must be exactly a coarse→fine prefix (Year, Month, …) with no gaps or reordering.
        var distinct = seen.Distinct().ToList();
        var expected = Enum.GetValues<DateGranularity>().Take(distinct.Count).ToList();
        if (!distinct.SequenceEqual(expected))
        {
            throw new PzConnectorException(
                $"{subject}: path tokens must be contiguous coarse→fine ({{yyyy}}→{{MM}}→{{dd}}→{{HH}}→{{mm}}), got [{string.Join(", ", distinct)}]",
                isTransient: false);
        }

        return distinct[^1];
    }

    private static readonly string[] CanonicalFormats =
    [
        "yyyy-MM-ddTHH:mm:ss.ffffff", "yyyy-MM-ddTHH:mm:ss", "yyyy-MM-dd",
    ];

    /// <summary>Parses a canonical watermark string (DatasetSpec.WatermarkValue form) as a UTC instant.</summary>
    public static DateTimeOffset ParseCanonical(string value)
    {
        if (DateTimeOffset.TryParseExact(value, CanonicalFormats, System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal,
                out var dt))
        {
            return dt;
        }
        throw new PzConnectorException(
            $"cannot parse watermark value '{value}' as a date/timestamp for a templated path", isTransient: false);
    }

    /// <summary>Full substitution of every token from a single instant. Tokens finer than the pattern's
    /// granularity are still substituted with their instant value (this is the exact-render form used
    /// write-side to compute one row's destination folder).</summary>
    public static string Render(string pattern, DateTimeOffset instant)
    {
        var s = pattern;
        s = s.Replace("{yyyy}", instant.Year.ToString("D4", System.Globalization.CultureInfo.InvariantCulture));
        s = s.Replace("{yy}", (instant.Year % 100).ToString("D2", System.Globalization.CultureInfo.InvariantCulture));
        s = s.Replace("{MM}", instant.Month.ToString("D2", System.Globalization.CultureInfo.InvariantCulture));
        s = s.Replace("{dd}", instant.Day.ToString("D2", System.Globalization.CultureInfo.InvariantCulture));
        s = s.Replace("{HH}", instant.Hour.ToString("D2", System.Globalization.CultureInfo.InvariantCulture));
        s = s.Replace("{mm}", instant.Minute.ToString("D2", System.Globalization.CultureInfo.InvariantCulture));
        return s;
    }

    /// <summary>Leading run of characters before the first wildcard (<c>*</c>/<c>?</c>) or token (<c>{</c>).
    /// The server-side prefix an object store can narrow on. Lifted from AzureSource.StaticPrefix so every
    /// connector shares it.</summary>
    public static string StaticPrefix(string pattern)
    {
        var idx = pattern.IndexOfAny(['*', '?', '{']);
        return idx < 0 ? pattern : pattern[..idx];
    }

    /// <summary>Minimal aligned prefix cover of [lo, hi] (both inclusive at the pattern's finest bucket).
    /// Greedy: at each position emit the coarsest aligned bucket that fits wholly inside the window, then
    /// advance past it. Coarsening replaces finer tokens with '*' (collapsing adjacent globs).</summary>
    public static IReadOnlyList<string> WindowCover(string pattern, DateTimeOffset lo, DateTimeOffset hi)
    {
        var finest = Validate(pattern, "path");
        var cur = Floor(lo, finest);
        var lastExclusive = AddOne(Floor(hi, finest), finest); // one finest-bucket past hi's bucket
        var result = new List<string>();

        while (cur < lastExclusive)
        {
            var level = finest;
            // Try coarsest→finest; pick the coarsest that is aligned here and whose whole bucket fits.
            for (var l = DateGranularity.Year; l <= finest; l++)
            {
                if (IsAligned(cur, l) && AddOne(cur, l) <= lastExclusive)
                {
                    level = l;
                    break;
                }
            }
            result.Add(RenderCoarsened(pattern, cur, level));
            cur = AddOne(cur, level);
        }
        return result;
    }

    private static DateTimeOffset Floor(DateTimeOffset dt, DateGranularity level) => level switch
    {
        DateGranularity.Year => new(dt.Year, 1, 1, 0, 0, 0, TimeSpan.Zero),
        DateGranularity.Month => new(dt.Year, dt.Month, 1, 0, 0, 0, TimeSpan.Zero),
        DateGranularity.Day => new(dt.Year, dt.Month, dt.Day, 0, 0, 0, TimeSpan.Zero),
        DateGranularity.Hour => new(dt.Year, dt.Month, dt.Day, dt.Hour, 0, 0, TimeSpan.Zero),
        _ => new(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, 0, TimeSpan.Zero),
    };

    private static DateTimeOffset AddOne(DateTimeOffset dt, DateGranularity level) => level switch
    {
        DateGranularity.Year => dt.AddYears(1),
        DateGranularity.Month => dt.AddMonths(1),
        DateGranularity.Day => dt.AddDays(1),
        DateGranularity.Hour => dt.AddHours(1),
        _ => dt.AddMinutes(1),
    };

    private static bool IsAligned(DateTimeOffset dt, DateGranularity level) => level switch
    {
        DateGranularity.Year => dt is { Month: 1, Day: 1, Hour: 0, Minute: 0 },
        DateGranularity.Month => dt is { Day: 1, Hour: 0, Minute: 0 },
        DateGranularity.Day => dt is { Hour: 0, Minute: 0 },
        DateGranularity.Hour => dt.Minute == 0,
        _ => true,
    };

    /// <summary>Render with tokens finer than <paramref name="level"/> replaced by '*', collapsing any
    /// resulting adjacent stars within a segment (e.g. "{HH}*" at day granularity → "*").</summary>
    private static string RenderCoarsened(string pattern, DateTimeOffset instant, DateGranularity level)
    {
        string Sub(string p, string token, DateGranularity tokenLevel, string value)
            => p.Replace(token, tokenLevel <= level ? value : "*");

        var inv = System.Globalization.CultureInfo.InvariantCulture;   // determinism: byte-stable rendered paths
        var s = pattern;
        s = Sub(s, "{yyyy}", DateGranularity.Year, instant.Year.ToString("D4", inv));
        s = Sub(s, "{yy}", DateGranularity.Year, (instant.Year % 100).ToString("D2", inv));
        s = Sub(s, "{MM}", DateGranularity.Month, instant.Month.ToString("D2", inv));
        s = Sub(s, "{dd}", DateGranularity.Day, instant.Day.ToString("D2", inv));
        s = Sub(s, "{HH}", DateGranularity.Hour, instant.Hour.ToString("D2", inv));
        s = Sub(s, "{mm}", DateGranularity.Minute, instant.Minute.ToString("D2", inv));
        return CollapseStars().Replace(s, "*");   // "**" → "*", within-segment only (no '/' in the class)
    }

    [GeneratedRegex(@"\*{2,}")]
    private static partial Regex CollapseStars();
}

public enum DateGranularity { Year, Month, Day, Hour, Minute }
