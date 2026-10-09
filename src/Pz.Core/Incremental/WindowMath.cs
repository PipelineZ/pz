using System.Globalization;
using System.Numerics;
using Pz.Core.Loading;

namespace Pz.Core.Incremental;

/// <summary>Pure arithmetic over the CANONICAL watermark string forms the engine stores
/// (int/bigint/decimal = invariant digits, date = yyyy-MM-dd, timestamp =
/// yyyy-MM-ddTHH:mm:ss.ffffff — see DatasetSpec.WatermarkValue's doc). Lives in Pz.Core (not Pz.Engine)
/// because DagCompiler's compile-time window validation needs the same rules and layering is strictly
/// downward. No clock, no I/O — window bounds are a pure function of (type, lower, window, until).
/// typeName values are the AllowedCursorTypes set; an unknown type throws ArgumentOutOfRangeException
/// (callers validate first — reaching that throw is an engine bug, not a config error).</summary>
public static class WindowMath
{
    private const string TimestampFormat = "yyyy-MM-ddTHH:mm:ss.ffffff";
    private const string DateFormat = "yyyy-MM-dd";

    /// <summary>True when this cursor type has arithmetic here. Callers reading a type out of
    /// `watermarks.json` MUST check this first: <see cref="TryCanonicalize"/> and <see cref="Compare"/>
    /// both end in <see cref="ArgumentOutOfRangeException"/> for anything else, which is an engine bug for
    /// the compile-time callers that validate up front — but a plain fact of life for `pz state`, whose
    /// input is a file a human may have hand-edited.</summary>
    public static bool IsKnownType(string typeName) =>
        typeName is "int" or "bigint" or "decimal" or "date" or "timestamp";

    public static bool TryCanonicalize(string typeName, string raw, out string canonical)
    {
        canonical = "";
        switch (typeName)
        {
            case "int" or "bigint":
                if (!BigInteger.TryParse(raw, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var i))
                {
                    return false;
                }

                canonical = i.ToString(CultureInfo.InvariantCulture);
                return true;
            case "decimal":
                // Strict parse (decimal point + optional leading sign only — mirrors int/bigint's rigor:
                // no thousands separators, no whitespace), then RE-SERIALIZE so canonical is always the
                // invariant form. decimal.ToString preserves scale (12.50 stays "12.50", never "12.5")
                // and strips a leading '+', so canonicalized values compare and round-trip cleanly.
                if (!decimal.TryParse(raw, NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign,
                        CultureInfo.InvariantCulture, out var dec))
                {
                    return false;
                }

                canonical = dec.ToString(CultureInfo.InvariantCulture);
                return true;
            case "date":
                if (!DateOnly.TryParseExact(raw, DateFormat, CultureInfo.InvariantCulture, DateTimeStyles.None, out var d))
                {
                    return false;
                }

                canonical = d.ToString(DateFormat, CultureInfo.InvariantCulture);
                return true;
            case "timestamp":
                // Accept the full canonical form, a seconds-precision form, or a bare date — all
                // normalized to the single canonical layout so Compare/AddWindow never re-parse variants.
                // Every pz timestamp is UTC by convention (the http connector renders watermarks with a
                // trailing Z), so a single UTC designator on a datetime form is accepted and normalized
                // away; "2020-01-01Z" is not ISO-8601 and numeric offsets stay rejected.
                var text = raw.EndsWith('Z') && raw.Contains('T') ? raw[..^1] : raw;
                string[] accepted = [TimestampFormat, "yyyy-MM-ddTHH:mm:ss", DateFormat];
                if (!DateTime.TryParseExact(text, accepted, CultureInfo.InvariantCulture, DateTimeStyles.None, out var ts))
                {
                    return false;
                }

                canonical = ts.ToString(TimestampFormat, CultureInfo.InvariantCulture);
                return true;
            default:
                throw new ArgumentOutOfRangeException(nameof(typeName), typeName, "unknown cursor type");
        }
    }

    public static bool TryValidateWindow(string typeName, string rawWindow, out string? error)
    {
        switch (typeName)
        {
            case "int" or "bigint" or "decimal":
                if (!BigInteger.TryParse(rawWindow, NumberStyles.None, CultureInfo.InvariantCulture, out var n) || n <= 0)
                {
                    error = $"a numeric cursor's max_window must be a positive integer delta (got '{rawWindow}')";
                    return false;
                }

                error = null;
                return true;
            case "date":
                if (!DurationParser.TryParse(rawWindow, out var dateSpan) || dateSpan <= TimeSpan.Zero)
                {
                    error = $"a date cursor's max_window must be a positive duration like 1d or 7d (got '{rawWindow}')";
                    return false;
                }

                if (dateSpan.Ticks % TimeSpan.TicksPerDay != 0)
                {
                    error = $"a date cursor's max_window must be whole days (got '{rawWindow}')";
                    return false;
                }

                error = null;
                return true;
            case "timestamp":
                if (!DurationParser.TryParse(rawWindow, out var span) || span <= TimeSpan.Zero)
                {
                    error = $"a timestamp cursor's max_window must be a positive duration like 500ms, 90s, 6h, or 1d (got '{rawWindow}')";
                    return false;
                }

                error = null;
                return true;
            default:
                throw new ArgumentOutOfRangeException(nameof(typeName), typeName, "unknown cursor type");
        }
    }

    public static string AddWindow(string typeName, string canonicalLower, string rawWindow)
    {
        switch (typeName)
        {
            case "int" or "bigint":
                return (BigInteger.Parse(canonicalLower, CultureInfo.InvariantCulture)
                    + BigInteger.Parse(rawWindow, CultureInfo.InvariantCulture)).ToString(CultureInfo.InvariantCulture);
            case "decimal":
                return (decimal.Parse(canonicalLower, CultureInfo.InvariantCulture)
                    + decimal.Parse(rawWindow, CultureInfo.InvariantCulture)).ToString(CultureInfo.InvariantCulture);
            case "date":
            {
                DurationParser.TryParse(rawWindow, out var span);
                return DateOnly.ParseExact(canonicalLower, DateFormat, CultureInfo.InvariantCulture)
                    .AddDays((int)(span.Ticks / TimeSpan.TicksPerDay))
                    .ToString(DateFormat, CultureInfo.InvariantCulture);
            }
            case "timestamp":
            {
                DurationParser.TryParse(rawWindow, out var span);
                return DateTime.ParseExact(canonicalLower, TimestampFormat, CultureInfo.InvariantCulture)
                    .Add(span)
                    .ToString(TimestampFormat, CultureInfo.InvariantCulture);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(typeName), typeName, "unknown cursor type");
        }
    }

    public static int Compare(string typeName, string canonicalA, string canonicalB) => typeName switch
    {
        "int" or "bigint" => BigInteger.Parse(canonicalA, CultureInfo.InvariantCulture)
            .CompareTo(BigInteger.Parse(canonicalB, CultureInfo.InvariantCulture)),
        "decimal" => decimal.Parse(canonicalA, CultureInfo.InvariantCulture)
            .CompareTo(decimal.Parse(canonicalB, CultureInfo.InvariantCulture)),
        "date" => DateOnly.ParseExact(canonicalA, DateFormat, CultureInfo.InvariantCulture)
            .CompareTo(DateOnly.ParseExact(canonicalB, DateFormat, CultureInfo.InvariantCulture)),
        "timestamp" => DateTime.ParseExact(canonicalA, TimestampFormat, CultureInfo.InvariantCulture)
            .CompareTo(DateTime.ParseExact(canonicalB, TimestampFormat, CultureInfo.InvariantCulture)),
        _ => throw new ArgumentOutOfRangeException(nameof(typeName), typeName, "unknown cursor type"),
    };

    public static string Min(string typeName, string canonicalA, string canonicalB) =>
        Compare(typeName, canonicalA, canonicalB) <= 0 ? canonicalA : canonicalB;

    /// <summary>The <c>until</c> value that stops at the moment the run started, for a job that keeps a windowed
    /// source current rather than backfilling to a fixed point.</summary>
    public const string UntilNow = "now";

    /// <summary>Only a time can be "now": a numeric cursor has no relation to the clock.</summary>
    public static bool SupportsNow(string typeName) => typeName is "date" or "timestamp";

    /// <summary>One run's slice. <see cref="Upper"/> is <c>lower + max_window</c> clamped to the stop;
    /// <see cref="UpperInclusive"/> is false only for the last window of <c>until: now</c>, which stops just before
    /// the run started so a date cursor never loads half of today. <see cref="Empty"/> means the watermark is
    /// already at or past the stop. <see cref="ReachesStop"/> says whether this slice reaches the stop (the source is
    /// caught up once it loads), and is null when there is no stop at all.</summary>
    public sealed record Window(string Upper, bool UpperInclusive, bool Empty, bool? ReachesStop, string? Stop);

    /// <summary>Computes the slice a windowed run loads from <paramref name="canonicalLower"/>.
    /// <paramref name="until"/> is canonical, <see cref="UntilNow"/>, or null; <paramref name="runStartedAt"/>
    /// resolves <c>now</c> (UTC; its date for a date cursor).</summary>
    public static Window ComputeWindow(string typeName, string canonicalLower, string rawWindow, string? until,
        DateTimeOffset runStartedAt)
    {
        var stop = until;
        var stopExclusive = false;
        if (string.Equals(until, UntilNow, StringComparison.Ordinal))
        {
            stop = typeName switch
            {
                "date" => DateOnly.FromDateTime(runStartedAt.UtcDateTime).ToString(DateFormat, CultureInfo.InvariantCulture),
                "timestamp" => runStartedAt.UtcDateTime.ToString(TimestampFormat, CultureInfo.InvariantCulture),
                _ => throw new ArgumentException($"'until: now' needs a date or timestamp cursor, not '{typeName}'", nameof(until)),
            };
            stopExclusive = true;
        }

        var upper = AddWindow(typeName, canonicalLower, rawWindow);
        if (stop is not null)
        {
            upper = Min(typeName, upper, stop);
        }

        var reachesStop = stop is null ? (bool?)null : Compare(typeName, upper, stop) >= 0;
        return new Window(upper, UpperInclusive: !(stopExclusive && reachesStop == true),
            Empty: Compare(typeName, upper, canonicalLower) <= 0, reachesStop, stop);
    }
}
