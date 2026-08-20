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
}
