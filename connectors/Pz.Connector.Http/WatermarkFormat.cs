using System.Globalization;
using System.Text;

namespace Pz.Connector.Http;

/// <summary>The `watermark_format` dataset option: how a timestamp or date cursor renders in
/// `{{ watermark }}` and `{{ window_upper }}`. Every format has a unit; the lower bound (exclusive) is
/// floored to it, so at most the boundary unit is re-read, and the upper bound (inclusive) is rounded up
/// to the next unit when it has a remainder, so the (lower, upper] window is never under-covered. Output
/// is always UTC, in the invariant culture.</summary>
internal sealed class WatermarkFormat
{
    private enum Kind { Custom, EpochSeconds, EpochMilliseconds }

    /// <summary>Calendar units, coarsest first; a custom format's letters must be a contiguous run from
    /// the first.</summary>
    private enum Unit { Year, Month, Day, Hour, Minute, Second, Fraction }

    private const string IsoPattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    /// <summary>Today's rendering: ISO-8601 UTC to whole seconds.</summary>
    public static WatermarkFormat Iso8601 { get; } = new("iso8601", Kind.Custom, IsoPattern, Unit.Second, 0)
    {
        HasUtcMarker = true,
    };

    private readonly Kind _kind;
    private readonly string _pattern;
    private readonly Unit _unit;
    private readonly int _fractionDigits;

    private WatermarkFormat(string name, Kind kind, string pattern, Unit unit, int fractionDigits)
    {
        Name = name;
        _kind = kind;
        _pattern = pattern;
        _unit = unit;
        _fractionDigits = fractionDigits;
    }

    /// <summary>The value as written, for messages.</summary>
    public string Name { get; }

    /// <summary>True for the epoch formats: the rendered value is an integer, so a whole-value JSON body
    /// binding sends it as a JSON number.</summary>
    public bool IsNumeric => _kind != Kind.Custom;

    /// <summary>True for `iso8601` and the epoch formats, which name an instant on their own, so a time
    /// zone has nothing to apply to.</summary>
    public bool IsAbsolute => _kind != Kind.Custom || ReferenceEquals(this, Iso8601);

    /// <summary>True when the format writes a UTC marker (a literal `Z`, or quoted text holding `Z` or
    /// `UTC`): its wall time must then be UTC, so no other time zone may apply.</summary>
    public bool HasUtcMarker { get; private init; }

    /// <summary>True when the format renders no time of day, so it suits a `date` cursor.</summary>
    public bool IsDateOnly => _kind == Kind.Custom && _unit <= Unit.Day;

    /// <summary>The format for <paramref name="text"/>, or null with <paramref name="error"/> set.</summary>
    public static WatermarkFormat? Parse(string text, out string? error)
    {
        error = null;
        switch (text)
        {
            case "iso8601":
                return Iso8601;
            case "epoch_s":
                return new WatermarkFormat(text, Kind.EpochSeconds, "", Unit.Second, 0);
            case "epoch_ms":
                return new WatermarkFormat(text, Kind.EpochMilliseconds, "", Unit.Fraction, 3);
            case "date":
                return new WatermarkFormat(text, Kind.Custom, "yyyy-MM-dd", Unit.Day, 0);
        }

        return ParseCustom(text, out error);
    }

    private static WatermarkFormat? ParseCustom(string text, out string? error)
    {
        const string Accepted = "iso8601, epoch_s, epoch_ms, date, or a custom format such as 'yyyy-MM-dd HH:mm:ss'";
        error = null;
        if (text.Length == 0)
        {
            error = $"'watermark_format' is empty; use {Accepted}";
            return null;
        }

        var seen = new HashSet<Unit>();
        var utcMarker = false;
        var fractionDigits = 0;
        var pattern = new StringBuilder();
        for (var i = 0; i < text.Length;)
        {
            var c = text[i];
            if (c == '\'')
            {
                var end = text.IndexOf('\'', i + 1);
                if (end < 0)
                {
                    error = $"'watermark_format' '{text}' has an unclosed quote";
                    return null;
                }

                // .NET reads a backslash inside quoted text as an escape (a trailing one swallows the closing
                // quote and the render throws); a double quote opens another literal.
                var quoted = text[(i + 1)..end];
                if (quoted.IndexOfAny(['\\', '"']) >= 0)
                {
                    error = $"'watermark_format' '{text}' has a backslash or a double quote inside quoted text";
                    return null;
                }

                utcMarker |= quoted.Contains('Z') || quoted.Contains("UTC", StringComparison.OrdinalIgnoreCase);
                pattern.Append(text, i, end - i + 1);
                i = end + 1;
                continue;
            }

            if (c is '-' or '_' or '/' or '.' or ':' or ',' or ' ' or 'T' or 'Z')
            {
                utcMarker |= c == 'Z';
                // Quoted, so '/' and ':' stay literal instead of becoming culture separators.
                pattern.Append('\'').Append(c).Append('\'');
                i++;
                continue;
            }

            var run = 1;
            while (i + run < text.Length && text[i + run] == c)
            {
                run++;
            }

            (Unit Unit, int[] Runs)? letter = c switch
            {
                'y' => (Unit.Year, [2, 4]),
                'M' => (Unit.Month, [1, 2]),
                'd' => (Unit.Day, [1, 2]),
                'H' => (Unit.Hour, [1, 2]),
                'm' => (Unit.Minute, [1, 2]),
                's' => (Unit.Second, [1, 2]),
                'f' => (Unit.Fraction, [1, 2, 3, 4, 5, 6, 7]),
                _ => null,
            };
            if (letter is null)
            {
                error = $"'watermark_format' '{text}' uses '{c}'; a custom format may only use the letters " +
                    "y M d H m s f, the separators - _ / . : , and space, the literals T and Z, and 'quoted text'";
                return null;
            }

            if (!letter.Value.Runs.Contains(run))
            {
                error = $"'watermark_format' '{text}' has '{new string(c, run)}'; '{c}' may appear " +
                    $"{string.Join(" or ", letter.Value.Runs.Select(n => new string(c, n)))} times in a row";
                return null;
            }

            if (!seen.Add(letter.Value.Unit))
            {
                error = $"'watermark_format' '{text}' uses '{c}' twice";
                return null;
            }

            if (c == 'f')
            {
                fractionDigits = run;
            }

            pattern.Append(text, i, run);
            i += run;
        }

        if (seen.Count == 0)
        {
            error = $"'watermark_format' '{text}' renders no date or time; use {Accepted}";
            return null;
        }

        // A gap (no hour between day and minute, no year at all) leaves a value that names no instant,
        // and no unit to floor or round up to.
        var finest = seen.Max();
        for (var unit = Unit.Year; unit <= finest; unit++)
        {
            if (!seen.Contains(unit))
            {
                error = $"'watermark_format' '{text}' skips the {unit.ToString().ToLowerInvariant()}: a custom " +
                    "format must give every unit from the year down to its finest one";
                return null;
            }
        }

        return new WatermarkFormat(text, Kind.Custom, pattern.ToString(), finest, fractionDigits)
        {
            HasUtcMarker = utcMarker,
        };
    }

    /// <summary>The largest wall-clock shift a time-zone transition makes (Troll in Antarctica moves 2 h).
    /// A bound that lands in a skipped or repeated hour is moved this far past the transition before it is
    /// floored or rounded up again, so the API reads it as an instant on the safe side whichever way it
    /// resolves the local time.</summary>
    private static readonly TimeSpan MaxTransitionShift = TimeSpan.FromHours(3);

    /// <summary>Renders <paramref name="value"/> (a stored watermark or window bound) as the lower bound,
    /// or as the upper bound when <paramref name="upper"/>. A custom or `date` format renders the wall time
    /// in <paramref name="zone"/> (UTC when null) and floors or rounds up there. When the floored lower
    /// bound is a local time the zone skips or repeats, the earlier safe wall time is used, and the later
    /// one for the upper bound, so a DST transition never narrows the window. Throws a permanent
    /// <see cref="Pz.Connectors.Abstractions.PzConnectorException"/> (without the value) when rounding up
    /// would pass year 9999.</summary>
    public string Render(DateTimeOffset value, bool upper, TimeZoneInfo? zone = null)
    {
        var utc = value.ToUniversalTime();
        if (_kind != Kind.Custom || zone is null || zone == TimeZoneInfo.Utc)
        {
            var wall = Bound(utc.DateTime, upper);
            return Format(wall);
        }

        var local = TimeZoneInfo.ConvertTimeFromUtc(utc.UtcDateTime, zone);
        var bound = Bound(local, upper);
        for (var i = 0; i < 4 && !IsSafe(bound, utc, upper, zone); i++)
        {
            bound = upper
                ? Bound(Shift(bound, MaxTransitionShift), upper: true)
                : Bound(Shift(bound, -MaxTransitionShift), upper: false);
        }

        return Format(bound);
    }

    private string Format(DateTime wall) => _kind switch
    {
        Kind.EpochSeconds => new DateTimeOffset(wall, TimeSpan.Zero).ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture),
        Kind.EpochMilliseconds => new DateTimeOffset(wall, TimeSpan.Zero).ToUnixTimeMilliseconds()
            .ToString(CultureInfo.InvariantCulture),
        _ => wall.ToString(_pattern.Length == 1 ? "%" + _pattern : _pattern, CultureInfo.InvariantCulture),
    };

    /// <summary>A lower bound is safe when every instant the zone maps it to is at or before the value; an
    /// upper bound when every one is at or after it. A skipped local time maps to no instant, so it is
    /// never safe: the API might resolve it either way.</summary>
    private static bool IsSafe(DateTime wall, DateTimeOffset value, bool upper, TimeZoneInfo zone)
    {
        wall = DateTime.SpecifyKind(wall, DateTimeKind.Unspecified);
        if (zone.IsInvalidTime(wall))
        {
            return false;
        }

        var offsets = zone.IsAmbiguousTime(wall) ? zone.GetAmbiguousTimeOffsets(wall) : [zone.GetUtcOffset(wall)];
        var instants = offsets.Select(o => new DateTimeOffset(wall, o));
        return upper ? instants.All(i => i >= value) : instants.All(i => i <= value);
    }

    private static DateTime Shift(DateTime wall, TimeSpan by)
    {
        try
        {
            return wall.Add(by);
        }
        catch (ArgumentOutOfRangeException)
        {
            throw Overflow();
        }
    }

    private DateTime Bound(DateTime wall, bool upper)
    {
        var floored = Floor(wall);
        if (!upper || wall <= floored)
        {
            return floored;
        }

        try
        {
            return Next(floored);
        }
        catch (ArgumentOutOfRangeException)
        {
            throw Overflow();
        }
    }

    private static Pz.Connectors.Abstractions.PzConnectorException Overflow() => new(
        "'watermark_format' cannot round the window's upper bound up: it would pass 9999-12-31; declare an " +
        "earlier 'until:'", isTransient: false);

    private DateTime Floor(DateTime t) => _unit switch
    {
        Unit.Year => new DateTime(t.Year, 1, 1),
        Unit.Month => new DateTime(t.Year, t.Month, 1),
        Unit.Day => t.Date,
        Unit.Hour => new DateTime(t.Year, t.Month, t.Day, t.Hour, 0, 0),
        Unit.Minute => new DateTime(t.Year, t.Month, t.Day, t.Hour, t.Minute, 0),
        Unit.Second => new DateTime(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second),
        _ => new DateTime(t.Ticks - t.Ticks % FractionTicks),
    };

    private DateTime Next(DateTime floored) => _unit switch
    {
        Unit.Year => floored.AddYears(1),
        Unit.Month => floored.AddMonths(1),
        Unit.Day => floored.AddDays(1),
        Unit.Hour => floored.AddHours(1),
        Unit.Minute => floored.AddMinutes(1),
        Unit.Second => floored.AddSeconds(1),
        _ => floored.AddTicks(FractionTicks),
    };

    private long FractionTicks => (long)Math.Pow(10, 7 - _fractionDigits);
}
