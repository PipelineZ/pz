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
    public static WatermarkFormat Iso8601 { get; } = new("iso8601", Kind.Custom, IsoPattern, Unit.Second, 0);

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

                pattern.Append(text, i, end - i + 1);
                i = end + 1;
                continue;
            }

            if (c is '-' or '_' or '/' or '.' or ':' or ',' or ' ' or 'T' or 'Z')
            {
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

        return new WatermarkFormat(text, Kind.Custom, pattern.ToString(), finest, fractionDigits);
    }

    /// <summary>Renders <paramref name="value"/> (a stored watermark or window bound, already parsed as
    /// UTC) as the lower bound, or as the upper bound when <paramref name="upper"/>.</summary>
    public string Render(DateTimeOffset value, bool upper)
    {
        var utc = value.ToUniversalTime();
        var floored = Floor(utc);
        var bound = upper && utc > floored ? Next(floored) : floored;
        return _kind switch
        {
            Kind.EpochSeconds => bound.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture),
            Kind.EpochMilliseconds => bound.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture),
            _ => bound.ToString(_pattern.Length == 1 ? "%" + _pattern : _pattern, CultureInfo.InvariantCulture),
        };
    }

    private DateTimeOffset Floor(DateTimeOffset t) => _unit switch
    {
        Unit.Year => new DateTimeOffset(t.Year, 1, 1, 0, 0, 0, TimeSpan.Zero),
        Unit.Month => new DateTimeOffset(t.Year, t.Month, 1, 0, 0, 0, TimeSpan.Zero),
        Unit.Day => new DateTimeOffset(t.Year, t.Month, t.Day, 0, 0, 0, TimeSpan.Zero),
        Unit.Hour => new DateTimeOffset(t.Year, t.Month, t.Day, t.Hour, 0, 0, TimeSpan.Zero),
        Unit.Minute => new DateTimeOffset(t.Year, t.Month, t.Day, t.Hour, t.Minute, 0, TimeSpan.Zero),
        Unit.Second => new DateTimeOffset(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, TimeSpan.Zero),
        _ => new DateTimeOffset(t.UtcTicks - t.UtcTicks % FractionTicks, TimeSpan.Zero),
    };

    private DateTimeOffset Next(DateTimeOffset floored) => _unit switch
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
