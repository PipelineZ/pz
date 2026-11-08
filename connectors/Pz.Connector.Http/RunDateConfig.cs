using System.Globalization;

namespace Pz.Connector.Http;

/// <summary>The `run_date` dataset option: what `{{ run_date }}` renders to. The run's date in
/// <see cref="Zone"/>, shifted by <see cref="OffsetDays"/> whole days, formatted with
/// <see cref="Format"/> in the invariant culture. The format admits only the date letters and
/// literal separators, so it cannot render times or culture-dependent names.</summary>
internal sealed record RunDateConfig(int OffsetDays, TimeZoneInfo Zone, string Format)
{
    public static RunDateConfig Default { get; } = new(0, TimeZoneInfo.Utc, "yyyy-MM-dd");

    private const string FormatChars = "yMd-_/. ";

    public static RunDateConfig Parse(object? raw, List<string> errors)
    {
        if (raw is null)
        {
            return Default;
        }

        if (raw is not IReadOnlyDictionary<string, object?> block)
        {
            errors.Add("'run_date' must be a map with optional 'offset_days', 'timezone' and 'format'");
            return Default;
        }

        string? Get(string key) => block.TryGetValue(key, out var v) ? YamlScalarText.Of(v) : null;

        var offsetDays = Default.OffsetDays;
        if (Get("offset_days") is { } offsetRaw
            && !int.TryParse(offsetRaw, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out offsetDays))
        {
            errors.Add($"'run_date.offset_days' must be an integer, got '{offsetRaw}'");
        }

        var zone = Default.Zone;
        if (Get("timezone") is { } zoneId)
        {
            zone = FindZone(zoneId, errors) ?? Default.Zone;
        }

        var format = Get("format") ?? Default.Format;
        if (format.Length == 0 || format.Any(c => !FormatChars.Contains(c)))
        {
            errors.Add("run_date.format may only use y, M, d and - _ / . separators " +
                $"(and spaces), got '{format}'");
        }

        return new RunDateConfig(offsetDays, zone, format);
    }

    private static TimeZoneInfo? FindZone(string id, List<string> errors)
    {
        try
        {
            return TimeZoneInfo.FindSystemTimeZoneById(id);
        }
        catch (Exception ex) when (ex is TimeZoneNotFoundException or InvalidTimeZoneException)
        {
            errors.Add(TimeZoneInfo.GetSystemTimeZones().Count == 0
                ? $"'run_date.timezone' '{id}' cannot be resolved: this machine has no time zone database " +
                    "(install tzdata, or omit 'timezone' to use UTC)"
                : $"'run_date.timezone' '{id}' is not a known time zone id (use an IANA id such as " +
                    "'Europe/Bucharest')");
            return null;
        }
    }

    public string Render(DateTimeOffset utcNow)
    {
        var local = TimeZoneInfo.ConvertTime(utcNow, Zone);
        var date = DateOnly.FromDateTime(local.DateTime).AddDays(OffsetDays);
        // A one-letter format ("d") would be read as a standard format specifier; '%' forces the
        // custom reading.
        return date.ToString(Format.Length == 1 ? "%" + Format : Format, CultureInfo.InvariantCulture);
    }
}
