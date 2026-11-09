using Pz.Connector.Http;

namespace Pz.Connector.Http.Tests;

public class WatermarkFormatTests
{
    private static readonly DateTimeOffset Exact = new(2026, 11, 9, 12, 34, 56, TimeSpan.Zero);
    private static readonly DateTimeOffset Fractional = Exact.AddTicks(1_234_567); // .1234567

    private static WatermarkFormat Format(string text)
    {
        var format = WatermarkFormat.Parse(text, out var error);
        Assert.Null(error);
        return format!;
    }

    [Theory]
    [InlineData("iso8601", "2026-11-09T12:34:56Z", "2026-11-09T12:34:57Z")]
    [InlineData("epoch_s", "1794227696", "1794227697")]
    [InlineData("epoch_ms", "1794227696123", "1794227696124")]
    [InlineData("date", "2026-11-09", "2026-11-10")]
    [InlineData("yyyy-MM-dd HH:mm:ss", "2026-11-09 12:34:56", "2026-11-09 12:34:57")]
    [InlineData("yyyy-MM-dd'T'HH:mm", "2026-11-09T12:34", "2026-11-09T12:35")]
    [InlineData("yyyyMMddHH", "2026110912", "2026110913")]
    [InlineData("yyyy-MM", "2026-11", "2026-12")]
    [InlineData("yyyy", "2026", "2027")]
    [InlineData("yyyy-MM-ddTHH:mm:ss.fffZ", "2026-11-09T12:34:56.123Z", "2026-11-09T12:34:56.124Z")]
    [InlineData("dd/MM/yyyy", "09/11/2026", "10/11/2026")]
    public void Lower_floors_and_upper_rounds_up_per_unit(string format, string lower, string upper)
    {
        Assert.Equal(lower, Format(format).Render(Fractional, upper: false));
        Assert.Equal(upper, Format(format).Render(Fractional, upper: true));
    }

    [Theory]
    [InlineData("iso8601", "2026-11-09T12:34:56Z")]
    [InlineData("epoch_s", "1794227696")]
    [InlineData("epoch_ms", "1794227696000")]
    [InlineData("yyyy-MM-dd HH:mm:ss.fff", "2026-11-09 12:34:56.000")]
    public void An_upper_bound_on_the_unit_is_not_rounded(string format, string expected)
        => Assert.Equal(expected, Format(format).Render(Exact, upper: true));

    [Fact]
    public void Upper_rounding_carries_across_a_year_boundary()
    {
        var lastSecond = new DateTimeOffset(2026, 12, 31, 23, 59, 59, TimeSpan.Zero).AddTicks(1);
        Assert.Equal("2027-01-01", Format("date").Render(lastSecond, upper: true));
        Assert.Equal("2027-01", Format("yyyy-MM").Render(lastSecond, upper: true));
    }

    [Fact]
    public void Output_is_utc_whatever_the_offset()
        => Assert.Equal("2026-11-09 10:34:56",
            Format("yyyy-MM-dd HH:mm:ss").Render(new DateTimeOffset(2026, 11, 9, 12, 34, 56, TimeSpan.FromHours(2)), false));

    [Fact]
    public void Separators_never_become_culture_separators()
        => Assert.Equal("2026/11/09 12:34", Format("yyyy/MM/dd HH:mm").Render(Exact, false));

    [Fact]
    public void Epoch_formats_are_numeric_and_others_are_not()
    {
        Assert.True(Format("epoch_s").IsNumeric);
        Assert.True(Format("epoch_ms").IsNumeric);
        Assert.False(Format("iso8601").IsNumeric);
        Assert.False(Format("yyyyMMdd").IsNumeric);
    }

    [Theory]
    [InlineData("date", true)]
    [InlineData("yyyyMMdd", true)]
    [InlineData("yyyy-MM", true)]
    [InlineData("iso8601", false)]
    [InlineData("epoch_s", false)]
    [InlineData("yyyy-MM-dd HH", false)]
    public void Date_only_formats(string format, bool dateOnly) => Assert.Equal(dateOnly, Format(format).IsDateOnly);

    [Theory]
    [InlineData("", "empty")]
    [InlineData("HH:mm", "skips the year")]
    [InlineData("yyyy-MM-dd mm", "skips the hour")]
    [InlineData("yyyy-dd", "skips the month")]
    [InlineData("yyyy-MMM-dd", "'MMM'")]
    [InlineData("yyyy-MM-ddd", "'ddd'")]
    [InlineData("yyy", "'yyy'")]
    [InlineData("yyyy-MM-dd hh:mm", "uses 'h'")]
    [InlineData("yyyy-MM-ddTHH:mm:sszzz", "uses 'z'")]
    [InlineData("yyyy-MM-dd'T", "unclosed quote")]
    [InlineData("yyyy-MM-dd yyyy", "twice")]
    [InlineData("'literal only'", "renders no date or time")]
    [InlineData("epoch_us", "uses 'e'")]
    public void Bad_formats_say_why(string format, string reason)
    {
        Assert.Null(WatermarkFormat.Parse(format, out var error));
        Assert.Contains(reason, error, StringComparison.Ordinal);
        Assert.Contains("watermark_format", error, StringComparison.Ordinal);
    }

    // ---- quoted text, overflow ----

    [Theory]
    [InlineData("yyyy'a\\'")]
    [InlineData("yyyy'\\d'-MM")]
    [InlineData("yyyy'say \"x\"'")]
    public void Backslash_or_double_quote_in_quoted_text_is_refused(string format)
    {
        Assert.Null(WatermarkFormat.Parse(format, out var error));
        Assert.Contains("backslash or a double quote", error, StringComparison.Ordinal);
    }

    [Fact]
    public void Quoted_text_renders_literally_even_with_format_letters()
        => Assert.Equal("2026-11-09 at %hh:tt day",
            Format("yyyy-MM-dd' at %hh:tt day'").Render(Exact, upper: false));

    [Theory]
    [InlineData("yyyy")]
    [InlineData("date")]
    [InlineData("epoch_s")]
    [InlineData("yyyy-MM-dd HH:mm")]
    public void Rounding_up_past_year_9999_is_a_permanent_error(string format)
    {
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(
            () => Format(format).Render(DateTimeOffset.MaxValue.AddTicks(-1), upper: true));
        Assert.False(ex.IsTransient);
        Assert.Contains("9999-12-31", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void The_last_instant_still_renders_as_a_lower_bound()
        => Assert.Equal("9999", Format("yyyy").Render(DateTimeOffset.MaxValue, upper: false));

    // ---- time zones ----

    private static TimeZoneInfo Zone(string id) => TimeZoneInfo.FindSystemTimeZoneById(id);

    private static DateTimeOffset Utc(string text) => DateTimeOffset.Parse(text + "Z", System.Globalization.CultureInfo.InvariantCulture);

    [Fact]
    public void A_zone_renders_and_floors_the_local_wall_time()
    {
        var ny = Zone("America/New_York");
        var t = Utc("2026-11-09T02:00:00.5"); // 21:00:00.5 EST on the 8th
        Assert.Equal("2026-11-08", Format("date").Render(t, false, ny));
        Assert.Equal("2026-11-09", Format("date").Render(t, true, ny));
        Assert.Equal("2026-11-08 21:00:00", Format("yyyy-MM-dd HH:mm:ss").Render(t, false, ny));
        Assert.Equal("2026-11-08 21:00:01", Format("yyyy-MM-dd HH:mm:ss").Render(t, true, ny));
    }

    [Fact]
    public void A_zone_east_of_utc_moves_the_date_forward()
        => Assert.Equal("2026-11-09", Format("date").Render(Utc("2026-11-08T23:30:00"), false, Zone("Europe/Bucharest")));

    [Theory]
    // Fall back: 01:00-02:00 happens twice. A lower bound whose later reading is after the value steps
    // back before the repeated hour; one whose readings are both at or before the value stays.
    [InlineData("2026-11-01T05:30:00", false, "yyyy-MM-dd HH", "2026-10-31 22")]
    [InlineData("2026-11-01T06:30:00", false, "yyyy-MM-dd HH", "2026-11-01 01")]
    [InlineData("2026-11-01T05:30:00", true, "yyyy-MM-dd HH", "2026-11-01 02")]
    // An upper bound whose earlier reading is before the value steps past the repeated hour.
    [InlineData("2026-11-01T06:30:30", true, "yyyy-MM-dd HH:mm", "2026-11-01 04:31")]
    // Spring forward: 02:00-03:00 does not exist; an upper bound there steps past the gap.
    [InlineData("2026-03-08T06:30:00", true, "yyyy-MM-dd HH", "2026-03-08 05")]
    [InlineData("2026-03-08T06:30:00", false, "yyyy-MM-dd HH", "2026-03-08 01")]
    public void Dst_transitions_never_narrow_the_window(string utc, bool upper, string format, string expected)
        => Assert.Equal(expected, Format(format).Render(Utc(utc), upper, Zone("America/New_York")));

    [Fact]
    public void A_lower_bound_floored_into_a_half_hour_gap_steps_back_before_it()
        // Lord Howe skips 02:00-02:30 on 2026-10-04; 02:45 floors to the missing 02:00.
        => Assert.Equal("2026-10-03 23",
            Format("yyyy-MM-dd HH").Render(Utc("2026-10-03T15:45:00"), false, Zone("Australia/Lord_Howe")));

    [Fact]
    public void Absolute_formats_ignore_a_zone_and_utc_markers_are_detected()
    {
        Assert.True(Format("iso8601").IsAbsolute);
        Assert.True(Format("epoch_ms").IsAbsolute);
        Assert.False(Format("date").IsAbsolute);
        Assert.False(Format("yyyy-MM-dd HH:mm:ss").IsAbsolute);
        Assert.True(Format("yyyy-MM-ddTHH:mm:ssZ").HasUtcMarker);
        Assert.True(Format("yyyy-MM-dd HH:mm' UTC'").HasUtcMarker);
        Assert.False(Format("yyyy-MM-dd HH:mm").HasUtcMarker);
    }

    // Apia skipped 2011-12-30 entirely (UTC-10 to UTC+14). A bound in the skipped day needs more than four
    // 3-hour steps to reach a wall time that exists; one that stays unsafe must fail, never be returned.
    [Fact]
    public void A_skipped_day_steps_until_the_bound_is_safe()
        => Assert.Equal("2011-12-31 00",
            Format("yyyy-MM-dd HH").Render(Utc("2011-12-30T09:30:00"), true, Zone("Pacific/Apia")));

    [Fact]
    public void A_bound_still_unsafe_after_the_last_pass_is_a_permanent_error()
    {
        var ex = Assert.Throws<Pz.Connectors.Abstractions.PzConnectorException>(() =>
            Format("yyyy-MM-dd HH").Render(Utc("2011-12-30T09:30:00"), true, Zone("Pacific/Apia"), maxPasses: 1));
        Assert.False(ex.IsTransient);
        Assert.Contains("watermark_time_zone", ex.Message, StringComparison.Ordinal);
    }

    // Santiago starts DST at midnight: 2026-09-06 00:00 does not exist, so a date bound steps a whole day.
    [Fact]
    public void A_date_format_in_a_zone_that_skips_midnight_stays_safe()
    {
        var santiago = Zone("America/Santiago");
        Assert.Equal("2026-09-05", Format("date").Render(Utc("2026-09-06T14:00:00"), false, santiago));
        Assert.Equal("2026-09-07", Format("date").Render(Utc("2026-09-06T02:00:00"), true, santiago));
    }
}
