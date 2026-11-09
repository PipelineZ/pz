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
}
