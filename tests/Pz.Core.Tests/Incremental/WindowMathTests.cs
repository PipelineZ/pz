using Pz.Core.Incremental;

namespace Pz.Core.Tests.Incremental;

public class WindowMathTests
{
    // --- TryCanonicalize ---

    [Theory]
    [InlineData("int", "100", "100")]
    [InlineData("bigint", "9223372036854775807", "9223372036854775807")]
    [InlineData("decimal", "12.50", "12.50")]
    [InlineData("decimal", "+12.50", "12.50")]
    [InlineData("decimal", "-3.5", "-3.5")]
    [InlineData("date", "2020-01-01", "2020-01-01")]
    [InlineData("timestamp", "2020-01-01T00:00:00.000000", "2020-01-01T00:00:00.000000")]
    [InlineData("timestamp", "2020-01-01", "2020-01-01T00:00:00.000000")]        // bare date promoted
    [InlineData("timestamp", "2020-01-01T06:30:00", "2020-01-01T06:30:00.000000")] // fraction padded
    // Every pz timestamp is UTC by convention (the http connector even RENDERS watermarks with a
    // trailing Z), so the UTC designator is accepted and normalized away. Numeric offsets are not.
    [InlineData("timestamp", "2020-01-01T06:30:00Z", "2020-01-01T06:30:00.000000")]
    [InlineData("timestamp", "2020-01-01T06:30:00.000000Z", "2020-01-01T06:30:00.000000")]
    public void Canonicalizes_valid_values(string type, string raw, string expected)
    {
        Assert.True(WindowMath.TryCanonicalize(type, raw, out var canonical));
        Assert.Equal(expected, canonical);
    }

    [Theory]
    [InlineData("int", "abc")]
    [InlineData("int", "1.5")]
    [InlineData("bigint", "")]
    [InlineData("decimal", "1,234.50")]
    [InlineData("decimal", " 12.50 ")]
    [InlineData("date", "01/01/2020")]
    [InlineData("date", "2020-13-01")]
    [InlineData("timestamp", "yesterday")]
    [InlineData("timestamp", "2020-01-01T06:30:00+02:00")] // numeric offsets stay rejected
    [InlineData("timestamp", "2020-01-01Z")]               // Z on a bare date is not ISO-8601
    public void Rejects_invalid_values(string type, string raw)
    {
        Assert.False(WindowMath.TryCanonicalize(type, raw, out _));
    }

    // --- TryValidateWindow ---

    [Theory]
    [InlineData("int", "1000")]
    [InlineData("bigint", "500000")]
    [InlineData("decimal", "100")]     // integer delta on a decimal cursor is legal
    [InlineData("date", "1d")]
    [InlineData("date", "7d")]
    [InlineData("timestamp", "1d")]
    [InlineData("timestamp", "6h")]
    [InlineData("timestamp", "90s")]
    public void Accepts_valid_windows(string type, string window)
    {
        Assert.True(WindowMath.TryValidateWindow(type, window, out var error));
        Assert.Null(error);
    }

    [Theory]
    [InlineData("int", "1d")]          // duration on numeric cursor
    [InlineData("int", "0")]           // non-positive
    [InlineData("int", "-5")]
    [InlineData("decimal", "1.5")]     // fractional delta not supported
    [InlineData("date", "6h")]         // sub-day duration on date cursor
    [InlineData("date", "36h")]        // not whole days
    [InlineData("date", "1000")]       // digits on temporal cursor
    [InlineData("timestamp", "0s")]    // non-positive
    [InlineData("timestamp", "1000")]  // digits on temporal cursor
    public void Rejects_invalid_windows(string type, string window)
    {
        Assert.False(WindowMath.TryValidateWindow(type, window, out var error));
        Assert.NotNull(error);
    }

    // --- AddWindow ---

    [Theory]
    [InlineData("int", "100", "50", "150")]
    [InlineData("bigint", "9000000000", "1000", "9000001000")]
    [InlineData("decimal", "12.50", "100", "112.50")]
    [InlineData("date", "2020-01-30", "3d", "2020-02-02")]
    [InlineData("timestamp", "2020-01-01T23:00:00.000000", "6h", "2020-01-02T05:00:00.000000")]
    [InlineData("timestamp", "2020-01-01T00:00:00.500000", "500ms", "2020-01-01T00:00:01.000000")]
    public void Adds_window_per_type(string type, string lower, string window, string expected)
    {
        Assert.Equal(expected, WindowMath.AddWindow(type, lower, window));
    }

    // --- Compare / Min ---

    [Theory]
    [InlineData("int", "9", "10", -1)]           // string compare would say "9" > "10" — must be numeric
    [InlineData("bigint", "100", "100", 0)]
    [InlineData("decimal", "12.5", "12.50", 0)]  // numerically equal despite different text
    [InlineData("date", "2020-01-02", "2020-01-01", 1)]
    [InlineData("timestamp", "2020-01-01T00:00:00.000001", "2020-01-01T00:00:00.000000", 1)]
    public void Compares_per_type(string type, string a, string b, int expectedSign)
    {
        Assert.Equal(expectedSign, Math.Sign(WindowMath.Compare(type, a, b)));
    }

    [Fact]
    public void Min_returns_smaller_canonical()
    {
        Assert.Equal("2026-07-01", WindowMath.Min("date", "2026-07-04", "2026-07-01"));
        Assert.Equal("100", WindowMath.Min("int", "100", "150"));
    }

    // --- ComputeWindow: one run's slice and whether it reaches the stop ---

    private static readonly DateTimeOffset RunStart = new(2026, 10, 9, 2, 0, 0, TimeSpan.Zero);

    [Fact]
    public void A_window_short_of_until_is_behind_and_inclusive()
    {
        var w = WindowMath.ComputeWindow("bigint", "10", "10", "25", RunStart);
        Assert.Equal("20", w.Upper);
        Assert.True(w.UpperInclusive);
        Assert.False(w.Empty);
        Assert.False(w.ReachesStop);
    }

    [Fact]
    public void A_window_clamped_to_until_reaches_the_stop()
    {
        // The run that loads the last slice is the caught-up one: no extra empty run.
        var w = WindowMath.ComputeWindow("bigint", "10", "10", "15", RunStart);
        Assert.Equal("15", w.Upper);
        Assert.True(w.UpperInclusive);
        Assert.True(w.ReachesStop);
    }

    [Fact]
    public void A_watermark_already_at_until_is_an_empty_window_at_the_stop()
    {
        var w = WindowMath.ComputeWindow("bigint", "15", "10", "15", RunStart);
        Assert.True(w.Empty);
        Assert.True(w.ReachesStop);
    }

    [Fact]
    public void No_until_has_no_stop()
    {
        var w = WindowMath.ComputeWindow("bigint", "10", "10", null, RunStart);
        Assert.Equal("20", w.Upper);
        Assert.Null(w.ReachesStop);
    }

    [Fact]
    public void Until_now_stops_just_before_the_run_started_on_a_timestamp_cursor()
    {
        var w = WindowMath.ComputeWindow("timestamp", "2026-10-08T23:30:00.000000", "1h", WindowMath.UntilNow, RunStart);
        Assert.Equal("2026-10-09T00:30:00.000000", w.Upper);   // an intermediate window: inclusive, behind
        Assert.True(w.UpperInclusive);
        Assert.False(w.ReachesStop);

        var last = WindowMath.ComputeWindow("timestamp", "2026-10-09T01:30:00.000000", "1h", WindowMath.UntilNow, RunStart);
        Assert.Equal("2026-10-09T02:00:00.000000", last.Upper);
        Assert.False(last.UpperInclusive);                       // c < run start
        Assert.True(last.ReachesStop);
    }

    [Fact]
    public void Until_now_on_a_date_cursor_loads_up_to_yesterday()
    {
        var w = WindowMath.ComputeWindow("date", "2026-10-07", "7d", WindowMath.UntilNow, RunStart);
        Assert.Equal("2026-10-09", w.Upper);
        Assert.False(w.UpperInclusive);                          // < 2026-10-09: today is never half-loaded
        Assert.True(w.ReachesStop);
    }

    [Theory]
    [InlineData("date", true)]
    [InlineData("timestamp", true)]
    [InlineData("int", false)]
    [InlineData("bigint", false)]
    [InlineData("decimal", false)]
    public void Until_now_needs_a_time_cursor(string type, bool supported)
    {
        Assert.Equal(supported, WindowMath.SupportsNow(type));
    }
}
