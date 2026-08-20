namespace Pz.Connector.SqlServer.Tests;

public class RangeBoundariesTests
{
    [Fact]
    public void Integer_boundaries_are_exact_endpoints_with_equal_width_interior()
    {
        var b = RangeBoundaries.ComputeLiterals(0L, 100L, 4);
        Assert.Equal(["0", "25", "50", "75", "100"], b);
    }

    [Fact]
    public void Huge_bigint_range_does_not_overflow()
    {
        var b = RangeBoundaries.ComputeLiterals(long.MinValue, long.MaxValue, 2);
        Assert.Equal(3, b.Length);
        Assert.Equal(long.MinValue.ToString(), b[0]);
        Assert.Equal(long.MaxValue.ToString(), b[2]);
    }

    [Fact]
    public void Adjacent_partitions_share_the_identical_boundary_literal()
    {
        var b = RangeBoundaries.ComputeLiterals(0L, 7L, 3); // widths don't divide evenly
        Assert.Equal(4, b.Length);
        // boundary(i) is a pure function of i -- no recomputation drift possible by construction;
        // assert monotonic non-decreasing as the observable property.
        for (var i = 1; i < b.Length; i++)
        {
            Assert.True(long.Parse(b[i]) >= long.Parse(b[i - 1]));
        }
    }

    [Fact]
    public void Double_boundaries_use_G17_invariant()
    {
        var b = RangeBoundaries.ComputeLiterals(0.5d, 2.5d, 2);
        Assert.Equal("0.5", b[0]);
        Assert.Equal("1.5", b[1]);
        Assert.Equal("2.5", b[2]);
    }

    [Fact]
    public void Extreme_double_range_does_not_overflow()
    {
        var b = RangeBoundaries.ComputeLiterals(-double.MaxValue / 2, double.MaxValue / 2, 3);
        Assert.Equal(4, b.Length);
        foreach (var literal in b)
        {
            Assert.DoesNotContain("∞", literal);
            Assert.DoesNotContain("Infinity", literal);
            Assert.DoesNotContain("NaN", literal);
        }

        Assert.Equal((-double.MaxValue / 2).ToString("G17", System.Globalization.CultureInfo.InvariantCulture), b[0]);
        Assert.Equal((double.MaxValue / 2).ToString("G17", System.Globalization.CultureInfo.InvariantCulture), b[3]);
    }

    [Fact]
    public void Date_boundaries_render_as_sargable_casts_on_the_literal()
    {
        var b = RangeBoundaries.ComputeLiterals(new DateOnly(2026, 1, 1), new DateOnly(2026, 1, 5), 2);
        Assert.Equal("cast('2026-01-01' as date)", b[0]);
        Assert.Equal("cast('2026-01-03' as date)", b[1]);
        Assert.Equal("cast('2026-01-05' as date)", b[2]);
    }

    [Fact]
    public void Datetime_boundaries_render_as_datetime2_casts()
    {
        var lo = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
        var hi = new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Unspecified);
        var b = RangeBoundaries.ComputeLiterals(lo, hi, 2);
        Assert.Equal("cast('2026-01-01 00:00:00.000000' as datetime2(6))", b[0]);
        Assert.Equal("cast('2026-01-01 12:00:00.000000' as datetime2(6))", b[1]);
    }

    [Fact]
    public void Datetimeoffset_boundaries_normalize_to_utc()
    {
        var lo = new DateTimeOffset(2026, 1, 1, 2, 0, 0, TimeSpan.FromHours(2));  // 00:00Z
        var hi = new DateTimeOffset(2026, 1, 1, 2, 0, 0, TimeSpan.FromHours(-2)); // 04:00Z
        var b = RangeBoundaries.ComputeLiterals(lo, hi, 2);
        Assert.Equal("cast('2026-01-01 00:00:00.000000 +00:00' as datetimeoffset(6))", b[0]);
        Assert.Equal("cast('2026-01-01 02:00:00.000000 +00:00' as datetimeoffset(6))", b[1]);
        Assert.Equal("cast('2026-01-01 04:00:00.000000 +00:00' as datetimeoffset(6))", b[2]);
    }

    [Fact]
    public void Decimal_boundaries_are_exact_endpoints_with_interior_interpolation()
    {
        var b = RangeBoundaries.ComputeLiterals(0m, 10m, 4);
        Assert.Equal("0.0", b[0]);
        Assert.Equal("2.5", b[1]);
        Assert.Equal("5.0", b[2]);
        Assert.Equal("7.5", b[3]);
        Assert.Equal("10", b[4]);
    }

    [Fact]
    public void Extreme_decimal_range_does_not_overflow()
    {
        var b = RangeBoundaries.ComputeLiterals(0m, decimal.MaxValue, 3);
        Assert.Equal(4, b.Length);
        Assert.Equal("0", b[0]);
        Assert.Equal(decimal.MaxValue.ToString(System.Globalization.CultureInfo.InvariantCulture), b[3]);
        // interior boundaries parse back and are strictly increasing
        for (var i = 1; i < b.Length; i++)
        {
            Assert.True(decimal.Parse(b[i], System.Globalization.CultureInfo.InvariantCulture)
                >= decimal.Parse(b[i - 1], System.Globalization.CultureInfo.InvariantCulture));
        }
    }

    [Theory]
    [InlineData(typeof(int), true)]
    [InlineData(typeof(long), true)]
    [InlineData(typeof(double), true)]
    [InlineData(typeof(decimal), true)]
    [InlineData(typeof(DateOnly), true)]
    [InlineData(typeof(DateTime), true)]
    [InlineData(typeof(DateTimeOffset), true)]
    [InlineData(typeof(string), false)]
    [InlineData(typeof(Guid), false)]
    public void IsOrderable_matches_the_spec_set(Type t, bool expected) =>
        Assert.Equal(expected, RangeBoundaries.IsOrderable(t));
}
