using System.Globalization;

namespace Pz.Connector.Postgres.Tests;

/// <summary>Offline tests of the equal-width range-partition boundary literals.</summary>
public sealed class PostgresBoundaryTests
{
    [Fact]
    public void Extreme_double_range_does_not_overflow()
    {
        var b = PostgresSource.ComputeBoundaryLiterals(-double.MaxValue / 2, double.MaxValue / 2, 3);

        Assert.Equal(4, b.Length);
        Assert.All(b, literal => Assert.True(double.IsFinite(double.Parse(literal, CultureInfo.InvariantCulture)), literal));
        Assert.Equal((-double.MaxValue / 2).ToString("G17", CultureInfo.InvariantCulture), b[0]);
        Assert.Equal((double.MaxValue / 2).ToString("G17", CultureInfo.InvariantCulture), b[3]);
    }

    [Fact]
    public void Extreme_decimal_range_does_not_overflow()
    {
        var b = PostgresSource.ComputeBoundaryLiterals(0m, decimal.MaxValue, 3);

        Assert.Equal(4, b.Length);
        Assert.Equal("0", b[0]);
        Assert.Equal(decimal.MaxValue.ToString(CultureInfo.InvariantCulture), b[3]);
        for (var i = 1; i < b.Length; i++)
        {
            Assert.True(decimal.Parse(b[i], CultureInfo.InvariantCulture) >= decimal.Parse(b[i - 1], CultureInfo.InvariantCulture));
        }
    }

    [Fact]
    public void Adjacent_partitions_share_a_boundary_and_the_ends_are_exact()
    {
        Assert.Equal(["0", "25", "50", "75", "100"], PostgresSource.ComputeBoundaryLiterals(0L, 100L, 4));
        Assert.Equal(["0.5", "1.5", "2.5"], PostgresSource.ComputeBoundaryLiterals(0.5d, 2.5d, 2));
    }
}
