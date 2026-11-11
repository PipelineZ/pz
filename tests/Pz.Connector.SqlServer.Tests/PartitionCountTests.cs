using Pz.Connectors.Abstractions;
using Pz.Shared.Partitioning;

namespace Pz.Connector.SqlServer.Tests;

public class PartitionCountTests
{
    private const int Max = 16;

    private static DatasetSpec Spec(object? partitions) =>
        new("mssql", "orders", new Dictionary<string, object?> { ["partitions"] = partitions });

    [Fact]
    public void Absent_or_null_is_one_partition()
    {
        Assert.Equal(1, PartitionCount.Parse(new DatasetSpec("mssql", "orders", new Dictionary<string, object?>()), Max));
        Assert.Equal(1, PartitionCount.Parse(Spec(null), Max));
    }

    [Theory]
    [InlineData(3)]
    [InlineData(3L)]
    [InlineData("3")]
    public void Whole_numbers_parse(object value) => Assert.Equal(3, PartitionCount.Parse(Spec(value), Max));

    [Theory]
    [InlineData(3.7)]
    [InlineData(3.0)]
    [InlineData(true)]
    [InlineData("3.0")]
    [InlineData("four")]
    [InlineData(5_000_000_000L)]
    public void Non_integers_are_refused_not_coerced(object value)
    {
        var ex = Assert.Throws<PzConnectorException>(() => PartitionCount.Parse(Spec(value), Max));
        Assert.False(ex.IsTransient);
        Assert.Contains("'partitions' must be an integer", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Decimal_is_refused() =>
        Assert.Throws<PzConnectorException>(() => PartitionCount.Parse(Spec(3m), Max));

    [Theory]
    [InlineData(0)]
    [InlineData(17)]
    public void Out_of_range_is_refused(int value)
    {
        var ex = Assert.Throws<PzConnectorException>(() => PartitionCount.Parse(Spec(value), Max));
        Assert.Contains("must be between 1 and 16", ex.Message, StringComparison.Ordinal);
    }
}
