namespace Pz.Connector.SqlServer.Tests;

public class PartitionPlanningTests
{
    [Fact]
    public void Builds_half_open_ranges_with_inclusive_tail_and_null_bucket_on_partition_zero()
    {
        var selects = SqlServerSource.BuildPartitionSelects("select * from [dbo].[t]", "id", ["0", "50", "100"]);
        Assert.Equal(2, selects.Length);
        Assert.Equal(
            "select * from (select * from [dbo].[t]) q where (([id] is null or ([id] >= 0 and [id] < 50)))",
            selects[0]);
        Assert.Equal(
            "select * from (select * from [dbo].[t]) q where (([id] >= 50 and [id] <= 100))",
            selects[1]);
    }

    [Fact]
    public void Quotes_the_partition_column()
    {
        var selects = SqlServerSource.BuildPartitionSelects("select 1", "weird]col", ["0", "1"]);
        Assert.Contains("[weird]]col]", selects[0], StringComparison.Ordinal);
    }
}
