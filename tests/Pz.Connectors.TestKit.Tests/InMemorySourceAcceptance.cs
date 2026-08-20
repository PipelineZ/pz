using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Pz.Connectors.TestKit.Reference;

public class InMemorySourceAcceptance : SourceConnectorAcceptanceTests
{
    protected override ISourceConnector CreateSource() => new InMemoryConnector();

    protected override ConnectorConfig ValidConfig => ConnectorConfig.Empty;

    protected override DatasetSpec SmallDataset => new("mem", "numbers",
        new Dictionary<string, object?> { ["rows"] = 500L, ["partitions"] = 2 });

    protected override DatasetSpec? LargeDataset => new("mem", "numbers",
        new Dictionary<string, object?> { ["rows"] = 500_000L });

    protected override DatasetSpec? TransientFailureDataset => new("mem", "numbers",
        new Dictionary<string, object?> { ["rows"] = 10_000L, ["fail_read_at_batch"] = 0, ["fail_transient"] = true });

    protected override DatasetSpec? GetSpecWithPartitionOverride(int partitions) => new("mem", "numbers",
        new Dictionary<string, object?> { ["rows"] = 500L, ["partitions"] = partitions });

    protected override DatasetSpec? BoundedWindowDataset => new DatasetSpec("mem", "numbers",
        new Dictionary<string, object?> { ["rows"] = 11L })
    {
        WatermarkCursor = "id",
        WatermarkValue = "3",
        WatermarkUpperBound = "7",
    };

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task PlanReadAsync_rejects_invalid_partition_count(int partitions)
    {
        var connector = new InMemoryConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(ConnectorConfig.Empty, CancellationToken.None);
        var spec = new DatasetSpec("mem", "numbers",
            new Dictionary<string, object?> { ["rows"] = 100L, ["partitions"] = partitions });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("numbers", ex.Message);
        Assert.Contains(partitions.ToString(), ex.Message);
    }
}
