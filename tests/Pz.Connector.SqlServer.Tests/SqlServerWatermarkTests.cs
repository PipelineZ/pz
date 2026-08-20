using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

[Collection("sqlserver")]
public sealed class SqlServerWatermarkTests(MsSqlContainerFixture fixture)
{
    private async Task<long> CountAsync(DatasetSpec spec)
    {
        var connector = new SqlServerConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(
            new ConnectorConfig(new Dictionary<string, object?>
            {
                ["host"] = fixture.Host, ["port"] = fixture.Port, ["database"] = fixture.Database,
                ["user"] = fixture.User, ["password"] = fixture.Password,
                ["trust_server_certificate"] = true,
            }), CancellationToken.None);
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        var total = 0L;
        foreach (var p in partitions)
        {
            await foreach (var batch in p.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                total += batch.Length;
                batch.Dispose();
            }
        }

        return total;
    }

    [SkippableFact]
    public async Task Watermark_lower_bound_filters_server_side()
    {
        DockerFacts.SkipUnlessDocker();
        var spec = new DatasetSpec("ms", "orders", new Dictionary<string, object?>())
        {
            WatermarkCursor = "id",
            WatermarkValue = "99",
        };
        Assert.Equal(50, await CountAsync(spec)); // ids 100..149
    }

    [SkippableFact]
    public async Task Bounded_window_applies_both_bounds()
    {
        DockerFacts.SkipUnlessDocker();
        var spec = new DatasetSpec("ms", "orders", new Dictionary<string, object?>())
        {
            WatermarkCursor = "id",
            WatermarkValue = "99",
            WatermarkUpperBound = "119",
        };
        Assert.Equal(20, await CountAsync(spec)); // 100..119
    }

    [SkippableFact]
    public async Task Partitioned_read_with_watermark_loses_and_duplicates_nothing()
    {
        DockerFacts.SkipUnlessDocker();
        var spec = new DatasetSpec("ms", "orders", new Dictionary<string, object?>
        {
            ["partition_column"] = "id", ["partitions"] = 4,
        })
        {
            WatermarkCursor = "id",
            WatermarkValue = "49",
        };
        Assert.Equal(100, await CountAsync(spec)); // 50..149 across 4 partitions
    }
}
