using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

[Collection("sqlserver")]
public sealed class MsUnmappedTypeTests(MsSqlContainerFixture fixture)
{
    [SkippableFact]
    public async Task Unmapped_column_fails_permanent_with_cast_hint()
    {
        DockerFacts.SkipUnlessDocker();
        var connector = new SqlServerConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(
            new ConnectorConfig(new Dictionary<string, object?>
            {
                ["host"] = fixture.Host, ["port"] = fixture.Port, ["database"] = fixture.Database,
                ["user"] = fixture.User, ["password"] = fixture.Password,
                ["trust_server_certificate"] = true,
            }), CancellationToken.None);

        var spec = new DatasetSpec("ms", "unmapped", new Dictionary<string, object?>
        {
            ["query"] = "select * from dbo.unmapped",
        });
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.GetSchemaAsync(spec, CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("c_bin", ex.Message, StringComparison.Ordinal);
        Assert.Contains("varbinary", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("cast", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [SkippableFact]
    public async Task Cast_in_query_is_a_working_workaround()
    {
        DockerFacts.SkipUnlessDocker();
        var connector = new SqlServerConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(
            new ConnectorConfig(new Dictionary<string, object?>
            {
                ["host"] = fixture.Host, ["port"] = fixture.Port, ["database"] = fixture.Database,
                ["user"] = fixture.User, ["password"] = fixture.Password,
                ["trust_server_certificate"] = true,
            }), CancellationToken.None);

        var spec = new DatasetSpec("ms", "unmapped_cast", new Dictionary<string, object?>
        {
            ["query"] = "select id, cast(c_time as nvarchar(16)) as c_time, " +
                        "cast(c_xml as nvarchar(max)) as c_xml from dbo.unmapped",
        });

        // Reads cleanly end to end via the same partition-read loop as MsTypeMatrixTests.
        var schema = (await source.GetSchemaAsync(spec, CancellationToken.None)).Schema;
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        var batches = new List<RecordBatch>();
        await foreach (var b in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            batches.Add(b);
        }

        var batch = Assert.Single(batches);
        Assert.Equal(1, batch.Length);

        object? Get(string column, int row)
        {
            var i = batch.Schema.GetFieldIndex(column);
            var a = batch.Column(i);
            return a.IsNull(row) ? null : a switch
            {
                Int32Array x => x.GetValue(row),
                StringArray x => x.GetString(row),
                _ => throw new InvalidOperationException(a.GetType().Name),
            };
        }

        Assert.Equal(1, Get("id", 0));
        Assert.Equal("10:30:00", Get("c_time", 0));
        Assert.Equal("<a/>", Get("c_xml", 0));
    }
}
