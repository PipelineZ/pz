using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.SqlServer.Tests;

/// <summary>Generous-bound smoke test: 200k narrow rows through the bulk-load sink path must land in
/// single-digit seconds even on cold CI. 90s is a never-flake ceiling that still catches a regression
/// from the bulk path back to row-by-row inserts (which would take minutes, not seconds).</summary>
[Collection("sqlserver")]
public sealed class BulkThroughputSmokeTests(MsSqlContainerFixture fixture)
{
    [SkippableFact]
    public async Task Append_bulk_loads_200k_rows_within_a_generous_bound()
    {
        DockerFacts.SkipUnlessDocker();
        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: false),
        ], null);

        var connector = new SqlServerConnector();
        await using var sink = await ((ISinkConnector)connector).OpenAsync(
            new ConnectorConfig(new Dictionary<string, object?>
            {
                ["host"] = fixture.Host, ["port"] = fixture.Port, ["database"] = fixture.Database,
                ["user"] = fixture.User, ["password"] = fixture.Password,
                ["trust_server_certificate"] = true,
            }), CancellationToken.None);
        var spec = new OutputSpec("ms", "bulk_smoke", "replace", "fail_on_change",
            new Dictionary<string, object?>());

        var started = System.Diagnostics.Stopwatch.StartNew();
        await using var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None);
        for (var chunk = 0; chunk < 10; chunk++)
        {
            var ids = new Int64Array.Builder();
            var names = new StringArray.Builder();
            for (var i = 0; i < 20_000; i++)
            {
                ids.Append((chunk * 20_000L) + i);
                names.Append($"row-{i}");
            }

            using var batch = new RecordBatch(schema, [ids.Build(), names.Build()], 20_000);
            await session.WriteBatchAsync(batch, CancellationToken.None);
        }

        var result = await session.CommitAsync(CancellationToken.None);
        started.Stop();

        Assert.Equal(200_000, result.RowsWritten);
        // Bulk path lands 200k narrow rows in single-digit seconds even on cold CI; row-by-row would
        // take minutes. 90s = never-flake bound that still catches a bulk->row regression.
        Assert.True(started.Elapsed < TimeSpan.FromSeconds(90),
            $"bulk load took {started.Elapsed} -- did the bulk path regress to row-by-row?");
    }
}
