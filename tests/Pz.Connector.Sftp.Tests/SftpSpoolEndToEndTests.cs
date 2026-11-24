using System.Text;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Formats;
using Pz.DuckDb;
using Pz.TestSupport;
using Xunit;

namespace Pz.Connector.Sftp.Tests;

/// <summary>Against a live SFTP server: DuckDB-written nested parquet spooled into partition folders lands under
/// the rendered folders with the output's object name, and sftp's own source reads it back intact.</summary>
[Collection("sftp")]
public sealed class SftpSpoolEndToEndTests(SftpContainerFixture fixture) : IAsyncLifetime
{
    private readonly string _local = Path.Combine(Path.GetTempPath(), "pz-sftp-spool-e2e", Guid.NewGuid().ToString("N"));
    private readonly string _prefix = $"spool-{Guid.NewGuid():N}";

    private ConnectorConfig Config => new(new Dictionary<string, object?>
    {
        ["host"] = fixture.Host,
        ["port"] = fixture.Port,
        ["username"] = SftpContainerFixture.PasswordUser,
        ["password"] = SftpContainerFixture.Password,
        ["root"] = "upload",
    });

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_local);
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        try { Directory.Delete(_local, recursive: true); } catch { /* best-effort */ }
        return Task.CompletedTask;
    }

    [SkippableFact]
    public async Task Spooled_nested_parquet_lands_in_partition_folders_and_reads_back()
    {
        DockerFacts.SkipUnlessDocker();
        var january = Path.Combine(_local, "0.parquet");
        var february = Path.Combine(_local, "1.parquet");
        await using (var duck = DuckSession.Open(Path.Combine(_local, "spool.duckdb")))
        {
            await duck.ExecuteAsync($"copy (select 1 as id, ['a', 'b'] as tags, {{'n': 1}} as rec) to '{january}' (format parquet)");
            await duck.ExecuteAsync($"copy (select 2 as id, []::varchar[] as tags, null::struct(n int) as rec) to '{february}' (format parquet)");
        }

        var connector = new SftpConnector();
        var spec = new OutputSpec("sftp", "events", "replace", "fail_on_change", new Dictionary<string, object?>
        {
            ["path"] = _prefix + "/{yyyy}/{MM}",
            ["format"] = "parquet",
            ["partition_by"] = "at",
        });
        await using (var sink = await ((ISinkConnector)connector).OpenAsync(Config, CancellationToken.None))
        {
            var spooling = Assert.IsAssignableFrom<ISpoolingSink>(sink);
            Assert.True(spooling.TryGetSpool(spec, out _));
            var result = await spooling.CommitSpoolAsync(spec,
                [new SpooledFile(_prefix + "/2024/01", january, 1), new SpooledFile(_prefix + "/2024/02", february, 1)],
                CancellationToken.None);
            Assert.Equal(2, result.RowsWritten);
        }

        Assert.Equal("{\"id\":1,\"tags\":[\"a\",\"b\"],\"rec\":{\"n\":1}}\n", await ReadBackAsync(connector, "2024/01"));
        Assert.Equal("{\"id\":2,\"tags\":[],\"rec\":null}\n", await ReadBackAsync(connector, "2024/02"));
    }

    private async Task<string> ReadBackAsync(SftpConnector connector, string folder)
    {
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);
        var partitions = await source.PlanReadAsync(new DatasetSpec("sftp", "events", new Dictionary<string, object?>
        {
            ["path"] = $"{_prefix}/{folder}/events.parquet",
            ["format"] = "parquet",
        }), ReadHints.None, CancellationToken.None);

        var output = new MemoryStream();
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                using (batch)
                {
                    await NdjsonCodec.WriteAsync(batch, output, CancellationToken.None);
                }
            }
        }

        return Encoding.UTF8.GetString(output.ToArray());
    }
}
