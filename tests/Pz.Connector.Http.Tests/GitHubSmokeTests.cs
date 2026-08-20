using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.TestSupport;

namespace Pz.Connector.Http.Tests;

public class GitHubSmokeTests
{
    [SkippableFact]
    public async Task Reads_two_link_header_pages_of_public_issues()
    {
        DockerFacts.SkipIfOffline();

        var connector = new HttpConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(
            new Dictionary<string, object?> { ["base_url"] = "https://api.github.com" }),
            CancellationToken.None);
        var spec = new DatasetSpec("gh", "issues", new Dictionary<string, object?>
        {
            ["path"] = "/repos/duckdb/duckdb/issues",
            ["query"] = new Dictionary<string, object?> { ["state"] = "all", ["per_page"] = "5" },
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "link_header" },
            ["max_pages"] = 2L,
        });

        long rows = 0;
        try
        {
            var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
            await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                rows += batch.Length;
                batch.Dispose();
            }
        }
        catch (PzConnectorException ex) when (ex.IsTransient || ex.Message.Contains("HTTP 403"))
        {
            Skip.If(true, $"github unreachable or rate-limited: {ex.Message}");
        }

        Assert.True(rows > 5, $"expected a second page beyond per_page=5, got {rows} row(s)");
        Assert.True(rows <= 10, $"max_pages=2 x per_page=5 should cap at 10, got {rows}");
    }
}
