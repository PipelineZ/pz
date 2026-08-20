using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Xunit;

namespace Pz.Connector.Http.Tests;

/// <summary>Delta-link (change-feed) mode: a Graph-style `@odata.deltaLink` feed. Pagination is
/// mechanical (Link header, exercised elsewhere in <see cref="HttpPartitionTests"/>); the delta
/// pointer is a separate, orthogonal signal resolved from each page's body — only the terminal
/// page of the feed carries it.</summary>
public sealed class HttpDeltaLinkTests
{
    private static Dictionary<string, object?> Options(string path) => new()
    {
        ["path"] = path,
        ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "link_header" },
        ["items"] = "/value",
        ["delta_pointer"] = "/@odata.deltaLink",
        ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
    };

    private static async Task<IDatasetPartition> OpenPartitionAsync(
        StubHttpServer server, DatasetSpec spec, Dictionary<string, object?>? connectionExtra = null)
    {
        var connection = new Dictionary<string, object?> { ["base_url"] = server.BaseUrl.ToString() };
        foreach (var (k, v) in connectionExtra ?? []) connection[k] = v;
        var connector = new HttpConnector();
        var source = await connector.OpenAsync(new ConnectorConfig(connection), CancellationToken.None);
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        return partitions[0];
    }

    private static async Task DrainAsync(IDatasetPartition partition)
    {
        await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            batch.Dispose();
        }
    }

    [Fact]
    public async Task First_run_reads_all_pages_and_captures_the_terminal_delta_link()
    {
        await using var server = new StubHttpServer();
        var deltaLink = $"{server.BaseUrl}feed?$deltatoken=abc123";
        server.Map("/feed", req => req.Url.Query.Contains("page=2")
            // Terminal page: no Link header (pagination naturally stops here) + deltaLink.
            ? new StubResponse(200, $$"""{"value":[{"id":2}],"@odata.deltaLink":"{{deltaLink}}"}""")
            // Page 1: items + a Link header pointing at page 2 (Graph itself embeds nextLink in
            // the body; link_header is the strategy this stub can drive deterministically).
            : new StubResponse(200, """{"value":[{"id":1}]}""",
                new Dictionary<string, string> { ["Link"] = $"<{server.BaseUrl}feed?page=2>; rel=\"next\"" }));

        var spec = new DatasetSpec("stub", "feed", Options("/feed"));
        var partition = await OpenPartitionAsync(server, spec);
        await DrainAsync(partition);

        Assert.Equal(2, server.Requests.Count);

        var syncPartition = Assert.IsAssignableFrom<ISyncStatePartition>(partition);
        Assert.True(syncPartition.TryGetSyncStateCandidate(out var candidate));
        Assert.Equal(deltaLink, candidate);
    }

    [Fact]
    public async Task Replay_targets_the_stored_delta_url_verbatim_not_the_configured_path()
    {
        await using var server = new StubHttpServer();
        var priorToken = $"{server.BaseUrl}feed?$deltatoken=already-synced";
        // Terminal immediately: no Link header, empty value array — read completes in one request.
        server.Map("/feed", _ => new StubResponse(200, """{"value":[]}"""));

        var spec = new DatasetSpec("stub", "feed", Options("/feed")) { PriorSyncState = priorToken };
        var partition = await OpenPartitionAsync(server, spec);
        await DrainAsync(partition);

        Assert.Single(server.Requests);
        Assert.Equal(new Uri(priorToken), server.Requests[0].Url);
    }

    [Fact]
    public async Task Expired_delta_link_throws_permanent_error_naming_full_refresh()
    {
        await using var server = new StubHttpServer();
        // The resume token lives under a server-defined param ($deltatoken), NOT the connector's
        // configured auth param — no auth is configured at all here, so nothing could coincidentally
        // overwrite/mask it. Only whole-query redaction keeps this out of the message.
        var priorToken = $"{server.BaseUrl}feed?$deltatoken=SECRETTOKEN123";
        server.Map("/feed", _ => new StubResponse(410, """{"error":"Gone"}"""));

        var spec = new DatasetSpec("stub", "feed", Options("/feed")) { PriorSyncState = priorToken };
        var partition = await OpenPartitionAsync(server, spec);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => DrainAsync(partition));

        Assert.False(ex.IsTransient);
        Assert.Contains("--full-refresh", ex.Message);
        Assert.DoesNotContain("SECRETTOKEN123", ex.Message);
    }

    [Fact]
    public async Task Permanent_error_body_echoing_the_replayed_delta_url_does_not_leak_the_token()
    {
        // Redact(Uri) (the 410-expiry path above) is sync-aware (whole-query redaction), but the
        // generic 4xx path additionally surfaces
        // Redact(Snippet(text)) -- the response BODY -- via the string overload, which only masks
        // configured SecretQueryParams. A server that echoes the request URL in a 403/404 body leaks
        // the server-defined `$deltatoken` param (not a configured auth param) through that snippet.
        await using var server = new StubHttpServer();
        var priorToken = $"{server.BaseUrl}feed?$deltatoken=SECRETTOKEN123";
        server.Map("/feed", req => new StubResponse(403,
            $$"""{"error":"Forbidden","requestUrl":"{{req.Url}}"}"""));

        var spec = new DatasetSpec("stub", "feed", Options("/feed")) { PriorSyncState = priorToken };
        var partition = await OpenPartitionAsync(server, spec);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => DrainAsync(partition));

        Assert.False(ex.IsTransient);
        Assert.Contains("403", ex.Message);
        Assert.DoesNotContain("SECRETTOKEN123", ex.Message);
    }

    [Fact]
    public async Task First_run_sync_read_hitting_410_is_a_normal_permanent_error_not_expiry()
    {
        // No PriorSyncState: this is not a replay, so a 410 here means a bad path/endpoint, not an
        // expired token — it must not be mislabeled as expiry or told to --full-refresh.
        await using var server = new StubHttpServer();
        server.Map("/feed", _ => new StubResponse(410, """{"error":"Gone"}"""));

        var spec = new DatasetSpec("stub", "feed", Options("/feed"));
        var partition = await OpenPartitionAsync(server, spec);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => DrainAsync(partition));

        Assert.False(ex.IsTransient);
        Assert.Contains("410", ex.Message);
        Assert.DoesNotContain("--full-refresh", ex.Message);
        Assert.DoesNotContain("expired", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
