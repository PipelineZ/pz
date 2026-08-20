using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Xunit;

namespace Pz.Connector.Http.Tests;

/// <summary>End-to-end proof of one full sync cycle across two simulated runs: run 1 starts at the
/// configured <c>path</c> (no prior state) and captures a terminal delta link T1; run 2 replays T1
/// verbatim as its first request and captures a fresh terminal delta link T2. The store/advancement
/// commit-gating that would carry T1 from run 1 into run 2's <see cref="DatasetSpec.PriorSyncState"/>
/// is unit-tested elsewhere (<c>SyncStateStore</c>, <c>SyncStateAdvancement</c>); this test exercises
/// only the connector's own replay/capture contract, chaining the two runs by hand.</summary>
public sealed class HttpSyncEndToEndTests
{
    private static Dictionary<string, object?> Options(string path) => new()
    {
        ["path"] = path,
        ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "link_header" },
        ["items"] = "/value",
        ["delta_pointer"] = "/@odata.deltaLink",
        ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
    };

    private static async Task<IDatasetPartition> OpenPartitionAsync(StubHttpServer server, DatasetSpec spec)
    {
        var connection = new Dictionary<string, object?> { ["base_url"] = server.BaseUrl.ToString() };
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
    public async Task Two_runs_chain_the_captured_token_first_run_to_replay_to_fresh_capture()
    {
        await using var server = new StubHttpServer();
        var deltaLinkT1 = $"{server.BaseUrl}feed?$deltatoken=T1";
        var deltaLinkT2 = $"{server.BaseUrl}feed?$deltatoken=T2";

        // Run 1: /feed is the configured path. Terminal page carries T1.
        server.Map("/feed", _ => new StubResponse(
            200, $$"""{"value":[{"id":1}],"@odata.deltaLink":"{{deltaLinkT1}}"}"""));

        var run1Spec = new DatasetSpec("stub", "feed", Options("/feed"));
        var run1Partition = await OpenPartitionAsync(server, run1Spec);
        await DrainAsync(run1Partition);

        Assert.Single(server.Requests);
        Assert.Equal("/feed", server.Requests[0].Url.AbsolutePath);
        Assert.Empty(server.Requests[0].Url.Query);

        var run1SyncPartition = Assert.IsAssignableFrom<ISyncStatePartition>(run1Partition);
        Assert.True(run1SyncPartition.TryGetSyncStateCandidate(out var token1));
        Assert.Equal(deltaLinkT1, token1);

        // Run 2: simulates the engine replaying T1 as PriorSyncState (as SyncStateStore/advancement
        // would have persisted after run 1's downstream sinks committed). The replay URL itself
        // (T1's full querystring) now serves a fresh terminal page carrying T2.
        server.Map("/feed", req => req.Url.Query.Contains("$deltatoken=T1")
            ? new StubResponse(200, $$"""{"value":[{"id":2}],"@odata.deltaLink":"{{deltaLinkT2}}"}""")
            : new StubResponse(200, """{"value":[]}"""));

        var run2Spec = new DatasetSpec("stub", "feed", Options("/feed")) { PriorSyncState = token1 };
        var run2Partition = await OpenPartitionAsync(server, run2Spec);
        await DrainAsync(run2Partition);

        // Run 2's first request must target T1's URL verbatim, not the configured path.
        var run2FirstRequest = server.Requests[1];
        Assert.Equal(new Uri(token1!), run2FirstRequest.Url);

        var run2SyncPartition = Assert.IsAssignableFrom<ISyncStatePartition>(run2Partition);
        Assert.True(run2SyncPartition.TryGetSyncStateCandidate(out var token2));
        Assert.Equal(deltaLinkT2, token2);
        Assert.NotEqual(token1, token2);
    }
}
