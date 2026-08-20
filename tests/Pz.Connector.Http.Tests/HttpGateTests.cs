using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Pz.Engine.Resilience;

namespace Pz.Connector.Http.Tests;

/// <summary>HTTP connector adoption of the engine's IOperationGate.
/// <see cref="HttpPartition.FetchPageAsync"/> is the single wrap site —
/// every fact here drives a full <see cref="HttpSource"/>/<see cref="HttpPartition"/> read rather
/// than poking FetchPageAsync directly, since the gate is threaded in via
/// <see cref="IOperationGateAware.UseOperationGate"/> on the source, not the partition.</summary>
public class HttpGateTests
{
    /// <summary>Local stand-in for <c>Pz.Engine.Tests.Execution.FixedRandom</c> — that helper is
    /// internal to Pz.Engine.Tests and not referenceable here. Same contract: NextDouble() pinned at
    /// 0.5 maps to exactly zero jitter in RetryPolicy.ComputeDelay.</summary>
    private sealed class FixedRandom(double value) : Random
    {
        public override double NextDouble() => value;
    }

    private static async Task<(long Rows, StubHttpServer Server)> ReadAllAsync(StubHttpServer server,
        Dictionary<string, object?> options, IOperationGate? gate = null)
    {
        var connection = new Dictionary<string, object?> { ["base_url"] = server.BaseUrl.ToString() };
        var connector = new HttpConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(connection), CancellationToken.None);
        if (gate is not null)
        {
            Assert.True(source is IOperationGateAware,
                "a connector declaring GatedOperations must implement IOperationGateAware on its ISource");
            ((IOperationGateAware)source).UseOperationGate(gate);
        }

        var partitions = await source.PlanReadAsync(new DatasetSpec("s", "d", options),
            ReadHints.None, CancellationToken.None);
        long rows = 0;
        await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        return (rows, server);
    }

    [Fact]
    public async Task Reads_route_through_gate()
    {
        await using var server = new StubHttpServer();
        server.Map("/items", req => req.Url.Query.Contains("page=2")
            ? new StubResponse(200, """[{"id":2}]""")
            : new StubResponse(200, """[{"id":1}]""", new Dictionary<string, string>
                { ["Link"] = $"<{server.BaseUrl}items?page=2>; rel=\"next\"" }));

        var gate = new CountingOperationGate();
        var (rows, _) = await ReadAllAsync(server, new()
        {
            ["path"] = "/items",
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "link_header" },
        }, gate);

        Assert.Equal(2, rows);
        Assert.Equal(["http.get_page", "http.get_page"], gate.Labels);
    }

    [Fact]
    public async Task Op_retry_refetches_only_failed_page()
    {
        await using var server = new StubHttpServer();
        var page2Hits = 0;
        server.Map("/items", req =>
        {
            if (req.Url.Query.Contains("page=2"))
            {
                page2Hits++;
                return page2Hits == 1
                    ? new StubResponse(429, """{"msg":"slow down"}""",
                        new Dictionary<string, string> { ["Retry-After"] = "1" })
                    : new StubResponse(200, """[{"id":2}]""");
            }

            return new StubResponse(200, """[{"id":1}]""", new Dictionary<string, string>
                { ["Link"] = $"<{server.BaseUrl}items?page=2>; rel=\"next\"" });
        });

        var delays = new List<TimeSpan>();
        var policy = new RetryPolicy(3, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30));
        var gate = new OperationGate(policy, pacing: null, new FixedRandom(0.5), (d, _) =>
        {
            delays.Add(d);
            return Task.CompletedTask;
        });

        var (rows, srv) = await ReadAllAsync(server, new()
        {
            ["path"] = "/items",
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "link_header" },
        }, gate);

        Assert.Equal(2, rows);
        Assert.Equal(1, srv.Requests.Count(r => !r.Url.Query.Contains("page=2"))); // page1 fetched once
        Assert.Equal(2, srv.Requests.Count(r => r.Url.Query.Contains("page=2"))); // page2: 429 then 200
        Assert.Equal(TimeSpan.FromSeconds(1), Assert.Single(delays)); // Retry-After honored at op level
    }

    [Fact]
    public async Task Budget_reported_from_headers()
    {
        await using var server = new StubHttpServer();
        var now = DateTimeOffset.UtcNow;
        server.Map("/items", _ => new StubResponse(200, """[{"id":1}]""", new Dictionary<string, string>
        {
            ["X-RateLimit-Remaining"] = "0",
            ["X-RateLimit-Reset"] = "30",
        }));

        var gate = new CountingOperationGate();
        var (rows, _) = await ReadAllAsync(server, new() { ["path"] = "/items" }, gate);

        Assert.Equal(1, rows);
        var budget = Assert.Single(gate.Budgets);
        Assert.Equal(0, budget.Remaining);
        Assert.True(Math.Abs((budget.ResetAt - now.AddSeconds(30)).TotalSeconds) < 10,
            $"expected ResetAt near {now.AddSeconds(30):O}, got {budget.ResetAt:O}");
    }

    [Fact]
    public async Task No_gate_still_works()
    {
        await using var server = new StubHttpServer();
        server.Map("/items", req => req.Url.Query.Contains("page=2")
            ? new StubResponse(200, """[{"id":2}]""")
            : new StubResponse(200, """[{"id":1}]""", new Dictionary<string, string>
                { ["Link"] = $"<{server.BaseUrl}items?page=2>; rel=\"next\"" }));

        var (rows, _) = await ReadAllAsync(server, new()
        {
            ["path"] = "/items",
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "link_header" },
        }); // no gate configured: behaves exactly as it does when gated operations don't exist

        Assert.Equal(2, rows);
    }

    [Fact]
    public void Capability_declared()
    {
        var connector = new HttpConnector();

        Assert.True(connector.Capabilities.HasFlag(ConnectorCapabilities.GatedOperations));
        Assert.True(connector.Capabilities.HasFlag(ConnectorCapabilities.BoundedWindow));
        Assert.True(connector.Capabilities.HasFlag(ConnectorCapabilities.SyncState));
    }
}
