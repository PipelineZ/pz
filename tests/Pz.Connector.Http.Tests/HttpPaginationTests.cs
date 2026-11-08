using System.Text.Json;
using Json.Schema;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

/// <summary>The body-link (<c>next_url</c>) and offset/limit (<c>offset</c>) pagination strategies,
/// end to end through a real partition read against the stub server.</summary>
public class HttpPaginationTests
{
    private static async Task<long> ReadAllAsync(StubHttpServer server, Dictionary<string, object?> options,
        Dictionary<string, object?>? connectionExtra = null)
    {
        var connection = new Dictionary<string, object?> { ["base_url"] = server.BaseUrl.ToString() };
        foreach (var (k, v) in connectionExtra ?? []) connection[k] = v;
        await using var source = await new HttpConnector().OpenAsync(new ConnectorConfig(connection),
            CancellationToken.None);
        var partitions = await source.PlanReadAsync(new DatasetSpec("s", "d", options), ReadHints.None,
            CancellationToken.None);
        long rows = 0;
        await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        return rows;
    }

    private static Dictionary<string, object?> Pagination(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    private static string QueryValue(Uri url, string name)
        => url.Query.TrimStart('?').Split('&').Select(p => p.Split('=')).Where(p => p[0] == name)
            .Select(p => p[1]).SingleOrDefault() ?? "";

    [Fact]
    public async Task Next_url_follows_a_graph_style_body_link_across_pages()
    {
        await using var server = new StubHttpServer();
        server.Map("/users", req => req.Url.Query.Contains("skiptoken=B")
            ? new StubResponse(200, """{"value":[{"id":3}]}""")
            : new StubResponse(200,
                $$"""{"value":[{"id":1},{"id":2}],"@odata.nextLink":"{{server.BaseUrl}}users?$skiptoken=B"}"""));

        var rows = await ReadAllAsync(server, new()
        {
            ["path"] = "/users",
            ["items"] = "/value",
            ["pagination"] = Pagination(("strategy", "next_url"), ("pointer", "/@odata.nextLink")),
        });

        Assert.Equal(3, rows);
        Assert.Equal(2, server.Requests.Count);
    }

    [Fact]
    public async Task Next_url_refuses_a_body_link_to_a_foreign_host()
    {
        await using var server = new StubHttpServer();
        server.Map("/items", _ => new StubResponse(200,
            """{"data":[{"id":1}],"next":"https://attacker.example.net/steal"}"""));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(server, new()
        {
            ["path"] = "/items",
            ["items"] = "/data",
            ["pagination"] = Pagination(("strategy", "next_url"), ("pointer", "/next")),
        }, new() { ["auth"] = new Dictionary<string, object?> { ["type"] = "bearer", ["token"] = "t" } }));

        Assert.False(ex.IsTransient);
        Assert.Contains("allow_hosts", ex.Message);
        Assert.Single(server.Requests);
    }

    [Fact]
    public async Task Offset_lands_every_row_when_the_api_clamps_the_limit()
    {
        // Asked for 100 per page, the API serves at most 2: advancing by the requested size would
        // skip rows 3..5 entirely.
        await using var server = new StubHttpServer();
        var all = Enumerable.Range(1, 5).Select(i => $$"""{"id":{{i}}}""").ToArray();
        server.Map("/items", req =>
        {
            var offset = int.Parse(QueryValue(req.Url, "offset"));
            return new StubResponse(200, $$"""{"data":[{{string.Join(',', all.Skip(offset).Take(2))}}]}""");
        });

        var rows = await ReadAllAsync(server, new()
        {
            ["path"] = "/items",
            ["items"] = "/data",
            ["pagination"] = Pagination(("strategy", "offset"), ("size", 100L)),
        });

        Assert.Equal(5, rows);
        Assert.Equal(["0", "2", "4", "5"], server.Requests.Select(r => QueryValue(r.Url, "offset")));
        Assert.All(server.Requests, r => Assert.Equal("100", QueryValue(r.Url, "limit")));
    }

    [Fact]
    public async Task Offset_with_stop_on_short_page_skips_the_trailing_empty_request()
    {
        await using var server = new StubHttpServer();
        server.Map("/items", req => QueryValue(req.Url, "skip") == "0"
            ? new StubResponse(200, """[{"id":1},{"id":2}]""")
            : new StubResponse(200, """[{"id":3}]"""));

        var rows = await ReadAllAsync(server, new()
        {
            ["path"] = "/items",
            ["pagination"] = Pagination(("strategy", "offset"), ("param", "skip"), ("size_param", "top"),
                ("size", 2L), ("stop_on_short_page", true)),
        });

        Assert.Equal(3, rows);
        Assert.Equal(2, server.Requests.Count);
        Assert.Equal("2", QueryValue(server.Requests[0].Url, "top"));
    }

    [Fact]
    public void Next_url_requires_a_pointer()
    {
        var ex = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(new DatasetSpec("s", "d", new Dictionary<string, object?>
        {
            ["path"] = "/items",
            ["pagination"] = Pagination(("strategy", "next_url")),
        })));
        Assert.Contains("'next_url' requires 'pointer'", ex.Message);
    }

    [Fact]
    public void Offset_refuses_a_negative_start_and_a_non_positive_size_together()
    {
        var ex = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(new DatasetSpec("s", "d", new Dictionary<string, object?>
        {
            ["path"] = "/items",
            ["pagination"] = Pagination(("strategy", "offset"), ("start", -1L), ("size", 0L)),
        })));
        Assert.Contains("'start'", ex.Message);
        Assert.Contains("'size'", ex.Message);
    }

    [Fact]
    public void Unknown_strategy_lists_every_accepted_one()
    {
        var ex = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(new DatasetSpec("s", "d", new Dictionary<string, object?>
        {
            ["path"] = "/items",
            ["pagination"] = Pagination(("strategy", "bogus")),
        })));
        Assert.Contains("accepted: page, offset, link_header, cursor, next_url", ex.Message);
    }

    [Theory]
    [InlineData("""{"path":"/u","pagination":{"strategy":"next_url","pointer":"/@odata.nextLink"}}""")]
    [InlineData("""{"path":"/u","pagination":{"strategy":"offset","size":50,"size_param":"limit","stop_on_short_page":true}}""")]
    public void Dataset_schema_accepts_the_new_strategies(string json)
    {
        var schema = JsonSchema.FromText(new HttpConnector().DatasetConfigSchema);
        Assert.True(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>(json)).IsValid);
    }
}
