using System.Text.Json.Nodes;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

/// <summary>POST reads end to end through a real partition against the stub server: the body on the
/// wire, body pagination, redirects, the request-level guards and <c>error_pointer</c>.</summary>
public class HttpPostReadTests
{
    private static async Task<long> ReadAllAsync(StubHttpServer server, Dictionary<string, object?> options,
        string? watermarkCursor = null)
    {
        var connection = new Dictionary<string, object?> { ["base_url"] = server.BaseUrl.ToString() };
        await using var source = await new HttpConnector().OpenAsync(new ConnectorConfig(connection),
            CancellationToken.None);
        var partitions = await source.PlanReadAsync(
            new DatasetSpec("s", "d", options) { WatermarkCursor = watermarkCursor }, ReadHints.None,
            CancellationToken.None);
        long rows = 0;
        await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        return rows;
    }

    private static Dictionary<string, object?> Post(string path, object? body, params (string Key, object? Value)[] extra)
    {
        var options = new Dictionary<string, object?> { ["path"] = path, ["method"] = "post", ["body"] = body };
        foreach (var (k, v) in extra)
        {
            options[k] = v;
        }

        return options;
    }

    private static Dictionary<string, object?> Map(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    private static JsonNode BodyOf(StubRequest request) => JsonNode.Parse(request.Body)!;

    [Fact]
    public async Task A_post_read_sends_the_json_body()
    {
        await using var server = new StubHttpServer();
        server.Map("/search", _ => new StubResponse(200, """{"items":[{"id":1},{"id":2}]}"""));

        var rows = await ReadAllAsync(server, Post("/search",
            Map(("size", 500L), ("q", "x"), ("flags", new List<object?> { true, 1.5 })), ("items", "/items")));

        Assert.Equal(2, rows);
        var request = Assert.Single(server.Requests);
        Assert.Equal("POST", request.Method);
        Assert.StartsWith("application/json", request.Headers["Content-Type"]);
        Assert.Equal("""{"size":500,"q":"x","flags":[true,1.5]}""", BodyOf(request).ToJsonString());
    }

    [Fact]
    public async Task Form_and_string_bodies_are_sent_as_declared()
    {
        await using var server = new StubHttpServer();
        server.Map("/form", _ => new StubResponse(200, """[{"id":1}]"""));
        server.Map("/graphql", _ => new StubResponse(200, """[{"id":1}]"""));

        await ReadAllAsync(server, Post("/form", Map(("a", 1L), ("b", "x")), ("body_format", "form")));
        await ReadAllAsync(server, Post("/graphql", "{ items { id } }", ("content_type", "application/graphql")));

        var form = server.Requests.Single(r => r.Url.AbsolutePath == "/form");
        Assert.Equal("POST", form.Method);
        Assert.Equal("a=1&b=x", form.Body);
        Assert.StartsWith("application/x-www-form-urlencoded", form.Headers["Content-Type"]);

        var text = server.Requests.Single(r => r.Url.AbsolutePath == "/graphql");
        Assert.Equal("POST", text.Method);
        Assert.Equal("{ items { id } }", text.Body);
        Assert.StartsWith("application/graphql", text.Headers["Content-Type"]);
    }

    [Fact]
    public async Task A_content_type_with_parameters_is_sent_as_written()
    {
        await using var server = new StubHttpServer();
        server.Map("/search", _ => new StubResponse(200, "[]"));

        await ReadAllAsync(server, Post("/search", Map(("q", "x")), ("content_type", "application/json; charset=utf-8")));

        Assert.Equal("application/json; charset=utf-8", Assert.Single(server.Requests).Headers["Content-Type"]);
    }

    [Fact]
    public async Task Offset_in_body_pages_through_and_stops_on_empty()
    {
        await using var server = new StubHttpServer();
        var all = Enumerable.Range(1, 5).Select(i => $$"""{"id":{{i}}}""").ToArray();
        server.Map("/_search", req =>
        {
            var from = BodyOf(req)["from"]!.GetValue<int>();
            return new StubResponse(200, $$$"""{"hits":{"hits":[{{{string.Join(',', all.Skip(from).Take(2))}}}]}}""");
        });

        var rows = await ReadAllAsync(server, Post("/_search", Map(("query", Map(("match_all", Map())))),
            ("items", "/hits/hits"),
            ("pagination", Map(("strategy", "offset"), ("location", "body"), ("param", "/from"),
                ("size_param", "/size"), ("size", 2L)))));

        Assert.Equal(5, rows);
        Assert.Equal([0, 2, 4, 5], server.Requests.Select(r => BodyOf(r)["from"]!.GetValue<int>()));
        Assert.All(server.Requests, r => Assert.Equal(2, BodyOf(r)["size"]!.GetValue<int>()));
        Assert.All(server.Requests, r => Assert.Equal("/_search", r.Url.PathAndQuery));
    }

    [Fact]
    public async Task Graphql_cursor_in_variables_pages_through()
    {
        await using var server = new StubHttpServer();
        server.Map("/graphql", req =>
        {
            var after = BodyOf(req)["variables"]?["after"]?.GetValue<string>();
            return after switch
            {
                null => new StubResponse(200,
                    """{"data":{"items":{"nodes":[{"id":1},{"id":2}],"pageInfo":{"endCursor":"c1"}}}}"""),
                "c1" => new StubResponse(200,
                    """{"data":{"items":{"nodes":[{"id":3}],"pageInfo":{"endCursor":"c2"}}}}"""),
                _ => new StubResponse(200,
                    """{"data":{"items":{"nodes":[{"id":4}],"pageInfo":{"endCursor":null}}}}"""),
            };
        });

        var rows = await ReadAllAsync(server, Post("/graphql",
            Map(("query", "query($after: String) { items(after: $after) { id } }"), ("variables", Map())),
            ("items", "/data/items/nodes"),
            ("pagination", Map(("strategy", "cursor"), ("location", "body"), ("param", "/variables/after"),
                ("pointer", "/data/items/pageInfo/endCursor")))));

        Assert.Equal(4, rows);
        Assert.Equal(3, server.Requests.Count);
        Assert.Equal([null, "c1", "c2"],
            server.Requests.Select(r => BodyOf(r)["variables"]?["after"]?.GetValue<string>()));
    }

    [Fact]
    public async Task Next_url_re_posts_the_body()
    {
        await using var server = new StubHttpServer();
        server.Map("/search", req => req.Url.Query.Contains("page=2")
            ? new StubResponse(200, """{"items":[{"id":2}]}""")
            : new StubResponse(200, $$"""{"items":[{"id":1}],"next":"{{server.BaseUrl}}search?page=2"}"""));

        var rows = await ReadAllAsync(server, Post("/search", Map(("q", "x")), ("items", "/items"),
            ("pagination", Map(("strategy", "next_url"), ("pointer", "/next")))));

        Assert.Equal(2, rows);
        Assert.Equal(2, server.Requests.Count);
        Assert.All(server.Requests, r => Assert.Equal("POST", r.Method));
        Assert.Equal(server.Requests[0].Body, server.Requests[1].Body);
        Assert.Equal("""{"q":"x"}""", server.Requests[1].Body);
        Assert.Contains("page=2", server.Requests[1].Url.Query);
    }

    [Fact]
    public async Task Redirect_307_keeps_post_and_body_303_switches_to_get()
    {
        await using var server = new StubHttpServer();
        server.Map("/moved", _ => new StubResponse(307, "",
            new Dictionary<string, string> { ["Location"] = "/target" }));
        server.Map("/see-other", _ => new StubResponse(303, "",
            new Dictionary<string, string> { ["Location"] = "/result" }));
        server.Map("/target", _ => new StubResponse(200, """[{"id":1}]"""));
        server.Map("/result", _ => new StubResponse(200, """[{"id":1}]"""));

        await ReadAllAsync(server, Post("/moved", Map(("q", "kept"))));
        await ReadAllAsync(server, Post("/see-other", Map(("q", "dropped"))));

        var hops = server.Requests.Select(r => (r.Method, r.Url.AbsolutePath, r.Body)).ToArray();
        Assert.Equal(
        [
            ("POST", "/moved", """{"q":"kept"}"""),
            ("POST", "/target", """{"q":"kept"}"""),
            ("POST", "/see-other", """{"q":"dropped"}"""),
            ("GET", "/result", ""),
        ], hops);
    }

    [Fact]
    public async Task A_post_next_link_to_a_foreign_host_is_refused_before_sending()
    {
        await using var server = new StubHttpServer();
        server.Map("/search", _ => new StubResponse(200,
            """{"items":[{"id":1}],"next":"https://attacker.example.net/steal"}"""));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(server,
            Post("/search", Map(("q", "x")), ("items", "/items"),
                ("pagination", Map(("strategy", "next_url"), ("pointer", "/next"))))));

        Assert.False(ex.IsTransient);
        Assert.Contains("allow_hosts", ex.Message);
        Assert.Single(server.Requests);
    }

    [Fact]
    public async Task Body_paging_that_does_not_advance_fails_fast()
    {
        await using var server = new StubHttpServer();
        // The endpoint ignores the cursor and always hands back the same token.
        server.Map("/search", _ => new StubResponse(200, """{"items":[{"id":1}],"next":"t1"}"""));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(server,
            Post("/search", Map(("q", "SECRET-MARKER-123")), ("items", "/items"),
                ("pagination", Map(("strategy", "cursor"), ("location", "body"), ("param", "/after"),
                    ("pointer", "/next"))))));

        Assert.False(ex.IsTransient);
        Assert.Contains("not advancing", ex.Message);
        Assert.DoesNotContain("SECRET-MARKER-123", ex.Message);
        Assert.Equal(2, server.Requests.Count);
    }

    [Fact]
    public async Task Max_pages_on_a_body_paged_incremental_read_sets_truncation()
    {
        await using var server = new StubHttpServer();
        string[] pages = ["""[{"id":6},{"id":5}]""", """[{"id":4},{"id":3}]""", """[{"id":2}]"""];
        server.Map("/search", req =>
        {
            var page = BodyOf(req)["page"]!.GetValue<int>();
            return new StubResponse(200, page <= pages.Length ? pages[page - 1] : "[]");
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(server,
            Post("/search", Map(("q", "x")), ("cursor", "id"), ("cursor_type", "bigint"), ("max_pages", 2L),
                ("pagination", Map(("strategy", "page"), ("location", "body"), ("param", "/page")))),
            watermarkCursor: "id"));

        Assert.False(ex.IsTransient);
        Assert.Contains("not ascending", ex.Message);
        Assert.Contains("max_pages", ex.Message);
    }

    [Fact]
    public async Task Body_paged_reads_record_checkpoint_boundaries()
    {
        await using var server = new StubHttpServer();
        server.Map("/search", req => (BodyOf(req)["after"]?.GetValue<string>()) switch
        {
            null => new StubResponse(200, """{"items":[{"id":1},{"id":2}],"next":"c1"}"""),
            "c1" => new StubResponse(200, """{"items":[{"id":3},{"id":4}],"next":"c2"}"""),
            _ => new StubResponse(200, """{"items":[{"id":5}]}"""),
        });

        var connection = new Dictionary<string, object?> { ["base_url"] = server.BaseUrl.ToString() };
        await using var source = await new HttpConnector().OpenAsync(new ConnectorConfig(connection),
            CancellationToken.None);
        var partitions = await source.PlanReadAsync(new DatasetSpec("s", "d", Post("/search", Map(("q", "x")),
            ("items", "/items"),
            ("pagination", Map(("strategy", "cursor"), ("location", "body"), ("param", "/after"),
                ("pointer", "/next"))))), ReadHints.None, CancellationToken.None);
        var partition = Assert.IsAssignableFrom<ICheckpointingPartition>(partitions[0]);

        var tokens = new List<string>();
        await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            batch.Dispose();
            if (partition.TryGetCheckpoint(out var token))
            {
                tokens.Add(token!);
            }
        }

        Assert.Equal(2, tokens.Count);
        Assert.NotEqual(tokens[0], tokens[1]);
    }

    [Fact]
    public async Task Graphql_errors_with_status_200_fail_the_read()
    {
        await using var server = new StubHttpServer();
        server.Map("/graphql", _ => new StubResponse(200,
            """{"errors":[{"message":"Field 'x' doesn't exist"}],"data":null}"""));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(server,
            Post("/graphql", Map(("query", "{ x }")), ("items", "/data/items"), ("error_pointer", "/errors"))));

        Assert.False(ex.IsTransient);
        Assert.Contains("/errors", ex.Message);
        Assert.Contains("Field 'x'", ex.Message);
    }

    [Fact]
    public async Task Graphql_partial_data_with_errors_fails_too()
    {
        await using var server = new StubHttpServer();
        server.Map("/graphql", _ => new StubResponse(200,
            """{"data":{"items":[{"id":1}]},"errors":[{"message":"items.2 not authorised"}]}"""));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(server,
            Post("/graphql", Map(("query", "{ items { id } }")), ("items", "/data/items"),
                ("error_pointer", "/errors"))));

        Assert.False(ex.IsTransient);
        Assert.Contains("not authorised", ex.Message);
    }

    [Fact]
    public async Task Empty_errors_array_does_not_fail()
    {
        await using var server = new StubHttpServer();
        server.Map("/graphql", _ => new StubResponse(200, """{"data":{"items":[{"id":1},{"id":2}]},"errors":[]}"""));

        var rows = await ReadAllAsync(server, Post("/graphql", Map(("query", "{ items { id } }")),
            ("items", "/data/items"), ("error_pointer", "/errors")));

        Assert.Equal(2, rows);
    }

    [Fact]
    public async Task No_message_contains_the_request_body()
    {
        await using var server = new StubHttpServer();
        server.Map("/search", _ => new StubResponse(400, """{"error":"bad request"}"""));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(server,
            Post("/search", Map(("q", "SECRET-MARKER-123")))));

        Assert.False(ex.IsTransient);
        Assert.Contains("400", ex.Message);
        Assert.DoesNotContain("SECRET-MARKER-123", ex.Message);
    }

    [Fact]
    public async Task Get_reads_are_unchanged()
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
            ["pagination"] = Map(("strategy", "next_url"), ("pointer", "/@odata.nextLink")),
        });

        Assert.Equal(3, rows);
        Assert.Equal(["/users", "/users?$skiptoken=B"], server.Requests.Select(r => r.Url.PathAndQuery));
        Assert.All(server.Requests, r =>
        {
            Assert.Equal("GET", r.Method);
            Assert.Equal("", r.Body);
            Assert.False(r.Headers.ContainsKey("Content-Type"));
        });
    }
}
