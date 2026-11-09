using System.Buffers.Text;
using System.Text;
using System.Text.Json.Nodes;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

/// <summary>Cursor paging through headers: a token read from a response header (<c>pagination.header</c>),
/// a token sent in a request header (<c>location: header</c>), the v4 checkpoint token that carries it, and
/// the guards (repeats, header injection, collisions with connection headers).</summary>
public sealed class HttpHeaderPagingTests : IAsyncLifetime
{
    private readonly StubHttpServer _server = new();

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync() => await _server.DisposeAsync();

    private static Dictionary<string, object?> Map(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    private static string Query(StubRequest r, string name)
        => System.Web.HttpUtility.ParseQueryString(r.Url.Query)[name] ?? "";

    private static string? Header(StubRequest r, string name) => r.Headers.TryGetValue(name, out var v) ? v : null;

    private static Dictionary<string, string> H(string name, string value) => new() { [name] = value };

    private async Task<(ISource Source, ICheckpointingPartition Partition)> OpenAsync(
        Dictionary<string, object?> options, Dictionary<string, object?>? connectionExtra = null)
    {
        var connection = new Dictionary<string, object?> { ["base_url"] = _server.BaseUrl.ToString() };
        foreach (var (k, v) in connectionExtra ?? [])
        {
            connection[k] = v;
        }

        var source = await new HttpConnector().OpenAsync(new ConnectorConfig(connection), CancellationToken.None);
        var partitions = await source.PlanReadAsync(new DatasetSpec("s", "d", options), ReadHints.None,
            CancellationToken.None);
        return (source, Assert.IsAssignableFrom<ICheckpointingPartition>(Assert.Single(partitions)));
    }

    /// <summary>Reads to the end; returns the row count and every checkpoint offered along the way.</summary>
    private static async Task<(long Rows, List<string> Tokens)> DrainAsync(ICheckpointingPartition partition)
    {
        long rows = 0;
        var tokens = new List<string>();
        await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            rows += batch.Length;
            batch.Dispose();
            if (partition.TryGetCheckpoint(out var token) && token is not null)
            {
                tokens.Add(token);
            }
        }

        return (rows, tokens);
    }

    private async Task<long> ReadAllAsync(Dictionary<string, object?> options,
        Dictionary<string, object?>? connectionExtra = null)
    {
        var (source, partition) = await OpenAsync(options, connectionExtra);
        await using var _ = source;
        return (await DrainAsync(partition)).Rows;
    }

    private static PzConnectorException ParseError(Dictionary<string, object?> options)
        => Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(new DatasetSpec("s", "d", options)));

    /// <summary>/items serves three pages keyed by the token it is given (via <paramref name="tokenOf"/>):
    /// none → page A (next t1), t1 → page B (next t2), t2 → page C (no next). The next token goes in the
    /// X-Next-Cursor response header and in the body's /next.</summary>
    private void MapThreePages(Func<StubRequest, string> tokenOf)
        => _server.Map("/items", r =>
        {
            var (rows, next) = tokenOf(r) switch
            {
                "" => ("""[{"id":1},{"id":2}]""", "t1"),
                "t1" => ("""[{"id":3}]""", "t2"),
                "t2" => ("""[{"id":4}]""", (string?)null),
                var other => throw new InvalidOperationException($"unexpected token '{other}'"),
            };
            var body = next is null ? $$"""{"data":{{rows}}}""" : $$"""{"data":{{rows}},"next":"{{next}}"}""";
            return new StubResponse(200, body, next is null ? null : H("X-Next-Cursor", next));
        });

    [Fact]
    public async Task Response_header_token_is_sent_as_a_query_param()
    {
        MapThreePages(r => Query(r, "cursor"));

        var rows = await ReadAllAsync(Map(("path", "/items"), ("items", "/data"),
            ("pagination", Map(("strategy", "cursor"), ("header", "X-Next-Cursor"), ("param", "cursor")))));

        Assert.Equal(4, rows);
        Assert.Equal(["", "t1", "t2"], _server.Requests.Select(r => Query(r, "cursor")));
    }

    [Fact]
    public async Task Body_token_is_sent_in_a_request_header_and_the_url_stays_put()
    {
        MapThreePages(r => Header(r, "X-Page-Token") ?? "");

        var rows = await ReadAllAsync(Map(("path", "/items"), ("query", Map(("q", "x"))), ("items", "/data"),
            ("pagination", Map(("strategy", "cursor"), ("pointer", "/next"), ("location", "header"),
                ("param", "X-Page-Token")))));

        Assert.Equal(4, rows);
        Assert.Equal([null, "t1", "t2"], _server.Requests.Select(r => Header(r, "X-Page-Token")));
        Assert.All(_server.Requests, r => Assert.Equal("/items?q=x", r.Url.PathAndQuery));
    }

    [Fact]
    public async Task Header_token_goes_back_in_a_header()
    {
        _server.Map("/items", r =>
        {
            var token = Header(r, "x-ms-continuation") ?? "";
            return token switch
            {
                "" => new StubResponse(200, """{"data":[{"id":1}]}""", H("x-ms-continuation", "c1")),
                "c1" => new StubResponse(200, """{"data":[{"id":2}]}""", H("x-ms-continuation", "")),
                _ => new StubResponse(500, "unexpected"),
            };
        });

        var rows = await ReadAllAsync(Map(("path", "/items"), ("items", "/data"),
            ("pagination", Map(("strategy", "cursor"), ("header", "x-ms-continuation"), ("location", "header"),
                ("param", "x-ms-continuation")))));

        Assert.Equal(2, rows);
        Assert.Equal(2, _server.Requests.Count);
    }

    [Fact]
    public async Task Post_read_keeps_its_body_and_sends_the_token_in_a_header()
    {
        MapThreePages(r => Header(r, "X-Page-Token") ?? "");

        var rows = await ReadAllAsync(Map(("path", "/items"), ("method", "post"), ("body", Map(("q", "x"))),
            ("items", "/data"),
            ("pagination", Map(("strategy", "cursor"), ("header", "X-Next-Cursor"), ("location", "header"),
                ("param", "X-Page-Token")))));

        Assert.Equal(4, rows);
        Assert.All(_server.Requests, r => Assert.Equal("""{"q":"x"}""", r.Body));
        Assert.Equal([null, "t1", "t2"], _server.Requests.Select(r => Header(r, "X-Page-Token")));
    }

    [Fact]
    public async Task Body_location_takes_its_token_from_a_response_header()
    {
        MapThreePages(r => JsonNode.Parse(r.Body)?["after"]?.GetValue<string>() ?? "");

        var rows = await ReadAllAsync(Map(("path", "/items"), ("method", "post"), ("body", Map(("q", "x"))),
            ("items", "/data"),
            ("pagination", Map(("strategy", "cursor"), ("header", "X-Next-Cursor"), ("location", "body"),
                ("param", "/after")))));

        Assert.Equal(4, rows);
        Assert.Equal(["""{"q":"x"}""", """{"q":"x","after":"t1"}""", """{"q":"x","after":"t2"}"""],
            _server.Requests.Select(r => r.Body));
    }

    [Fact]
    public async Task A_token_sent_twice_is_a_repeated_request()
    {
        _server.Map("/items", _ => new StubResponse(200, """{"data":[{"id":1}]}""", H("X-Next-Cursor", "tok-xyz")));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(Map(("path", "/items"),
            ("items", "/data"),
            ("pagination", Map(("strategy", "cursor"), ("header", "X-Next-Cursor"), ("location", "header"),
                ("param", "X-Page-Token"))))));

        Assert.False(ex.IsTransient);
        Assert.Contains("not advancing", ex.Message);
        Assert.Contains("page token header", ex.Message);
        Assert.DoesNotContain("tok-xyz", ex.Message);
        Assert.Equal(2, _server.Requests.Count);
    }

    [Theory]
    [InlineData("a\r\nX-Evil: 1")]
    [InlineData("tökén")]
    [InlineData("a\u0001b")]
    public async Task A_token_a_header_cannot_carry_fails_without_echoing_it(string token)
    {
        _server.Map("/items", _ => new StubResponse(200,
            new JsonObject { ["data"] = new JsonArray(new JsonObject { ["id"] = 1 }), ["next"] = token }.ToJsonString()));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(Map(("path", "/items"),
            ("items", "/data"),
            ("pagination", Map(("strategy", "cursor"), ("pointer", "/next"), ("location", "header"),
                ("param", "X-Page-Token"))))));

        Assert.False(ex.IsTransient);
        Assert.Contains("X-Page-Token", ex.Message);
        Assert.DoesNotContain(token, ex.Message);
        Assert.Single(_server.Requests);
    }

    [Fact]
    public async Task A_page_header_the_connection_also_sets_is_refused_before_any_request()
    {
        _server.Map("/items", _ => new StubResponse(200, """{"data":[]}"""));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(Map(("path", "/items"),
            ("items", "/data"),
            ("pagination", Map(("strategy", "cursor"), ("pointer", "/next"), ("location", "header"),
                ("param", "x-page-token")))),
            new() { ["headers"] = Map(("X-Page-Token", "fixed")) }));

        Assert.False(ex.IsTransient);
        Assert.Contains("x-page-token", ex.Message);
        Assert.Contains("headers", ex.Message);
        Assert.Empty(_server.Requests);
    }

    [Fact]
    public async Task A_page_header_the_api_key_auth_also_sets_is_refused()
    {
        // The first request carries the key alone; the second would carry the key and the token.
        _server.Map("/items", _ => new StubResponse(200, """{"data":[{"id":1}],"next":"t1"}"""));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(Map(("path", "/items"),
            ("items", "/data"),
            ("pagination", Map(("strategy", "cursor"), ("pointer", "/next"), ("location", "header"),
                ("param", "X-Page-Token")))),
            new() { ["auth"] = Map(("type", "api_key"), ("key", "k3y-secret"), ("header", "X-Page-Token")) }));

        Assert.False(ex.IsTransient, ex.Message);
        Assert.Single(_server.Requests);
        Assert.Contains("X-Page-Token", ex.Message);
        Assert.Contains("auth", ex.Message);
        Assert.DoesNotContain("k3y-secret", ex.Message);
    }

    [Fact]
    public async Task Checkpoint_is_a_v4_token_with_only_the_page_header_and_resumes_there()
    {
        MapThreePages(r => Header(r, "X-Page-Token") ?? "");
        var options = Map(("path", "/items"), ("items", "/data"),
            ("pagination", Map(("strategy", "cursor"), ("pointer", "/next"), ("location", "header"),
                ("param", "X-Page-Token"))));
        var secrets = new Dictionary<string, object?>
        {
            ["headers"] = Map(("X-Tenant-Key", "tenant-s3cret")),
            ["auth"] = Map(("type", "bearer"), ("token", "bearer-s3cret")),
        };

        var (source, partition) = await OpenAsync(options, secrets);
        List<string> tokens;
        await using (source)
        {
            tokens = (await DrainAsync(partition)).Tokens;
        }

        Assert.Equal(2, tokens.Count);
        Assert.All(tokens, t => Assert.StartsWith("v4:", t));
        var decoded = Encoding.UTF8.GetString(Base64Url.DecodeFromChars(tokens[0].AsSpan(3)));
        Assert.Contains("\"X-Page-Token\"", decoded);
        Assert.Contains("\"t1\"", decoded);
        Assert.DoesNotContain("s3cret", decoded);
        Assert.DoesNotContain("X-Tenant-Key", decoded);

        var before = _server.Requests.Count;
        var (again, resumed) = await OpenAsync(options, secrets);
        await using (again)
        {
            Assert.True(resumed.TryResumeFrom(tokens[1]));
            Assert.Equal(1, (await DrainAsync(resumed)).Rows);
        }

        Assert.Equal("t2", Header(Assert.Single(_server.Requests.Skip(before)), "X-Page-Token"));
    }

    [Fact]
    public async Task V4_tokens_are_refused_by_a_dataset_that_does_not_page_by_that_header()
    {
        var url = new Uri(_server.BaseUrl, "/items");
        var v4 = HttpCheckpointToken.Encode(
            new HttpPageRequest(HttpMethod.Get, url, null, null) { PageHeader = ("X-Page-Token", "t1") }, isPost: false);
        Assert.StartsWith("v4:", v4);

        var (plain, plainPartition) = await OpenAsync(Map(("path", "/items"), ("items", "/data"),
            ("pagination", Map(("strategy", "cursor"), ("pointer", "/next"), ("param", "cursor")))));
        await using (plain)
        {
            Assert.False(plainPartition.TryResumeFrom(v4));
        }

        var (other, otherPartition) = await OpenAsync(Map(("path", "/items"), ("items", "/data"),
            ("pagination", Map(("strategy", "cursor"), ("pointer", "/next"), ("location", "header"),
                ("param", "X-Other")))));
        await using (other)
        {
            Assert.False(otherPartition.TryResumeFrom(v4));
        }

        var (same, samePartition) = await OpenAsync(Map(("path", "/items"), ("items", "/data"),
            ("pagination", Map(("strategy", "cursor"), ("pointer", "/next"), ("location", "header"),
                ("param", "x-page-token")))));
        await using (same)
        {
            Assert.True(samePartition.TryResumeFrom(v4));
            // The header-less form is a first page (a for_each list token names one).
            Assert.True(samePartition.TryResumeFrom(url.ToString()));
        }
    }

    [Fact]
    public void V4_token_round_trips_a_post_body_and_refuses_garbage()
    {
        var request = new HttpPageRequest(HttpMethod.Post, new Uri("https://h.example/s"),
            JsonNode.Parse("""{"q":1}"""), null) { PageHeader = ("X-Page-Token", "abc") };

        var token = HttpCheckpointToken.Encode(request, isPost: true);

        Assert.StartsWith("v4:", token);
        Assert.True(HttpCheckpointToken.TryDecode(token, true, HttpMethod.Post, HttpCheckpointToken.BodyKind.Json,
            out var back, pageHeader: "X-Page-Token"));
        Assert.True(back!.SameAs(request));
        Assert.False(HttpCheckpointToken.TryDecode(token, true, HttpMethod.Post, HttpCheckpointToken.BodyKind.Json,
            out _));
        Assert.False(HttpCheckpointToken.TryDecode("v4:not-base64!", true, HttpMethod.Post,
            HttpCheckpointToken.BodyKind.Json, out _, pageHeader: "X-Page-Token"));
        var noHeader = "v4:" + Base64Url.EncodeToString(Encoding.UTF8.GetBytes("""{"url":"https://h.example/s","body":{}}"""));
        Assert.False(HttpCheckpointToken.TryDecode(noHeader, true, HttpMethod.Post, HttpCheckpointToken.BodyKind.Json,
            out _, pageHeader: "X-Page-Token"));
        var badValue = "v4:" + Base64Url.EncodeToString(Encoding.UTF8.GetBytes(
            """{"url":"https://h.example/s","body":{},"header":{"name":"X-Page-Token","value":"a\nb"}}"""));
        Assert.False(HttpCheckpointToken.TryDecode(badValue, true, HttpMethod.Post, HttpCheckpointToken.BodyKind.Json,
            out _, pageHeader: "X-Page-Token"));
    }

    [Fact]
    public void Request_identity_includes_the_page_header()
    {
        var url = new Uri("https://h.example/items");
        var a = new HttpPageRequest(HttpMethod.Get, url, null, null) { PageHeader = ("X-Page-Token", "a") };
        var b = a with { PageHeader = ("X-Page-Token", "b") };
        var none = new HttpPageRequest(HttpMethod.Get, url, null, null);

        Assert.False(a.SameAs(b));
        Assert.False(a.SameAs(none));
        Assert.NotEqual(a.KeyDigest, b.KeyDigest);
        Assert.Equal(none.KeyDigest, new HttpPageRequest(HttpMethod.Get, url, null, null).KeyDigest);
        Assert.True(a.SameAs(a with { }));
        Assert.Equal(("X-Page-Token", "a"), a.RedirectedTo(new Uri("https://h.example/moved"), 302).PageHeader);
    }

    [Fact]
    public async Task A_for_each_list_pages_by_header()
    {
        _server.Map("/orders", r => (Header(r, "X-Page-Token") ?? "") switch
        {
            "" => new StubResponse(200, """{"data":[{"id":"o1"}]}""", H("X-Next", "p2")),
            "p2" => new StubResponse(200, """{"data":[{"id":"o2"}]}"""),
            _ => new StubResponse(500, "unexpected"),
        });
        _server.MapPrefix("/orders/", r => new StubResponse(200,
            $$"""{"lines":[{"sku":"{{r.Url.AbsolutePath.Split('/')[2]}}"}]}"""));

        var rows = await ReadAllAsync(Map(("path", "/orders/{{ item.order_id }}/lines"), ("items", "/lines"),
            ("for_each", Map(("path", "/orders"), ("items", "/data"), ("fields", Map(("order_id", "/id"))),
                ("pagination", Map(("strategy", "cursor"), ("header", "X-Next"), ("location", "header"),
                    ("param", "X-Page-Token")))))));

        Assert.Equal(2, rows);
    }

    [Fact]
    public void Cursor_needs_exactly_one_token_source()
    {
        var both = ParseError(Map(("path", "/items"),
            ("pagination", Map(("strategy", "cursor"), ("pointer", "/n"), ("header", "X-N"), ("param", "c")))));
        var neither = ParseError(Map(("path", "/items"),
            ("pagination", Map(("strategy", "cursor"), ("param", "c")))));

        Assert.Contains("exactly one of 'pointer' or 'header'", both.Message);
        Assert.Contains("exactly one of 'pointer' or 'header'", neither.Message);
    }

    [Fact]
    public void Header_options_are_validated_together()
    {
        var ex = ParseError(Map(("path", "/items"),
            ("pagination", Map(("strategy", "page"), ("header", "X-N"), ("location", "header")))));

        Assert.Contains("'header' is only valid with strategy 'cursor'", ex.Message);
        Assert.Contains("'location: header' is only valid with strategy 'cursor'", ex.Message);
    }

    [Fact]
    public void Location_is_checked_on_get_reads_too()
    {
        var ex = ParseError(Map(("path", "/items"),
            ("pagination", Map(("strategy", "cursor"), ("pointer", "/n"), ("param", "c"), ("location", "cookie")))));

        Assert.Contains("'location' must be 'query', 'body' or 'header'", ex.Message);
    }

    [Theory]
    [InlineData("Authorization")]
    [InlineData("content-type")]
    [InlineData("Host")]
    [InlineData("Cookie")]
    public void A_header_pz_owns_cannot_carry_the_token(string name)
    {
        var ex = ParseError(Map(("path", "/items"),
            ("pagination", Map(("strategy", "cursor"), ("pointer", "/n"), ("location", "header"), ("param", name)))));

        Assert.Contains($"'{name}'", ex.Message);
        Assert.Contains("cannot carry the page token", ex.Message);
    }

    [Theory]
    [InlineData("X Page")]
    [InlineData("X-Page:")]
    [InlineData("")]
    public void Header_names_must_be_http_tokens(string name)
    {
        var send = ParseError(Map(("path", "/items"),
            ("pagination", Map(("strategy", "cursor"), ("pointer", "/n"), ("location", "header"), ("param", name)))));
        var read = ParseError(Map(("path", "/items"),
            ("pagination", Map(("strategy", "cursor"), ("header", name), ("param", "c")))));

        Assert.Contains("not a valid HTTP header name", send.Message);
        Assert.Contains("not a valid HTTP header name", read.Message);
    }

    [Theory]
    [InlineData("""{"path":"/u","pagination":{"strategy":"cursor","header":"X-Next","param":"c"}}""", true)]
    [InlineData("""{"path":"/u","pagination":{"strategy":"cursor","pointer":"/n","location":"header","param":"X-T"}}""", true)]
    [InlineData("""{"path":"/u","for_each":{"path":"/o","fields":{"id":"/id"},"pagination":{"strategy":"cursor","header":"X-N","location":"header","param":"X-T"}}}""", true)]
    [InlineData("""{"path":"/u","pagination":{"strategy":"cursor","pointer":"/n","location":"cookie","param":"c"}}""", false)]
    public void Dataset_schema_knows_the_header_options(string json, bool valid)
    {
        var schema = Json.Schema.JsonSchema.FromText(new HttpConnector().DatasetConfigSchema);
        Assert.Equal(valid, schema.Evaluate(System.Text.Json.JsonSerializer.Deserialize<System.Text.Json.JsonElement>(json)).IsValid);
    }
}
