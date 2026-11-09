using System.Buffers.Text;
using System.Text;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

/// <summary>Where header paging, <c>for_each.values</c>/<c>dedupe</c> and <c>stop_at_watermark</c> meet the session
/// sign-in, <c>on_status</c> rules, <c>epoch_unit</c>, <c>watermark_format</c> and column pointers.</summary>
public sealed class HttpPagingIntegrationTests : IAsyncLifetime
{
    private readonly StubHttpServer _server = new();
    private readonly List<string> _notices = [];

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync() => await _server.DisposeAsync();

    private static Dictionary<string, object?> Map(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    private static string Query(StubRequest r, string name)
        => System.Web.HttpUtility.ParseQueryString(r.Url.Query)[name] ?? "";

    private static string? Header(StubRequest r, string name) => r.Headers.TryGetValue(name, out var v) ? v : null;

    private static Dictionary<string, object?> Session(Dictionary<string, object?> inject) => Map(
        ("type", "session"),
        ("login", Map(("path", "/auth/login"), ("body", Map(("user", "me"), ("password", "pw"))))),
        ("token_pointer", "/session/token"), ("ttl", "30m"), ("inject", inject));

    private void MapLogin()
        => _server.Map("/auth/login", _ => new StubResponse(200, """{"session":{"token":"sess-tok"}}"""));

    private async Task<(ISource Source, ICheckpointingPartition Partition)> OpenAsync(DatasetSpec spec,
        Dictionary<string, object?>? connectionExtra = null)
    {
        var connection = new Dictionary<string, object?> { ["base_url"] = _server.BaseUrl.ToString() };
        foreach (var (k, v) in connectionExtra ?? [])
        {
            connection[k] = v;
        }

        var source = await new HttpConnector().OpenAsync(new ConnectorConfig(connection), CancellationToken.None);
        ((INoticeAware)source).UseNotice(_notices.Add);
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        return (source, Assert.IsAssignableFrom<ICheckpointingPartition>(Assert.Single(partitions)));
    }

    private static async Task<(List<object?[]> Rows, List<string> Tokens)> Drain(ICheckpointingPartition partition)
    {
        var rows = new List<object?[]>();
        var tokens = new List<string>();
        await foreach (var batch in partition.ReadAsync(new BatchOptions(), CancellationToken.None))
        {
            using (batch)
            {
                for (var r = 0; r < batch.Length; r++)
                {
                    var row = new object?[batch.ColumnCount];
                    for (var c = 0; c < batch.ColumnCount; c++)
                    {
                        row[c] = FileReaderRows.Value(batch.Column(c), r);
                    }

                    rows.Add(row);
                }
            }

            if (partition.TryGetCheckpoint(out var token) && token is not null)
            {
                tokens.Add(token);
            }
        }

        return (rows, tokens);
    }

    private async Task<(List<object?[]> Rows, List<string> Tokens)> Read(DatasetSpec spec,
        Dictionary<string, object?>? connectionExtra = null, string? resumeFrom = null)
    {
        var (source, partition) = await OpenAsync(spec, connectionExtra);
        await using (source)
        {
            if (resumeFrom is not null)
            {
                Assert.True(partition.TryResumeFrom(resumeFrom));
            }

            return await Drain(partition);
        }
    }

    private static Dictionary<string, object?> HeaderPaged(string header) => Map(("path", "/items"), ("items", "/data"),
        ("pagination", Map(("strategy", "cursor"), ("pointer", "/next"), ("location", "header"), ("param", header))));

    [Fact]
    public async Task A_session_header_that_is_also_the_page_header_is_refused_at_plan()
    {
        // The session sign-in replaces its header on every request, so the page token would silently vanish.
        MapLogin();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(new DatasetSpec("s", "d",
            HeaderPaged("X-Session")), new() { ["auth"] = Session(Map(("header", "x-session"))) }));

        Assert.False(ex.IsTransient);
        Assert.Contains("X-Session", ex.Message);
        Assert.Contains("auth.inject.header", ex.Message);
        Assert.Empty(_server.Requests);
    }

    [Fact]
    public async Task A_session_query_token_stays_out_of_v4_tokens_and_a_resume_sends_the_current_one()
    {
        MapLogin();
        _server.Map("/items", r => Query(r, "s") != "sess-tok"
            ? new StubResponse(401, "no session")
            : (Header(r, "X-Page-Token") ?? "") switch
            {
                "" => new StubResponse(200, """{"data":[{"id":1}],"next":"t1"}"""),
                "t1" => new StubResponse(200, """{"data":[{"id":2}],"next":"t2"}"""),
                _ => new StubResponse(200, """{"data":[{"id":3}]}"""),
            });
        var spec = new DatasetSpec("s", "d", HeaderPaged("X-Page-Token"));
        var auth = new Dictionary<string, object?> { ["auth"] = Session(Map(("query", "s"))) };

        var (_, tokens) = await Read(spec, auth);

        Assert.Equal(2, tokens.Count);
        var decoded = Encoding.UTF8.GetString(Base64Url.DecodeFromChars(tokens[1].AsSpan(3)));
        Assert.StartsWith("v4:", tokens[1]);
        Assert.DoesNotContain("sess-tok", decoded);
        Assert.Contains("\"t2\"", decoded);

        var before = _server.Requests.Count(r => r.Url.AbsolutePath == "/items");
        var (rows, _) = await Read(spec, auth, resumeFrom: tokens[1]);

        Assert.Single(rows);
        var resumed = _server.Requests.Where(r => r.Url.AbsolutePath == "/items").Skip(before).Single();
        Assert.Equal("sess-tok", Query(resumed, "s"));
        Assert.Equal("t2", Header(resumed, "X-Page-Token"));
    }

    [Fact]
    public async Task Stop_at_watermark_compares_epoch_cursors_as_instants_and_the_query_renders_epoch_seconds()
    {
        // 2026-01-01T00:00:00Z is 1767225600.
        _server.Map("/events", r => Query(r, "cursor") == "2"
            ? new StubResponse(200, """{"data":[{"ts":1767225606},{"ts":1767225605},{"ts":1767225604}],"next":"3"}""")
            : new StubResponse(200, """{"data":[{"ts":1767225610},{"ts":1767225609}],"next":"2"}"""));
        var options = Map(("path", "/events"), ("items", "/data"), ("query", Map(("since", "{{ watermark }}"))),
            ("pagination", Map(("strategy", "cursor"), ("pointer", "/next"), ("param", "cursor"))),
            ("cursor", "ts"), ("cursor_type", "timestamp"), ("epoch_unit", "s"), ("watermark_format", "epoch_s"),
            ("cursor_order", "desc"), ("stop_at_watermark", true));

        var (rows, _) = await Read(new DatasetSpec("s", "d", options)
        {
            WatermarkCursor = "ts",
            WatermarkValue = "2026-01-01T00:00:05.000000",
        });

        Assert.Equal([10L, 9L, 6L], rows.Select(r => ((DateTimeOffset)r[3]!).ToUnixTimeSeconds() - 1767225600));
        Assert.Equal(2, _server.Requests.Count);
        Assert.All(_server.Requests, r => Assert.Equal("1767225605", Query(r, "since")));
        Assert.Contains("dropped 2 records", Assert.Single(_notices));
    }

    [Fact]
    public async Task Stop_at_watermark_reads_a_cursor_column_through_its_pointer()
    {
        _server.Map("/events", _ => new StubResponse(200, """
            {"data":[{"id":3,"meta":{"at":"2026-01-01T00:00:09Z"}},{"id":2,"meta":{"at":"2026-01-01T00:00:05Z"}},
                     {"id":1,"meta":{"at":"2026-01-01T00:00:01Z"}}]}
            """));
        var options = Map(("path", "/events"), ("items", "/data"), ("cursor", "at"), ("cursor_order", "desc"),
            ("stop_at_watermark", true),
            ("columns", new Dictionary<string, string> { ["id"] = "int", ["at"] = "timestamp" }),
            ("column_pointers", Map(("at", "/meta/at"))));

        var (rows, _) = await Read(new DatasetSpec("s", "d", options)
        {
            WatermarkCursor = "at",
            WatermarkValue = "2026-01-01T00:00:05.000000",
        });

        Assert.Equal([3], rows.Select(r => (int)r[0]!));
        Assert.Contains("dropped 2 records", Assert.Single(_notices));
    }

    [Fact]
    public async Task Dedupe_skips_before_any_request_and_an_on_status_skip_counts_separately()
    {
        _server.MapPrefix("/regions/", r => r.Url.AbsolutePath == "/regions/b/sales"
            ? new StubResponse(404, "no such region")
            : new StubResponse(200, $$"""{"data":[{"sale":"{{r.Url.AbsolutePath.Split('/')[2]}}"}]}"""));
        var options = Map(("path", "/regions/{{ item.region }}/sales"), ("items", "/data"),
            ("on_status", new List<object?> { Map(("status", 404L), ("action", "skip")) }),
            ("for_each", Map(("dedupe", true),
                ("values", new List<object?> { Map(("region", "a")), Map(("region", "b")), Map(("region", "a")),
                    Map(("region", "b")), Map(("region", "c")) }))));

        var (rows, _) = await Read(new DatasetSpec("s", "d", options));

        Assert.Equal(2, rows.Count);
        Assert.Equal(["/regions/a/sales", "/regions/b/sales", "/regions/c/sales"],
            _server.Requests.Select(r => r.Url.AbsolutePath));
        Assert.Contains(_notices, n => n.Contains("skipped 1 items whose first request matched an on_status skip rule (first: item region=b)"));
        Assert.Contains(_notices, n => n.Contains("for_each.dedupe skipped 2 duplicate list records (first: item region=a)"));
    }

    public static TheoryData<string, Dictionary<string, object?>> QueryPagingClashes() => new()
    {
        { "page", Map(("path", "/items"), ("pagination", Map(("strategy", "page")))) },
        { "Per_Page", Map(("path", "/items"), ("pagination", Map(("strategy", "page"), ("size_param", "per_page"), ("size", 10L)))) },
        { "LIMIT", Map(("path", "/items"), ("pagination", Map(("strategy", "offset")))) },
        { "c%75rsor", Map(("path", "/items"), ("pagination", Map(("strategy", "cursor"), ("pointer", "/next"), ("param", "cursor")))) },
        { "after", Map(("path", "/orders/{{ item.id }}"), ("for_each", Map(("path", "/orders"), ("fields", Map(("id", "/id"))),
            ("pagination", Map(("strategy", "cursor"), ("header", "X-Next"), ("param", "after")))))) },
    };

    [Theory]
    [MemberData(nameof(QueryPagingClashes))]
    public async Task A_session_query_token_named_like_a_query_paging_param_is_refused_at_plan(string inject,
        Dictionary<string, object?> options)
    {
        MapLogin();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(new DatasetSpec("s", "d", options),
            new() { ["auth"] = Session(Map(("query", inject))) }));

        Assert.False(ex.IsTransient);
        Assert.Contains("auth.inject.query", ex.Message);
        Assert.Empty(_server.Requests);
    }

    [Fact]
    public async Task Paging_keys_that_are_not_in_the_query_do_not_clash_with_a_session_query_token()
    {
        MapLogin();
        // One row on the first page (body page 1 / no page header), then the end.
        _server.Map("/items", r => r.Body.Contains("\"page\":2") || Header(r, "page") is not null
            ? new StubResponse(200, """{"data":[]}""")
            : new StubResponse(200, """{"data":[{"id":1}],"next":"n2"}"""));
        var body = Map(("path", "/items"), ("method", "post"), ("body", Map(("page", 0L))), ("items", "/data"),
            ("pagination", Map(("strategy", "page"), ("location", "body"), ("param", "/page"))));
        var header = Map(("path", "/items"), ("items", "/data"),
            ("pagination", Map(("strategy", "cursor"), ("pointer", "/next"), ("location", "header"), ("param", "page"))));

        Assert.Single((await Read(new DatasetSpec("s", "d", body), new() { ["auth"] = Session(Map(("query", "page"))) })).Rows);
        Assert.Single((await Read(new DatasetSpec("s", "d", header), new() { ["auth"] = Session(Map(("query", "page"))) })).Rows);
    }
}
