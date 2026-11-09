using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

/// <summary>What `on_status` rules and the connection's `retry_after` do to a real read: classification,
/// skips and their notices, precedence against the built-in special cases, and the shared pause.</summary>
public sealed class HttpStatusRulesReadTests : IAsyncLifetime
{
    private readonly StubHttpServer _server = new();
    private readonly List<string> _notices = [];
    private readonly string _scratchRoot = Directory.CreateTempSubdirectory("pz-status-rules-").FullName;

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        await _server.DisposeAsync();
        Directory.Delete(_scratchRoot, recursive: true);
    }

    private static Dictionary<string, object?> Map(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    private static List<object?> Rules(params Dictionary<string, object?>[] rules) => [.. rules];

    private static StubResponse Status(int status, string body, params (string Name, string Value)[] headers)
        => new(status, body, headers.ToDictionary(h => h.Name, h => h.Value));

    private HttpSource Source(TimeProvider? time = null, params (string Key, object? Value)[] connectionExtra)
    {
        var settings = new Dictionary<string, object?> { ["base_url"] = _server.BaseUrl.ToString() };
        foreach (var (k, v) in connectionExtra)
        {
            settings[k] = v;
        }

        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(settings), errors);
        Assert.Empty(errors);
        var source = new HttpSource(connection!.Bind(TimeProvider.System), time ?? TimeProvider.System);
        source.UseNotice(_notices.Add);
        return source;
    }

    private async Task<long> Read(Dictionary<string, object?> options, string? checkpoint = null,
        TimeProvider? time = null, params (string Key, object? Value)[] connectionExtra)
    {
        await using var source = Source(time, connectionExtra);
        var partitions = await source.PlanReadAsync(new DatasetSpec("s", "d", options), ReadHints.None,
            CancellationToken.None);
        var partition = Assert.Single(partitions);
        if (partition is HttpFilePartition file)
        {
            file.ScratchRoot = _scratchRoot;
        }

        if (checkpoint is not null)
        {
            Assert.True(((ICheckpointingPartition)partition).TryResumeFrom(checkpoint));
        }

        long rows = 0;
        await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        return rows;
    }

    private Task<PzConnectorException> Fails(Dictionary<string, object?> options, string? checkpoint = null,
        params (string Key, object? Value)[] connectionExtra)
        => Assert.ThrowsAsync<PzConnectorException>(() => Read(options, checkpoint, null, connectionExtra));

    private static Dictionary<string, object?> Paged(object? onStatus)
    {
        var options = Map(("path", "/r"), ("pagination", Map(("strategy", "page"))));
        if (onStatus is not null)
        {
            options["on_status"] = onStatus;
        }

        return options;
    }

    // --- transient / permanent ---

    [Fact]
    public async Task Transient_rule_makes_a_403_retryable_with_the_rules_wait()
    {
        _server.Map("/r", _ => Status(403, """{"message":"You have exceeded a secondary rate limit"}"""));

        var ex = await Fails(Paged(Rules(
            Map(("status", 403), ("body_contains", "secondary rate limit"), ("action", "transient"),
                ("retry_after", "60s")))));

        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(60), ex.RetryAfter);
        Assert.Contains("HTTP 403 from", ex.Message);
        Assert.EndsWith("(on_status[0]: transient)", ex.Message);
    }

    [Fact]
    public async Task Rule_that_does_not_match_the_body_leaves_the_built_in_classification()
    {
        _server.Map("/r", _ => Status(403, """{"message":"Resource not accessible by integration"}"""));

        var ex = await Fails(Paged(Rules(
            Map(("status", 403), ("body_contains", "rate limit"), ("action", "transient")))));

        Assert.False(ex.IsTransient);
        Assert.Contains("Resource not accessible by integration", ex.Message);
        Assert.EndsWith("(check the endpoint path and auth config)", ex.Message);
    }

    [Fact]
    public async Task Response_Retry_After_beats_the_configured_header_which_beats_the_rule()
    {
        var calls = 0;
        _server.Map("/r", _ => ++calls switch
        {
            1 => Status(403, "slow down", ("Retry-After", "7"), ("X-Wait", "20")),
            2 => Status(403, "slow down", ("X-Wait", "20")),
            _ => Status(403, "slow down"),
        });
        var options = Paged(Rules(Map(("status", 403), ("action", "transient"), ("retry_after", "45s"))));
        var retryAfter = ("retry_after", (object?)Map(("header", "X-Wait"), ("kind", "seconds")));

        Assert.Equal(TimeSpan.FromSeconds(7), (await Fails(options, null, retryAfter)).RetryAfter);
        Assert.Equal(TimeSpan.FromSeconds(20), (await Fails(options, null, retryAfter)).RetryAfter);
        Assert.Equal(TimeSpan.FromSeconds(45), (await Fails(options, null, retryAfter)).RetryAfter);
    }

    [Fact]
    public async Task Transient_rule_without_any_hint_leaves_the_wait_to_the_engine()
    {
        _server.Map("/r", _ => Status(400, "try again later"));

        var ex = await Fails(Paged(Rules(Map(("status", 400), ("body_contains", "try again"), ("action", "transient")))));

        Assert.True(ex.IsTransient);
        Assert.Null(ex.RetryAfter);
    }

    [Fact]
    public async Task Permanent_rule_makes_a_503_final_with_a_redacted_snippet()
    {
        _server.Map("/r", _ => Status(503, """{"error":"account suspended"}"""));

        var ex = await Fails(Paged(Rules(Map(("status", "5xx"), ("body_contains", "suspended"), ("action", "permanent")))));

        Assert.False(ex.IsTransient);
        Assert.Contains("""{"error":"account suspended"}""", ex.Message);
        Assert.EndsWith("(on_status[0]: permanent)", ex.Message);
    }

    [Fact]
    public async Task Permanent_rule_snippet_masks_secret_query_params()
    {
        _server.Map("/r", req => Status(400, $"bad request for {req.Url}"));

        var ex = await Fails(Paged(Rules(Map(("status", 400), ("action", "permanent")))), null,
            ("auth", Map(("type", "api_key"), ("key", "s3cr3t"), ("param", "api_key"))));

        Assert.DoesNotContain("s3cr3t", ex.Message);
        Assert.Contains("api_key=***", ex.Message);
    }

    [Fact]
    public async Task Rules_apply_to_later_pages_too()
    {
        _server.Map("/r", req => req.Url.Query.Contains("page=2")
            ? Status(403, "secondary rate limit")
            : Status(200, """[{"id":1}]"""));

        var ex = await Fails(Paged(Rules(
            Map(("status", 403), ("body_contains", "rate limit"), ("action", "transient")))));

        Assert.True(ex.IsTransient);
        Assert.Contains("page=2", ex.Message);
    }

    [Fact]
    public async Task Without_rules_classification_is_unchanged()
    {
        _server.Map("/r", _ => Status(403, "secondary rate limit"));

        var ex = await Fails(Paged(null));

        Assert.False(ex.IsTransient);
        Assert.Equal($"http source 's.d': HTTP 403 from {_server.BaseUrl}r?page=1: secondary rate limit " +
            "(check the endpoint path and auth config)", ex.Message);
    }

    // --- connection retry_after on built-in throttles ---

    [Fact]
    public async Task Configured_header_supplies_the_wait_of_a_429_without_Retry_After()
    {
        _server.Map("/r", _ => Status(429, "{}", ("x-ms-retry-after-ms", "2500")));

        var ex = await Fails(Paged(null), null,
            ("retry_after", Map(("header", "x-ms-retry-after-ms"), ("kind", "ms"))));

        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromMilliseconds(2500), ex.RetryAfter);
        Assert.Equal($"http source 's.d': HTTP 429 from {_server.BaseUrl}r?page=1", ex.Message);
    }

    [Fact]
    public async Task Configured_header_does_not_apply_to_a_500()
    {
        _server.Map("/r", _ => Status(500, "{}", ("X-Wait", "30")));

        var ex = await Fails(Paged(null), null, ("retry_after", Map(("header", "X-Wait"), ("kind", "seconds"))));

        Assert.True(ex.IsTransient);
        Assert.Null(ex.RetryAfter);
    }

    [Fact]
    public async Task Unreadable_configured_header_is_named_in_the_message()
    {
        _server.Map("/r", _ => Status(503, "{}", ("X-RateLimit-Reset", "tomorrow")));

        var ex = await Fails(Paged(null), null, ("retry_after", Map(("header", "X-RateLimit-Reset"), ("kind", "epoch"))));

        Assert.Null(ex.RetryAfter);
        Assert.EndsWith("(retry_after header 'X-RateLimit-Reset' value is not a non-negative number; " +
            "the engine's backoff applies)", ex.Message);
    }

    [Fact]
    public async Task Configured_epoch_header_is_read_against_the_injected_clock()
    {
        var clock = new ManualClock();
        clock.Advance(TimeSpan.FromSeconds(1_700_000_000));
        _server.Map("/r", _ => Status(429, "{}", ("X-RateLimit-Reset", "1700000090")));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Paged(null), null, clock,
            ("retry_after", Map(("header", "X-RateLimit-Reset"), ("kind", "epoch")))));

        Assert.Equal(TimeSpan.FromSeconds(90), ex.RetryAfter);
    }

    [Fact]
    public async Task Configured_header_supplies_the_wait_of_a_sink_429()
    {
        _server.Map("/ingest", _ => Status(429, "{}", ("X-Wait", "9")));
        ISinkConnector connector = new HttpConnector();
        await using var sink = await connector.OpenAsync(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = _server.BaseUrl.ToString(),
            ["retry_after"] = Map(("header", "X-Wait"), ("kind", "seconds")),
        }), CancellationToken.None);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(f => f.Name("id").DataType(Apache.Arrow.Types.Int64Type.Default)).Build();
        await using var session = await sink.BeginWriteAsync(
            new OutputSpec("api", "out", "append", "fail_on_change", Map(("path", "/ingest"))), schema,
            CancellationToken.None);
        using var batch = new Apache.Arrow.RecordBatch(schema,
            [new Apache.Arrow.Int64Array.Builder().Append(1).Build()], 1);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await session.WriteBatchAsync(batch, CancellationToken.None));

        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(9), ex.RetryAfter);
    }

    // --- skip ---

    [Fact]
    public async Task Skip_on_the_first_request_lands_no_rows_and_says_so()
    {
        _server.Map("/r", _ => Status(400, """{"error":"No data for period"}"""));

        var rows = await Read(Paged(Rules(
            Map(("status", new List<object?> { 400, 422 }), ("body_contains", "no data for period"), ("action", "skip")))));

        Assert.Equal(0, rows);
        Assert.Single(_server.Requests);
        Assert.Equal($"http source 's.d': skipped the read: HTTP 400 from {_server.BaseUrl}r?page=1 matched " +
            "on_status[0] (skip); no rows", Assert.Single(_notices));
    }

    [Fact]
    public async Task Skip_on_a_later_page_fails_naming_the_rule()
    {
        _server.Map("/r", req => req.Url.Query.Contains("page=2")
            ? Status(400, "no data for period")
            : Status(200, """[{"id":1}]"""));

        var ex = await Fails(Paged(Rules(Map(("status", 400), ("action", "skip")))));

        Assert.False(ex.IsTransient);
        Assert.Contains("matched on_status[0] (skip), but skip applies only to the first request", ex.Message);
        Assert.Empty(_notices);
    }

    [Fact]
    public async Task Skip_does_not_apply_to_the_first_request_of_a_resumed_attempt()
    {
        _server.Map("/r", _ => Status(400, "no data for period"));
        var resume = HttpCheckpointToken.Encode(
            new HttpPageRequest(HttpMethod.Get, new Uri(_server.BaseUrl, "r?page=3"), null, null), false);

        var ex = await Fails(Paged(Rules(Map(("status", 400), ("action", "skip")))), resume);

        Assert.Contains("skip applies only to the first request", ex.Message);
    }

    [Fact]
    public async Task Skip_works_with_page_read_ahead()
    {
        _server.Map("/r", _ => Status(400, "no data for period"));
        var options = Paged(Rules(Map(("status", 400), ("action", "skip"))));
        options["pagination"] = Map(("strategy", "page"), ("concurrency", 3));

        Assert.Equal(0, await Read(options));
        Assert.Contains("skipped the read: HTTP 400", Assert.Single(_notices));
    }

    [Fact]
    public async Task Skip_on_a_cursor_feed_ends_the_crawl()
    {
        _server.Map("/r", _ => Status(404, "no such report"));
        var options = Map(("path", "/r"), ("items", "/data"),
            ("pagination", Map(("strategy", "cursor"), ("pointer", "/next"), ("param", "cursor"))),
            ("on_status", Rules(Map(("status", 404), ("action", "skip")))));

        Assert.Equal(0, await Read(options));
        Assert.Single(_server.Requests);
    }

    [Fact]
    public async Task Skip_on_a_direct_file_download_lands_no_rows_and_says_so()
    {
        _server.Map("/export.csv", _ => Status(400, "no data for period"));

        var rows = await Read(Map(("path", "/export.csv"), ("file", Map(("format", "csv"))),
            ("on_status", Rules(Map(("status", 400), ("body_contains", "no data"), ("action", "skip"))))));

        Assert.Equal(0, rows);
        Assert.Equal($"http source 's.d': download: skipped the read: HTTP 400 from {_server.BaseUrl}export.csv " +
            "matched on_status[0] (skip); no rows", Assert.Single(_notices));
    }

    [Fact]
    public async Task File_download_rules_match_beyond_the_snippet_bytes()
    {
        var body = new string('x', 4000) + " QUOTA EXCEEDED";
        _server.Map("/export.csv", _ => Status(400, body));

        var ex = await Fails(Map(("path", "/export.csv"), ("file", Map(("format", "csv"))),
            ("on_status", Rules(Map(("status", 400), ("body_contains", "quota exceeded"), ("action", "transient"))))));

        Assert.True(ex.IsTransient);
        Assert.Contains("(on_status[0]: transient)", ex.Message);
    }

    [Fact]
    public async Task Export_start_response_is_classified_by_the_rules()
    {
        _server.Map("/exports", _ => Status(403, "export quota: try later"));
        var options = Map(("path", "/exports"), ("method", "post"), ("body", Map(("kind", "all"))),
            ("file", Map(("format", "csv"))),
            ("export", Map(("job_id", "/id"), ("status_path", "/exports/{job_id}"), ("status_pointer", "/state"),
                ("ready", new List<object?> { "done" }), ("download_pointer", "/url"))),
            ("on_status", Rules(Map(("status", 403), ("body_contains", "try later"), ("action", "transient")))));

        var ex = await Fails(options);

        Assert.True(ex.IsTransient);
        Assert.Contains("(on_status[0]: transient)", ex.Message);
    }

    // --- precedence ---

    [Fact]
    public async Task Rule_on_a_static_auth_401_applies()
    {
        _server.Map("/r", _ => Status(401, "token expired, refresh it"));

        var ex = await Fails(Paged(Rules(Map(("status", 401), ("action", "transient")))), null,
            ("auth", Map(("type", "bearer"), ("token", "t"))));

        Assert.True(ex.IsTransient);
        Assert.Contains("(on_status[0]: transient)", ex.Message);
    }

    [Fact]
    public async Task Expired_delta_link_beats_the_rules()
    {
        _server.Map("/delta", _ => Status(410, "gone"));
        var options = Map(("path", "/r"), ("items", "/value"), ("delta_pointer", "/@odata.deltaLink"),
            ("on_status", Rules(Map(("status", 410), ("action", "transient")))));
        await using var source = Source();
        var partitions = await source.PlanReadAsync(
            new DatasetSpec("s", "d", options) { PriorSyncState = new Uri(_server.BaseUrl, "delta?token=x").ToString() },
            ReadHints.None, CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batch.Dispose();
            }
        });

        Assert.False(ex.IsTransient);
        Assert.Contains("--full-refresh", ex.Message);
    }

    // --- for_each ---

    private static Dictionary<string, object?> Orders(Dictionary<string, object?>? forEachExtra = null,
        params (string Key, object? Value)[] extra)
    {
        var block = Map(("path", "/orders"), ("items", "/data"), ("fields", Map(("id", "/id"))));
        foreach (var (k, v) in forEachExtra ?? [])
        {
            block[k] = v;
        }

        var options = Map(("path", "/orders/{{ item.id }}/lines"), ("for_each", block));
        foreach (var (k, v) in extra)
        {
            options[k] = v;
        }

        return options;
    }

    private void MapOrderList()
        => _server.Map("/orders", _ => Status(200, """{"data":[{"id":"a"},{"id":"b"},{"id":"c"}]}"""));

    private void MapOrders()
    {
        MapOrderList();
        _server.MapPrefix("/orders/", req => req.Url.AbsolutePath switch
        {
            "/orders/b/lines" => Status(403, """{"error":"order archived"}"""),
            "/orders/c/lines" => Status(404, "not found"),
            _ => Status(200, """[{"sku":"x"}]"""),
        });
    }

    [Fact]
    public async Task Item_skip_rules_are_counted_with_on_missing_skips()
    {
        MapOrders();

        var rows = await Read(Orders(Map(("on_missing", "skip")),
            ("on_status", Rules(Map(("status", 403), ("body_contains", "archived"), ("action", "skip"))))));

        Assert.Equal(1, rows);
        Assert.Equal("http source 's.d': skipped 2 items whose first request returned 404/410 or matched an " +
            "on_status skip rule (first: item id=b)", Assert.Single(_notices));
    }

    [Fact]
    public async Task Item_skip_rule_without_on_missing_names_only_the_rule()
    {
        MapOrderList();
        _server.MapPrefix("/orders/", req => req.Url.AbsolutePath == "/orders/b/lines"
            ? Status(403, "order archived")
            : Status(200, """[{"sku":"x"}]"""));

        var rows = await Read(Orders(null, ("on_status", Rules(Map(("status", 403), ("action", "skip"))))));

        Assert.Equal(2, rows);
        Assert.Equal("http source 's.d': skipped 1 items whose first request matched an on_status skip rule " +
            "(first: item id=b)", Assert.Single(_notices));
    }

    [Fact]
    public async Task Item_rule_comes_before_on_missing()
    {
        MapOrders();

        var ex = await Fails(Orders(Map(("on_missing", "skip")),
            ("on_status", Rules(Map(("status", 404), ("action", "permanent")), Map(("status", 403), ("action", "skip"))))));

        Assert.False(ex.IsTransient);
        Assert.Contains("(on_status[0]: permanent)", ex.Message);
        Assert.Contains("id=c", ex.Message);
    }

    [Fact]
    public async Task On_missing_wording_is_unchanged_without_skip_rules()
    {
        MapOrderList();
        _server.MapPrefix("/orders/", req => req.Url.AbsolutePath == "/orders/c/lines"
            ? Status(404, "not found")
            : Status(200, """[{"sku":"x"}]"""));

        await Read(Orders(Map(("on_missing", "skip")),
            ("on_status", Rules(Map(("status", 500), ("action", "permanent"))))));

        Assert.Equal("http source 's.d': skipped 1 items that returned 404/410 (first: item id=c)", Assert.Single(_notices));
    }

    [Fact]
    public async Task List_rules_cover_only_the_list_request()
    {
        _server.Map("/orders", _ => Status(400, "no orders for period"));

        var rows = await Read(Orders(Map(("on_status", Rules(Map(("status", 400), ("action", "skip")))))));

        Assert.Equal(0, rows);
        Assert.Single(_server.Requests);
        Assert.Contains("matched for_each.on_status[0] (skip); no rows", Assert.Single(_notices));
    }

    [Fact]
    public async Task Dataset_rules_do_not_cover_the_list_request()
    {
        _server.Map("/orders", _ => Status(400, "no orders for period"));

        var ex = await Fails(Orders(null, ("on_status", Rules(Map(("status", 400), ("action", "skip"))))));

        Assert.EndsWith("(check the endpoint path and auth config)", ex.Message);
    }

    [Fact]
    public async Task File_item_skip_is_counted_not_noticed_alone()
    {
        _server.Map("/orders", _ => Status(200, """{"data":[{"id":"a"},{"id":"b"}]}"""));
        _server.MapPrefix("/orders/", req => req.Url.AbsolutePath == "/orders/b/lines"
            ? Status(400, "no export")
            : Status(200, "sku\nx\n", ("Content-Type", "text/csv")));
        var options = Orders(null, ("file", Map(("format", "csv"))),
            ("on_status", Rules(Map(("status", 400), ("action", "skip")))));

        Assert.Equal(1, await Read(options));
        Assert.Equal("http source 's.d': skipped 1 items whose first request matched an on_status skip rule " +
            "(first: item id=b)", Assert.Single(_notices));
    }

    // --- the shared pause ---

    [Fact]
    public async Task Transient_rule_wait_moves_the_shared_pause()
    {
        _server.Map("/r", _ => Status(403, "secondary rate limit"));
        var clock = new ManualClock();
        var pause = new ThrottlePause(clock);
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
            { ["base_url"] = _server.BaseUrl.ToString() }), errors)!;
        var config = HttpDatasetConfig.Parse(new DatasetSpec("s", "d",
            Paged(Rules(Map(("status", 403), ("action", "transient"), ("retry_after", "30s"))))));
        using var client = HttpSource.CreateClient(connection);
        var sender = new HttpRequestSender(client, client, connection, null, null, clock, "t", pause: pause,
            rules: config.OnStatus);
        using var sent = await sender.SendAsync(new HttpPageRequest(HttpMethod.Get, new Uri(_server.BaseUrl, "r"), null,
            null), streamBody: false, credentialsOnlyToOrigin: false, CancellationToken.None);

        var ex = sender.StatusError(sent, await sent.Response.Content.ReadAsStringAsync());

        Assert.Equal(TimeSpan.FromSeconds(30), ex.RetryAfter);
        var wait = pause.WaitAsync(CancellationToken.None);
        Assert.False(wait.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(30));
        await wait;
    }

    [Fact]
    public async Task Configured_header_wait_on_a_429_moves_the_shared_pause()
    {
        _server.Map("/r", _ => Status(429, "{}", ("X-Wait", "12")));
        var clock = new ManualClock();
        var pause = new ThrottlePause(clock);
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = _server.BaseUrl.ToString(),
            ["retry_after"] = Map(("header", "X-Wait"), ("kind", "seconds")),
        }), errors)!;
        using var client = HttpSource.CreateClient(connection);
        var sender = new HttpRequestSender(client, client, connection, null, null, clock, "t", pause: pause);
        using var sent = await sender.SendAsync(new HttpPageRequest(HttpMethod.Get, new Uri(_server.BaseUrl, "r"), null,
            null), streamBody: false, credentialsOnlyToOrigin: false, CancellationToken.None);

        Assert.Equal(TimeSpan.FromSeconds(12), sender.StatusError(sent, "{}").RetryAfter);
        var wait = pause.WaitAsync(CancellationToken.None);
        Assert.False(wait.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(12));
        await wait;
    }
}
