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

    /// <summary>What the last <see cref="Read"/>'s partition reported to the engine (<see cref="ISkippedReadPartition"/>);
    /// null when it does not implement the interface.</summary>
    private bool? _readSkipped;
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

        _readSkipped = (partition as ISkippedReadPartition)?.ReadSkipped;
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
            "ignored)", ex.Message);
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
            "on_status[0] (skip); 0 rows landed", Assert.Single(_notices));
    }

    [Fact]
    public async Task A_skip_is_reported_to_the_engine_and_a_plain_404_or_an_empty_read_is_not()
    {
        // The engine keeps the watermark of a skipped read where it was; it must not do that for a read that
        // merely found nothing.
        _server.Map("/r", _ => Status(400, "no data for period"));
        await Read(Paged(Rules(Map(("status", 400), ("action", "skip")))));
        Assert.True(_readSkipped);

        _server.Map("/export.csv", _ => Status(400, "no data for period"));
        await Read(Map(("path", "/export.csv"), ("file", Map(("format", "csv"))),
            ("on_status", Rules(Map(("status", 400), ("action", "skip"))))));
        Assert.True(_readSkipped);

        _server.Map("/empty", _ => Status(200, "[]"));
        await Read(Map(("path", "/empty")));
        Assert.False(_readSkipped);

        // Prefix routes match in registration order, so one route answers by phase.
        var phase = 0;
        _server.Map("/orders", _ => Status(200, """{"data":[{"id":"a"},{"id":"b"}]}"""));
        _server.MapPrefix("/o/", r => (phase, r.Url.AbsolutePath) switch
        {
            (0, "/o/a") => Status(404, "no such order"),
            (0, _) => Status(200, """{"lines":[{"sku":"x"}]}"""),
            (_, "/o/a") => Status(200, """{"lines":[{"sku":"x"}]}"""),
            _ => Status(400, "no data for period"),
        });
        var options = Map(("path", "/o/{{ item.id }}"), ("items", "/lines"),
            ("for_each", Map(("path", "/orders"), ("items", "/data"), ("fields", Map(("id", "/id"))),
                ("on_missing", "skip"))));

        // on_missing: skip (a 404 item) is not a rule skip.
        Assert.Equal(1, await Read(options));
        Assert.False(_readSkipped);

        phase = 1;
        options["on_status"] = Rules(Map(("status", 400), ("action", "skip")));
        Assert.Equal(1, await Read(options));
        Assert.True(_readSkipped);
    }

    [Fact]
    public async Task An_item_skip_before_a_checkpoint_is_still_reported_after_a_resume()
    {
        // Attempt 1 skips item b by a rule, lands c (offering a checkpoint past b), then fails on d. The resumed
        // attempt starts at d and never sees b again, but must still tell the engine the read skipped data.
        var failD = true;
        _server.Map("/orders", _ => Status(200, """{"data":[{"id":"a"},{"id":"b"},{"id":"c"},{"id":"d"}]}"""));
        _server.MapPrefix("/o/", r => r.Url.AbsolutePath switch
        {
            "/o/b" => Status(400, "no data for period"),
            "/o/d" when failD => Status(400, "boom"),
            _ => Status(200, """{"lines":[{"sku":"x"}]}"""),
        });
        var options = Map(("path", "/o/{{ item.id }}"), ("items", "/lines"),
            ("for_each", Map(("path", "/orders"), ("items", "/data"), ("fields", Map(("id", "/id"))))),
            ("on_status", Rules(Map(("status", 400), ("body_contains", "no data"), ("action", "skip")))));

        string? token = null;
        await using (var source = Source())
        {
            var partition = Assert.IsAssignableFrom<ICheckpointingPartition>(Assert.Single(
                await source.PlanReadAsync(new DatasetSpec("s", "d", options), ReadHints.None, CancellationToken.None)));
            await Assert.ThrowsAsync<PzConnectorException>(async () =>
            {
                await foreach (var batch in partition.ReadAsync(new BatchOptions(MaxRowsPerBatch: 1), CancellationToken.None))
                {
                    batch.Dispose();
                    if (partition.TryGetCheckpoint(out var offered) && offered is not null)
                    {
                        token = offered;
                    }
                }
            });
        }

        Assert.True(HttpCheckpointToken.TryDecodeForEach(token!, out var position));
        Assert.Equal(3, position.Index);
        Assert.True(position.RuleSkipped);

        failD = false;
        var mark = _server.Requests.Count;
        Assert.Equal(1, await Read(options, token));
        Assert.DoesNotContain(_server.Requests.Skip(mark), r => r.Url.AbsolutePath == "/o/b");
        Assert.True(_readSkipped);
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
            "matched on_status[0] (skip); 0 rows landed", Assert.Single(_notices));
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

        var rows = await Read(Orders(null,
            ("on_status", Rules(Map(("status", 403), ("body_contains", "archived"), ("action", "skip"))))));

        Assert.Equal(2, rows);
        Assert.Equal("http source 's.d': skipped 1 items whose first request matched an on_status skip rule " +
            "(first: item id=b)", Assert.Single(_notices));
    }

    [Fact]
    public async Task Item_rule_comes_before_on_missing()
    {
        MapOrders();

        var ex = await Fails(Orders(Map(("on_missing", "skip")),
            ("on_status", Rules(Map(("status", 404), ("action", "permanent")),
                Map(("status", 403), ("body_contains", "archived"), ("action", "skip"))))));

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
        Assert.Equal($"http source 's.d': skipped the read: HTTP 400 from {_server.BaseUrl}orders matched " +
            "for_each.on_status[0] (skip); 0 rows landed", Assert.Single(_notices));
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

        var body = await sent.Response.Content.ReadAsStringAsync();
        var ex = sender.StatusError(sent, body, stage: null, sender.MatchRule(sent, body));

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

        Assert.Equal(TimeSpan.FromSeconds(12), sender.StatusError(sent, "{}", stage: null, sender.MatchRule(sent, "{}")).RetryAfter);
        var wait = pause.WaitAsync(CancellationToken.None);
        Assert.False(wait.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(12));
        await wait;
    }

    [Fact]
    public async Task Past_epoch_in_the_configured_header_falls_through_to_the_rules_wait()
    {
        var clock = new ManualClock();
        clock.Advance(TimeSpan.FromSeconds(1_700_000_000));
        _server.Map("/r", _ => Status(403, "secondary rate limit", ("X-RateLimit-Reset", "1699999990")));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(
            Paged(Rules(Map(("status", 403), ("body_contains", "rate limit"), ("action", "transient"),
                ("retry_after", "60s")))), null, clock,
            ("retry_after", Map(("header", "X-RateLimit-Reset"), ("kind", "epoch")))));

        Assert.Equal(TimeSpan.FromSeconds(60), ex.RetryAfter);
        Assert.Contains("(retry_after header 'X-RateLimit-Reset' states no wait; ignored)", ex.Message);
    }

    [Fact]
    public async Task Past_epoch_on_a_429_leaves_the_wait_to_the_engine()
    {
        var clock = new ManualClock();
        clock.Advance(TimeSpan.FromSeconds(1_700_000_000));
        _server.Map("/r", _ => Status(429, "{}", ("X-RateLimit-Reset", "1699999990")));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Paged(null), null, clock,
            ("retry_after", Map(("header", "X-RateLimit-Reset"), ("kind", "epoch")))));

        Assert.True(ex.IsTransient);
        Assert.Null(ex.RetryAfter);
    }

    // --- an item 404 under on_missing: skip never waits for the error body unless a rule could match it ---

    private static async Task<long> ReadStalledMissingItem(Dictionary<string, object?>? extra)
    {
        await using var hostile = new HostileServer((_, request) => request.StartsWith("GET /orders/b/", StringComparison.Ordinal)
            ? HostileReply.StallMidBody(404, "text/plain", "gone", 100_000)
            : request.StartsWith("GET /orders/", StringComparison.Ordinal)
                ? HostileReply.Raw("HTTP/1.1 200 OK\nContent-Type: text/csv\nContent-Length: 6\nConnection: close\n\nsku\nx\n")
                : HostileReply.Json("""{"data":[{"id":"a"},{"id":"b"}]}"""));
        var errors = new List<string>();
        // A one-second idle limit: reading the stalled body at all would fail the read as a stall.
        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = hostile.BaseUrl.ToString(),
            ["timeout_seconds"] = "1",
        }), errors)!;
        Assert.Empty(errors);
        var options = Map(("path", "/orders/{{ item.id }}/lines.csv"), ("file", Map(("format", "csv"))),
            ("for_each", Map(("path", "/orders"), ("items", "/data"), ("fields", Map(("id", "/id"))),
                ("on_missing", "skip"))));
        foreach (var (k, v) in extra ?? [])
        {
            options[k] = v;
        }

        await using var source = new HttpSource(connection);
        var partition = Assert.Single(await source.PlanReadAsync(new DatasetSpec("s", "d", options), ReadHints.None,
            CancellationToken.None));
        long rows = 0;
        await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        return rows;
    }

    [Fact]
    public async Task On_missing_file_item_skips_without_reading_the_body_when_there_are_no_rules()
    {
        Assert.Equal(1, await ReadStalledMissingItem(null));
    }

    [Fact]
    public async Task On_missing_file_item_skips_without_reading_the_body_when_no_rule_covers_the_status()
    {
        Assert.Equal(1, await ReadStalledMissingItem(new()
        {
            ["on_status"] = Rules(Map(("status", 400), ("body_contains", "quota"), ("action", "transient"))),
        }));
    }

    // --- coverage the review asked for ---

    [Fact]
    public async Task Token_authenticator_401_wins_over_a_matching_rule()
    {
        _server.Map("/token", _ => Status(200, """{"access_token":"t1","token_type":"Bearer","expires_in":3600}"""));
        _server.Map("/r", _ => Status(401, "invalid token"));

        var ex = await Fails(Paged(Rules(Map(("status", 401), ("action", "transient")))), null,
            ("auth", Map(("type", "oauth2_client_credentials"), ("token_url", new Uri(_server.BaseUrl, "token").ToString()),
                ("client_id", "c"), ("client_secret", "s3cret"))));

        Assert.False(ex.IsTransient);
        Assert.Contains("rejected a freshly issued token (HTTP 401)", ex.Message);
        Assert.DoesNotContain("on_status", ex.Message);
    }

    [Fact]
    public async Task Class_rule_never_reaches_a_proxy_407()
    {
        _server.Map("/r", _ => Status(407, "proxy auth required"));

        var ex = await Fails(Paged(Rules(Map(("status", "4xx"), ("action", "transient")))));

        Assert.False(ex.IsTransient);
        Assert.Contains("the proxy refused authentication (HTTP 407)", ex.Message);
        Assert.DoesNotContain("on_status", ex.Message);
    }

    [Fact]
    public async Task Class_rule_whose_body_does_not_match_leaves_a_503_transient()
    {
        _server.Map("/r", _ => Status(503, "busy"));

        var ex = await Fails(Paged(Rules(Map(("status", "5xx"), ("body_contains", "suspended"), ("action", "permanent")))));

        Assert.True(ex.IsTransient);
        Assert.DoesNotContain("on_status", ex.Message);
    }

    [Fact]
    public async Task Non_matching_rule_on_an_item_404_falls_back_to_on_missing()
    {
        MapOrderList();
        _server.MapPrefix("/orders/", req => req.Url.AbsolutePath == "/orders/b/lines"
            ? Status(404, "not found")
            : Status(200, """[{"sku":"x"}]"""));

        var rows = await Read(Orders(Map(("on_missing", "skip")),
            ("on_status", Rules(Map(("status", 404), ("body_contains", "quota"), ("action", "transient"))))));

        Assert.Equal(2, rows);
        Assert.Equal("http source 's.d': skipped 1 items that returned 404/410 (first: item id=b)", Assert.Single(_notices));
    }

    [Fact]
    public async Task Non_matching_rule_on_a_file_item_404_falls_back_to_on_missing()
    {
        MapOrderList();
        _server.MapPrefix("/orders/", req => req.Url.AbsolutePath == "/orders/b/lines"
            ? Status(404, "not found")
            : Status(200, "sku\nx\n", ("Content-Type", "text/csv")));

        var rows = await Read(Orders(Map(("on_missing", "skip")), ("file", Map(("format", "csv"))),
            ("on_status", Rules(Map(("status", 404), ("body_contains", "quota"), ("action", "transient"))))));

        Assert.Equal(2, rows);
        Assert.Equal("http source 's.d': skipped 1 items that returned 404/410 (first: item id=b)", Assert.Single(_notices));
    }

    [Fact]
    public async Task Skip_rule_matching_a_read_ahead_page_after_the_first_fails()
    {
        _server.Map("/r", req => req.Url.Query.Contains("page=1")
            ? Status(200, """[{"id":1}]""")
            : Status(400, "no data for period"));
        var options = Paged(Rules(Map(("status", 400), ("action", "skip"))));
        options["pagination"] = Map(("strategy", "page"), ("concurrency", 3));

        var ex = await Fails(options);

        Assert.False(ex.IsTransient);
        Assert.Contains("matched on_status[0] (skip), but skip applies only to the first request", ex.Message);
        Assert.Empty(_notices);
    }

    [Fact]
    public async Task Body_contains_matches_a_gzip_encoded_error_body()
    {
        using var buffer = new MemoryStream();
        using (var gzip = new System.IO.Compression.GZipStream(buffer, System.IO.Compression.CompressionLevel.Fastest, true))
        {
            gzip.Write("""{"message":"secondary rate limit"}"""u8);
        }

        _server.Map("/r", _ => new StubResponse(403, "", new Dictionary<string, string> { ["Content-Encoding"] = "gzip" })
        {
            RawBody = buffer.ToArray(),
        });

        var ex = await Fails(Paged(Rules(Map(("status", 403), ("body_contains", "rate limit"), ("action", "transient")))));

        Assert.True(ex.IsTransient);
        Assert.EndsWith("(on_status[0]: transient)", ex.Message);
    }

    private async Task<PzConnectorException> SinkFails(StubResponse response)
    {
        _server.Map("/ingest", _ => response);
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
        return await Assert.ThrowsAsync<PzConnectorException>(
            async () => await session.WriteBatchAsync(batch, CancellationToken.None));
    }

    [Fact]
    public async Task Configured_header_supplies_the_wait_of_a_sink_503()
    {
        var ex = await SinkFails(Status(503, "{}", ("X-Wait", "4")));

        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(4), ex.RetryAfter);
    }

    [Fact]
    public async Task Configured_header_does_not_apply_to_a_sink_500()
    {
        var ex = await SinkFails(Status(500, "{}", ("X-Wait", "4")));

        Assert.True(ex.IsTransient);
        Assert.Null(ex.RetryAfter);
    }

    [Fact]
    public async Task Unreadable_configured_header_is_named_on_a_sink_429()
    {
        var ex = await SinkFails(Status(429, "{}", ("X-Wait", "later")));

        Assert.Null(ex.RetryAfter);
        Assert.EndsWith("(retry_after header 'X-Wait' value is not a non-negative number; ignored)", ex.Message);
    }

    // --- run-time guard: a skip never applies to a read that carries a watermark ---

    private async Task<PzConnectorException> SkipWithWatermark(Dictionary<string, object?> options,
        Func<DatasetSpec, DatasetSpec> stamp)
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
            { ["base_url"] = _server.BaseUrl.ToString() }), errors)!;
        Assert.Empty(errors);
        // Options that validate as not incremental (a raw-mode SQL-declared incremental looks like this), read
        // with a spec that carries the engine's watermark.
        var config = HttpDatasetConfig.Parse(new DatasetSpec("s", "d", options));
        var spec = stamp(new DatasetSpec("s", "d", options));
        using var client = HttpSource.CreateClient(connection);
        IDatasetPartition partition = config.IsFileMode
            ? new HttpFilePartition(client, client, connection, config, spec, TimeProvider.System, null)
                { Notice = _notices.Add, ScratchRoot = _scratchRoot }
            : new HttpPartition(client, client, connection, config, spec, TimeProvider.System) { Notice = _notices.Add };
        return await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batch.Dispose();
            }
        });
    }

    [Fact]
    public async Task Skip_on_a_read_with_a_watermark_cursor_fails()
    {
        _server.Map("/r", _ => Status(400, "no data for period"));

        var ex = await SkipWithWatermark(Paged(Rules(Map(("status", 400), ("action", "skip")))),
            spec => spec with { WatermarkCursor = "pz_fetched_at", WatermarkValue = "2026-11-08T00:00:00.000000" });

        // A raw-mode read handed a watermark cursor fails before its first request (it has no cursor option to
        // filter on), so the skip never gets the chance to drop the read.
        Assert.False(ex.IsTransient);
        Assert.Contains("incremental dataset needs the raw-mode 'cursor'", ex.Message);
        Assert.Empty(_notices);
        Assert.Empty(_server.Requests);
    }

    [Fact]
    public async Task Skip_on_a_read_with_only_an_upper_bound_fails()
    {
        _server.Map("/r", _ => Status(400, "no data for period"));

        var ex = await SkipWithWatermark(Paged(Rules(Map(("status", 400), ("action", "skip")))),
            spec => spec with { WatermarkUpperBound = "2026-11-09T00:00:00.000000" });

        Assert.False(ex.IsTransient);
        Assert.Contains("matched on_status[0] (skip), but skip is refused on incremental reads", ex.Message);
        Assert.Empty(_notices);
    }

    [Fact]
    public async Task Skip_on_a_file_download_with_a_watermark_fails()
    {
        _server.Map("/export.csv", _ => Status(400, "no data for period"));

        var ex = await SkipWithWatermark(Map(("path", "/export.csv"), ("file", Map(("format", "csv"))),
                ("on_status", Rules(Map(("status", 400), ("action", "skip"))))),
            spec => spec with { WatermarkUpperBound = "2026-11-09T00:00:00.000000" });

        Assert.False(ex.IsTransient);
        Assert.Contains("matched on_status[0] (skip), but skip is refused on incremental reads", ex.Message);
        Assert.Empty(_notices);
    }
}
