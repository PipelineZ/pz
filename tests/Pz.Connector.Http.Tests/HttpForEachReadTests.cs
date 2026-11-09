using System.Buffers;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Apache.Arrow.Memory;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

public sealed class HttpForEachReadTests : IAsyncLifetime
{
    private readonly StubHttpServer _server = new();
    private readonly List<string> _notices = [];

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync() => await _server.DisposeAsync();

    private sealed class RecordingGate : IOperationGate
    {
        public List<(string Label, bool Idempotent)> Calls { get; } = [];

        public Task<T> ExecuteAsync<T>(string opLabel, bool idempotent, Func<CancellationToken, Task<T>> op,
            CancellationToken ct)
        {
            lock (Calls)
            {
                Calls.Add((opLabel, idempotent));
            }

            return op(ct);
        }

        public void ReportBudget(int remaining, DateTimeOffset resetAt)
        {
        }
    }

    private HttpSource Source(TimeProvider? time = null, Dictionary<string, object?>? auth = null)
    {
        var errors = new List<string>();
        var settings = new Dictionary<string, object?> { ["base_url"] = _server.BaseUrl.ToString() };
        if (auth is not null)
        {
            settings["auth"] = auth;
        }

        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(settings), errors);
        Assert.Empty(errors);
        var source = new HttpSource(connection!, time ?? TimeProvider.System);
        source.UseNotice(_notices.Add);
        return source;
    }

    private async Task<List<object?[]>> Read(DatasetSpec spec, IOperationGate? gate = null, TimeProvider? time = null,
        Dictionary<string, object?>? auth = null)
    {
        await using var source = Source(time, auth);
        if (gate is not null)
        {
            source.UseOperationGate(gate);
        }

        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        var rows = new List<object?[]>();
        await foreach (var batch in Assert.Single(partitions).ReadAsync(new BatchOptions(), CancellationToken.None))
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
        }

        return rows;
    }

    private static DatasetSpec Spec(Dictionary<string, object?> options) => new("s", "d", options);

    private static Dictionary<string, object?> Map(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    private static string Query(StubRequest r, string name)
        => System.Web.HttpUtility.ParseQueryString(r.Url.Query)[name] ?? "";

    private List<string> RequestLines()
        => [.. _server.Requests.Select(r => r.Url.PathAndQuery)];

    /// <summary>Orders o1..o3, item request /orders/{id}/lines with page-number paging.</summary>
    private static Dictionary<string, object?> OrdersOptions(Dictionary<string, object?>? forEachExtra = null,
        Dictionary<string, object?>? extra = null)
    {
        var forEach = Map(("path", "/orders"), ("items", "/data"), ("fields", Map(("order_id", "/id"))));
        foreach (var (k, v) in forEachExtra ?? [])
        {
            forEach[k] = v;
        }

        var options = Map(("path", "/orders/{{ item.order_id }}/lines"), ("items", "/lines"),
            ("pagination", Map(("strategy", "page"))), ("for_each", forEach));
        foreach (var (k, v) in extra ?? [])
        {
            options[k] = v;
        }

        return options;
    }

    /// <summary>Serves /orders/{id}/lines?page=n: <paramref name="pages"/>[id] pages of one line each, then an
    /// empty page.</summary>
    private void MapLines(Dictionary<string, int> pages, Func<string, int, StubResponse?>? overrideFor = null)
    {
        _server.MapPrefix("/orders/", r =>
        {
            var id = r.Url.AbsolutePath.Split('/')[2];
            var page = int.Parse(Query(r, "page"));
            if (overrideFor?.Invoke(id, page) is { } custom)
            {
                return custom;
            }

            return page <= pages.GetValueOrDefault(id)
                ? new StubResponse(200, $$"""{"lines":[{"sku":"{{id}}-{{page}}","qty":{{page}}}]}""")
                : new StubResponse(200, """{"lines":[]}""");
        });
    }

    private void MapOrders(params string[] ids)
        => _server.Map("/orders", _ => new StubResponse(200,
            new JsonObject { ["data"] = new JsonArray([.. ids.Select(id => (JsonNode)new JsonObject { ["id"] = id })]) }
                .ToJsonString()));

    [Fact]
    public async Task Orders_to_lines_spec_example()
    {
        _server.Map("/orders", r => Query(r, "cursor") == "X"
            ? new StubResponse(200, """{"data":[{"id":"o4","customer":{"name":"Dee"},"updated_at":"2026-02-04T00:00:00Z"}]}""")
            : new StubResponse(200, """
                {"data":[{"id":"o1","customer":{"name":"Ann"},"updated_at":"2026-02-01T00:00:00Z"},
                         {"id":"o2","customer":{"name":"Bob"},"updated_at":"2026-02-02T00:00:00Z"},
                         {"id":"o3","customer":{"name":"Cy"},"updated_at":"2026-02-03T00:00:00Z"}],
                 "next_cursor":"X"}
                """));
        MapLines(new() { ["o1"] = 2, ["o2"] = 1, ["o3"] = 3, ["o4"] = 1 });

        var spec = Spec(Map(
            ("for_each", Map(("path", "/orders"), ("query", Map(("updated_since", "{{ watermark }}"))),
                ("items", "/data"),
                ("pagination", Map(("strategy", "cursor"), ("param", "cursor"), ("pointer", "/next_cursor"))),
                ("fields", Map(("order_id", "/id"), ("customer", "/customer/name"), ("updated_at", "/updated_at"))))),
            ("path", "/orders/{{ item.order_id }}/lines"),
            ("items", "/lines"),
            ("pagination", Map(("strategy", "page"), ("param", "page"))),
            ("columns", new Dictionary<string, string>
            {
                ["order_id"] = "varchar", ["customer"] = "varchar", ["updated_at"] = "timestamp",
                ["sku"] = "varchar", ["qty"] = "int",
            }))) with { WatermarkCursor = "updated_at", WatermarkValue = "2026-01-01T00:00:00.500000" };

        var rows = await Read(spec);

        Assert.Equal(
        [
            "/orders?updated_since=2026-01-01T00%3A00%3A00Z",
            "/orders/o1/lines?page=1", "/orders/o1/lines?page=2", "/orders/o1/lines?page=3",
            "/orders/o2/lines?page=1", "/orders/o2/lines?page=2",
            "/orders/o3/lines?page=1", "/orders/o3/lines?page=2", "/orders/o3/lines?page=3", "/orders/o3/lines?page=4",
            "/orders?updated_since=2026-01-01T00%3A00%3A00Z&cursor=X",
            "/orders/o4/lines?page=1", "/orders/o4/lines?page=2",
        ], RequestLines());
        Assert.Equal(
        [
            "o1|Ann|o1-1|1", "o1|Ann|o1-2|2", "o2|Bob|o2-1|1",
            "o3|Cy|o3-1|1", "o3|Cy|o3-2|2", "o3|Cy|o3-3|3", "o4|Dee|o4-1|1",
        ], rows.Select(r => $"{r[0]}|{r[1]}|{r[3]}|{r[4]}"));
        Assert.All(rows, r => Assert.NotNull(r[2]));
        Assert.Equal(DateTimeOffset.Parse("2026-02-03T00:00:00Z"), rows[3][2]);
        Assert.Empty(_notices);
    }

    [Fact]
    public async Task Empty_list_page_then_more()
    {
        _server.Map("/orders", r => Query(r, "cursor") == "X"
            ? new StubResponse(200, """{"data":[{"id":"a"},{"id":"b"}]}""")
            : new StubResponse(200, """{"data":[],"next_cursor":"X"}"""));
        MapLines(new() { ["a"] = 1, ["b"] = 1 });

        var rows = await Read(Spec(OrdersOptions(Map(
            ("pagination", Map(("strategy", "cursor"), ("param", "cursor"), ("pointer", "/next_cursor")))))));

        Assert.Equal(2, rows.Count);
        Assert.Equal(["/orders", "/orders?cursor=X", "/orders/a/lines?page=1", "/orders/a/lines?page=2",
            "/orders/b/lines?page=1", "/orders/b/lines?page=2"], RequestLines());
    }

    [Fact]
    public async Task Post_list_with_body_paging_then_get_items()
    {
        _server.Map("/orders/search", r => JsonNode.Parse(r.Body)!["cursor"]?.GetValue<string>() == "X"
            ? new StubResponse(200, """{"data":[{"id":"o2"}]}""")
            : new StubResponse(200, """{"data":[{"id":"o1"}],"next":"X"}"""));
        MapLines(new() { ["o1"] = 1, ["o2"] = 1 });

        var rows = await Read(Spec(OrdersOptions(Map(
            ("path", "/orders/search"), ("method", "post"),
            ("body", Map(("status", "open"), ("cursor", ""))),
            ("pagination", Map(("strategy", "cursor"), ("location", "body"), ("param", "/cursor"),
                ("pointer", "/next")))))));

        Assert.Equal(2, rows.Count);
        var requests = _server.Requests;
        Assert.Equal(["POST /orders/search", "GET /orders/o1/lines", "GET /orders/o1/lines",
            "POST /orders/search", "GET /orders/o2/lines", "GET /orders/o2/lines"],
            requests.Select(r => $"{r.Method} {r.Url.AbsolutePath}"));
        Assert.Equal("open", JsonNode.Parse(requests[0].Body)!["status"]!.GetValue<string>());
        Assert.Equal("X", JsonNode.Parse(requests[3].Body)!["cursor"]!.GetValue<string>());
        Assert.Equal("", requests[1].Body);
    }

    [Fact]
    public async Task On_missing_skip_first_request_only()
    {
        MapOrders("o1", "o2", "o3");
        var failLater = false;
        MapLines(new() { ["o1"] = 1, ["o3"] = 2 }, (id, page) =>
            id == "o2" && page == 1 ? new StubResponse(404, "gone")
            : failLater && id == "o3" && page == 2 ? new StubResponse(404, "gone")
            : null);
        var options = OrdersOptions(Map(("on_missing", "skip")));

        var rows = await Read(Spec(options));

        Assert.Equal(["o1-1", "o3-1", "o3-2"], rows.Select(r => (string)r[0]!).Select(p =>
            JsonNode.Parse(p)!["sku"]!.GetValue<string>()));
        var notice = Assert.Single(_notices);
        Assert.Equal("http source 's.d': skipped 1 items that returned 404/410 (first: item order_id=o2)", notice);

        failLater = true;
        _notices.Clear();
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(options)));
        Assert.False(ex.IsTransient);
        Assert.Contains("HTTP 404", ex.Message);
        Assert.EndsWith("(item order_id=o3)", ex.Message);
        Assert.Empty(_notices);
    }

    [Fact]
    public async Task On_missing_error_default()
    {
        MapOrders("o1", "o2", "o3");
        MapLines(new() { ["o1"] = 1, ["o3"] = 1 }, (id, _) => id == "o2" ? new StubResponse(410, "gone") : null);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(OrdersOptions())));

        Assert.False(ex.IsTransient);
        Assert.Contains("HTTP 410", ex.Message);
        Assert.EndsWith("(item order_id=o2)", ex.Message);
        Assert.IsType<PzConnectorException>(ex.InnerException);
        Assert.DoesNotContain(_server.Requests, r => r.Url.AbsolutePath.Contains("o3"));
    }

    [Fact]
    public async Task Transient_item_error_keeps_class()
    {
        MapOrders("o1", "o2");
        MapLines(new() { ["o1"] = 1 }, (id, _) => id == "o2"
            ? new StubResponse(503, "busy", new Dictionary<string, string> { ["Retry-After"] = "7" })
            : null);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => Read(Spec(OrdersOptions(Map(("on_missing", "skip"))))));

        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(7), ex.RetryAfter);
        Assert.Contains("HTTP 503", ex.Message);
        Assert.EndsWith("(item order_id=o2)", ex.Message);
    }

    [Fact]
    public async Task Max_items_stops_with_notice()
    {
        MapOrders("o1", "o2", "o3");
        MapLines(new() { ["o1"] = 1, ["o2"] = 1, ["o3"] = 1 });

        var rows = await Read(Spec(OrdersOptions(Map(("max_items", 2)))));

        Assert.Equal(2, rows.Count);
        Assert.DoesNotContain(_server.Requests, r => r.Url.AbsolutePath.Contains("o3"));
        Assert.Equal("http source 's.d': stopped after max_items (2) items; more list records were available",
            Assert.Single(_notices));
    }

    [Fact]
    public async Task Max_items_reached_at_end_of_list_sends_no_notice()
    {
        MapOrders("o1", "o2");
        MapLines(new() { ["o1"] = 1, ["o2"] = 1 });

        var rows = await Read(Spec(OrdersOptions(Map(("max_items", 2)))));

        Assert.Equal(2, rows.Count);
        Assert.Empty(_notices);
    }

    [Fact]
    public async Task List_max_pages_stops_with_notice()
    {
        _server.Map("/orders", r => Query(r, "cursor") == "X"
            ? new StubResponse(200, """{"data":[{"id":"o2"}]}""")
            : new StubResponse(200, """{"data":[{"id":"o1"}],"next_cursor":"X"}"""));
        MapLines(new() { ["o1"] = 1, ["o2"] = 1 });

        var rows = await Read(Spec(OrdersOptions(Map(("max_pages", 1),
            ("pagination", Map(("strategy", "cursor"), ("param", "cursor"), ("pointer", "/next_cursor")))))));

        Assert.Single(rows);
        Assert.DoesNotContain(_server.Requests, r => r.Url.Query.Contains("cursor=X"));
        Assert.Equal("http source 's.d': stopped after for_each.max_pages (1) list pages; more list pages were available",
            Assert.Single(_notices));
    }

    [Fact]
    public async Task Item_crawl_not_advancing_is_caught()
    {
        MapOrders("o1", "o2");
        _server.MapPrefix("/orders/", r => new StubResponse(200, """{"lines":[{"sku":"x"}]}""",
            new Dictionary<string, string> { ["Link"] = $"<{_server.BaseUrl}orders/o1/lines>; rel=\"next\"" }));
        var options = OrdersOptions();
        options["pagination"] = Map(("strategy", "link_header"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(options)));

        Assert.False(ex.IsTransient);
        Assert.Contains("pagination is not advancing", ex.Message);
        Assert.EndsWith("(item order_id=o1)", ex.Message);
    }

    [Fact]
    public async Task Item_pages_do_not_count_against_other_items()
    {
        // "Already requested" is per item crawl: two items whose first pages link to the same shared
        // page are not a loop.
        MapOrders("o1", "o2");
        _server.MapPrefix("/orders/", _ => new StubResponse(200, """{"lines":[{"sku":"x"}]}""",
            new Dictionary<string, string> { ["Link"] = $"<{_server.BaseUrl}shared>; rel=\"next\"" }));
        _server.Map("/shared", _ => new StubResponse(200, """{"lines":[{"sku":"s"}]}"""));
        var options = OrdersOptions();
        options["pagination"] = Map(("strategy", "link_header"));

        var rows = await Read(Spec(options));

        Assert.Equal(4, rows.Count);
        Assert.Equal(["/orders", "/orders/o1/lines", "/shared", "/orders/o2/lines", "/shared"], RequestLines());
    }

    [Fact]
    public async Task Missing_item_field_names_list_page_and_record()
    {
        _server.Map("/orders", _ => new StubResponse(200, """{"data":[{"id":"o1"},{"name":"no id"}]}"""));
        MapLines(new() { ["o1"] = 1 });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(OrdersOptions())));

        Assert.False(ex.IsTransient);
        Assert.Contains("item field 'order_id' is missing from the record", ex.Message);
        Assert.Contains($"list page '{_server.BaseUrl}orders', record 1", ex.Message);
    }

    [Fact]
    public async Task List_and_items_go_through_gate()
    {
        MapOrders("o1", "o2");
        MapLines(new() { ["o1"] = 1, ["o2"] = 1 });
        var gate = new RecordingGate();

        await Read(Spec(OrdersOptions()), gate);

        Assert.Equal(5, _server.Requests.Count);
        Assert.Equal(Enumerable.Repeat(("http.get_page", true), 5), gate.Calls);
    }

    [Fact]
    public async Task Run_date_shared_by_list_and_items()
    {
        var clock = new ManualClock();
        clock.Advance(DateTimeOffset.Parse("2026-11-07T23:59:59Z") - clock.GetUtcNow());
        var start = clock.GetUtcNow();
        _server.Map("/exports", _ =>
        {
            // Midnight passes while the list is fetched.
            clock.Advance(TimeSpan.FromSeconds(5));
            return new StubResponse(200, """{"data":[{"id":"e1"},{"id":"e2"}]}""");
        });
        _server.MapPrefix("/exports/", _ =>
        {
            clock.Advance(TimeSpan.FromSeconds(5));
            return new StubResponse(200, """{"lines":[{"n":1}]}""");
        });

        var rows = await Read(Spec(Map(
            ("for_each", Map(("path", "/exports"), ("query", Map(("date", "{{ run_date }}"))), ("items", "/data"),
                ("fields", Map(("id", "/id"))))),
            ("run_date", Map(("format", "yyyy-MM-dd"))),
            ("path", "/exports/{{ run_date }}/{{ item.id }}"),
            ("items", "/lines"))), time: clock);

        Assert.Equal(["/exports?date=2026-11-07", "/exports/2026-11-07/e1", "/exports/2026-11-07/e2"],
            RequestLines());
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal(start, r[2]));
        Assert.Equal(["""{"id":"e1"}""", """{"id":"e2"}"""], rows.Select(r => r[3]));
    }

    [Fact]
    public async Task On_missing_skip_covers_410()
    {
        MapOrders("o1", "o2", "o3");
        MapLines(new() { ["o1"] = 1, ["o3"] = 1 }, (id, page) => id == "o2" && page == 1 ? new StubResponse(410, "gone") : null);

        var rows = await Read(Spec(OrdersOptions(Map(("on_missing", "skip")))));

        Assert.Equal(["o1-1", "o3-1"], rows.Select(r => JsonNode.Parse((string)r[0]!)!["sku"]!.GetValue<string>()));
        Assert.Equal("http source 's.d': skipped 1 items that returned 404/410 (first: item order_id=o2)",
            Assert.Single(_notices));
    }

    [Fact]
    public async Task Item_max_pages_truncation_is_reported()
    {
        MapOrders("o1", "o2", "o3");
        // o3's first page is empty, so max_pages did not cut it short.
        MapLines(new() { ["o1"] = 2, ["o2"] = 3 });

        var rows = await Read(Spec(OrdersOptions(extra: Map(("max_pages", 1)))));

        Assert.Equal(2, rows.Count);
        Assert.Equal("http source 's.d': max_pages stopped 2 items before their last page (first: item order_id=o1)",
            Assert.Single(_notices));
    }

    [Fact]
    public async Task Max_items_reached_at_end_of_list_page_does_not_fetch_the_next_page()
    {
        _server.Map("/orders", r => Query(r, "cursor") == "X"
            ? new StubResponse(500, "must not be requested")
            : new StubResponse(200, """{"data":[{"id":"o1"},{"id":"o2"}],"next_cursor":"X"}"""));
        MapLines(new() { ["o1"] = 1, ["o2"] = 1 });

        var rows = await Read(Spec(OrdersOptions(Map(("max_items", 2),
            ("pagination", Map(("strategy", "cursor"), ("param", "cursor"), ("pointer", "/next_cursor")))))));

        Assert.Equal(2, rows.Count);
        Assert.DoesNotContain(_server.Requests, r => r.Url.Query.Contains("cursor=X"));
        Assert.Equal("http source 's.d': stopped after max_items (2) items; more list records may be available",
            Assert.Single(_notices));
    }

    [Fact]
    public async Task Secret_in_list_page_url_is_redacted_in_item_errors()
    {
        _server.Map("/orders", r => Query(r, "page") == "2"
            ? new StubResponse(200, """{"data":[{"name":"no id"}]}""")
            : new StubResponse(200, """{"data":[{"id":"o1"}]}""", new Dictionary<string, string>
            {
                ["Link"] = $"<{_server.BaseUrl}orders?page=2&api_key=TOPSECRET>; rel=\"next\"",
            }));
        MapLines(new() { ["o1"] = 1 });
        var options = OrdersOptions(Map(("pagination", Map(("strategy", "link_header")))));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(options),
            auth: new() { ["type"] = "api_key", ["key"] = "TOPSECRET", ["param"] = "api_key" }));

        Assert.Contains("item field 'order_id' is missing from the record", ex.Message);
        Assert.Contains("api_key=***", ex.Message);
        Assert.Contains("record 0", ex.Message);
        Assert.DoesNotContain("TOPSECRET", ex.Message);
    }

    [Fact]
    public async Task Item_identity_appears_once_in_a_conversion_error()
    {
        MapOrders("abc");
        MapLines(new() { ["abc"] = 1 });
        var options = OrdersOptions(extra: Map(("columns", new Dictionary<string, string>
            { ["order_id"] = "int", ["sku"] = "varchar" })));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(options)));

        Assert.Contains("column 'order_id'", ex.Message);
        Assert.Single(Regex.Matches(ex.Message, Regex.Escape("item order_id=abc")));
    }

    /// <summary>Counts the buffers it hands out that are not yet disposed.</summary>
    private sealed class CountingAllocator : MemoryAllocator
    {
        private int _outstanding;
        private int _allocated;

        public int Outstanding => Volatile.Read(ref _outstanding);

        public int Allocated => Volatile.Read(ref _allocated);

        protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
        {
            Interlocked.Increment(ref _allocated);
            Interlocked.Increment(ref _outstanding);
            bytesAllocated = length;
            return new Owner(new byte[length], this);
        }

        private sealed class Owner(byte[] buffer, CountingAllocator allocator) : IMemoryOwner<byte>
        {
            private int _disposed;

            public Memory<byte> Memory => buffer;

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) == 0)
                {
                    Interlocked.Decrement(ref allocator._outstanding);
                }
            }
        }
    }

    [Fact]
    public async Task Held_batch_is_disposed_when_the_lookahead_throws()
    {
        MapOrders("o1");
        _server.MapPrefix("/orders/", _ => new StubResponse(200, """{"lines":[{"sku":"a","qty":1},{"sku":"b","qty":"x"}]}"""));
        var options = OrdersOptions(extra: Map(("columns", new Dictionary<string, string>
            { ["sku"] = "varchar", ["qty"] = "int" })));
        options.Remove("pagination");
        var allocator = new CountingAllocator();

        await using var source = Source();
        var partition = Assert.IsType<HttpForEachPartition>(
            Assert.Single(await source.PlanReadAsync(Spec(options), ReadHints.None, CancellationToken.None)));
        partition.Allocator = allocator;
        var yielded = 0;
        // Row "a" is built into a batch and held while the next one is read; row "b" fails to convert.
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var batch in partition.ReadAsync(new BatchOptions(TargetBatchBytes: 1, MaxRowsPerBatch: 1),
                               CancellationToken.None))
            {
                yielded++;
                batch.Dispose();
            }
        });

        Assert.Contains("column 'qty'", ex.Message);
        Assert.Equal(0, yielded);
        Assert.True(allocator.Allocated > 0);
        Assert.Equal(0, allocator.Outstanding);
    }

    [Fact]
    public async Task Read_ahead_batch_is_disposed_when_the_read_stops_at_a_yield()
    {
        MapOrders("o1");
        _server.MapPrefix("/orders/", _ => new StubResponse(200, """{"lines":[{"sku":"a"},{"sku":"b"},{"sku":"c"}]}"""));
        var options = OrdersOptions(extra: Map(("columns", new Dictionary<string, string> { ["sku"] = "varchar" })));
        options.Remove("pagination");
        var allocator = new CountingAllocator();

        await using var source = Source();
        var partition = Assert.IsType<HttpForEachPartition>(
            Assert.Single(await source.PlanReadAsync(Spec(options), ReadHints.None, CancellationToken.None)));
        partition.Allocator = allocator;
        var yielded = 0;
        // One row per batch: batch "b" is read ahead while "a" is yielded; the consumer stops right there.
        await foreach (var batch in partition.ReadAsync(new BatchOptions(TargetBatchBytes: 1, MaxRowsPerBatch: 1),
                           CancellationToken.None))
        {
            yielded++;
            batch.Dispose();
            break;
        }

        Assert.Equal(1, yielded);
        Assert.True(allocator.Allocated > 0);
        Assert.Equal(0, allocator.Outstanding);
    }

    [Fact]
    public async Task Plain_datasets_unchanged()
    {
        await using var source = Source();
        var plain = await source.PlanReadAsync(Spec(Map(("path", "/items"))), ReadHints.None, CancellationToken.None);
        var forEach = await source.PlanReadAsync(Spec(OrdersOptions()), ReadHints.None, CancellationToken.None);

        Assert.IsType<HttpPartition>(Assert.Single(plain));
        var partition = Assert.IsType<HttpForEachPartition>(Assert.Single(forEach));
        Assert.Equal("s.d", partition.PartitionId);
        Assert.False(partition.TryGetCheckpoint(out _));
        Assert.False(partition.TryResumeFrom("https://example.com/x"));
    }
}
