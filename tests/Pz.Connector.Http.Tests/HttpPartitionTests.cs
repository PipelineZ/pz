using Apache.Arrow;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

public class HttpPartitionTests
{
    private static async Task<(long Rows, int Batches, StubHttpServer Server)> ReadAllAsync(StubHttpServer server,
        Dictionary<string, object?> options, Dictionary<string, object?>? connectionExtra = null,
        BatchOptions? batchOptions = null, string? watermarkCursor = null)
    {
        var connection = new Dictionary<string, object?> { ["base_url"] = server.BaseUrl.ToString() };
        foreach (var (k, v) in connectionExtra ?? []) connection[k] = v;
        var connector = new HttpConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(connection), CancellationToken.None);
        var partitions = await source.PlanReadAsync(
            new DatasetSpec("s", "d", options) { WatermarkCursor = watermarkCursor },
            ReadHints.None, CancellationToken.None);
        long rows = 0;
        var batches = 0;
        await foreach (var batch in partitions[0].ReadAsync(batchOptions ?? BatchOptions.Default, CancellationToken.None))
        {
            batches++;
            rows += batch.Length;
            batch.Dispose();
        }

        return (rows, batches, server);
    }

    private static void MapPages(StubHttpServer server, params string[] pages) =>
        server.Map("/items", req =>
        {
            var q = req.Url.Query;
            var page = q.Contains("page=") ? int.Parse(q.Split("page=")[1].Split('&')[0]) : 1;
            return new StubResponse(200, page <= pages.Length ? pages[page - 1] : "[]");
        });

    private static Dictionary<string, object?> GuardedOptions(long? maxPages, string? cursorOrder = null)
    {
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/items",
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "page" },
            ["cursor"] = "id",
            ["cursor_type"] = "bigint",
        };
        if (maxPages is { } cap) options["max_pages"] = cap;
        if (cursorOrder is not null) options["cursor_order"] = cursorOrder;
        return options;
    }

    [Fact]
    public async Task Truncated_ascending_crawl_succeeds_and_lands_rows()
    {
        await using var server = new StubHttpServer();
        MapPages(server, """[{"id":1},{"id":2}]""", """[{"id":3},{"id":4}]""", """[{"id":5}]""");
        var (rows, _, _) = await ReadAllAsync(server, GuardedOptions(maxPages: 2), watermarkCursor: "id");
        Assert.Equal(4, rows); // truncated at page 2, but ascending: contiguous prefix, safe
    }

    [Fact]
    public async Task Truncated_descending_crawl_fails_permanent()
    {
        await using var server = new StubHttpServer();
        MapPages(server, """[{"id":6},{"id":5}]""", """[{"id":4},{"id":3}]""", """[{"id":2}]""");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAllAsync(server, GuardedOptions(maxPages: 2), watermarkCursor: "id"));
        Assert.False(ex.IsTransient);
        Assert.Contains("not ascending", ex.Message);
        Assert.Contains("max_pages", ex.Message);
    }

    [Fact]
    public async Task Completed_descending_crawl_succeeds()
    {
        await using var server = new StubHttpServer();
        MapPages(server, """[{"id":4},{"id":3}]""", """[{"id":2},{"id":1}]"""); // page 3 -> [] ends it
        var (rows, _, _) = await ReadAllAsync(server, GuardedOptions(maxPages: null), watermarkCursor: "id");
        Assert.Equal(4, rows); // whole slice landed; order is irrelevant on completion
    }

    [Fact]
    public async Task Truncated_all_equal_cursors_fail()
    {
        await using var server = new StubHttpServer();
        MapPages(server, """[{"id":7},{"id":7}]""", """[{"id":7},{"id":7}]""", """[{"id":7}]""");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAllAsync(server, GuardedOptions(maxPages: 2), watermarkCursor: "id"));
        Assert.Contains("not ascending", ex.Message); // ascent unprovable => contiguity unprovable
    }

    [Fact]
    public async Task Truncated_descending_contract_mode_crawl_fails()
    {
        // Contract mode: the cursor ordinal comes from the Columns key order (here: name=0, id=1),
        // not the raw envelope's fixed slot -- a wrong ordinal would track the wrong column.
        await using var server = new StubHttpServer();
        MapPages(server,
            """[{"name":"a","id":6},{"name":"b","id":5}]""",
            """[{"name":"c","id":4},{"name":"d","id":3}]""",
            """[{"name":"e","id":2}]""");
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/items",
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "page" },
            ["columns"] = new Dictionary<string, string> { ["name"] = "varchar", ["id"] = "bigint" },
            ["max_pages"] = 2L,
        };
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAllAsync(server, options, watermarkCursor: "id"));
        Assert.False(ex.IsTransient);
        Assert.Contains("not ascending", ex.Message);
    }

    [Fact]
    public async Task Guard_names_a_false_asc_declaration()
    {
        await using var server = new StubHttpServer();
        MapPages(server, """[{"id":6},{"id":5}]""", """[{"id":4},{"id":3}]""", """[{"id":2}]""");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAllAsync(server, GuardedOptions(maxPages: 2, cursorOrder: "asc"), watermarkCursor: "id"));
        Assert.Contains("cursor_order: asc", ex.Message);
    }

    // SpecBuilder stamps WatermarkCursor (value still null) on a plain incremental dataset's FIRST
    // run: the guard must treat a cursor-set/value-null spec exactly like any other incremental spec.
    [Fact]
    public async Task First_run_shape_cursor_set_value_null_truncated_descending_crawl_fails_permanent()
    {
        await using var server = new StubHttpServer();
        MapPages(server, """[{"id":6},{"id":5}]""", """[{"id":4},{"id":3}]""", """[{"id":2}]""");
        var connection = new Dictionary<string, object?> { ["base_url"] = server.BaseUrl.ToString() };
        var connector = new HttpConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(connection), CancellationToken.None);
        var spec = new DatasetSpec("s", "d", GuardedOptions(maxPages: 2)) { WatermarkCursor = "id", WatermarkValue = null };
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batch.Dispose();
            }
        });

        Assert.False(ex.IsTransient);
        Assert.Contains("not ascending", ex.Message);
        Assert.Contains("max_pages", ex.Message);
    }

    // A resumed attempt (TryResumeFrom) never saw the prior attempt's prefix, so even an
    // ASCENDING-looking tail proves nothing about the full crawl's
    // ordering -- max_pages truncation on a resumed attempt must fail loudly regardless of
    // monotonicity, unlike a from-scratch attempt (see Truncated_ascending_crawl_succeeds_and_lands_rows
    // above, which allows exactly this shape when there was no resume).
    [Fact]
    public async Task Resumed_attempt_truncated_by_max_pages_fails_even_when_ascending()
    {
        await using var server = new StubHttpServer();
        var page2Link = $"{server.BaseUrl}items?page=2";
        var page3Link = $"{server.BaseUrl}items?page=3";
        server.Map("/items", req =>
        {
            var q = req.Url.Query;
            if (q.Contains("page=3"))
            {
                return new StubResponse(200, """[{"id":5},{"id":6}]"""); // terminal: no Link header
            }

            if (q.Contains("page=2"))
            {
                return new StubResponse(200, """[{"id":3},{"id":4}]""",
                    new Dictionary<string, string> { ["Link"] = $"<{page3Link}>; rel=\"next\"" });
            }

            return new StubResponse(200, """[{"id":1},{"id":2}]""",
                new Dictionary<string, string> { ["Link"] = $"<{page2Link}>; rel=\"next\"" });
        });

        var options = new Dictionary<string, object?>
        {
            ["path"] = "/items",
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "link_header" },
            ["cursor"] = "id",
            ["cursor_type"] = "bigint",
            ["max_pages"] = 1L,
        };
        var connection = new Dictionary<string, object?> { ["base_url"] = server.BaseUrl.ToString() };
        var connector = new HttpConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(connection), CancellationToken.None);
        var spec = new DatasetSpec("s", "d", options) { WatermarkCursor = "id" };
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        var partition = Assert.IsAssignableFrom<ICheckpointingPartition>(partitions[0]);
        Assert.True(partition.TryResumeFrom(page2Link));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batch.Dispose();
            }
        });

        Assert.False(ex.IsTransient);
        Assert.Contains("resumed", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("cannot prove the full crawl's ordering", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Non_incremental_truncated_descending_crawl_is_unaffected()
    {
        await using var server = new StubHttpServer();
        MapPages(server, """[{"id":6},{"id":5}]""", """[{"id":4},{"id":3}]""", """[{"id":2}]""");
        // No watermarkCursor: bounded snapshot, no watermark to corrupt -- guard stays off.
        var (rows, _, _) = await ReadAllAsync(server, GuardedOptions(maxPages: 2));
        Assert.Equal(4, rows);
    }

    [Fact]
    public async Task Follows_link_header_across_pages_and_lands_all_rows()
    {
        await using var server = new StubHttpServer();
        server.Map("/items", req => req.Url.Query.Contains("page=2")
            ? new StubResponse(200, """[{"id":3}]""")
            : new StubResponse(200, """[{"id":1},{"id":2}]""", new Dictionary<string, string>
                { ["Link"] = $"<{server.BaseUrl}items?page=2>; rel=\"next\"" }));

        var (rows, _, _) = await ReadAllAsync(server, new()
        {
            ["path"] = "/items",
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "link_header" },
        });

        Assert.Equal(3, rows);
    }

    [Fact]
    public async Task Http_429_is_transient_with_retry_after()
    {
        await using var server = new StubHttpServer();
        server.Map("/items", _ => new StubResponse(429, """{"msg":"slow down"}""",
            new Dictionary<string, string> { ["Retry-After"] = "30" }));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => ReadAllAsync(server, new() { ["path"] = "/items" }));
        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(30), ex.RetryAfter);
        Assert.Contains("429", ex.Message);
        Assert.Contains("'s.d'", ex.Message);
    }

    [Fact]
    public async Task Http_404_is_permanent_with_status_and_snippet()
    {
        await using var server = new StubHttpServer();
        server.Map("/items", _ => new StubResponse(404, """{"message":"Not Found"}"""));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => ReadAllAsync(server, new() { ["path"] = "/items" }));
        Assert.False(ex.IsTransient);
        Assert.Contains("404", ex.Message);
        Assert.Contains("Not Found", ex.Message);
    }

    [Fact]
    public async Task Api_key_in_query_is_redacted_in_error_messages()
    {
        await using var server = new StubHttpServer();
        server.Map("/items", _ => new StubResponse(404, "{}"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(server,
            new() { ["path"] = "/items" },
            new()
            {
                ["auth"] = new Dictionary<string, object?>
                    { ["type"] = "api_key", ["key"] = "TOPSECRET", ["param"] = "api_key" },
            }));

        Assert.DoesNotContain("TOPSECRET", ex.Message);
        Assert.Contains("api_key=***", ex.Message);
    }

    [Fact]
    public async Task Non_advancing_pagination_fails_permanent()
    {
        await using var server = new StubHttpServer();
        server.Map("/items", _ => new StubResponse(200,
            """{ "data": [{"id":1}], "next": "same-token" }"""));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(server, new()
        {
            ["path"] = "/items",
            ["items"] = "/data",
            ["pagination"] = new Dictionary<string, object?>
                { ["strategy"] = "cursor", ["pointer"] = "/next", ["param"] = "cursor" },
        }));

        Assert.False(ex.IsTransient);
        Assert.Contains("not advancing", ex.Message);
    }

    [Fact]
    public async Task Page_strategy_stamps_page_and_size_params_on_the_first_request()
    {
        await using var server = new StubHttpServer();
        server.Map("/items", _ => new StubResponse(200, """[{"id":1}]"""));

        await ReadAllAsync(server, new()
        {
            ["path"] = "/items",
            ["pagination"] = new Dictionary<string, object?>
            {
                ["strategy"] = "page", ["param"] = "page", ["start"] = 1L,
                ["size_param"] = "per_page", ["size"] = 50L,
            },
            ["max_pages"] = 2L,
        });

        // Without page/size on the FIRST request the API serves its default page size:
        // smaller than `size` silently skips rows once page 2 jumps ahead; larger
        // re-delivers them. The first request must pin both.
        Assert.Equal(2, server.Requests.Count);
        Assert.Contains("page=1", server.Requests[0].Url.Query);
        Assert.Contains("per_page=50", server.Requests[0].Url.Query);
        Assert.Contains("page=2", server.Requests[1].Url.Query);
        Assert.Contains("per_page=50", server.Requests[1].Url.Query);
    }

    [Fact]
    public async Task Max_pages_caps_the_crawl()
    {
        await using var server = new StubHttpServer();
        server.Map("/items", _ => new StubResponse(200, """[{"id":1}]"""));  // page strategy never ends

        var (rows, _, _) = await ReadAllAsync(server, new()
        {
            ["path"] = "/items",
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "page" },
            ["max_pages"] = 3L,
        });

        Assert.Equal(3, rows);
        Assert.Equal(3, server.Requests.Count);
    }

    [Fact]
    public async Task Contract_mode_lands_typed_columns()
    {
        await using var server = new StubHttpServer();
        server.Map("/items", _ => new StubResponse(200, """[{"id":7,"name":"x","drift":true}]"""));

        var connector = new HttpConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(
            new Dictionary<string, object?> { ["base_url"] = server.BaseUrl.ToString() }), CancellationToken.None);
        var partitions = await source.PlanReadAsync(new DatasetSpec("s", "d", new Dictionary<string, object?>
        {
            ["path"] = "/items",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        }), ReadHints.None, CancellationToken.None);

        var rowsSeen = 0;
        await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            rowsSeen += batch.Length;
            Assert.Equal(7L, ((Int64Array)batch.Column(0)).GetValue(0));
            Assert.Equal("x", ((StringArray)batch.Column(1)).GetString(0));
            batch.Dispose();
        }

        Assert.NotEqual(0, rowsSeen);
    }

    [Fact]
    public async Task Network_failure_is_transient_and_names_the_url()
    {
        // Port 1 is not a listener anywhere in this test run: the connect attempt fails fast with
        // a socket-level refusal, never a request that reaches a server.
        var connection = new Dictionary<string, object?> { ["base_url"] = "http://127.0.0.1:1/" };
        var connector = new HttpConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(connection), CancellationToken.None);
        var partitions = await source.PlanReadAsync(new DatasetSpec("s", "d",
            new Dictionary<string, object?> { ["path"] = "/items" }), ReadHints.None, CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batch.Dispose();
            }
        });

        Assert.True(ex.IsTransient);
        Assert.Contains("127.0.0.1:1", ex.Message);
    }

    [Fact]
    public async Task Invalid_json_response_is_permanent_and_names_the_dataset()
    {
        await using var server = new StubHttpServer();
        server.Map("/items", _ => new StubResponse(200, "not json {{{"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => ReadAllAsync(server, new() { ["path"] = "/items" }));

        Assert.False(ex.IsTransient);
        Assert.Contains("'s.d'", ex.Message);
        Assert.Contains("not valid JSON", ex.Message);
    }

    [Fact]
    public async Task Items_pointer_resolving_to_a_string_is_permanent_error_wrong_shape()
    {
        await using var server = new StubHttpServer();
        server.Map("/items", _ => new StubResponse(200, """{"data": {"items": "a string"}}"""));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(server, new()
        {
            ["path"] = "/items",
            ["items"] = "/data/items",
        }));

        Assert.False(ex.IsTransient);
        Assert.Contains("/data/items", ex.Message);
        Assert.Contains("neither an array nor an object", ex.Message);
    }

    [Fact]
    public async Task Items_pointer_missing_entirely_is_permanent_error_not_empty()
    {
        // Pins the ACTUAL current behavior: a pointer segment that isn't present at all fails
        // JsonPointer.TryResolve, and HttpPartition treats that as an error — not an empty page.
        await using var server = new StubHttpServer();
        server.Map("/items", _ => new StubResponse(200, """{"data": {}}"""));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(server, new()
        {
            ["path"] = "/items",
            ["items"] = "/data/items",
        }));

        Assert.False(ex.IsTransient);
        Assert.Contains("/data/items", ex.Message);
        Assert.Contains("not found", ex.Message);
    }

    [Fact]
    public async Task Small_max_rows_per_batch_yields_multiple_batches_mid_page()
    {
        await using var server = new StubHttpServer();
        server.Map("/items", _ => new StubResponse(200,
            """[{"id":1},{"id":2},{"id":3},{"id":4},{"id":5}]"""));

        var (rows, batches, _) = await ReadAllAsync(server, new() { ["path"] = "/items" },
            batchOptions: new BatchOptions(MaxRowsPerBatch: 2));

        Assert.Equal(5, rows);
        Assert.True(batches >= 2, $"expected at least 2 batches, got {batches}");
    }

    [Fact]
    public async Task Pathed_base_url_preserves_prefix_in_dataset_requests()
    {
        // base_url has a path prefix (no trailing slash) — the dataset path must resolve relative
        // to the FULL base, not root-relative to the host (which would drop 'api/v2').
        await using var server = new StubHttpServer();
        server.Map("/api/v2/items", _ => new StubResponse(200, """[{"id":1}]"""));

        var (rows, _, srv) = await ReadAllAsync(server, new() { ["path"] = "/items" },
            new() { ["base_url"] = server.BaseUrl + "api/v2" });

        Assert.Equal(1, rows);
        Assert.Single(srv.Requests);
        Assert.Equal("/api/v2/items", srv.Requests[0].Url.AbsolutePath);
    }

    [Fact]
    public async Task Api_key_leaked_in_error_body_snippet_is_redacted()
    {
        await using var server = new StubHttpServer();
        server.Map("/items", _ => new StubResponse(404,
            """{"error":"forbidden, retry https://x/items?api_key=TOPSECRET"}"""));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAllAsync(server,
            new() { ["path"] = "/items" },
            new()
            {
                ["auth"] = new Dictionary<string, object?>
                    { ["type"] = "api_key", ["key"] = "TOPSECRET", ["param"] = "api_key" },
            }));

        Assert.DoesNotContain("TOPSECRET", ex.Message);
        Assert.Contains("api_key=***", ex.Message);
    }
}
