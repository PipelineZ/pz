using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

/// <summary>Red-team suite: the HTTP connector treated as if the endpoint on the other side were
/// actively trying to break it. Every test asserts one of the three standing invariants —
/// <b>never hang, never silently lose records, never silently duplicate records</b> — plus the
/// secret-hygiene rule, against a transport-level-hostile endpoint (<see cref="HostileServer"/>)
/// or a scripted well-formed one (<see cref="StubHttpServer"/>).
///
/// Bounding rule: every read runs under <see cref="Bound"/>. A test that trips the bound is
/// reporting an unbounded crawl, not a slow one — so the bound is asserted explicitly rather than
/// left to the runner's hang detector.</summary>
public class HostileApiRedTeamTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);

    private sealed record ReadOutcome(long Rows, int Batches);

    private static async Task<ReadOutcome> ReadAsync(Uri baseUrl, Dictionary<string, object?> options,
        Dictionary<string, object?>? connectionExtra = null, string? watermarkCursor = null,
        CancellationToken ct = default)
    {
        var connection = new Dictionary<string, object?> { ["base_url"] = baseUrl.ToString() };
        foreach (var (k, v) in connectionExtra ?? [])
        {
            connection[k] = v;
        }

        var connector = new HttpConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(connection), ct);
        var partitions = await source.PlanReadAsync(
            new DatasetSpec("api", "items", options) { WatermarkCursor = watermarkCursor },
            ReadHints.None, ct);

        long rows = 0;
        var batches = 0;
        await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, ct))
        {
            batches++;
            rows += batch.Length;
            batch.Dispose();
        }

        return new ReadOutcome(rows, batches);
    }

    private static Dictionary<string, object?> RawOptions(string? strategy = null,
        Dictionary<string, object?>? pagination = null, long? maxPages = null)
    {
        var options = new Dictionary<string, object?> { ["path"] = "/items" };
        if (pagination is not null)
        {
            options["pagination"] = pagination;
        }
        else if (strategy is not null)
        {
            options["pagination"] = new Dictionary<string, object?> { ["strategy"] = strategy };
        }

        if (maxPages is { } cap)
        {
            options["max_pages"] = cap;
        }

        return options;
    }

    private static CancellationTokenSource Bounded() => new(Bound);

    // ---------------------------------------------------------------------------------------
    // Transport-level hostility: resets, stalls, truncation.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task Connection_reset_before_any_response_is_transient_not_a_hang()
    {
        await using var server = new HostileServer((_, _) => HostileReply.ResetImmediately);
        using var cts = Bounded();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(server.BaseUrl, RawOptions(), ct: cts.Token));

        Assert.False(cts.IsCancellationRequested); // returned on its own, not via the bound
        Assert.True(ex.IsTransient, "a connection reset is retryable, not a config error");
    }

    [Fact]
    public async Task Connection_reset_midway_through_the_body_is_transient_and_lands_nothing()
    {
        // Content-Length promises a full page; the peer sends half of it and then RSTs.
        await using var server = new HostileServer((_, _) =>
            HostileReply.Truncated("""[{"id":1},{"id":2},{"i""", declaredLength: 4096, reset: true));
        using var cts = Bounded();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(server.BaseUrl, RawOptions(), ct: cts.Token));

        Assert.False(cts.IsCancellationRequested);
        Assert.True(ex.IsTransient, "a mid-body reset is retryable");
    }

    [Fact]
    public async Task Body_shorter_than_content_length_never_lands_a_partial_page()
    {
        // No reset — a clean FIN after an under-length body. The danger is parsing the prefix and
        // treating the surviving rows as a complete page: silent record loss.
        await using var server = new HostileServer((_, _) =>
            HostileReply.Truncated("""{"items":[{"id":1},{"id":2}""", declaredLength: 9000));
        using var cts = Bounded();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(server.BaseUrl, RawOptions(), ct: cts.Token));

        Assert.False(cts.IsCancellationRequested);
        Assert.True(ex.IsTransient, "a truncated body is a transport failure, so retryable");
    }

    [Fact]
    public async Task A_stalled_endpoint_does_not_hang_the_read_forever()
    {
        // The endpoint accepts the connection and never answers. Something must bound this: either
        // the connector's own request timeout, or nothing at all (in which case the caller's token
        // is the only escape and a production run stalls for HttpClient's 100s default per page).
        await using var server = new HostileServer((_, _) => HostileReply.Stall);
        using var cts = Bounded();

        // A 2s per-request timeout rather than the 30s default, so the assertion is about the bound
        // existing and being configurable, not about waiting out the default.
        var ex = await Record.ExceptionAsync(() =>
            ReadAsync(server.BaseUrl, RawOptions(),
                new Dictionary<string, object?> { ["timeout_seconds"] = 2 }, ct: cts.Token));

        Assert.NotNull(ex);
        Assert.False(cts.IsCancellationRequested,
            "the read must bound itself on a black-holed request, not lean on the caller's token");
        var connectorException = Assert.IsType<PzConnectorException>(ex);
        Assert.True(connectorException.IsTransient, "a timeout is retryable");
    }

    [Fact]
    public async Task Empty_200_body_fails_loudly_rather_than_landing_zero_rows()
    {
        await using var server = new HostileServer((_, _) => HostileReply.Json(""));
        using var cts = Bounded();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(server.BaseUrl, RawOptions(), ct: cts.Token));

        Assert.False(ex.IsTransient);
        Assert.Contains("not valid JSON", ex.Message);
    }

    [Fact]
    public async Task Malformed_json_fails_loudly_and_lands_nothing()
    {
        await using var server = new HostileServer((_, _) =>
            HostileReply.Json("""[{"id":1},{"id":"""));
        using var cts = Bounded();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(server.BaseUrl, RawOptions(), ct: cts.Token));

        Assert.False(ex.IsTransient);
        Assert.Contains("not valid JSON", ex.Message);
    }

    [Fact]
    public async Task Html_error_page_served_with_200_fails_loudly()
    {
        // A proxy or WAF in front of the API answering 200 with an HTML interstitial.
        await using var server = new HostileServer((_, _) =>
            HostileReply.Json("<html><body>Access denied</body></html>"));
        using var cts = Bounded();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(server.BaseUrl, RawOptions(), ct: cts.Token));

        Assert.False(ex.IsTransient);
        Assert.Contains("not valid JSON", ex.Message);
    }

    // ---------------------------------------------------------------------------------------
    // Status-code classification.
    // ---------------------------------------------------------------------------------------

    [Theory]
    [InlineData(408)]
    [InlineData(429)]
    [InlineData(500)]
    [InlineData(502)]
    [InlineData(503)]
    [InlineData(504)]
    public async Task Retryable_statuses_classify_transient(int status)
    {
        await using var server = new HostileServer((_, _) =>
            HostileReply.Json("""{"error":"nope"}""", status));
        using var cts = Bounded();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(server.BaseUrl, RawOptions(), ct: cts.Token));

        Assert.True(ex.IsTransient, $"HTTP {status} must be retryable");
        Assert.Contains(status.ToString(), ex.Message);
    }

    [Theory]
    [InlineData(400)]
    [InlineData(401)]
    [InlineData(403)]
    [InlineData(404)]
    [InlineData(422)]
    public async Task Permanent_statuses_classify_permanent(int status)
    {
        await using var server = new HostileServer((_, _) =>
            HostileReply.Json("""{"error":"nope"}""", status));
        using var cts = Bounded();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(server.BaseUrl, RawOptions(), ct: cts.Token));

        Assert.False(ex.IsTransient, $"HTTP {status} must not be retried forever");
        Assert.Contains(status.ToString(), ex.Message);
    }

    [Fact]
    public async Task Rate_limit_surfaces_the_servers_retry_after_hint()
    {
        await using var server = new HostileServer((_, _) =>
            HostileReply.Json("""{"error":"slow down"}""", 429, ("Retry-After", "42")));
        using var cts = Bounded();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(server.BaseUrl, RawOptions(), ct: cts.Token));

        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(42), ex.RetryAfter);
    }

    [Fact]
    public async Task An_absurd_retry_after_is_capped_by_the_policy_that_performs_the_wait()
    {
        // A hostile 429 asking pz to sleep for a decade. The connector reports the hint verbatim by
        // design (classification only — the engine owns retry policy), so the cap that stops this
        // being a ten-year hang lives in RetryPolicy. Asserted here so the two halves stay honest
        // about each other.
        await using var server = new HostileServer((_, _) =>
            HostileReply.Json("""{"error":"go away"}""", 429, ("Retry-After", "315360000")));
        using var cts = Bounded();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(server.BaseUrl, RawOptions(), ct: cts.Token));

        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(315360000), ex.RetryAfter);

        var policy = Pz.Engine.Resilience.RetryPolicy.Default;
        var delay = policy.ComputeDelay(attempt: 1, ex.RetryAfter, new Random(1));
        Assert.True(delay <= policy.MaxDelay * 1.25, // MaxDelay plus the policy's ±25% jitter
            $"a server-supplied Retry-After must be capped by the policy, but it waited {delay}");
    }

    [Fact]
    public async Task Auth_failure_mid_crawl_fails_the_whole_read()
    {
        // Page 1 succeeds, then the OAuth token expires. The read must fail rather than return a
        // truncated result that would advance a watermark past rows never fetched.
        await using var server = new HostileServer((index, _) => index == 0
            ? HostileReply.Json("""{"items":[{"id":1},{"id":2}],"next":"c2"}""")
            : HostileReply.Json("""{"error":"token expired"}""", 401));
        using var cts = Bounded();

        var options = RawOptions(pagination: new Dictionary<string, object?>
        {
            ["strategy"] = "cursor", ["pointer"] = "/next", ["param"] = "cursor",
        });
        options["items"] = "/items";

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(server.BaseUrl, options, ct: cts.Token));

        Assert.False(ex.IsTransient);
        Assert.Contains("401", ex.Message);
    }

    // ---------------------------------------------------------------------------------------
    // Pagination: missing, repeated, backwards, invalid, never-ending cursors.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task Missing_cursor_ends_the_crawl_cleanly()
    {
        await using var server = new HostileServer((index, _) => index == 0
            ? HostileReply.Json("""{"items":[{"id":1}],"next":"c2"}""")
            : HostileReply.Json("""{"items":[{"id":2}]}""")); // no cursor: done
        using var cts = Bounded();

        var options = RawOptions(pagination: new Dictionary<string, object?>
        {
            ["strategy"] = "cursor", ["pointer"] = "/next", ["param"] = "cursor",
        });
        options["items"] = "/items";

        var outcome = await ReadAsync(server.BaseUrl, options, ct: cts.Token);
        Assert.Equal(2, outcome.Rows);
    }

    [Fact]
    public async Task A_cursor_that_never_changes_is_rejected_not_followed_forever()
    {
        await using var server = new HostileServer((_, _) =>
            HostileReply.Json("""{"items":[{"id":1}],"next":"same"}"""));
        using var cts = Bounded();

        var options = RawOptions(pagination: new Dictionary<string, object?>
        {
            ["strategy"] = "cursor", ["pointer"] = "/next", ["param"] = "cursor",
        });
        options["items"] = "/items";

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(server.BaseUrl, options, ct: cts.Token));

        Assert.False(cts.IsCancellationRequested);
        Assert.Contains("not advancing", ex.Message);
    }

    [Fact]
    public async Task A_cursor_that_cycles_between_two_pages_is_rejected_not_followed_forever()
    {
        // The depth-1 "next == current" guard does not see an A -> B -> A -> B cycle.
        await using var server = new HostileServer((index, _) => index % 2 == 0
            ? HostileReply.Json("""{"items":[{"id":1}],"next":"b"}""")
            : HostileReply.Json("""{"items":[{"id":2}],"next":"a"}"""));
        using var cts = Bounded();

        var options = RawOptions(pagination: new Dictionary<string, object?>
        {
            ["strategy"] = "cursor", ["pointer"] = "/next", ["param"] = "cursor",
        });
        options["items"] = "/items";

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(server.BaseUrl, options, ct: cts.Token));

        Assert.False(cts.IsCancellationRequested,
            "a two-page pagination cycle must be detected, not crawled until the bound");
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public async Task A_never_ending_cursor_is_bounded()
    {
        // Every page is fresh, non-empty and offers another cursor: the crawl has no natural end.
        // Unbounded here means an unbounded staging table and a run that never finishes.
        await using var server = new HostileServer((index, _) =>
            HostileReply.Json($$"""{"items":[{"id":{{index}}}],"next":"c{{index + 1}}"}"""));
        using var cts = Bounded();

        var ceiling = HttpPartition.UnboundedPageCeiling;
        HttpPartition.UnboundedPageCeiling = 200; // the real 50 000 takes ~40s to reach
        try
        {
            var options = RawOptions(pagination: new Dictionary<string, object?>
            {
                ["strategy"] = "cursor", ["pointer"] = "/next", ["param"] = "cursor",
            });
            options["items"] = "/items";

            var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
                ReadAsync(server.BaseUrl, options, ct: cts.Token));

            Assert.False(cts.IsCancellationRequested,
                $"crawl was still running after {server.RequestCount} pages — an endless feed must be " +
                "bounded by the connector, not by the operator noticing");
            Assert.False(ex.IsTransient);
            Assert.Contains("max_pages", ex.Message);
        }
        finally
        {
            HttpPartition.UnboundedPageCeiling = ceiling;
        }
    }

    [Fact]
    public async Task An_unparseable_next_link_does_not_silently_truncate_the_crawl()
    {
        // Page 1 has more data behind it, but the Link header is garbage. Treating "I cannot parse
        // the continuation" as "there is no continuation" loses every remaining row silently.
        await using var server = new HostileServer((_, _) =>
            HostileReply.Json("""[{"id":1}]""", 200, ("Link", "<%%not a uri%%>; rel=\"next\"")));
        using var cts = Bounded();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(server.BaseUrl, RawOptions("link_header"), ct: cts.Token));

        Assert.False(ex.IsTransient);
    }

    [Fact]
    public async Task An_empty_page_mid_crawl_does_not_truncate_the_remaining_pages()
    {
        // Page 2 is empty but still advertises page 3. Microsoft Graph delta feeds and filtered
        // GitHub queries both do this. Stopping on the empty page loses page 3 silently.
        await using var server = new HostileServer((index, _) => index switch
        {
            0 => HostileReply.Json("""{"items":[{"id":1}],"next":"c2"}"""),
            1 => HostileReply.Json("""{"items":[],"next":"c3"}"""),
            _ => HostileReply.Json("""{"items":[{"id":3}]}"""),
        });
        using var cts = Bounded();

        var options = RawOptions(pagination: new Dictionary<string, object?>
        {
            ["strategy"] = "cursor", ["pointer"] = "/next", ["param"] = "cursor",
        });
        options["items"] = "/items";

        var outcome = await ReadAsync(server.BaseUrl, options, ct: cts.Token);

        Assert.Equal(2, outcome.Rows); // id 1 and id 3 — the empty page is a gap, not the end
    }

    // ---------------------------------------------------------------------------------------
    // Redirects and server-controlled URLs: credential hygiene.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task A_redirect_to_another_host_does_not_carry_the_api_key_header()
    {
        await using var attacker = new HostileServer((_, _) => HostileReply.Json("""[]"""));
        await using var api = new HostileServer((_, _) =>
            HostileReply.Json("", 302, ("Location", new Uri(attacker.BaseUrl, "items").ToString())));
        using var cts = Bounded();

        await Record.ExceptionAsync(() => ReadAsync(api.BaseUrl, RawOptions(), new Dictionary<string, object?>
        {
            ["auth"] = new Dictionary<string, object?>
            {
                ["type"] = "api_key", ["key"] = "s3cr3t-key", ["header"] = "X-Api-Key",
            },
        }, ct: cts.Token));

        Assert.DoesNotContain(attacker.Requests, r =>
            r.Contains("s3cr3t-key", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task A_redirect_to_another_host_does_not_carry_configured_headers()
    {
        await using var attacker = new HostileServer((_, _) => HostileReply.Json("""[]"""));
        await using var api = new HostileServer((_, _) =>
            HostileReply.Json("", 302, ("Location", new Uri(attacker.BaseUrl, "items").ToString())));
        using var cts = Bounded();

        await Record.ExceptionAsync(() => ReadAsync(api.BaseUrl, RawOptions(), new Dictionary<string, object?>
        {
            ["headers"] = new Dictionary<string, object?> { ["X-Tenant-Token"] = "header-s3cr3t" },
        }, ct: cts.Token));

        Assert.DoesNotContain(attacker.Requests, r =>
            r.Contains("header-s3cr3t", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task A_server_supplied_next_link_pointing_at_another_host_is_not_followed_with_credentials()
    {
        // The Link header is entirely under the endpoint's control. Following it cross-host turns
        // any compromised or malicious API into a credential exfiltration channel.
        await using var attacker = new HostileServer((_, _) => HostileReply.Json("""[]"""));
        await using var api = new HostileServer((_, _) => HostileReply.Json("""[{"id":1}]""", 200,
            ("Link", $"<{new Uri(attacker.BaseUrl, "items")}>; rel=\"next\"")));
        using var cts = Bounded();

        await Record.ExceptionAsync(() => ReadAsync(api.BaseUrl, RawOptions("link_header"),
            new Dictionary<string, object?>
            {
                ["auth"] = new Dictionary<string, object?>
                {
                    ["type"] = "bearer", ["token"] = "bearer-s3cr3t",
                },
            }, ct: cts.Token));

        Assert.DoesNotContain(attacker.Requests, r =>
            r.Contains("bearer-s3cr3t", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task A_redirect_loop_terminates()
    {
        await using var server = new HostileServer((_, _) =>
            HostileReply.Json("", 302, ("Location", "/items")));
        using var cts = Bounded();

        var ex = await Record.ExceptionAsync(() => ReadAsync(server.BaseUrl, RawOptions(), ct: cts.Token));

        Assert.NotNull(ex);
        Assert.False(cts.IsCancellationRequested, "a redirect loop must terminate on its own");
    }

    // ---------------------------------------------------------------------------------------
    // Schema drift mid-crawl.
    // ---------------------------------------------------------------------------------------

    [Fact]
    public async Task A_declared_column_changing_type_mid_crawl_fails_with_a_coded_error()
    {
        await using var server = new HostileServer((index, _) => index == 0
            ? HostileReply.Json("""{"items":[{"id":1}],"next":"c2"}""")
            : HostileReply.Json("""{"items":[{"id":"not-a-number"}]}"""));
        using var cts = Bounded();

        var options = RawOptions(pagination: new Dictionary<string, object?>
        {
            ["strategy"] = "cursor", ["pointer"] = "/next", ["param"] = "cursor",
        });
        options["items"] = "/items";
        options["columns"] = new Dictionary<string, string> { ["id"] = "bigint" };

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(server.BaseUrl, options, ct: cts.Token));

        Assert.False(ex.IsTransient);
        Assert.Contains("id", ex.Message);
    }

    [Fact]
    public async Task Items_pointer_vanishing_mid_crawl_fails_rather_than_ending_the_crawl()
    {
        await using var server = new HostileServer((index, _) => index == 0
            ? HostileReply.Json("""{"items":[{"id":1}],"next":"c2"}""")
            : HostileReply.Json("""{"data":[{"id":2}],"next":"c3"}"""));
        using var cts = Bounded();

        var options = RawOptions(pagination: new Dictionary<string, object?>
        {
            ["strategy"] = "cursor", ["pointer"] = "/next", ["param"] = "cursor",
        });
        options["items"] = "/items";

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(server.BaseUrl, options, ct: cts.Token));

        Assert.False(ex.IsTransient);
        Assert.Contains("items", ex.Message);
    }

    // ---------------------------------------------------------------------------------------
    // Response size.
    // ---------------------------------------------------------------------------------------

    /// <summary>Every page is read with <c>ReadAsStringAsync</c> into one string and then parsed into a
    /// whole JsonNode DOM, so the peak cost of a page is a multiple of its wire size and nothing in
    /// the read loop caps it. The two dials that bound a hostile response — how long the client will
    /// wait, and how many bytes it will buffer — live on the HttpClient, so that is where the
    /// invariant is asserted: an endpoint must not be able to hold a run for minutes or stream it
    /// into an OOM.</summary>
    [Fact]
    public void The_http_client_bounds_request_time_and_response_size()
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(
            new ConnectorConfig(new Dictionary<string, object?>
            {
                ["base_url"] = "http://127.0.0.1:1/",
            }), errors);
        Assert.Empty(errors);

        using var client = HttpSource.CreateClient(connection!);

        Assert.True(client.Timeout <= TimeSpan.FromMinutes(1),
            $"a stalled endpoint holds each page fetch for {client.Timeout} before the engine sees a failure");
        Assert.True(client.MaxResponseContentBufferSize <= 512L * 1024 * 1024,
            $"a hostile endpoint may stream {client.MaxResponseContentBufferSize} bytes into one buffered page");
    }
}
