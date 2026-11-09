using System.Text;
using Apache.Arrow;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Xunit;

namespace Pz.Connector.Http.Tests;

/// <summary>Export jobs end to end against stub servers: start (POST), poll (GET) driven by a
/// <see cref="ManualClock"/> the test advances once the poll loop is parked on its wait, and the
/// download of the result.</summary>
public sealed class HttpExportJobTests : IAsyncLifetime
{
    private static readonly DateTimeOffset Now = new(2026, 11, 8, 6, 0, 0, TimeSpan.Zero);
    private const string Csv = "id,name\n1,a\n2,b\n";

    private readonly StubHttpServer _server = new();
    private readonly StubHttpServer _files = new();
    private readonly ManualClock _clock = new();
    private Task<TimeSpan>? _nextTimer;

    /// <summary>Every notice the sources these tests build emitted, in order.</summary>
    private readonly List<string> _notices = [];

    /// <summary>The connection timeout these tests configure. The download arms an idle-read timer of
    /// this length per read; the clock keeps those out of <see cref="ManualClock.NextTimerAsync"/>, so
    /// advancing past a poll wait never fires a stall.</summary>
    private static readonly TimeSpan IdleTimeout = TimeSpan.FromSeconds(77);

    public HttpExportJobTests()
    {
        _clock.Advance(Now - _clock.GetUtcNow());
        _clock.Unannounced = IdleTimeout;
    }

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        await _server.DisposeAsync();
        await _files.DisposeAsync();
    }

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

    private HttpSource Source(Dictionary<string, object?>? auth = null, bool allowFiles = true,
        string? maxDownloadMb = null, string? timeoutSeconds = null)
    {
        var values = new Dictionary<string, object?>
        {
            ["base_url"] = _server.BaseUrl.ToString(),
            ["auth"] = auth ?? new Dictionary<string, object?> { ["type"] = "bearer", ["token"] = "secret-token" },
            ["headers"] = new Dictionary<string, object?> { ["X-Conn"] = "header-secret" },
            ["timeout_seconds"] = timeoutSeconds
                ?? IdleTimeout.TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture),
        };
        if (maxDownloadMb is not null)
        {
            values["max_download_mb"] = maxDownloadMb;
        }

        if (allowFiles)
        {
            values["allow_hosts"] = new List<object?> { _files.BaseUrl.Host };
        }

        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(values), errors);
        Assert.Empty(errors);
        var source = new HttpSource(connection!, _clock);
        source.UseNotice(n =>
        {
            lock (_notices)
            {
                _notices.Add(n);
            }
        });
        return source;
    }

    private static DatasetSpec Spec(Dictionary<string, object?> export, Action<Dictionary<string, object?>>? edit = null)
    {
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/reports",
            ["method"] = "post",
            ["body"] = new Dictionary<string, object?> { ["metric"] = "visits" },
            ["export"] = export,
            ["file"] = new Dictionary<string, object?> { ["format"] = "csv" },
            ["columns"] = new Dictionary<string, string> { ["id"] = "int", ["name"] = "varchar" },
        };
        edit?.Invoke(options);
        return new DatasetSpec("stub", "traffic", options);
    }

    private static Dictionary<string, object?> Export(params (string Key, object? Value)[] extra)
    {
        var export = new Dictionary<string, object?>
        {
            ["job_id"] = "/id",
            ["status_path"] = "/reports/{job_id}",
            ["status_pointer"] = "/state",
            ["ready"] = new List<object?> { "done" },
            ["failed"] = new List<object?> { "failed", "cancelled" },
            ["message_pointer"] = "/error/message",
            ["download_pointer"] = "/result/url",
        };
        foreach (var (key, value) in extra)
        {
            if (value is null)
            {
                export.Remove(key);
            }
            else
            {
                export[key] = value;
            }
        }

        return export;
    }

    private static StubResponse Json(string body, int status = 200, Dictionary<string, string>? headers = null)
    {
        var all = new Dictionary<string, string> { ["Content-Type"] = "application/json" };
        foreach (var (k, v) in headers ?? [])
        {
            all[k] = v;
        }

        return new StubResponse(status, body, all);
    }

    private static StubResponse File(string text)
        => new(200, "", new Dictionary<string, string> { ["Content-Type"] = "text/csv" })
        {
            RawBody = Encoding.UTF8.GetBytes(text),
        };

    /// <summary>Maps the start endpoint (job <paramref name="jobJson"/>) and a status endpoint that
    /// answers each poll with the next state (the last one repeats).</summary>
    private void MapJob(string statusPath, string jobJson, params string[] states)
    {
        _server.Map("/reports", _ => Json($$"""{"id":{{jobJson}}}""", 202));
        var polls = 0;
        _server.Map(statusPath, _ =>
        {
            var state = states[Math.Min(polls++, states.Length - 1)];
            return Json(state);
        });
    }

    private static string Ready(string url) => "{\"state\":\"done\",\"result\":{\"url\":\"" + url + "\"}}";

    private string FilesUrl(string pathAndQuery) => new Uri(_files.BaseUrl, pathAndQuery).ToString();

    /// <summary>Reads the dataset while advancing the clock by exactly each wait the read parks on.
    /// Returns the rows and the waits, in order.</summary>
    private async Task<(List<object?[]> Rows, List<TimeSpan> Waits)> Read(DatasetSpec spec, HttpSource? source = null,
        IOperationGate? gate = null)
    {
        var owned = source is null;
        source ??= Source();
        if (gate is not null)
        {
            source.UseOperationGate(gate);
        }

        try
        {
            var partition = Assert.Single(await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));
            var work = Collect(partition);
            var waits = new List<TimeSpan>();
            while (true)
            {
                // A pending wait for the next timer outlives a read that finished without one, so the
                // next read on this clock picks it up instead of losing the timer it reports.
                var next = _nextTimer ??= _clock.NextTimerAsync();
                var done = await Task.WhenAny(work, next).WaitAsync(TimeSpan.FromSeconds(30));
                if (done == work)
                {
                    return (await work, waits);
                }

                _nextTimer = null;
                var due = await next;
                waits.Add(due);
                _clock.Advance(due);
            }
        }
        finally
        {
            if (owned)
            {
                await source.DisposeAsync();
            }
        }
    }

    /// <summary>Like <see cref="Read"/> for a read that must fail: the failure and the waits it parked on.</summary>
    private async Task<(PzConnectorException Error, List<TimeSpan> Waits)> ReadFailing(DatasetSpec spec,
        HttpSource? source = null)
    {
        var waits = new List<TimeSpan>();
        var owned = source is null;
        source ??= Source();
        try
        {
            var partition = Assert.Single(await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));
            var work = Collect(partition);
            while (true)
            {
                var next = _nextTimer ??= _clock.NextTimerAsync();
                var done = await Task.WhenAny(work, next).WaitAsync(TimeSpan.FromSeconds(30));
                if (done == work)
                {
                    return (await Assert.ThrowsAsync<PzConnectorException>(() => work), waits);
                }

                _nextTimer = null;
                var due = await next;
                waits.Add(due);
                _clock.Advance(due);
            }
        }
        finally
        {
            if (owned)
            {
                await source.DisposeAsync();
            }
        }
    }

    private static async Task<List<object?[]>> Collect(IDatasetPartition partition)
    {
        var rows = new List<object?[]>();
        await foreach (var batch in partition.ReadAsync(new BatchOptions(), CancellationToken.None))
        {
            using (batch)
            {
                rows.AddRange(Rows(batch));
            }
        }

        return rows;
    }

    private static IEnumerable<object?[]> Rows(RecordBatch batch)
    {
        for (var r = 0; r < batch.Length; r++)
        {
            var row = new object?[batch.ColumnCount];
            for (var c = 0; c < batch.ColumnCount; c++)
            {
                row[c] = FileReaderRows.Value(batch.Column(c), r);
            }

            yield return row;
        }
    }

    private IEnumerable<StubRequest> Polls(string path) => _server.Requests.Where(r => r.Url.AbsolutePath == path);

    [Fact]
    public async Task Start_poll_ready_download()
    {
        _server.Map("/files/abc.csv", _ => File(Csv));
        MapJob("/reports/j1", "\"j1\"", """{"state":"running"}""", """{"state":"running"}""",
            """{"state":"done","result":{"url":"/files/abc.csv"}}""");

        var (rows, waits) = await Read(Spec(Export()));

        Assert.Equal([[1, "a"], [2, "b"]], rows);
        Assert.Equal(3, Polls("/reports/j1").Count());
        Assert.All(Polls("/reports/j1"), r => Assert.Equal("GET", r.Method));
        Assert.Equal([TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10)], waits);
        var start = Assert.Single(_server.Requests, r => r.Url.AbsolutePath == "/reports");
        Assert.Equal("POST", start.Method);
        Assert.Contains("visits", start.Body);
    }

    [Fact]
    public async Task Failed_status_with_message()
    {
        var longMessage = "quota exceeded " + new string('x', 300);
        MapJob("/reports/j1", "\"j1\"", """{"state":"running"}""",
            "{\"state\":\"FAILED\",\"error\":{\"message\":\"" + longMessage + "\"}}");

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export())));

        Assert.False(ex.IsTransient);
        Assert.Contains("export job j1 failed with status 'FAILED': quota exceeded", ex.Message);
        Assert.Contains(longMessage[..200], ex.Message);
        Assert.DoesNotContain(longMessage[..201], ex.Message);
        Assert.Equal(2, Polls("/reports/j1").Count());
    }

    [Fact]
    public async Task Timeout_is_transient()
    {
        MapJob("/reports/j1", "\"j1\"", """{"state":"running"}""");

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => Read(Spec(Export(("timeout_minutes", "1"), ("poll_interval_seconds", "25")))));

        Assert.True(ex.IsTransient);
        Assert.Contains("'stub.traffic': export poll: export job j1 not ready after 1 minutes", ex.Message);
        // Waits of 25 s, 25 s, then the 10 s left before the deadline, with a poll after each.
        Assert.Equal(3, Polls("/reports/j1").Count());
        Assert.Equal(Now + TimeSpan.FromMinutes(1), _clock.GetUtcNow());
    }

    [Fact]
    public async Task Retry_after_overrides_interval()
    {
        _server.Map("/files/abc.csv", _ => File(Csv));
        _server.Map("/reports", _ => Json("""{"id":"j1"}""", 202));
        var polls = 0;
        _server.Map("/reports/j1", _ => ++polls switch
        {
            1 => Json("""{"state":"running"}""", headers: new() { ["Retry-After"] = "2" }),
            2 => Json("""{"state":"running"}""", headers: new() { ["Retry-After"] = "5000" }),
            _ => Json("""{"state":"done","result":{"url":"/files/abc.csv"}}"""),
        });

        var (rows, waits) = await Read(Spec(Export()));

        Assert.Equal(2, rows.Count);
        Assert.Equal([TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(600)], waits);
    }

    [Fact]
    public async Task Start_is_not_idempotent_polls_are()
    {
        _server.Map("/files/abc.csv", _ => File(Csv));
        MapJob("/reports/j1", "\"j1\"", """{"state":"running"}""",
            """{"state":"done","result":{"url":"/files/abc.csv"}}""");
        var gate = new RecordingGate();

        await Read(Spec(Export()), gate: gate);

        Assert.Equal(
        [
            ("http.export_start", false),
            ("http.export_poll", true),
            ("http.export_poll", true),
            ("http.download", true),
        ], gate.Calls);
    }

    [Fact]
    public async Task Presigned_cross_host_download_is_bare()
    {
        _files.Map("/bucket/abc.csv", _ => File(Csv));
        MapJob("/reports/j1", "\"j1\"",
            Ready(FilesUrl("/bucket/abc.csv?X-Amz-Signature=SIG")));

        var (rows, _) = await Read(Spec(Export()));

        Assert.Equal(2, rows.Count);
        var download = Assert.Single(_files.Requests);
        Assert.Equal("?X-Amz-Signature=SIG", download.Url.Query);
        Assert.False(download.Headers.ContainsKey("Authorization"));
        Assert.False(download.Headers.ContainsKey("X-Conn"));
        Assert.All(_server.Requests, r => Assert.Equal("Bearer secret-token", r.Headers["Authorization"]));
        Assert.All(_server.Requests, r => Assert.Equal("header-secret", r.Headers["X-Conn"]));
    }

    [Fact]
    public async Task Cross_host_download_must_be_allow_listed()
    {
        _files.Map("/bucket/abc.csv", _ => File(Csv));
        MapJob("/reports/j1", "\"j1\"",
            Ready(FilesUrl("/bucket/abc.csv?X-Amz-Signature=SIG")));

        await using var source = Source(allowFiles: false);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export()), source));

        Assert.False(ex.IsTransient);
        Assert.Contains("allow_hosts", ex.Message);
        Assert.Contains(_files.BaseUrl.GetLeftPart(UriPartial.Authority), ex.Message);
        Assert.DoesNotContain("SIG", ex.Message);
        Assert.Empty(_files.Requests);
    }

    [Fact]
    public async Task Download_query_never_in_messages()
    {
        _files.Map("/bucket/abc.csv", _ => new StubResponse(403,
            $"<Error>denied for {FilesUrl("/bucket/abc.csv?X-Amz-Signature=SECRET")}</Error>"));
        MapJob("/reports/j1", "\"j1\"",
            Ready(FilesUrl("/bucket/abc.csv?X-Amz-Signature=SECRET")));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export())));

        Assert.False(ex.IsTransient);
        Assert.Contains("'stub.traffic': download: HTTP 403", ex.Message);
        Assert.Contains("/bucket/abc.csv", ex.Message);
        Assert.DoesNotContain("SECRET", ex.Message);
    }

    [Fact]
    public async Task Poll_failure_names_the_stage()
    {
        _server.Map("/reports", _ => Json("""{"id":"j1"}""", 202));
        _server.Map("/reports/j1", _ => new StubResponse(500, "boom"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export())));

        Assert.True(ex.IsTransient);
        Assert.StartsWith("http source 'stub.traffic': export poll: HTTP 500 from ", ex.Message);
    }

    [Fact]
    public async Task Job_id_is_flattened_and_capped_in_messages()
    {
        var id = "a\r\nb" + new string('x', 150);
        // Fixed status_path: the id never reaches the URL (http.sys rejects encoded CR/LF in paths).
        MapJob("/reports/status", System.Text.Json.JsonSerializer.Serialize(id), """{"state":"failed"}""");

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => Read(Spec(Export(("status_path", "/reports/status")))));

        var shown = ("a b" + new string('x', 150))[..100] + "…";
        Assert.Contains($"export job {shown} failed with status 'failed'", ex.Message);
        Assert.DoesNotContain("\n", ex.Message);
        Assert.DoesNotContain(new string('x', 98), ex.Message);
    }

    [Fact]
    public async Task Relative_download_url_and_numeric_job_id()
    {
        _server.Map("/files/abc.csv", _ => File(Csv));
        MapJob("/reports/123", "123", """{"state":"done","result":{"url":"/files/abc.csv"}}""");

        var (rows, _) = await Read(Spec(Export()));

        Assert.Equal(2, rows.Count);
        Assert.Single(Polls("/reports/123"));
        var download = Assert.Single(_server.Requests, r => r.Url.AbsolutePath == "/files/abc.csv");
        Assert.Equal("Bearer secret-token", download.Headers["Authorization"]);
        Assert.Equal("header-secret", download.Headers["X-Conn"]);
    }

    [Fact]
    public async Task Download_path_template()
    {
        _server.Map("/reports/j1/file", _ => File(Csv));
        MapJob("/reports/j1", "\"j1\"", """{"state":"done","result":{"url":"/ignored.csv"}}""");

        var (rows, _) = await Read(Spec(Export(("download_pointer", null), ("download_path", "/reports/{job_id}/file"))));

        Assert.Equal(2, rows.Count);
        var download = Assert.Single(_server.Requests, r => r.Url.AbsolutePath == "/reports/j1/file");
        Assert.Equal("Bearer secret-token", download.Headers["Authorization"]);
        Assert.DoesNotContain(_server.Requests, r => r.Url.AbsolutePath == "/ignored.csv");
    }

    [Fact]
    public async Task Missing_job_id_is_permanent()
    {
        _server.Map("/reports", _ => Json("""{"jobId":"j1"}""", 202));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export())));

        Assert.False(ex.IsTransient);
        Assert.Contains("'/id'", ex.Message);
        Assert.Single(_server.Requests);
    }

    [Fact]
    public async Task Non_json_start_response_is_permanent()
    {
        _server.Map("/reports", _ => new StubResponse(200, "<html>queued</html>",
            new Dictionary<string, string> { ["Content-Type"] = "text/html" }));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export())));

        Assert.False(ex.IsTransient);
        Assert.Contains("'stub.traffic': export start: response from ", ex.Message);
        Assert.Contains("not valid JSON", ex.Message);
    }

    [Fact]
    public async Task Start_error_pointer_is_checked()
    {
        _server.Map("/reports", _ => Json("""{"id":"j1","errors":[{"message":"bad metric"}]}"""));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => Read(Spec(Export(), o => o["error_pointer"] = "/errors")));

        Assert.False(ex.IsTransient);
        Assert.Contains("bad metric", ex.Message);
        Assert.Single(_server.Requests);
    }

    [Fact]
    public async Task Run_date_is_read_once_across_midnight()
    {
        // 23:59:55 on 2026-11-08: the first 10 s poll wait crosses midnight before the download.
        _clock.Advance(new DateTimeOffset(2026, 11, 8, 23, 59, 55, TimeSpan.Zero) - _clock.GetUtcNow());
        _server.Map("/files/2026-11-08/j1.csv", _ => File(Csv));
        MapJob("/reports/2026-11-08/j1", "\"j1\"", """{"state":"done"}""");

        var (rows, waits) = await Read(Spec(
            Export(("status_path", "/reports/{{ run_date }}/{job_id}"), ("download_pointer", null),
                ("download_path", "/files/{{ run_date }}/{job_id}.csv")),
            o => o["query"] = new Dictionary<string, object?> { ["date"] = "{{ run_date }}" }));

        Assert.Equal(2, rows.Count);
        Assert.Equal([TimeSpan.FromSeconds(10)], waits);
        Assert.True(_clock.GetUtcNow().Day == 9);
        var start = Assert.Single(_server.Requests, r => r.Url.AbsolutePath == "/reports");
        Assert.Equal("?date=2026-11-08", start.Url.Query);
        Assert.Single(Polls("/reports/2026-11-08/j1"));
        Assert.Single(_server.Requests, r => r.Url.AbsolutePath == "/files/2026-11-08/j1.csv");
    }

    [Fact]
    public void Unknown_binding_in_export_paths_is_a_validation_error()
    {
        var spec = Spec(Export(("status_path", "/reports/{{ nope }}/{job_id}"), ("download_pointer", null),
            ("download_path", "/files/{{ nada }}")));

        var ex = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(spec));

        Assert.Contains("export.status_path: unknown binding 'nope'", ex.Message);
        Assert.Contains("export.download_path: unknown binding 'nada'", ex.Message);
    }

    [Fact]
    public async Task Cross_origin_download_does_not_get_api_key_query()
    {
        _files.Map("/bucket/abc.csv", _ => File(Csv));
        MapJob("/reports/j1", "\"j1\"",
            Ready(FilesUrl("/bucket/abc.csv?X-Amz-Signature=SIG")));
        await using var source = Source(new Dictionary<string, object?>
            { ["type"] = "api_key", ["key"] = "TOPSECRET", ["param"] = "api_key" });

        var (rows, _) = await Read(Spec(Export()), source);

        Assert.Equal(2, rows.Count);
        var download = Assert.Single(_files.Requests);
        Assert.Equal("?X-Amz-Signature=SIG", download.Url.Query);
        Assert.DoesNotContain("TOPSECRET", download.Url.ToString());
        Assert.All(_server.Requests, r => Assert.Contains("api_key=TOPSECRET", r.Url.Query));
    }

    [Fact]
    public async Task Bare_host_401_keeps_the_cached_token()
    {
        var tokens = 0;
        _server.Map("/token", _ => Json($$"""{"access_token":"t{{++tokens}}","token_type":"Bearer","expires_in":3600}"""));
        var downloads = 0;
        _files.Map("/bucket/abc.csv", _ => ++downloads == 1 ? new StubResponse(401, "expired link") : File(Csv));
        MapJob("/reports/j1", "\"j1\"",
            Ready(FilesUrl("/bucket/abc.csv?X-Amz-Signature=SIG")));
        await using var source = Source(new Dictionary<string, object?>
        {
            ["type"] = "oauth2_client_credentials",
            ["token_url"] = new Uri(_server.BaseUrl, "token").ToString(),
            ["client_id"] = "c",
            ["client_secret"] = "s3cret",
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export()), source));
        Assert.False(ex.IsTransient);
        Assert.Contains("HTTP 401", ex.Message);
        Assert.DoesNotContain("access token", ex.Message);

        // A second read on the same source reuses the cached token for its same-origin requests.
        var (rows, _) = await Read(Spec(Export()), source);
        Assert.Equal(2, rows.Count);
        Assert.Equal(1, tokens);
        Assert.All(_server.Requests.Where(r => r.Url.AbsolutePath != "/token"),
            r => Assert.Equal("Bearer t1", r.Headers["Authorization"]));
    }

    // --- on_status rules on the export stages ---

    private static Action<Dictionary<string, object?>> OnStatus(params Dictionary<string, object?>[] rules)
        => options => options["on_status"] = rules.Cast<object?>().ToList();

    private static Dictionary<string, object?> Rule(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    [Fact]
    public async Task Transient_rule_on_the_start_is_permanent_so_no_second_job_is_created()
    {
        _server.Map("/reports", _ => Json("""{"error":"export quota reached, try later"}""", 403));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export(),
            OnStatus(Rule(("status", 403), ("body_contains", "try later"), ("action", "transient"),
                ("retry_after", "30s"))))));

        Assert.False(ex.IsTransient);
        Assert.Null(ex.RetryAfter);
        Assert.Contains("export start: HTTP 403 from", ex.Message);
        Assert.Contains("matched on_status[0] (transient), but the export start is never retried by a rule", ex.Message);
        Assert.Single(_server.Requests, r => r.Url.AbsolutePath == "/reports");
    }

    [Fact]
    public async Task Built_in_503_on_the_start_is_unchanged()
    {
        _server.Map("/reports", _ => Json("{}", 503));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export(),
            OnStatus(Rule(("status", 400), ("action", "permanent"))))));

        Assert.True(ex.IsTransient);
        Assert.EndsWith("export start: HTTP 503 from " + _server.BaseUrl + "reports", ex.Message.Replace("POST ", ""));
    }

    [Fact]
    public async Task Permanent_rule_on_the_start_names_the_rule()
    {
        _server.Map("/reports", _ => Json("""{"error":"unknown metric"}""", 422));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export(),
            OnStatus(Rule(("status", 422), ("action", "permanent"))))));

        Assert.False(ex.IsTransient);
        Assert.EndsWith("(on_status[0]: permanent)", ex.Message);
    }

    [Fact]
    public async Task Transient_rule_on_a_poll_is_transient()
    {
        _server.Map("/reports", _ => Json("""{"id":"j1"}""", 202));
        _server.Map("/reports/j1", _ => Json("""{"error":"poll limit, try later"}""", 403));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export(),
            OnStatus(Rule(("status", 403), ("body_contains", "try later"), ("action", "transient"),
                ("retry_after", "20s"))))));

        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(20), ex.RetryAfter);
        Assert.Contains("export poll: HTTP 403 from", ex.Message);
        Assert.EndsWith("(on_status[0]: transient)", ex.Message);
    }

    [Fact]
    public async Task Permanent_rule_on_the_download_names_the_rule()
    {
        _files.Map("/files/abc.csv", _ => Json("""{"error":"link revoked"}""", 410));
        MapJob("/reports/j1", "\"j1\"", Ready(FilesUrl("files/abc.csv")));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export(),
            OnStatus(Rule(("status", 410), ("body_contains", "revoked"), ("action", "permanent"))))));

        Assert.False(ex.IsTransient);
        Assert.Contains("download: HTTP 410 from", ex.Message);
        Assert.EndsWith("(on_status[0]: permanent)", ex.Message);
    }

    // ---- export.cancel_path: the clean-up of a job the read abandons ----

    private static (string, object?) CancelPath(string path = "/reports/{job_id}/cancel") => ("cancel_path", path);

    /// <summary>An export with <c>cancel_path</c> whose cancel call the read helpers must not time out: the
    /// cancel's 10 s cap is kept out of <see cref="ManualClock.NextTimerAsync"/>, so polls wait 7 s instead
    /// of the default 10 s.</summary>
    private Dictionary<string, object?> CancellingExport(params (string Key, object? Value)[] extra)
    {
        _clock.Unannounce(TimeSpan.FromSeconds(10));
        return Export([CancelPath(), ("poll_interval_seconds", "7"), .. extra]);
    }

    private IEnumerable<StubRequest> Cancels(string path = "/reports/j1/cancel")
        => _server.Requests.Where(r => r.Url.AbsolutePath == path && r.Method != "GET");

    private List<string> Notices()
    {
        lock (_notices)
        {
            return [.. _notices];
        }
    }

    [Fact]
    public async Task Timeout_cancels_the_job_once_with_delete()
    {
        // Status and cancel share a path; the cancel is the only DELETE.
        _server.Map("/reports", _ => Json("""{"id":"j1"}""", 202));
        _server.Map("/reports/j1", r => r.Method == "DELETE" ? new StubResponse(204, "") : Json("""{"state":"running"}"""));
        var gate = new RecordingGate();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(
            Spec(CancellingExport(("timeout_minutes", "1"), ("poll_interval_seconds", "30"), CancelPath("/reports/{job_id}"))),
            gate: gate));

        Assert.True(ex.IsTransient);
        Assert.Contains("export job j1 not ready after 1 minutes", ex.Message);
        var cancel = Assert.Single(_server.Requests, r => r.Method == "DELETE");
        Assert.Equal("/reports/j1", cancel.Url.AbsolutePath);
        Assert.Equal("Bearer secret-token", cancel.Headers["Authorization"]);
        Assert.Equal("header-secret", cancel.Headers["X-Conn"]);
        Assert.Equal("", cancel.Body);
        Assert.Single(gate.Calls, c => c.Label == "http.export_cancel");
        Assert.Contains(("http.export_cancel", false), gate.Calls);
        Assert.Equal(["http source 'stub.traffic': export cancel: cancelled export job j1 (timed out)"], Notices());
    }

    [Fact]
    public async Task Failed_status_cancels_with_the_configured_method()
    {
        _server.Map("/reports/j1/cancel", _ => Json("{}"));
        MapJob("/reports/j1", "\"j1\"", """{"state":"failed"}""");

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => Read(Spec(CancellingExport(("cancel_method", "POST")))));

        Assert.False(ex.IsTransient);
        Assert.Contains("failed with status 'failed'", ex.Message);
        var cancel = Assert.Single(Cancels());
        Assert.Equal("POST", cancel.Method);
        Assert.Equal("", cancel.Body);
        Assert.Equal(["http source 'stub.traffic': export cancel: cancelled export job j1 (reported failure)"],
            Notices());
    }

    [Fact]
    public async Task Failed_cancel_is_a_notice_and_never_retried()
    {
        _server.Map("/reports/j1/cancel", _ => new StubResponse(500, "cancel broke"));
        MapJob("/reports/j1", "\"j1\"", """{"state":"failed"}""");
        var gate = new RetryingGate();

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(CancellingExport()), gate: gate));

        // The read's own failure is what surfaces, not the cancel's.
        Assert.Contains("failed with status 'failed'", ex.Message);
        Assert.Single(Cancels());
        var notice = Assert.Single(Notices());
        Assert.StartsWith("http source 'stub.traffic': export cancel: could not cancel export job j1 " +
            "(reported failure): ", notice);
        Assert.Contains("HTTP 500", notice);
    }

    [Fact]
    public async Task Rejected_cancel_reports_the_status_only()
    {
        _server.Map("/reports/j1/cancel", _ => new StubResponse(409, "job j1 already finished"));
        MapJob("/reports/j1", "\"j1\"", """{"state":"failed"}""");
        await using var source = Source(new Dictionary<string, object?>
            { ["type"] = "api_key", ["key"] = "TOPSECRET", ["param"] = "api_key" });

        await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(CancellingExport()), source));

        var cancel = Assert.Single(Cancels());
        Assert.Contains("api_key=TOPSECRET", cancel.Url.Query);
        var notice = Assert.Single(Notices());
        Assert.StartsWith("http source 'stub.traffic': export cancel: could not cancel export job j1 " +
            "(reported failure): HTTP 409 from DELETE ", notice);
        Assert.Contains("/reports/j1/cancel", notice);
        // The body is never read, so it is never quoted.
        Assert.DoesNotContain("already finished", notice);
        Assert.DoesNotContain("TOPSECRET", notice);
    }

    [Fact]
    public async Task Poll_failure_cancels_the_job()
    {
        _server.Map("/reports", _ => Json("""{"id":"j1"}""", 202));
        _server.Map("/reports/j1", _ => new StubResponse(400, "bad"));
        _server.Map("/reports/j1/cancel", _ => new StubResponse(202, ""));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(CancellingExport())));

        Assert.Contains("export poll: HTTP 400", ex.Message);
        Assert.Single(Cancels());
        Assert.Equal(["http source 'stub.traffic': export cancel: cancelled export job j1 (poll failed)"], Notices());
    }

    [Fact]
    public async Task Download_failure_never_cancels_a_ready_job()
    {
        _server.Map("/files/abc.csv", _ => new StubResponse(403, "denied"));
        _server.Map("/reports/j1/cancel", _ => new StubResponse(204, ""));
        MapJob("/reports/j1", "\"j1\"", """{"state":"done","result":{"url":"/files/abc.csv"}}""");

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export(CancelPath()))));

        Assert.Contains("download: HTTP 403", ex.Message);
        Assert.Empty(Cancels());
        Assert.Empty(Notices());
    }

    [Fact]
    public async Task Ready_job_without_a_download_url_is_not_cancelled()
    {
        _server.Map("/reports/j1/cancel", _ => new StubResponse(204, ""));
        MapJob("/reports/j1", "\"j1\"", """{"state":"done","result":{}}""");

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export(CancelPath()))));

        Assert.Contains("does not resolve to a URL", ex.Message);
        Assert.Empty(Cancels());
    }

    [Fact]
    public async Task Cancelled_read_during_download_does_not_cancel_a_ready_job()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var release = new ManualResetEventSlim();
        _server.Map("/files/abc.csv", _ =>
        {
            entered.TrySetResult();
            release.Wait(TimeSpan.FromSeconds(30));
            return File(Csv);
        });
        _server.Map("/reports/j1/cancel", _ => new StubResponse(204, ""));
        MapJob("/reports/j1", "\"j1\"", """{"state":"done","result":{"url":"/files/abc.csv"}}""");
        await using var source = Source();
        var partition = Assert.Single(await source.PlanReadAsync(Spec(Export(CancelPath())), ReadHints.None,
            CancellationToken.None));
        using var cts = new CancellationTokenSource();

        try
        {
            var reading = Task.Run(async () =>
            {
                await foreach (var batch in partition.ReadAsync(new BatchOptions(), cts.Token))
                {
                    batch.Dispose();
                }
            });
            _clock.Advance(await _clock.NextTimerAsync().WaitAsync(TimeSpan.FromSeconds(30)));
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            cts.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reading.WaitAsync(TimeSpan.FromSeconds(30)));
            Assert.Empty(Cancels());
            Assert.Empty(Notices());
        }
        finally
        {
            release.Set();
        }
    }

    [Fact]
    public async Task Successful_read_never_cancels()
    {
        _server.Map("/files/abc.csv", _ => File(Csv));
        _server.Map("/reports/j1/cancel", _ => new StubResponse(204, ""));
        MapJob("/reports/j1", "\"j1\"", """{"state":"done","result":{"url":"/files/abc.csv"}}""");

        var (rows, _) = await Read(Spec(Export(CancelPath())));

        Assert.Equal(2, rows.Count);
        Assert.Empty(Cancels());
        Assert.Empty(Notices());
    }

    [Fact]
    public async Task No_cancel_path_sends_no_cancel()
    {
        MapJob("/reports/j1", "\"j1\"", """{"state":"failed"}""");

        await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export())));

        Assert.Equal(["POST /reports", "GET /reports/j1"],
            _server.Requests.Select(r => $"{r.Method} {r.Url.AbsolutePath}"));
        Assert.Empty(Notices());
    }

    [Fact]
    public async Task No_job_id_means_nothing_to_cancel()
    {
        _server.Map("/reports", _ => new StubResponse(500, "down"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export(CancelPath()))));

        Assert.True(ex.IsTransient);
        Assert.Single(_server.Requests);
        Assert.Empty(Notices());
    }

    [Fact]
    public async Task Cancelled_read_still_cancels_the_job()
    {
        _server.Map("/reports/j1/cancel", _ => new StubResponse(204, ""));
        MapJob("/reports/j1", "\"j1\"", """{"state":"running"}""");
        await using var source = Source();
        var partition = Assert.Single(await source.PlanReadAsync(Spec(Export(CancelPath())), ReadHints.None,
            CancellationToken.None));
        using var cts = new CancellationTokenSource();

        var reading = Task.Run(async () =>
        {
            await foreach (var batch in partition.ReadAsync(new BatchOptions(), cts.Token))
            {
                batch.Dispose();
            }
        });
        // The read parks on its first poll wait; cancel it there.
        await _clock.NextTimerAsync().WaitAsync(TimeSpan.FromSeconds(30));
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reading.WaitAsync(TimeSpan.FromSeconds(30)));
        Assert.Single(Cancels());
        Assert.Empty(Polls("/reports/j1"));
        Assert.Equal(["http source 'stub.traffic': export cancel: cancelled export job j1 (read was cancelled)"],
            Notices());
    }

    [Fact]
    public async Task Cancel_that_hangs_is_capped_at_ten_seconds()
    {
        using var release = new ManualResetEventSlim();
        _server.Map("/reports/j1/cancel", _ =>
        {
            release.Wait(TimeSpan.FromSeconds(30));
            return new StubResponse(204, "");
        });
        MapJob("/reports/j1", "\"j1\"", """{"state":"failed"}""");

        try
        {
            // timeout_seconds is 77 here; the cancel's own cap (10 s) is the timer the read parks on.
            var (ex, waits) = await ReadFailing(Spec(Export(CancelPath())));

            Assert.Contains("failed with status 'failed'", ex.Message);
            Assert.Equal([TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10)], waits);
            Assert.Equal(["http source 'stub.traffic': export cancel: could not cancel export job j1 " +
                "(reported failure): no response within 10 s"], Notices());
        }
        finally
        {
            release.Set();
        }
    }

    [Fact]
    public async Task Cancel_is_capped_by_a_lower_timeout_seconds()
    {
        using var release = new ManualResetEventSlim();
        _server.Map("/reports/j1/cancel", _ =>
        {
            release.Wait(TimeSpan.FromSeconds(30));
            return new StubResponse(204, "");
        });
        MapJob("/reports/j1", "\"j1\"", """{"state":"failed"}""");
        await using var source = Source(timeoutSeconds: "3");

        try
        {
            var (_, waits) = await ReadFailing(Spec(Export(CancelPath())), source);

            Assert.Equal([TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(3)], waits);
            Assert.EndsWith("no response within 3 s", Assert.Single(Notices()));
        }
        finally
        {
            release.Set();
        }
    }

    [Fact]
    public async Task Cancel_that_hangs_after_a_node_timeout_ends_within_five_seconds()
    {
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _server.Map("/reports/j1/cancel", _ =>
        {
            entered.TrySetResult();
            release.Wait(TimeSpan.FromSeconds(30));
            return new StubResponse(204, "");
        });
        MapJob("/reports/j1", "\"j1\"", """{"state":"running"}""");
        await using var source = Source();
        var partition = Assert.Single(await source.PlanReadAsync(Spec(Export(CancelPath())), ReadHints.None,
            CancellationToken.None));
        // The engine's node timeout: the attempt token is cancelled while the job is still running.
        using var nodeTimeout = new CancellationTokenSource();

        try
        {
            var reading = Task.Run(async () =>
            {
                await foreach (var batch in partition.ReadAsync(new BatchOptions(), nodeTimeout.Token))
                {
                    batch.Dispose();
                }
            });
            await _clock.NextTimerAsync().WaitAsync(TimeSpan.FromSeconds(30));
            nodeTimeout.Cancel();
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(30));
            var cap = await _clock.NextTimerAsync().WaitAsync(TimeSpan.FromSeconds(30));
            Assert.Equal(TimeSpan.FromSeconds(5), cap);
            _clock.Advance(cap);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reading.WaitAsync(TimeSpan.FromSeconds(30)));
            Assert.Equal(["http source 'stub.traffic': export cancel: could not cancel export job j1 " +
                "(read was cancelled): no response within 5 s"], Notices());
        }
        finally
        {
            release.Set();
        }
    }

    [Fact]
    public async Task Early_stop_never_cancels_a_ready_job()
    {
        _server.Map("/files/a.csv", _ => File(Csv));
        _server.Map("/files/b.csv", _ => File(Csv));
        _server.Map("/reports/j1/cancel", _ => new StubResponse(204, ""));
        MapJob("/reports/j1", "\"j1\"", """{"state":"done","result":{"urls":["/files/a.csv","/files/b.csv"]}}""");
        await using var source = Source();
        var partition = Assert.Single(await source.PlanReadAsync(
            Spec(Export(CancelPath(), ("download_pointer", "/result/urls"))), ReadHints.None, CancellationToken.None));

        var reading = Task.Run(async () =>
        {
            await foreach (var batch in partition.ReadAsync(new BatchOptions(), CancellationToken.None))
            {
                batch.Dispose();
                break;
            }
        });
        var due = await _clock.NextTimerAsync().WaitAsync(TimeSpan.FromSeconds(30));
        _clock.Advance(due);
        await reading.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Empty(Cancels());
        Assert.DoesNotContain(_server.Requests, r => r.Url.AbsolutePath == "/files/b.csv");
        Assert.Empty(Notices());
    }

    [Fact]
    public void Unknown_binding_in_cancel_path_is_a_validation_error()
    {
        var ex = Assert.Throws<PzConnectorException>(
            () => HttpDatasetConfig.Parse(Spec(Export(CancelPath("/reports/{{ nope }}/{job_id}")))));

        Assert.Contains("export.cancel_path: unknown binding 'nope'", ex.Message);
    }

    [Fact]
    public async Task Cancel_path_expands_bindings_then_job_id()
    {
        _server.Map("/jobs/2026-11-08/j1", _ => new StubResponse(204, ""));
        MapJob("/reports/status", "\"j1\"", """{"state":"failed"}""");

        await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(CancellingExport(("status_path", "/reports/status"),
            CancelPath("/jobs/{{ run_date }}/{job_id}")))));

        var cancel = Assert.Single(_server.Requests, r => r.Method == "DELETE");
        Assert.Equal("/jobs/2026-11-08/j1", cancel.Url.AbsolutePath);
    }

    /// <summary>A gate that retries an idempotent op on a transient failure, like the engine's: proves the
    /// cancel is never offered for a retry.</summary>
    private sealed class RetryingGate : IOperationGate
    {
        public async Task<T> ExecuteAsync<T>(string opLabel, bool idempotent, Func<CancellationToken, Task<T>> op,
            CancellationToken ct)
        {
            for (var attempt = 1; ; attempt++)
            {
                try
                {
                    return await op(ct);
                }
                catch (PzConnectorException ex) when (idempotent && ex.IsTransient && attempt < 3)
                {
                }
            }
        }

        public void ReportBudget(int remaining, DateTimeOffset resetAt)
        {
        }
    }

    // ---- several result files ----

    private static (string, object?) Urls => ("download_pointer", "/result/urls");

    private static string ReadyFiles(params string[] urls)
        => "{\"state\":\"done\",\"result\":{\"urls\":[" + string.Join(",", urls.Select(u => $"\"{u}\"")) + "]}}";

    [Fact]
    public async Task Array_of_urls_reads_every_file_in_order()
    {
        _server.Map("/files/a.csv", _ => File("id,name\n1,a\n"));
        _files.Map("/bucket/b.csv", _ => File("id,name\n2,b\n3,c\n"));
        _server.Map("/files/c.csv", _ => File("id,name\n4,d\n"));
        MapJob("/reports/j1", "\"j1\"", ReadyFiles("/files/a.csv", FilesUrl("/bucket/b.csv?sig=S"), "/files/c.csv"));
        var gate = new RecordingGate();

        var (rows, _) = await Read(Spec(Export(Urls)), gate: gate);

        Assert.Equal([[1, "a"], [2, "b"], [3, "c"], [4, "d"]], rows);
        Assert.Equal(3, gate.Calls.Count(c => c == ("http.download", true)));
        Assert.False(Assert.Single(_files.Requests).Headers.ContainsKey("Authorization"));
        Assert.Empty(Notices());
    }

    [Fact]
    public async Task Single_element_array_is_one_file()
    {
        _server.Map("/files/a.csv", _ => File(Csv));
        MapJob("/reports/j1", "\"j1\"", ReadyFiles("/files/a.csv"));

        var (rows, _) = await Read(Spec(Export(Urls)));

        Assert.Equal([[1, "a"], [2, "b"]], rows);
    }

    [Fact]
    public async Task Empty_array_reads_empty_with_a_notice()
    {
        MapJob("/reports/j1", "\"j1\"", ReadyFiles());

        var (rows, _) = await Read(Spec(Export(Urls)));

        Assert.Empty(rows);
        Assert.Equal(["http source 'stub.traffic': export poll: export job j1's ready response lists no result " +
            "files at '/result/urls'; read as empty"], Notices());
    }

    [Fact]
    public async Task Bad_entry_fails_before_any_download()
    {
        _server.Map("/files/a.csv", _ => File(Csv));
        MapJob("/reports/j1", "\"j1\"", """{"state":"done","result":{"urls":["/files/a.csv",5]}}""");

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export(Urls))));

        Assert.False(ex.IsTransient);
        Assert.Contains("export.download_pointer '/result/urls' entry [1] is not a URL string", ex.Message);
        Assert.DoesNotContain(_server.Requests, r => r.Url.AbsolutePath == "/files/a.csv");
    }

    [Fact]
    public async Task Empty_string_entry_is_refused()
    {
        MapJob("/reports/j1", "\"j1\"", ReadyFiles("/files/a.csv", ""));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export(Urls))));

        Assert.Contains("entry [1] is not a URL string", ex.Message);
    }

    [Fact]
    public async Task Disallowed_host_entry_fails_before_any_download()
    {
        _server.Map("/files/a.csv", _ => File(Csv));
        MapJob("/reports/j1", "\"j1\"", ReadyFiles("/files/a.csv", FilesUrl("/bucket/b.csv?sig=SECRET")));
        await using var source = Source(allowFiles: false);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export(Urls)), source));

        Assert.False(ex.IsTransient);
        Assert.Contains("allow_hosts", ex.Message);
        Assert.Contains("[1]", ex.Message);
        Assert.DoesNotContain("SECRET", ex.Message);
        Assert.DoesNotContain(_server.Requests, r => r.Url.AbsolutePath == "/files/a.csv");
        Assert.Empty(_files.Requests);
    }

    [Fact]
    public async Task Object_at_pointer_is_still_refused()
    {
        MapJob("/reports/j1", "\"j1\"", """{"state":"done","result":{"urls":{"a":"/files/a.csv"}}}""");

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export(Urls))));

        Assert.Contains("does not resolve to a URL or a list of URLs", ex.Message);
    }

    [Fact]
    public async Task Each_file_gets_its_own_download_budget()
    {
        // Two files of 0.75 MiB each pass a 1 MiB max_download_mb; together they would not.
        var body = "id,name\n" + string.Concat(Enumerable.Repeat("1,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n", 16384));
        Assert.InRange(body.Length, 700_000, 1_000_000);
        _server.Map("/files/a.csv", _ => File(body));
        _server.Map("/files/b.csv", _ => File(body));
        MapJob("/reports/j1", "\"j1\"", ReadyFiles("/files/a.csv", "/files/b.csv"));
        await using var source = Source(maxDownloadMb: "1");

        var (rows, _) = await Read(Spec(Export(Urls)), source);

        Assert.Equal(2 * 16384, rows.Count);
    }

    [Fact]
    public async Task Oversized_second_file_fails_the_read()
    {
        var big = "id,name\n" + string.Concat(Enumerable.Repeat("1,aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n", 30000));
        _server.Map("/files/a.csv", _ => File(Csv));
        _server.Map("/files/b.csv", _ => File(big));
        MapJob("/reports/j1", "\"j1\"", ReadyFiles("/files/a.csv", "/files/b.csv"));
        await using var source = Source(maxDownloadMb: "1");

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export(Urls)), source));

        Assert.Contains("max_download_mb", ex.Message);
    }

    [Fact]
    public async Task File_column_names_each_rows_own_file()
    {
        _server.Map("/files/a.csv", _ => File("id,name\n1,a\n"));
        _server.Map("/files/b.csv", _ => File("id,name\n2,b\n"));
        MapJob("/reports/j1", "\"j1\"", ReadyFiles("/files/a.csv", "/files/b.csv"));

        var (rows, _) = await Read(Spec(Export(Urls), o =>
            o["file"] = new Dictionary<string, object?> { ["format"] = "csv", ["file_column"] = "src" }));

        Assert.Equal([[1, "a", "a.csv"], [2, "b", "b.csv"]], rows);
    }

    [Fact]
    public async Task Second_file_failure_never_cancels_a_ready_job()
    {
        _server.Map("/files/a.csv", _ => File(Csv));
        _server.Map("/files/b.csv", _ => new StubResponse(404, "gone"));
        _server.Map("/reports/j1/cancel", _ => new StubResponse(204, ""));
        MapJob("/reports/j1", "\"j1\"", ReadyFiles("/files/a.csv", "/files/b.csv"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export(Urls, CancelPath()))));

        Assert.Contains("download: HTTP 404", ex.Message);
        Assert.Contains("/files/b.csv", ex.Message);
        Assert.Empty(Cancels());
    }

    [Fact]
    public async Task Unmatched_members_notice_counts_files()
    {
        _server.Map("/files/a.zip", _ => ZipFile(("x.csv", "id,name\n1,a\n")));
        _server.Map("/files/b.zip", _ => ZipFile(("readme.txt", "nothing")));
        _server.Map("/files/c.zip", _ => ZipFile(("other.txt", "nothing")));
        MapJob("/reports/j1", "\"j1\"", ReadyFiles("/files/a.zip", "/files/b.zip", "/files/c.zip"));

        var (rows, _) = await Read(Spec(Export(Urls), o => o["file"] = new Dictionary<string, object?>
            { ["format"] = "csv", ["members"] = "*.csv", ["if_missing"] = "empty" }));

        Assert.Equal([[1, "a"]], rows);
        Assert.Equal(["http source 'stub.traffic': no zip entry matches file.members '*.csv' in 2 of 3 result " +
            "files; read those as empty (if_missing: empty)"], Notices());
    }

    [Fact]
    public async Task Single_file_unmatched_members_notice_is_unchanged()
    {
        _server.Map("/files/a.zip", _ => ZipFile(("readme.txt", "nothing")));
        MapJob("/reports/j1", "\"j1\"", """{"state":"done","result":{"url":"/files/a.zip"}}""");

        var (rows, _) = await Read(Spec(Export(), o => o["file"] = new Dictionary<string, object?>
            { ["format"] = "csv", ["members"] = "*.csv", ["if_missing"] = "empty" }));

        Assert.Empty(rows);
        Assert.Equal(["http source 'stub.traffic': no zip entry matches file.members '*.csv'; " +
            "read as empty (if_missing: empty)"], Notices());
    }

    [Fact]
    public async Task Duplicate_urls_are_read_once_with_a_notice()
    {
        _server.Map("/files/a.csv", _ => File("id,name\n1,a\n"));
        _server.Map("/files/b.csv", _ => File("id,name\n2,b\n"));
        var absolute = new Uri(_server.BaseUrl, "/files/a.csv").ToString();
        MapJob("/reports/j1", "\"j1\"", ReadyFiles("/files/a.csv", absolute, "/files/b.csv", "/files/a.csv"));

        var (rows, _) = await Read(Spec(Export(Urls)));

        Assert.Equal([[1, "a"], [2, "b"]], rows);
        Assert.Single(_server.Requests, r => r.Url.AbsolutePath == "/files/a.csv");
        Assert.Equal(["http source 'stub.traffic': export poll: export job j1's ready response lists 2 duplicate " +
            "result file URLs at '/result/urls'; each file is read once"], Notices());
    }

    [Fact]
    public async Task Urls_differing_only_in_query_are_distinct()
    {
        _files.Map("/bucket/a.csv", _ => File("id,name\n1,a\n"));
        MapJob("/reports/j1", "\"j1\"", ReadyFiles(FilesUrl("/bucket/a.csv?part=1"), FilesUrl("/bucket/a.csv?part=2")));

        var (rows, _) = await Read(Spec(Export(Urls)));

        Assert.Equal(2, rows.Count);
        Assert.Equal(2, _files.Requests.Count);
        Assert.Empty(Notices());
    }

    [Fact]
    public async Task More_than_1000_files_is_refused_before_any_download()
    {
        MapJob("/reports/j1", "\"j1\"", ReadyFiles(Enumerable.Range(0, 1001).Select(i => $"/files/{i}.csv").ToArray()));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(Export(Urls))));

        Assert.False(ex.IsTransient);
        Assert.Contains("export job j1's ready response lists 1001 result files at '/result/urls'; at most 1000 " +
            "are read per job", ex.Message);
        Assert.DoesNotContain(_server.Requests, r => r.Url.AbsolutePath.StartsWith("/files/", StringComparison.Ordinal));
    }

    /// <summary>A partition built by hand with <c>SkipMissing</c> (set only by for_each, which export refuses
    /// today), read while advancing the clock past each poll wait.</summary>
    private async Task<List<object?[]>> ReadSkippingMissing(DatasetSpec spec, Func<HttpFilePartition, Task>? after = null)
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = _server.BaseUrl.ToString(),
            ["timeout_seconds"] = IdleTimeout.TotalSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture),
        }), errors);
        Assert.Empty(errors);
        using var client = HttpSource.CreateClient(connection!);
        using var bare = HttpSource.CreateBareClient(connection!);
        var partition = new HttpFilePartition(client, bare, connection!, HttpDatasetConfig.Parse(spec), spec, _clock,
            gate: null) { SkipMissing = true, Notice = _notices.Add };
        var work = Collect(partition);
        while (true)
        {
            var next = _nextTimer ??= _clock.NextTimerAsync();
            if (await Task.WhenAny(work, next).WaitAsync(TimeSpan.FromSeconds(30)) == work)
            {
                var rows = await work;
                if (after is not null)
                {
                    await after(partition);
                }

                return rows;
            }

            _nextTimer = null;
            _clock.Advance(await next);
        }
    }

    [Fact]
    public async Task Missing_later_file_is_always_an_error()
    {
        _server.Map("/files/a.csv", _ => File(Csv));
        _server.Map("/files/b.csv", _ => new StubResponse(404, "gone"));
        MapJob("/reports/j1", "\"j1\"", ReadyFiles("/files/a.csv", "/files/b.csv"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadSkippingMissing(Spec(Export(Urls))));

        Assert.Contains("download: HTTP 404", ex.Message);
        Assert.Contains("/files/b.csv", ex.Message);
    }

    [Fact]
    public async Task Missing_first_of_several_files_is_an_error_too()
    {
        _server.Map("/files/a.csv", _ => new StubResponse(410, "gone"));
        _server.Map("/files/b.csv", _ => File(Csv));
        MapJob("/reports/j1", "\"j1\"", ReadyFiles("/files/a.csv", "/files/b.csv"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadSkippingMissing(Spec(Export(Urls))));

        Assert.Contains("download: HTTP 410", ex.Message);
    }

    [Fact]
    public async Task Missing_single_file_still_skips()
    {
        MapJob("/reports/j1", "\"j1\"", ReadyFiles("/files/a.csv"));

        var rows = await ReadSkippingMissing(Spec(Export(Urls)), p =>
        {
            Assert.True(p.Missing);
            return Task.CompletedTask;
        });

        Assert.Empty(rows);
    }

    private static StubResponse ZipFile(params (string Name, string Text)[] entries)
    {
        using var buffer = new MemoryStream();
        using (var zip = new System.IO.Compression.ZipArchive(buffer, System.IO.Compression.ZipArchiveMode.Create,
                   leaveOpen: true))
        {
            foreach (var (name, text) in entries)
            {
                using var writer = new StreamWriter(zip.CreateEntry(name).Open());
                writer.Write(text);
            }
        }

        return new StubResponse(200, "", new Dictionary<string, string> { ["Content-Type"] = "application/zip" })
        {
            RawBody = buffer.ToArray(),
        };
    }
}
