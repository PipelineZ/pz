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

    public HttpExportJobTests() => _clock.Advance(Now - _clock.GetUtcNow());

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

    private HttpSource Source(Dictionary<string, object?>? auth = null, bool allowFiles = true)
    {
        var values = new Dictionary<string, object?>
        {
            ["base_url"] = _server.BaseUrl.ToString(),
            ["auth"] = auth ?? new Dictionary<string, object?> { ["type"] = "bearer", ["token"] = "secret-token" },
            ["headers"] = new Dictionary<string, object?> { ["X-Conn"] = "header-secret" },
        };
        if (allowFiles)
        {
            values["allow_hosts"] = new List<object?> { _files.BaseUrl.Host };
        }

        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(values), errors);
        Assert.Empty(errors);
        return new HttpSource(connection!, _clock);
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
        Assert.Contains("export job j1 not ready after 1 minutes", ex.Message);
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
        Assert.Contains("HTTP 403", ex.Message);
        Assert.Contains("/bucket/abc.csv", ex.Message);
        Assert.DoesNotContain("SECRET", ex.Message);
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
}
