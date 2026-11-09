using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Xunit;

namespace Pz.Connector.Http.Tests;

public sealed class HttpDatedUrlTests : IAsyncLifetime
{
    private readonly StubHttpServer _server = new();
    private readonly ManualClock _clock = new();

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync() => await _server.DisposeAsync();

    private void SetClock(string utc) => _clock.Advance(DateTimeOffset.Parse(utc) - _clock.GetUtcNow());

    private static DatasetSpec Spec(Dictionary<string, object?> options) => new("stub", "dated", options);

    private async Task ReadAll(DatasetSpec spec, ConnectorRunContext? run = null)
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = _server.BaseUrl.ToString(),
        }), errors);
        Assert.Empty(errors);

        await using var source = new HttpSource(connection!, _clock);
        if (run is not null)
        {
            source.UseRunContext(run);
        }

        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(new BatchOptions(), CancellationToken.None))
            {
                batch.Dispose();
            }
        }
    }

    private static string QueryDate(StubRequest request)
        => request.Url.Query.TrimStart('?').Split('&').Single(p => p.StartsWith("date=", StringComparison.Ordinal))[5..];

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Every_attempt_of_a_run_renders_the_run_date_of_the_runs_start(bool file)
    {
        // An attempt retried after midnight must send what the first sent: the same dated URL, and so the same
        // export idempotency key.
        _server.MapPrefix("/exports/", _ => file
            ? new StubResponse(200, "id\n1\n", new Dictionary<string, string> { ["Content-Type"] = "text/csv" })
            : new StubResponse(200, "[]"));
        var run = new ConnectorRunContext("run-1", "node-a") { StartedAt = DateTimeOffset.Parse("2026-11-07T23:59:58Z") };
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/exports/{{ run_date }}/x",
            ["run_date"] = new Dictionary<string, object?> { ["format"] = "yyyy-MM-dd" },
        };
        if (file)
        {
            options["file"] = new Dictionary<string, object?> { ["format"] = "csv" };
            options["columns"] = new Dictionary<string, string> { ["id"] = "int" };
        }

        SetClock("2026-11-07T23:59:59Z");
        await ReadAll(Spec(options), run);
        SetClock("2026-11-08T00:00:05Z");
        await ReadAll(Spec(options), run);
        await ReadAll(Spec(options));

        Assert.Equal(["/exports/2026-11-07/x", "/exports/2026-11-07/x", "/exports/2026-11-08/x"],
            _server.Requests.Select(r => r.Url.AbsolutePath));
    }

    [Fact]
    public async Task Path_binding_expands_and_escapes()
    {
        SetClock("2026-11-07T12:00:00Z");
        _server.MapPrefix("/exports/", _ => new StubResponse(200, "[]"));

        await ReadAll(Spec(new()
        {
            ["path"] = "/exports/{{ run_date }}/x",
            ["run_date"] = new Dictionary<string, object?> { ["format"] = "yyyy/MM/dd" },
        }));

        var request = Assert.Single(_server.Requests);
        Assert.Equal("/exports/2026%2F11%2F07/x", request.Url.AbsolutePath);
    }

    [Fact]
    public async Task Run_date_defaults_to_utc_today()
    {
        SetClock("2026-11-08T23:30:00Z");
        _server.Map("/daily", _ => new StubResponse(200, "[]"));

        await ReadAll(Spec(new()
        {
            ["path"] = "/daily",
            ["query"] = new Dictionary<string, object?> { ["date"] = "{{ run_date }}" },
        }));

        Assert.Equal("2026-11-08", QueryDate(Assert.Single(_server.Requests)));
    }

    [Fact]
    public async Task Run_date_offset_and_timezone()
    {
        // 23:30Z is 01:30 on the 9th in Bucharest (UTC+2 in November); one day back is the 8th.
        SetClock("2026-11-08T23:30:00Z");
        _server.Map("/daily", _ => new StubResponse(200, "[]"));

        await ReadAll(Spec(new()
        {
            ["path"] = "/daily",
            ["query"] = new Dictionary<string, object?> { ["date"] = "{{ run_date }}" },
            ["run_date"] = new Dictionary<string, object?>
            {
                ["offset_days"] = -1, ["timezone"] = "Europe/Bucharest",
            },
        }));

        Assert.Equal("2026-11-08", QueryDate(Assert.Single(_server.Requests)));
    }

    [Fact]
    public async Task Run_date_is_read_once_per_partition()
    {
        SetClock("2026-11-08T23:30:00Z");
        _server.Map("/paged", req =>
        {
            if (req.Url.Query.Contains("page=2"))
            {
                return new StubResponse(200, "[]");
            }

            _clock.Advance(TimeSpan.FromHours(1)); // past midnight before page two is built
            return new StubResponse(200, """[{"a":1}]""");
        });

        await ReadAll(Spec(new()
        {
            ["path"] = "/paged",
            ["query"] = new Dictionary<string, object?> { ["date"] = "{{ run_date }}" },
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "page" },
        }));

        Assert.Equal(2, _server.Requests.Count);
        Assert.All(_server.Requests, r => Assert.Equal("2026-11-08", QueryDate(r)));
    }

    [Fact]
    public async Task Null_binding_in_path_is_an_error()
    {
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAll(Spec(new()
        {
            ["path"] = "/d/{{ watermark }}.csv",
        })));

        Assert.False(ex.IsTransient);
        Assert.Contains("watermark", ex.Message);
        Assert.Contains("initial", ex.Message);
        Assert.Empty(_server.Requests);
    }

    [Fact]
    public void Unknown_binding_in_path_is_a_validation_error()
    {
        var ex = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/d/{{ nope }}.csv",
        })));

        Assert.Contains("option 'path': unknown binding 'nope'", ex.Message);
    }

    [Fact]
    public void Run_date_validation_is_aggregate()
    {
        var ex = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/d",
            ["run_date"] = new Dictionary<string, object?>
            {
                ["offset_days"] = "x", ["timezone"] = "Mars/Base", ["format"] = "yyyy-MM-dd HH",
            },
        })));

        Assert.Contains("'run_date.offset_days' must be an integer, got 'x'", ex.Message);
        Assert.Contains("'run_date.timezone' 'Mars/Base' is not a known time zone id", ex.Message);
        Assert.Contains("run_date.format may only use y, M, d and - _ / . separators", ex.Message);
    }

    [Fact]
    public void Run_date_offset_days_is_bounded()
    {
        Assert.Equal(36500, HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/d",
            ["run_date"] = new Dictionary<string, object?> { ["offset_days"] = "36500" },
        })).RunDate.OffsetDays);

        var ex = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/d",
            ["run_date"] = new Dictionary<string, object?> { ["offset_days"] = "-36501", ["timezone"] = "Mars/Base" },
        })));

        Assert.Contains("'run_date.offset_days' must be between -36500 and 36500, got '-36501'", ex.Message);
        Assert.Contains("'run_date.timezone' 'Mars/Base' is not a known time zone id", ex.Message);
        var offsetSchema = System.Text.Json.Nodes.JsonNode.Parse(new HttpConnector().DatasetConfigSchema)!
            ["properties"]!["run_date"]!["properties"]!["offset_days"]!;
        Assert.Equal(-36500, (int)offsetSchema["minimum"]!);
        Assert.Equal(36500, (int)offsetSchema["maximum"]!);
    }

    [Fact]
    public async Task Window_upper_date_renders_plain_date()
    {
        _server.Map("/d/2026-11-07.zip", _ => new StubResponse(200, "[]"));

        await ReadAll(new DatasetSpec("stub", "dated", new Dictionary<string, object?>
        {
            ["path"] = "/d/{{ window_upper }}.zip",
            ["cursor"] = "day",
            ["cursor_type"] = "date",
        })
        {
            WatermarkCursor = "day",
            WatermarkValue = "2026-11-06",
            WatermarkUpperBound = "2026-11-07",
        });

        Assert.Equal("/d/2026-11-07.zip", Assert.Single(_server.Requests).Url.AbsolutePath);
    }
}
