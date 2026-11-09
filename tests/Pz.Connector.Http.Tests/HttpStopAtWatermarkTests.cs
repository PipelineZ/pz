using System.Text.Json;
using Json.Schema;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

/// <summary><c>cursor_order: desc</c> + <c>stop_at_watermark: true</c>: a newest-first feed stops at the first
/// page holding a record the watermark already covers, those records are dropped with a notice, and a feed
/// that is not newest-first, or a crawl cut short before the watermark, fails.</summary>
public sealed class HttpStopAtWatermarkTests : IAsyncLifetime
{
    private readonly StubHttpServer _server = new();
    private readonly List<string> _notices = [];

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync() => await _server.DisposeAsync();

    private static Dictionary<string, object?> Map(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    private HttpConnectionConfig Connection()
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(
            new Dictionary<string, object?> { ["base_url"] = _server.BaseUrl.ToString() }), errors);
        Assert.Empty(errors);
        return connection!;
    }

    private async Task<(List<object?[]> Rows, List<string> Tokens)> Read(DatasetSpec spec, string? resumeFrom = null)
    {
        await using var source = new HttpSource(Connection(), TimeProvider.System);
        source.UseNotice(_notices.Add);
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        return await Drain(Assert.IsAssignableFrom<ICheckpointingPartition>(Assert.Single(partitions)), resumeFrom);
    }

    private static async Task<(List<object?[]> Rows, List<string> Tokens)> Drain(ICheckpointingPartition partition,
        string? resumeFrom = null)
    {
        if (resumeFrom is not null)
        {
            Assert.True(partition.TryResumeFrom(resumeFrom));
        }

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

    /// <summary>A newest-first feed of <c>{"ts": n}</c> records, <paramref name="pages"/>[i] being page i + 1,
    /// chained with a body cursor.</summary>
    private void MapFeed(params string[] pages)
        => _server.Map("/events", r =>
        {
            var page = int.TryParse(System.Web.HttpUtility.ParseQueryString(r.Url.Query)["cursor"], out var p) ? p : 1;
            var next = page < pages.Length ? $",\"next\":\"{page + 1}\"" : "";
            return new StubResponse(200, $$"""{"data":[{{pages[page - 1]}}]{{next}}}""");
        });

    private static string Ts(params int?[] values)
        => string.Join(',', values.Select(v => v is null ? """{"ts":null}""" : $$"""{"ts":{{v}}}"""));

    private static Dictionary<string, object?> FeedOptions(params (string Key, object? Value)[] extra)
    {
        var options = Map(("path", "/events"), ("items", "/data"),
            ("pagination", Map(("strategy", "cursor"), ("pointer", "/next"), ("param", "cursor"))),
            ("cursor", "ts"), ("cursor_type", "bigint"), ("cursor_order", "desc"), ("stop_at_watermark", true));
        foreach (var (k, v) in extra)
        {
            options[k] = v;
        }

        return options;
    }

    private static DatasetSpec Spec(Dictionary<string, object?> options, string? watermark)
        => new DatasetSpec("s", "d", options) { WatermarkCursor = "ts", WatermarkValue = watermark };

    private static long?[] Cursors(List<object?[]> rows) => [.. rows.Select(r => (long?)r[3])];

    [Fact]
    public async Task Stops_at_the_first_page_holding_a_covered_record_and_drops_those_records()
    {
        MapFeed(Ts(10, 9), Ts(8, 7, 6), Ts(5, 4));

        var (rows, _) = await Read(Spec(FeedOptions(), watermark: "7"));

        Assert.Equal([10, 9, 8], Cursors(rows));
        Assert.Equal(2, _server.Requests.Count);
        var notice = Assert.Single(_notices);
        Assert.Contains("stop_at_watermark stopped the crawl at page 2", notice);
        Assert.Contains("dropped 2 records at or below the watermark", notice);
    }

    [Fact]
    public async Task Without_a_watermark_the_crawl_runs_to_the_end()
    {
        MapFeed(Ts(10, 9), Ts(8, 7, 6), Ts(5, 4));

        var (rows, _) = await Read(Spec(FeedOptions(), watermark: null));

        Assert.Equal(7, rows.Count);
        Assert.Equal(3, _server.Requests.Count);
        Assert.Empty(_notices);
    }

    [Fact]
    public async Task A_watermark_older_than_the_whole_feed_reads_it_all_without_a_notice()
    {
        MapFeed(Ts(10, 9), Ts(8));

        var (rows, _) = await Read(Spec(FeedOptions(), watermark: "1"));

        Assert.Equal(3, rows.Count);
        Assert.Empty(_notices);
    }

    [Fact]
    public async Task A_covered_first_page_lands_nothing_and_stops()
    {
        MapFeed(Ts(10, 9), Ts(8));

        var (rows, _) = await Read(Spec(FeedOptions(), watermark: "10"));

        Assert.Empty(rows);
        Assert.Single(_server.Requests);
        Assert.Contains("dropped 2 records", Assert.Single(_notices));
    }

    [Fact]
    public async Task Null_cursors_land_and_do_not_stop_the_crawl()
    {
        MapFeed(Ts(10, null), Ts(null, 6), Ts(5));

        var (rows, _) = await Read(Spec(FeedOptions(), watermark: "6"));

        Assert.Equal([10, null, null], Cursors(rows));
        Assert.Equal(2, _server.Requests.Count);
    }

    [Fact]
    public async Task A_feed_that_is_not_newest_first_fails_even_on_a_first_run()
    {
        MapFeed(Ts(10, 9), Ts(9, 11), Ts(5));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(FeedOptions(), watermark: null)));

        Assert.False(ex.IsTransient);
        Assert.Contains("stop_at_watermark needs a newest-first feed", ex.Message);
        Assert.Contains("page 2", ex.Message);
    }

    [Fact]
    public async Task An_increase_across_pages_fails_too()
    {
        MapFeed(Ts(10, 9), Ts(12));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Read(Spec(FeedOptions(), watermark: "1")));

        Assert.Contains("newest-first", ex.Message);
    }

    [Fact]
    public async Task Timestamp_cursors_in_contract_mode_compare_as_instants()
    {
        _server.Map("/events", _ => new StubResponse(200, """
            {"data":[{"id":3,"at":"2026-01-01T02:00:00+01:00"},{"id":2,"at":"2026-01-01T00:30:00Z"},
                     {"id":1,"at":"2026-01-01T00:00:00Z"}]}
            """));
        var options = Map(("path", "/events"), ("items", "/data"), ("cursor_order", "desc"), ("stop_at_watermark", true),
            ("columns", new Dictionary<string, string> { ["id"] = "int", ["at"] = "timestamp" }));

        var (rows, _) = await Read(new DatasetSpec("s", "d", options)
        {
            WatermarkCursor = "at",
            WatermarkValue = "2026-01-01T00:30:00.000000",
        });

        Assert.Equal([3], rows.Select(r => (int)r[0]!));
    }

    [Fact]
    public async Task Varchar_cursors_compare_ordinally()
    {
        _server.Map("/events", _ => new StubResponse(200, """{"data":[{"k":"b"},{"k":"a"},{"k":"B"},{"k":"A"}]}"""));
        var options = Map(("path", "/events"), ("items", "/data"), ("cursor", "k"), ("cursor_type", "varchar"),
            ("cursor_order", "desc"), ("stop_at_watermark", true));

        var (rows, _) = await Read(new DatasetSpec("s", "d", options) { WatermarkCursor = "k", WatermarkValue = "Z" });

        Assert.Equal(["b", "a"], rows.Select(r => (string)r[3]!));
    }

    [Fact]
    public async Task A_resumed_attempt_continues_and_stops_on_data()
    {
        MapFeed(Ts(10, 9), Ts(8, 7, 6), Ts(5, 4));
        var spec = Spec(FeedOptions(), watermark: "7");

        var (_, tokens) = await Read(spec);
        var before = _server.Requests.Count;
        var (rows, _) = await Read(spec, resumeFrom: Assert.Single(tokens));

        Assert.Equal([8], Cursors(rows));
        Assert.Single(_server.Requests.Skip(before));
    }

    [Fact]
    public async Task A_capped_crawl_that_did_not_reach_the_watermark_fails()
    {
        // max_pages is refused next to stop_at_watermark; this config bypasses validation to prove the guard.
        MapFeed(Ts(10, 9), Ts(8, 7), Ts(5));
        var spec = Spec(FeedOptions(), watermark: "6");
        var config = HttpDatasetConfig.Parse(spec with { Options = FeedOptions() }) with { MaxPages = 1 };
        var connection = Connection();
        using var client = HttpSource.CreateClient(connection);
        using var bare = HttpSource.CreateBareClient(connection);
        var partition = new HttpPartition(client, bare, connection, config, spec, TimeProvider.System);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Drain(partition));

        Assert.False(ex.IsTransient);
        Assert.Contains("stop_at_watermark", ex.Message);
        Assert.Contains("did not reach the watermark", ex.Message);
    }

    [Fact]
    public void Options_are_validated_all_at_once()
    {
        var message = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(new DatasetSpec("s", "d", Map(
            ("path", "/events"), ("cursor", "ts"), ("cursor_type", "bigint"), ("cursor_order", "asc"),
            ("stop_at_watermark", true), ("max_pages", 3L), ("delta_pointer", "/delta"),
            ("pagination", Map(("strategy", "page"), ("concurrency", 2L))))))).Message;

        Assert.Contains("'stop_at_watermark' requires 'cursor_order: desc'", message);
        Assert.Contains("'stop_at_watermark' cannot be combined with max_pages", message);
        Assert.Contains("'stop_at_watermark' cannot be combined with delta_pointer", message);
        Assert.Contains("'stop_at_watermark' cannot be combined with pagination.concurrency above 1", message);
    }

    [Fact]
    public void Stop_at_watermark_is_refused_with_for_each_and_file_and_must_be_a_boolean()
    {
        var forEach = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(new DatasetSpec("s", "d", Map(
            ("path", "/o/{{ item.id }}"), ("cursor", "ts"), ("cursor_type", "bigint"), ("cursor_order", "desc"),
            ("stop_at_watermark", true),
            ("for_each", Map(("path", "/o"), ("fields", Map(("id", "/id"))), ("stop_at_watermark", true)))))))
            .Message;
        var file = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(new DatasetSpec("s", "d", Map(
            ("path", "/f.csv"), ("cursor_order", "desc"), ("stop_at_watermark", true),
            ("columns", new Dictionary<string, string> { ["ts"] = "bigint" }), ("file", Map(("format", "csv")))))))
            .Message;
        var notBool = Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(new DatasetSpec("s", "d", Map(
            ("path", "/e"), ("cursor", "ts"), ("cursor_type", "bigint"), ("cursor_order", "desc"),
            ("stop_at_watermark", "yes"))))).Message;

        Assert.Contains("'stop_at_watermark' cannot be combined with for_each", forEach);
        Assert.Contains("for_each.stop_at_watermark is not allowed in the list request", forEach);
        Assert.Contains("'stop_at_watermark' cannot be combined with file", file);
        Assert.Contains("'stop_at_watermark' must be a boolean", notBool);
    }

    [Fact]
    public void Stop_at_watermark_false_changes_nothing()
    {
        var config = HttpDatasetConfig.Parse(new DatasetSpec("s", "d", Map(("path", "/e"), ("max_pages", 2L),
            ("stop_at_watermark", false))));

        Assert.False(config.StopAtWatermark);
    }

    [Fact]
    public void Schema_has_stop_at_watermark()
    {
        var schema = JsonSchema.FromText(new HttpConnector().DatasetConfigSchema);

        Assert.True(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/e","cursor_order":"desc","stop_at_watermark":true}""")).IsValid);
        Assert.False(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/e","stop_at_watermark":"yes"}""")).IsValid);
    }
}
