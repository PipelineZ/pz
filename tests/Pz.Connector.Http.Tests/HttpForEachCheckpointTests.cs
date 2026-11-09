using System.Buffers.Text;
using System.Text;
using System.Text.Json.Nodes;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

/// <summary>Checkpoints of a for_each read: a <c>v3:</c> token naming the list page, the record index and
/// the item crawl's next request; resume re-fetches the list page, skips to the index and continues the
/// item. Each test drives the partition the way the engine does: poll after every batch, stop, then
/// resume a fresh partition from the last token offered.</summary>
public sealed class HttpForEachCheckpointTests : IAsyncLifetime
{
    private readonly StubHttpServer _server = new();
    private readonly List<string> _notices = [];

    private readonly string _scratchRoot =
        Path.Combine(Path.GetTempPath(), "pz-for-each-checkpoint-tests", Guid.NewGuid().ToString("N"));

    public HttpForEachCheckpointTests() => Directory.CreateDirectory(_scratchRoot);

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        await _server.DisposeAsync();
        Directory.Delete(_scratchRoot, recursive: true);
    }

    private static Dictionary<string, object?> Map(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    private static DatasetSpec Spec(Dictionary<string, object?> options) => new("s", "d", options);

    private static string Query(StubRequest r, string name)
        => System.Web.HttpUtility.ParseQueryString(r.Url.Query)[name] ?? "";

    private async Task<(HttpSource Source, ICheckpointingPartition Partition)> Open(DatasetSpec spec)
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = _server.BaseUrl.ToString(),
        }), errors);
        Assert.Empty(errors);
        var source = new HttpSource(connection!, TimeProvider.System);
        source.UseNotice(_notices.Add);
        var partition = Assert.IsAssignableFrom<ICheckpointingPartition>(
            Assert.Single(await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None)));
        if (partition is HttpForEachPartition forEach)
        {
            forEach.ScratchRoot = _scratchRoot;
        }

        return (source, partition);
    }

    /// <summary>One attempt: optionally resumed from <paramref name="resumeFrom"/>, polling a checkpoint
    /// after every batch like the engine. Stops after the batch whose rows satisfy
    /// <paramref name="stopAfter"/>. Returns each batch's first column values with the token offered after
    /// it, and the last token offered.</summary>
    private async Task<Attempt> Run(DatasetSpec spec, string? resumeFrom = null,
        Func<IReadOnlyList<string>, bool>? stopAfter = null, BatchOptions? options = null)
    {
        var (source, partition) = await Open(spec);
        await using var _ = source;
        if (resumeFrom is not null)
        {
            Assert.True(partition.TryResumeFrom(resumeFrom));
        }

        var attempt = new Attempt();
        await foreach (var batch in partition.ReadAsync(options ?? new BatchOptions(), CancellationToken.None))
        {
            var values = new List<string>();
            using (batch)
            {
                for (var r = 0; r < batch.Length; r++)
                {
                    values.Add(Key((string)FileReaderRows.Value(batch.Column(0), r)!));
                }
            }

            string? offered = partition.TryGetCheckpoint(out var token) ? token : null;
            attempt.Batches.Add((values, offered));
            attempt.LastToken = offered ?? attempt.LastToken;
            if (stopAfter?.Invoke(values) == true)
            {
                break;
            }
        }

        return attempt;
    }

    private sealed class Attempt
    {
        public List<(List<string> Keys, string? Token)> Batches { get; } = [];
        public string? LastToken { get; set; }
        public List<string> Keys => [.. Batches.SelectMany(b => b.Keys)];
    }

    /// <summary>A raw row's line sku; a file row's own first column as is.</summary>
    private static string Key(string value)
        => value.StartsWith('{') ? JsonNode.Parse(value)!["sku"]!.GetValue<string>() : value;

    private List<string> RequestsSince(int start)
        => [.. _server.Requests.Skip(start).Select(r => r.Url.PathAndQuery)];

    /// <summary>Orders list, item request /orders/{id}/lines, paginated by page number when
    /// <paramref name="paged"/>.</summary>
    private static Dictionary<string, object?> OrdersOptions(bool paged, Dictionary<string, object?>? forEachExtra = null,
        bool pagedList = false)
    {
        var forEach = Map(("path", "/orders"), ("items", "/data"), ("fields", Map(("order_id", "/id"))));
        if (pagedList)
        {
            forEach["pagination"] = Map(("strategy", "page"));
        }

        foreach (var (k, v) in forEachExtra ?? [])
        {
            forEach[k] = v;
        }

        var options = Map(("path", "/orders/{{ item.order_id }}/lines"), ("items", "/lines"), ("for_each", forEach));
        if (paged)
        {
            options["pagination"] = Map(("strategy", "page"));
        }

        return options;
    }

    private static StubResponse OrdersPage(params string[] ids)
        => new(200, new JsonObject
        {
            ["data"] = new JsonArray([.. ids.Select(id => (JsonNode)new JsonObject { ["id"] = id })]),
        }.ToJsonString());

    /// <summary>/orders/{id}/lines: with ?page=n, <paramref name="pages"/> pages of one line each then an
    /// empty page; without a page param, one line.</summary>
    private void MapLines(int pages = 2, Func<string, int, StubResponse?>? overrideFor = null)
    {
        _server.MapPrefix("/orders/", r =>
        {
            var id = r.Url.AbsolutePath.Split('/')[2];
            var page = Query(r, "page") is { Length: > 0 } p ? int.Parse(p) : 1;
            if (overrideFor?.Invoke(id, page) is { } custom)
            {
                return custom;
            }

            return page <= pages
                ? new StubResponse(200, $$"""{"lines":[{"sku":"{{id}}-{{page}}"}]}""")
                : new StubResponse(200, """{"lines":[]}""");
        });
    }

    [Fact]
    public async Task Resume_mid_item_crawl()
    {
        _server.Map("/orders", _ => OrdersPage("o1", "o2", "o3", "o4"));
        MapLines();
        var spec = Spec(OrdersOptions(paged: true));

        var first = await Run(spec, stopAfter: keys => keys.Contains("o3-1"));

        Assert.Equal(["o1-1", "o1-2", "o2-1", "o2-2", "o3-1"], first.Keys);
        Assert.True(HttpCheckpointToken.TryDecodeForEach(first.LastToken!, out var list, out var index, out var item));
        Assert.Equal($"{_server.BaseUrl}orders", list);
        Assert.Equal(2, index);
        Assert.Equal($"{_server.BaseUrl}orders/o3/lines?page=2", item);

        var mark = _server.Requests.Count;
        var resumed = await Run(spec, first.LastToken);

        Assert.Equal(["o3-2", "o4-1", "o4-2"], resumed.Keys);
        Assert.Equal(["/orders", "/orders/o3/lines?page=2", "/orders/o3/lines?page=3",
            "/orders/o4/lines?page=1", "/orders/o4/lines?page=2", "/orders/o4/lines?page=3"], RequestsSince(mark));
    }

    [Fact]
    public async Task Resume_between_items()
    {
        _server.Map("/orders", _ => OrdersPage("o1", "o2", "o3", "o4"));
        MapLines();
        var spec = Spec(OrdersOptions(paged: false));

        var first = await Run(spec, stopAfter: keys => keys.Contains("o2-1"));

        // A single-page item's one batch carries the item-complete checkpoint.
        Assert.Equal(2, first.Batches.Count);
        Assert.All(first.Batches, b => Assert.NotNull(b.Token));
        Assert.True(HttpCheckpointToken.TryDecodeForEach(first.LastToken!, out _, out var index, out var item));
        Assert.Equal(2, index);
        Assert.Null(item);

        var mark = _server.Requests.Count;
        var resumed = await Run(spec, first.LastToken);

        Assert.Equal(["o3-1", "o4-1"], resumed.Keys);
        Assert.Equal(["/orders", "/orders/o3/lines", "/orders/o4/lines"], RequestsSince(mark));
    }

    [Fact]
    public async Task Resume_at_end_of_list_page()
    {
        _server.Map("/orders", r => Query(r, "page") switch
        {
            "1" => OrdersPage("o1", "o2"),
            "2" => OrdersPage("o3", "o4"),
            _ => OrdersPage(),
        });
        MapLines();
        var spec = Spec(OrdersOptions(paged: false, pagedList: true));

        var first = await Run(spec, stopAfter: keys => keys.Contains("o2-1"));

        // index == the page's record count, with the same list page: resume moves to the next page.
        Assert.True(HttpCheckpointToken.TryDecodeForEach(first.LastToken!, out var list, out var index, out var item));
        Assert.Equal($"{_server.BaseUrl}orders?page=1", list);
        Assert.Equal(2, index);
        Assert.Null(item);

        var mark = _server.Requests.Count;
        var resumed = await Run(spec, first.LastToken);

        Assert.Equal(["o3-1", "o4-1"], resumed.Keys);
        Assert.Equal(["/orders?page=1", "/orders?page=2", "/orders/o3/lines", "/orders/o4/lines", "/orders?page=3"],
            RequestsSince(mark));
    }

    [Fact]
    public async Task Shrunk_list_page_continues()
    {
        var shrunk = false;
        _server.Map("/orders", r => Query(r, "page") switch
        {
            "1" => shrunk ? OrdersPage("o1") : OrdersPage("o1", "o2", "o3"),
            "2" => OrdersPage("o4"),
            _ => OrdersPage(),
        });
        MapLines();
        var spec = Spec(OrdersOptions(paged: false, pagedList: true));

        var first = await Run(spec, stopAfter: keys => keys.Contains("o2-1"));
        Assert.True(HttpCheckpointToken.TryDecodeForEach(first.LastToken!, out _, out var index, out _));
        Assert.Equal(2, index);

        shrunk = true;
        var mark = _server.Requests.Count;
        var resumed = await Run(spec, first.LastToken);

        Assert.Equal(["o4-1"], resumed.Keys);
        Assert.Equal(["/orders?page=1", "/orders?page=2", "/orders/o4/lines", "/orders?page=3"], RequestsSince(mark));
    }

    [Fact]
    public async Task Token_formats_are_exclusive()
    {
        var listUrl = $"{_server.BaseUrl}orders";
        var itemUrl = $"{_server.BaseUrl}orders/o1/lines?page=2";
        var v3 = HttpCheckpointToken.EncodeForEach(listUrl, 0, itemUrl);
        var v2 = HttpCheckpointToken.Encode(new HttpPageRequest(HttpMethod.Post, new Uri(listUrl),
            JsonNode.Parse("""{"a":1}"""), null), isPost: true);

        // Without for_each: a v3 token is refused, GET or POST.
        var (getSource, plainGet) = await Open(Spec(Map(("path", "/orders"), ("items", "/data"))));
        await using (getSource)
        {
            Assert.False(plainGet.TryResumeFrom(v3));
            Assert.True(plainGet.TryResumeFrom(listUrl));
        }

        var (postSource, plainPost) = await Open(Spec(Map(("path", "/orders"), ("items", "/data"),
            ("method", "post"), ("body", Map(("a", 1L))))));
        await using (postSource)
        {
            Assert.False(plainPost.TryResumeFrom(v3));
        }

        // With for_each: plain and v2 are refused; a disallowed host anywhere inside v3 is refused.
        var (source, forEach) = await Open(Spec(OrdersOptions(paged: true)));
        await using (source)
        {
            Assert.False(forEach.TryResumeFrom(listUrl));
            Assert.False(forEach.TryResumeFrom(v2));
            Assert.False(forEach.TryResumeFrom(HttpCheckpointToken.EncodeForEach("https://evil.example/orders", 0, null)));
            Assert.False(forEach.TryResumeFrom(HttpCheckpointToken.EncodeForEach(listUrl, 0,
                "https://evil.example/orders/o1/lines?page=2")));
            Assert.False(forEach.TryResumeFrom(HttpCheckpointToken.EncodeForEach(listUrl, -1, null)));
            // The list request is a GET, so a v2 list token does not fit; nor does a v2 item token.
            Assert.False(forEach.TryResumeFrom(HttpCheckpointToken.EncodeForEach(v2, 0, null)));
            Assert.False(forEach.TryResumeFrom(HttpCheckpointToken.EncodeForEach(listUrl, 0, v2)));
            Assert.False(forEach.TryResumeFrom("v3:not-base64!"));
            Assert.False(forEach.TryResumeFrom("v3:" + Base64Url.EncodeToString("""{"list":1,"index":0}"""u8)));
            Assert.True(forEach.TryResumeFrom(v3));
            Assert.True(forEach.TryResumeFrom(HttpCheckpointToken.EncodeForEach(listUrl, 7, null)));
        }

        // File items have no position inside them: an item token is refused there.
        var (fileSource, file) = await Open(Spec(ExportsOptions()));
        await using (fileSource)
        {
            Assert.False(file.TryResumeFrom(HttpCheckpointToken.EncodeForEach($"{_server.BaseUrl}exports", 0,
                $"{_server.BaseUrl}exports/e1/file")));
            Assert.True(file.TryResumeFrom(HttpCheckpointToken.EncodeForEach($"{_server.BaseUrl}exports", 1, null)));
        }
    }

    [Fact]
    public async Task Skip_then_transient_then_retry()
    {
        _server.Map("/orders", _ => OrdersPage("o1", "o2", "o3", "o4"));
        var o3Fails = true;
        MapLines(overrideFor: (id, _) =>
            id == "o2" ? new StubResponse(404, "gone")
            : id == "o3" && o3Fails ? new StubResponse(503, "busy")
            : null);
        var spec = Spec(OrdersOptions(paged: false, Map(("on_missing", "skip"))));

        var (source, partition) = await Open(spec);
        string? token = null;
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await using var _ = source;
            await foreach (var batch in partition.ReadAsync(new BatchOptions(), CancellationToken.None))
            {
                batch.Dispose();
                if (partition.TryGetCheckpoint(out var offered))
                {
                    token = offered;
                }
            }
        });
        Assert.True(ex.IsTransient);
        Assert.Empty(_notices);
        // The last checkpoint is o1's completion, before the skipped o2.
        Assert.True(HttpCheckpointToken.TryDecodeForEach(token!, out _, out var index, out _));
        Assert.Equal(1, index);

        o3Fails = false;
        var resumed = await Run(spec, token);

        Assert.Equal(["o3-1", "o4-1"], resumed.Keys);
        Assert.Equal("http source 's.d': skipped 1 items that returned 404/410 (first: item order_id=o2)",
            Assert.Single(_notices));
    }

    [Fact]
    public async Task Skip_before_a_resumed_item_crawl_is_not_reported_by_the_retry()
    {
        // The skip count belongs to an attempt. A retry resumed past the skipped item never sees it, so
        // the final attempt reports none.
        _server.Map("/orders", _ => OrdersPage("o1", "o2", "o3", "o4"));
        var o3Fails = true;
        MapLines(overrideFor: (id, page) =>
            id == "o2" ? new StubResponse(404, "gone")
            : id == "o3" && page == 2 && o3Fails ? new StubResponse(503, "busy")
            : null);
        var spec = Spec(OrdersOptions(paged: true, Map(("on_missing", "skip"))));

        var (source, partition) = await Open(spec);
        string? token = null;
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await using var _ = source;
            await foreach (var batch in partition.ReadAsync(new BatchOptions(), CancellationToken.None))
            {
                batch.Dispose();
                if (partition.TryGetCheckpoint(out var offered))
                {
                    token = offered;
                }
            }
        });
        Assert.True(ex.IsTransient);
        Assert.True(HttpCheckpointToken.TryDecodeForEach(token!, out _, out var index, out var item));
        Assert.Equal(2, index);
        Assert.NotNull(item);

        o3Fails = false;
        var resumed = await Run(spec, token);

        Assert.Equal(["o3-2", "o4-1", "o4-2"], resumed.Keys);
        Assert.Empty(_notices);
    }

    private static Dictionary<string, object?> ExportsOptions()
        => Map(("for_each", Map(("path", "/exports"), ("items", "/exports"), ("fields", Map(("export_id", "/id"))))),
            ("path", "/exports/{{ item.export_id }}/file"), ("file", Map(("format", "csv"))),
            ("columns", new Dictionary<string, string> { ["name"] = "varchar", ["export_id"] = "varchar" }));

    [Fact]
    public async Task File_items_checkpoint_at_item_boundaries()
    {
        _server.Map("/exports", _ => new StubResponse(200, """{"exports":[{"id":"e1"},{"id":"e2"},{"id":"e3"}]}"""));
        _server.MapPrefix("/exports/", r => new StubResponse(200, r.Url.AbsolutePath switch
        {
            "/exports/e1/file" => "name\na\nb\n",
            "/exports/e2/file" => "name\nc\n",
            _ => "name\nd\n",
        }, new Dictionary<string, string> { ["Content-Type"] = "text/csv" }));
        var spec = Spec(ExportsOptions());
        var oneRow = new BatchOptions(TargetBatchBytes: 1, MaxRowsPerBatch: 1);

        var first = await Run(spec, options: oneRow, stopAfter: keys => keys.Contains("c"));

        var listUrl = $"{_server.BaseUrl}exports";
        Assert.Equal(
            [
                (["a"], null),
                (["b"], HttpCheckpointToken.EncodeForEach(listUrl, 1, null)),
                (["c"], HttpCheckpointToken.EncodeForEach(listUrl, 2, null)),
            ],
            first.Batches.Select(b => (b.Keys.ToArray(), b.Token)).ToList(),
            new BatchComparer());

        var mark = _server.Requests.Count;
        var resumed = await Run(spec, first.LastToken, options: oneRow);

        Assert.Equal(["d"], resumed.Keys);
        Assert.Equal(["/exports", "/exports/e3/file"], RequestsSince(mark));
    }

    private sealed class BatchComparer : IEqualityComparer<(string[] Keys, string? Token)>
    {
        public bool Equals((string[] Keys, string? Token) x, (string[] Keys, string? Token) y)
            => x.Keys.SequenceEqual(y.Keys) && x.Token == y.Token;

        public int GetHashCode((string[] Keys, string? Token) obj) => obj.Token?.GetHashCode() ?? 0;
    }

    [Fact]
    public void Existing_tokens_unchanged()
    {
        var url = new Uri("https://h.example/a?page=2");
        Assert.Equal("https://h.example/a?page=2",
            HttpCheckpointToken.Encode(new HttpPageRequest(HttpMethod.Get, url, null, null), isPost: false));

        static string V2(string json) => "v2:" + Base64Url.EncodeToString(Encoding.UTF8.GetBytes(json));
        Assert.Equal(V2("""{"url":"https://h.example/a?page=2","body":{"from":4}}"""),
            HttpCheckpointToken.Encode(new HttpPageRequest(HttpMethod.Post, url, JsonNode.Parse("""{"from":4}"""), null),
                isPost: true));
        Assert.Equal(V2("""{"url":"https://h.example/a?page=2","body":"a=1"}"""),
            HttpCheckpointToken.Encode(new HttpPageRequest(HttpMethod.Post, url, null, "a=1"), isPost: true));
        Assert.Equal(V2("""{"url":"https://h.example/a?page=2","body":null}"""),
            HttpCheckpointToken.Encode(new HttpPageRequest(HttpMethod.Post, url, null, null), isPost: true));

        Assert.Equal(
            "v3:" + Base64Url.EncodeToString(Encoding.UTF8.GetBytes(
                """{"list":"https://h.example/l","index":3,"item":"https://h.example/i?page=2"}""")),
            HttpCheckpointToken.EncodeForEach("https://h.example/l", 3, "https://h.example/i?page=2"));
    }
}
