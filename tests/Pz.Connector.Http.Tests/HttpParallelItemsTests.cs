using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

/// <summary><c>for_each.concurrency</c> above 1: up to N items of one list page in flight, a barrier before
/// the next list page, checkpoints only at barriers, the first failure in record order, and every batch
/// and scratch directory released however the read ends. Each item request blocks in the stub on a
/// per-item gate the test opens, so overlap is counted by the stub, never timed.</summary>
public sealed class HttpParallelItemsTests : IDisposable
{
    private static readonly Uri BaseUrl = new("https://api.example.com/");

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private readonly List<string> _notices = [];

    // This class's own scratch root: other test classes download into the shared temp dir in parallel.
    private readonly string _scratchRoot =
        Path.Combine(Path.GetTempPath(), "pz-parallel-items-tests", Guid.NewGuid().ToString("N"));

    public HttpParallelItemsTests() => Directory.CreateDirectory(_scratchRoot);

    public void Dispose() => Directory.Delete(_scratchRoot, recursive: true);

    /// <summary>Serves the list (a one-segment path) from <see cref="List"/> and items (a deeper path,
    /// the id its second segment) from <see cref="Item"/>. Every item request waits on its id's gate;
    /// the gates exist before the requests do, so a gate opened first simply lets the request through.</summary>
    private sealed class ItemsHandler : HttpMessageHandler
    {
        private readonly ConcurrentDictionary<string, TaskCompletionSource> _gates = new();
        private readonly ConcurrentDictionary<string, TaskCompletionSource> _arrivals = new();
        private readonly ConcurrentQueue<string> _requests = new();
        private readonly ConcurrentQueue<string> _cancelled = new();
        private readonly ConcurrentQueue<(string Query, int Completed, int InFlight)> _listRequests = new();
        private readonly TaskCompletionSource _cancelledEnough =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        private volatile bool _allOpen;
        private int _inFlight;
        private int _maxInFlight;
        private int _completed;

        public required Func<Uri, string> List { get; init; }

        /// <summary>Status, body and media type of an item; defaults to one JSON line.</summary>
        public Func<string, (int Status, string Body, string MediaType)> Item { get; init; } =
            id => (200, $$"""{"lines":[{"sku":"{{id}}-a"}]}""", "application/json");

        /// <summary>Items whose request times out (the client's own timeout, a TaskCanceledException while
        /// the read's token is not cancelled) once their gate opens.</summary>
        public Func<string, bool> TimesOut { get; init; } = _ => false;

        /// <summary>How many item requests must observe cancellation before <see cref="CancelledEnough"/>
        /// completes.</summary>
        public int CancelledTarget { get; init; } = int.MaxValue;

        public int MaxInFlight => Volatile.Read(ref _maxInFlight);

        public int InFlight => Volatile.Read(ref _inFlight);

        /// <summary>Item ids in request order.</summary>
        public IReadOnlyList<string> Requests => [.. _requests];

        public IReadOnlyList<string> Cancelled => [.. _cancelled];

        /// <summary>Each list request's query, with the item requests completed and in flight when it arrived.</summary>
        public IReadOnlyList<(string Query, int Completed, int InFlight)> ListRequests => [.. _listRequests];

        public Task CancelledEnough => _cancelledEnough.Task.WaitAsync(Bound);

        public void Open(params string[] ids)
        {
            foreach (var id in ids)
            {
                Gate(_gates, id).TrySetResult();
            }
        }

        public ItemsHandler OpenAll()
        {
            _allOpen = true;
            foreach (var gate in _gates.Values)
            {
                gate.TrySetResult();
            }

            return this;
        }

        /// <summary>Completes once the request for every one of <paramref name="ids"/> has reached the stub.</summary>
        public Task Arrived(params string[] ids)
            => Task.WhenAll(ids.Select(id => Gate(_arrivals, id).Task)).WaitAsync(Bound);

        private TaskCompletionSource Gate(ConcurrentDictionary<string, TaskCompletionSource> map, string id)
        {
            var gate = map.GetOrAdd(id, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
            if (map == _gates && _allOpen)
            {
                gate.TrySetResult();
            }

            return gate;
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var segments = request.RequestUri!.AbsolutePath.Trim('/').Split('/');
            if (segments.Length == 1)
            {
                _listRequests.Enqueue((request.RequestUri.Query, Volatile.Read(ref _completed), InFlight));
                return Respond(request, 200, List(request.RequestUri), "application/json");
            }

            var id = segments[1];
            _requests.Enqueue(id);
            var now = Interlocked.Increment(ref _inFlight);
            try
            {
                int seen;
                while (now > (seen = Volatile.Read(ref _maxInFlight))
                       && Interlocked.CompareExchange(ref _maxInFlight, now, seen) != seen)
                {
                }

                Gate(_arrivals, id).TrySetResult();
                try
                {
                    await Gate(_gates, id).Task.WaitAsync(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    _cancelled.Enqueue(id);
                    if (_cancelled.Count >= CancelledTarget)
                    {
                        _cancelledEnough.TrySetResult();
                    }

                    throw;
                }

                if (TimesOut(id))
                {
                    throw new TaskCanceledException("The request was canceled due to the configured HttpClient.Timeout");
                }

                var (status, body, mediaType) = Item(id);
                Interlocked.Increment(ref _completed);
                return Respond(request, status, body, mediaType);
            }
            finally
            {
                Interlocked.Decrement(ref _inFlight);
            }
        }

        private static HttpResponseMessage Respond(HttpRequestMessage request, int status, string body, string mediaType)
            => new((HttpStatusCode)status)
            {
                Content = new StringContent(body, Encoding.UTF8, mediaType),
                RequestMessage = request,
            };
    }

    /// <summary>List pages by the <c>page</c> query param (from 1); past the last, an empty page.</summary>
    private static Func<Uri, string> Paged(params string[][] pages) => url =>
    {
        var page = int.Parse(System.Web.HttpUtility.ParseQueryString(url.Query)["page"] ?? "1");
        return Records(page <= pages.Length ? pages[page - 1] : []);
    };

    private static string Records(string[] ids, string? next = null)
    {
        var body = new JsonObject
        {
            ["data"] = new JsonArray([.. ids.Select(id => (JsonNode)new JsonObject { ["id"] = id })]),
        };
        if (next is not null)
        {
            body["next"] = next;
        }

        return body.ToJsonString();
    }

    private static string[] Ids(int from, int to) => [.. Enumerable.Range(from, to - from + 1).Select(i => $"o{i}")];

    private static Dictionary<string, object?> Map(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    /// <summary>A for_each over /orders (field order_id from /id) with <paramref name="concurrency"/>; JSON
    /// items /orders/{id}/lines, or in file mode CSV downloads /orders/{id}/file.</summary>
    private static Dictionary<string, object?> Options(int concurrency, Dictionary<string, object?>? listPagination = null,
        Dictionary<string, object?>? forEachExtra = null, bool file = false)
    {
        var forEach = Map(("path", "/orders"), ("items", "/data"), ("fields", Map(("order_id", "/id"))),
            ("concurrency", concurrency));
        if (listPagination is not null)
        {
            forEach["pagination"] = listPagination;
        }

        foreach (var (k, v) in forEachExtra ?? [])
        {
            forEach[k] = v;
        }

        var columns = new Dictionary<string, string> { ["order_id"] = "varchar", ["sku"] = "varchar" };
        return file
            ? Map(("for_each", forEach), ("path", "/orders/{{ item.order_id }}/file"), ("file", Map(("format", "csv"))),
                ("columns", columns))
            : Map(("for_each", forEach), ("path", "/orders/{{ item.order_id }}/lines"), ("items", "/lines"),
                ("columns", columns));
    }

    /// <summary>Numbered list pages of <paramref name="size"/> records; a short page is the last.</summary>
    private static Dictionary<string, object?> SizedPages(int size)
        => Map(("strategy", "page"), ("size", size), ("stop_on_short_page", true));

    private HttpForEachPartition Partition(Dictionary<string, object?> options, ItemsHandler handler,
        IOperationGate? gate = null)
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = BaseUrl.ToString(),
        }), errors)!;
        Assert.Empty(errors);
        var spec = new DatasetSpec("s", "d", options);
        var config = HttpDatasetConfig.Parse(spec);
        var client = new HttpClient(handler) { BaseAddress = connection.BaseUrl };
        // As HttpSource plans it: concurrent items share one Retry-After pause.
        return new HttpForEachPartition(client, client, connection, config, spec, TimeProvider.System, gate,
            new ThrottlePause(TimeProvider.System))
        {
            Notice = message =>
            {
                lock (_notices)
                {
                    _notices.Add(message);
                }
            },
            ScratchRoot = _scratchRoot,
        };
    }

    private sealed class ReadLog
    {
        public List<(string Order, string Sku)> Rows { get; } = [];

        /// <summary>Each checkpoint offered, with the rows yielded before it.</summary>
        public List<(int RowsBefore, string Token)> Checkpoints { get; } = [];
    }

    private static readonly BatchOptions OneRowBatches = new(TargetBatchBytes: 1, MaxRowsPerBatch: 1);

    /// <summary>Drains the read, recording rows and every checkpoint offered after each batch.</summary>
    private static async Task<ReadLog> Drain(HttpForEachPartition partition, CancellationToken ct = default)
    {
        var log = new ReadLog();
        await foreach (var batch in partition.ReadAsync(OneRowBatches, ct))
        {
            using (batch)
            {
                for (var r = 0; r < batch.Length; r++)
                {
                    log.Rows.Add(((string)FileReaderRows.Value(batch.Column(0), r)!,
                        (string)FileReaderRows.Value(batch.Column(1), r)!));
                }
            }

            if (partition.TryGetCheckpoint(out var token) && token is not null)
            {
                log.Checkpoints.Add((log.Rows.Count, token));
            }
        }

        return log;
    }

    /// <summary>The query of a for_each token's list request.</summary>
    private static string ListQuery(HttpCheckpointToken.ForEachToken token)
    {
        Assert.True(HttpCheckpointToken.TryDecode(token.List, false, HttpMethod.Get, HttpCheckpointToken.KindOf(null),
            out var list));
        return list!.Url.Query;
    }

    private static HttpCheckpointToken.ForEachToken Decode(string token)
    {
        Assert.True(HttpCheckpointToken.TryDecodeForEach(token, out var decoded));
        return decoded;
    }

    private static string Identity(string id)
        => ItemValues.From(JsonNode.Parse($$"""{"id":"{{id}}"}"""), [("order_id", "/id")]).Identity();

    private static List<string> Orders(ReadLog log) => [.. log.Rows.Select(r => r.Order).Order(StringComparer.Ordinal)];

    private static List<string> Sorted(IEnumerable<string> ids) => [.. ids.Order(StringComparer.Ordinal)];

    [Fact]
    public async Task Overlap_and_barrier()
    {
        var handler = new ItemsHandler { List = Paged(Ids(1, 6), Ids(7, 10)) };
        var partition = Partition(Options(4, SizedPages(6)), handler);

        var read = Task.Run(() => Drain(partition));
        await handler.Arrived("o1", "o2", "o3", "o4");
        Assert.Equal(4, handler.InFlight);
        handler.OpenAll();
        var log = await read.WaitAsync(Bound);

        Assert.Equal(4, handler.MaxInFlight);
        Assert.Equal(Sorted(Ids(1, 10)), Orders(log));
        Assert.Equal(Sorted(Ids(1, 10).Select(id => $"{id}-a")), Sorted(log.Rows.Select(r => r.Sku)));
        // The second list page is requested only once every item of the first has been answered.
        Assert.Equal(["?page=1", "?page=2"], handler.ListRequests.Select(r => r.Query));
        Assert.Equal((6, 0), (handler.ListRequests[1].Completed, handler.ListRequests[1].InFlight));
        // Items are handed out in record order.
        Assert.Equal(Ids(1, 6), handler.Requests.Take(6).Order(StringComparer.Ordinal));
        Assert.Equal(["o1", "o2", "o3", "o4"], handler.Requests.Take(4).Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task Checkpoint_only_at_barrier_and_resume()
    {
        var twoLines = (string id) => (200, $$"""{"lines":[{"sku":"{{id}}-a"},{"sku":"{{id}}-b"}]}""", "application/json");
        var handler = new ItemsHandler { List = Paged(Ids(1, 6), Ids(7, 10)), Item = twoLines }.OpenAll();

        var log = await Drain(Partition(Options(3, SizedPages(6)), handler));

        Assert.Equal(20, log.Rows.Count);
        var (rowsBefore, token) = Assert.Single(log.Checkpoints);
        // Offered with the first page's last batch: exactly the first page's rows are behind it.
        Assert.Equal(12, rowsBefore);
        Assert.Equal(Sorted(Ids(1, 6).SelectMany(id => new[] { id, id })),
            Sorted(log.Rows.Take(12).Select(r => r.Order)));
        var barrier = Decode(token);
        Assert.Equal("?page=2", ListQuery(barrier));
        Assert.Equal((0, null, null, 0, null), (barrier.Index, barrier.Item, barrier.Anchor, barrier.Skipped,
            barrier.FirstSkipped));

        var resumed = new ItemsHandler { List = Paged(Ids(1, 6), Ids(7, 10)), Item = twoLines }.OpenAll();
        var partition = Partition(Options(3, SizedPages(6)), resumed);
        Assert.True(partition.TryResumeFrom(token));
        var second = await Drain(partition);

        Assert.Equal(["?page=2"], resumed.ListRequests.Select(r => r.Query));
        Assert.Equal(Sorted(Ids(7, 10)), Sorted(resumed.Requests));
        Assert.Equal(Sorted(Ids(7, 10).SelectMany(id => new[] { $"{id}-a", $"{id}-b" })),
            Sorted(second.Rows.Select(r => r.Sku)));
        Assert.Empty(second.Checkpoints);
    }

    [Fact]
    public async Task All_items_skipped_page()
    {
        // Page 1's items are all gone; page 2's are there; page 3 is short, so the last.
        var handler = new ItemsHandler
        {
            List = Paged(["o1", "o2"], ["o3", "o4"], ["o5"]),
            Item = id => id is "o1" or "o2"
                ? (404, """{"error":"gone"}""", "application/json")
                : (200, $$"""{"lines":[{"sku":"{{id}}-a"}]}""", "application/json"),
        }.OpenAll();
        var options = Options(2, SizedPages(2), Map(("on_missing", "skip")));

        var log = await Drain(Partition(options, handler));

        Assert.Equal(["o3", "o4", "o5"], Orders(log));
        // Page 1 yields nothing, so its barrier has no batch to ride on; the next barrier, offered with
        // page 2's last batch, carries page 1's skips. Nothing is offered mid-page.
        var (rowsBefore, token) = Assert.Single(log.Checkpoints);
        Assert.Equal(2, rowsBefore);
        var barrier = Decode(token);
        Assert.Equal("?page=3", ListQuery(barrier));
        Assert.Equal((0, null, 2, Identity("o1")), (barrier.Index, barrier.Item, barrier.Skipped, barrier.FirstSkipped));
        Assert.Contains($"skipped 2 items that returned 404/410 (first: {Identity("o1")})", Assert.Single(_notices));

        _notices.Clear();
        var resumed = new ItemsHandler { List = handler.List, Item = handler.Item }.OpenAll();
        var partition = Partition(options, resumed);
        Assert.True(partition.TryResumeFrom(token));
        var second = await Drain(partition);

        Assert.Equal(["o5"], Orders(second));
        Assert.Contains($"skipped 2 items that returned 404/410 (first: {Identity("o1")})", Assert.Single(_notices));
    }

    /// <summary>Holds item failures in the gate: the first one caught waits for the second, and the second
    /// waits until the stub has seen the other items cancelled, so the higher record fails first and the
    /// lower one still completes its failure after the cancellation.</summary>
    private sealed class HoldingGate(ItemsHandler handler) : IOperationGate
    {
        private readonly TaskCompletionSource _second = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _failures;

        public async Task<T> ExecuteAsync<T>(string opLabel, bool idempotent, Func<CancellationToken, Task<T>> op,
            CancellationToken ct)
        {
            try
            {
                return await op(ct);
            }
            catch (PzConnectorException)
            {
                if (Interlocked.Increment(ref _failures) == 1)
                {
                    await _second.Task.WaitAsync(Bound);
                }
                else
                {
                    _second.TrySetResult();
                    await handler.CancelledEnough;
                }

                throw;
            }
        }

        public void ReportBudget(int remaining, DateTimeOffset resetAt)
        {
        }
    }

    [Fact]
    public async Task First_failure_in_record_order_cancels()
    {
        // Records 2 (o3) and 5 (o6) fail; o6 fails first, then o3 once o4 and o5 have been cancelled.
        var handler = new ItemsHandler
        {
            List = Paged(Ids(1, 6)),
            Item = id => id is "o3" or "o6"
                ? (500, """{"error":"boom"}""", "application/json")
                : (200, $$"""{"lines":[{"sku":"{{id}}-a"}]}""", "application/json"),
            CancelledTarget = 2,
        };
        var partition = Partition(Options(4), handler, new HoldingGate(handler));
        handler.Open("o1", "o2");

        var read = Task.Run(() => Drain(partition));
        await handler.Arrived("o3", "o4", "o5", "o6");
        handler.Open("o6");
        handler.Open("o3");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => read.WaitAsync(Bound));

        Assert.EndsWith($"({Identity("o3")})", ex.Message);
        Assert.DoesNotContain(Identity("o6"), ex.Message);
        Assert.True(ex.IsTransient);
        Assert.Equal(["o4", "o5"], Sorted(handler.Cancelled));
    }

    /// <summary>Holds a timed-out request's failure until its item has been stopped, so it is recorded
    /// only after a higher record's failure has cancelled the page. <see cref="TimedOut"/> completes once
    /// the timeout has been caught, before the stop.</summary>
    private sealed class TimeoutAfterStopGate : IOperationGate
    {
        private readonly TaskCompletionSource _timedOut = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task TimedOut => _timedOut.Task.WaitAsync(Bound);

        public async Task<T> ExecuteAsync<T>(string opLabel, bool idempotent, Func<CancellationToken, Task<T>> op,
            CancellationToken ct)
        {
            try
            {
                return await op(ct);
            }
            catch (PzConnectorException ex) when (ex.InnerException is TaskCanceledException)
            {
                _timedOut.TrySetResult();
                var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                await using (ct.Register(() => stopped.TrySetResult()))
                {
                    await stopped.Task.WaitAsync(Bound);
                }

                throw;
            }
        }

        public void ReportBudget(int remaining, DateTimeOffset resetAt)
        {
        }
    }

    [Fact]
    public async Task Lower_record_timeout_after_stop_is_reported()
    {
        // o2 (record 1) fails first and stops the page; o1 (record 0) then fails with a timeout, a transient
        // PzConnectorException wrapping a TaskCanceledException. That is o1's own failure, not the stop.
        // o2 is answered only once o1 has timed out: answered earlier, the stop would reach o1's request
        // while it is still waiting, a bare cancellation that is rightly no failure of o1's.
        var handler = new ItemsHandler
        {
            List = Paged(Ids(1, 2)),
            Item = id => id == "o2"
                ? (500, """{"error":"boom"}""", "application/json")
                : (200, $$"""{"lines":[{"sku":"{{id}}-a"}]}""", "application/json"),
            TimesOut = id => id == "o1",
        };
        var gate = new TimeoutAfterStopGate();
        var partition = Partition(Options(2), handler, gate);

        var read = Task.Run(() => Drain(partition));
        await handler.Arrived("o1", "o2");
        handler.Open("o1");
        await gate.TimedOut;
        handler.Open("o2");
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => read.WaitAsync(Bound));

        Assert.Contains("timed out", ex.Message);
        Assert.EndsWith($"({Identity("o1")})", ex.Message);
        Assert.True(ex.IsTransient);
    }

    [Fact]
    public async Task Skip_notice_record_order()
    {
        // o5 is skipped first; o2, earlier in the list, is answered only after that.
        var handler = new ItemsHandler
        {
            List = Paged(Ids(1, 5)),
            Item = id => id is "o2" or "o5"
                ? (404, """{"error":"gone"}""", "application/json")
                : (200, $$"""{"lines":[{"sku":"{{id}}-a"}]}""", "application/json"),
        };
        var partition = Partition(Options(3, forEachExtra: Map(("on_missing", "skip"))), handler);
        handler.Open("o1", "o3", "o4", "o5");

        var read = Task.Run(() => Drain(partition));
        await handler.Arrived("o5");
        handler.Open("o2");
        var log = await read.WaitAsync(Bound);

        Assert.Equal(["o1", "o3", "o4"], Orders(log));
        Assert.Equal($"http source 's.d': skipped 2 items that returned 404/410 (first: {Identity("o2")})",
            Assert.Single(_notices));
    }

    [Fact]
    public async Task Max_items_first_records()
    {
        // The cap falls inside the page: the first four records only, and no further list page.
        var handler = new ItemsHandler { List = Paged(Ids(1, 6), Ids(7, 12)) }.OpenAll();

        var log = await Drain(Partition(Options(3, SizedPages(6), Map(("max_items", 4))), handler));

        Assert.Equal(Ids(1, 4), Orders(log));
        Assert.Equal(Ids(1, 4), Sorted(handler.Requests));
        Assert.Equal(["?page=1"], handler.ListRequests.Select(r => r.Query));
        Assert.Equal("http source 's.d': stopped after max_items (4) items; more list records were available",
            Assert.Single(_notices));
        Assert.Empty(log.Checkpoints);

        // The cap falls on the page's last record: no probe of the next list page.
        _notices.Clear();
        var exact = new ItemsHandler { List = Paged(Ids(1, 6), Ids(7, 12)) }.OpenAll();

        var second = await Drain(Partition(Options(3, SizedPages(6), Map(("max_items", 6))), exact));

        Assert.Equal(Ids(1, 6), Orders(second));
        Assert.Equal(["?page=1"], exact.ListRequests.Select(r => r.Query));
        Assert.Equal("http source 's.d': stopped after max_items (6) items; more list records may be available",
            Assert.Single(_notices));
    }

    private static (int, string, string) Csv(string id) => (200, $"sku\n{id}-a\n{id}-b\n", "text/csv");

    private string[] ScratchDirs() => Directory.GetDirectories(_scratchRoot);

    [Fact]
    public async Task File_items_parallel_scratch_cleanup()
    {
        var handler = new ItemsHandler { List = Paged(Ids(1, 4)), Item = Csv };
        var partition = Partition(Options(3, file: true), handler);

        var read = Task.Run(() => Drain(partition));
        await handler.Arrived("o1", "o2", "o3");
        // Each worker's download has its own scratch directory.
        Assert.Equal(3, ScratchDirs().Length);
        handler.OpenAll();
        var log = await read.WaitAsync(Bound);

        Assert.Equal(Sorted(Ids(1, 4).SelectMany(id => new[] { $"{id}-a", $"{id}-b" })),
            Sorted(log.Rows.Select(r => r.Sku)));
        Assert.Equal(3, handler.MaxInFlight);
        Assert.Empty(ScratchDirs());
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
    public async Task Cancel_disposes_queued_batches()
    {
        // JSON items of many rows each: the workers fill the channel while the consumer holds one batch,
        // then the consumer stops early.
        var lines = (string id) => (200,
            new JsonObject { ["lines"] = new JsonArray([.. Enumerable.Range(0, 20).Select(i => (JsonNode)new JsonObject { ["sku"] = $"{id}-{i}" })]) }
                .ToJsonString(), "application/json");
        var handler = new ItemsHandler { List = Paged(Ids(1, 8)), Item = lines }.OpenAll();
        var allocator = new CountingAllocator();
        var partition = Partition(Options(4), handler);
        partition.Allocator = allocator;

        await using (var batches = partition.ReadAsync(OneRowBatches, CancellationToken.None).GetAsyncEnumerator())
        {
            Assert.True(await batches.MoveNextAsync().AsTask().WaitAsync(Bound));
            batches.Current.Dispose();
            Assert.True(await batches.MoveNextAsync().AsTask().WaitAsync(Bound));
            batches.Current.Dispose();
        }

        Assert.True(allocator.Allocated > 2);
        Assert.Equal(0, allocator.Outstanding);

        // Cancelled while every worker is mid-download: no scratch directory is left behind.
        var files = new ItemsHandler { List = Paged(Ids(1, 6)), Item = Csv, CancelledTarget = 3 };
        var download = Partition(Options(3, file: true), files);
        using var cts = new CancellationTokenSource();

        var read = Task.Run(() => Drain(download, cts.Token));
        await files.Arrived("o1", "o2", "o3");
        Assert.Equal(3, ScratchDirs().Length);
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => read.WaitAsync(Bound));
        await files.CancelledEnough;
        Assert.Equal(["o1", "o2", "o3"], Sorted(files.Requests));
        Assert.Empty(ScratchDirs());
    }

    [Fact]
    public async Task Fewer_records_than_workers_and_empty_page()
    {
        // A cursor list (where an empty page is a gap, not the end): two records, an empty page, one record.
        var handler = new ItemsHandler
        {
            List = url => System.Web.HttpUtility.ParseQueryString(url.Query)["cursor"] switch
            {
                null => Records(["o1", "o2"], next: "c2"),
                "c2" => Records([], next: "c3"),
                _ => Records(["o3"]),
            },
        }.OpenAll();
        var options = Options(8, Map(("strategy", "cursor"), ("pointer", "/next"), ("param", "cursor")));

        var log = await Drain(Partition(options, handler));

        Assert.Equal(["o1", "o2", "o3"], Orders(log));
        Assert.Equal(["", "?cursor=c2", "?cursor=c3"], handler.ListRequests.Select(r => r.Query));
        Assert.True(handler.MaxInFlight <= 2);
        // The first page's barrier rides on its last batch; the empty page yields none to carry its own,
        // and the last page needs none.
        var (rowsBefore, token) = Assert.Single(log.Checkpoints);
        Assert.Equal(2, rowsBefore);
        Assert.Equal("?cursor=c2", ListQuery(Decode(token)));
    }
}
