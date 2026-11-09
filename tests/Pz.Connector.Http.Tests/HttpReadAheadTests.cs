using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json.Nodes;
using Apache.Arrow;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

/// <summary>Lowering <see cref="HttpPartition.UnboundedPageCeiling"/> is process-wide, so the read-ahead
/// tests run alone rather than alongside other classes' crawls.</summary>
[CollectionDefinition(nameof(ReadAheadCollection), DisableParallelization = true)]
public sealed class ReadAheadCollection;

/// <summary><c>pagination.concurrency</c> on <c>strategy: page</c>: up to N page requests in flight,
/// pages landing in page order, the crawl's stop and guard decisions made on consumption, and nothing
/// fetched ahead past the real end ever landing or failing the read. Each request blocks in the stub
/// on a per-page gate the test releases, so overlap is counted by the stub, never timed.</summary>
[Collection(nameof(ReadAheadCollection))]
public sealed class HttpReadAheadTests
{
    private static readonly Uri BaseUrl = new("https://api.example.com/");

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    /// <summary>Serves pages by number (the <c>page</c> query param, or <c>/page</c> of a JSON body).
    /// Every request waits on its page's gate; the gates exist before the requests do, so a release
    /// that comes first simply lets the request through.</summary>
    private sealed class PagedHandler(Func<int, (int Status, string Body)> respond) : HttpMessageHandler
    {
        private readonly ConcurrentDictionary<int, TaskCompletionSource> _gates = new();
        private readonly ConcurrentDictionary<int, TaskCompletionSource> _arrivals = new();
        private readonly ConcurrentQueue<(int Page, string? Body)> _requests = new();
        private int _inFlight;
        private int _maxInFlight;

        public int MaxInFlight => Volatile.Read(ref _maxInFlight);

        public int InFlight => Volatile.Read(ref _inFlight);

        /// <summary>Every 200 carries a rate-limit budget hint.</summary>
        public bool BudgetHeaders { get; init; }

        public IReadOnlyList<int> Pages => [.. _requests.Select(r => r.Page)];

        public IReadOnlyList<string?> Bodies => [.. _requests.Select(r => r.Body)];

        public void Release(params int[] pages)
        {
            foreach (var page in pages)
            {
                Gate(_gates, page).TrySetResult();
            }
        }

        public void ReleaseAll() => Release([.. Enumerable.Range(0, 64)]);

        /// <summary>Completes once the request for <paramref name="page"/> has reached the stub.</summary>
        public Task Arrived(int page) => Gate(_arrivals, page).Task.WaitAsync(Bound);

        private static TaskCompletionSource Gate(ConcurrentDictionary<int, TaskCompletionSource> map, int page)
            => map.GetOrAdd(page, _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            var body = request.Content is null ? null : await request.Content.ReadAsStringAsync(cancellationToken);
            var page = PageOf(request.RequestUri!, body);
            _requests.Enqueue((page, body));
            var now = Interlocked.Increment(ref _inFlight);
            try
            {
                int seen;
                while (now > (seen = Volatile.Read(ref _maxInFlight))
                       && Interlocked.CompareExchange(ref _maxInFlight, now, seen) != seen)
                {
                }

                Gate(_arrivals, page).TrySetResult();
                await Gate(_gates, page).Task.WaitAsync(cancellationToken);
                var (status, text) = respond(page);
                var response = new HttpResponseMessage((HttpStatusCode)status)
                {
                    Content = new StringContent(text, Encoding.UTF8, "application/json"),
                    RequestMessage = request,
                };
                if (BudgetHeaders && status == 200)
                {
                    response.Headers.Add("X-RateLimit-Remaining", "100");
                    response.Headers.Add("X-RateLimit-Reset", "60");
                }

                return response;
            }
            finally
            {
                Interlocked.Decrement(ref _inFlight);
            }
        }

        private static int PageOf(Uri url, string? body)
        {
            if (body is not null && JsonNode.Parse(body)?["page"] is { } inBody)
            {
                return inBody.GetValue<int>();
            }

            foreach (var pair in url.Query.TrimStart('?').Split('&'))
            {
                if (pair.StartsWith("page=", StringComparison.Ordinal))
                {
                    return int.Parse(pair["page=".Length..]);
                }
            }

            throw new InvalidOperationException($"no page number in {url}");
        }
    }

    /// <summary>Two rows per full page: ids page*10+1 and page*10+2.</summary>
    private static (int, string) Full(int page) => (200, $$"""[{"id":{{page * 10 + 1}}},{"id":{{page * 10 + 2}}}]""");

    private static (int, string) Empty => (200, "[]");

    private static (int, string) ServerError => (500, """{"error":"boom"}""");

    private static long[] Ids(params int[] pages) => [.. pages.SelectMany(p => new long[] { p * 10 + 1, p * 10 + 2 })];

    private static Dictionary<string, object?> Options(int? concurrency, Action<Dictionary<string, object?>>? pagination = null,
        int? maxPages = null)
    {
        var block = new Dictionary<string, object?> { ["strategy"] = "page" };
        if (concurrency is { } n)
        {
            block["concurrency"] = n;
        }

        pagination?.Invoke(block);
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/items",
            ["pagination"] = block,
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
        };
        if (maxPages is { } cap)
        {
            options["max_pages"] = cap;
        }

        return options;
    }

    /// <summary>Passes every op straight through, counting finished ops and budget hints.</summary>
    private sealed class CountingGate : IOperationGate
    {
        private readonly SemaphoreSlim _finished = new(0);
        private int _reported;

        public int Reported => Volatile.Read(ref _reported);

        public async Task<T> ExecuteAsync<T>(string opLabel, bool idempotent, Func<CancellationToken, Task<T>> op,
            CancellationToken ct)
        {
            try
            {
                return await op(ct);
            }
            finally
            {
                _finished.Release();
            }
        }

        public void ReportBudget(int remaining, DateTimeOffset resetAt) => Interlocked.Increment(ref _reported);

        /// <summary>Completes once <paramref name="count"/> more ops have finished.</summary>
        public async Task Finished(int count)
        {
            for (var i = 0; i < count; i++)
            {
                Assert.True(await _finished.WaitAsync(Bound), "an op never finished");
            }
        }
    }

    private static HttpPartition Partition(Dictionary<string, object?> options, PagedHandler handler,
        out HttpClient client, IOperationGate? gate = null)
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = BaseUrl.ToString(),
        }), errors)!;
        Assert.Empty(errors);
        var spec = new DatasetSpec("s", "d", options);
        var config = HttpDatasetConfig.Parse(spec);
        client = new HttpClient(handler) { BaseAddress = connection.BaseUrl };
        // As HttpSource plans it: a pause only for a read that can have requests in flight together.
        var pause = config.PageConcurrency > 1 ? new ThrottlePause(TimeProvider.System) : null;
        return new HttpPartition(client, client, connection, config, spec, TimeProvider.System, gate, pause);
    }

    private sealed class ReadLog
    {
        public List<long> Ids { get; } = [];

        public List<string> Checkpoints { get; } = [];
    }

    /// <summary>Drains the read, recording ids and every checkpoint offered after each batch.</summary>
    private static async Task<ReadLog> ReadAsync(HttpPartition partition, ReadLog? log = null)
    {
        log ??= new ReadLog();
        await foreach (var batch in partition.ReadAsync(new BatchOptions(), CancellationToken.None))
        {
            using (batch)
            {
                var ids = (Int64Array)batch.Column(0);
                for (var i = 0; i < batch.Length; i++)
                {
                    log.Ids.Add(ids.GetValue(i)!.Value);
                }
            }

            if (partition.TryGetCheckpoint(out var token))
            {
                log.Checkpoints.Add(token!);
            }
        }

        return log;
    }

    /// <summary>Waits until pages 1..<paramref name="n"/> are all in flight together, then lets every
    /// page through.</summary>
    private static async Task InFlight(PagedHandler handler, int n)
    {
        await Task.WhenAll(Enumerable.Range(1, n).Select(handler.Arrived));
        Assert.Equal(n, handler.MaxInFlight);
        handler.ReleaseAll();
    }

    [Fact]
    public async Task Pages_land_in_order_with_bounded_overlap()
    {
        var handler = new PagedHandler(p => p <= 5 ? Full(p) : Empty);
        var partition = Partition(Options(3), handler, out var client);
        using var _ = client;

        var read = ReadAsync(partition);
        // Three requests in flight before any page is answered...
        await Task.WhenAll(handler.Arrived(1), handler.Arrived(2), handler.Arrived(3));
        // ...answered out of order: the rows still land in page order.
        handler.Release(3);
        handler.Release(2);
        handler.Release(1);
        handler.ReleaseAll();
        var log = await read.WaitAsync(Bound);

        Assert.Equal(Ids(1, 2, 3, 4, 5), log.Ids);
        Assert.Equal(3, handler.MaxInFlight);
        var pages = handler.Pages.Order().ToArray();
        Assert.Equal([1, 2, 3, 4, 5, 6], pages.Take(6));
        // At most N-1 requests past the empty page 6, all discarded.
        Assert.All(pages.Skip(6), p => Assert.InRange(p, 7, 8));
    }

    [Fact]
    public async Task Exactly_full_last_page_then_empty()
    {
        // Pages 1-3 exactly full (size 2, stop_on_short_page), page 4 empty; anything fetched past it fails.
        var handler = new PagedHandler(p => p <= 3 ? Full(p) : p == 4 ? Empty : ServerError);
        var partition = Partition(Options(3, b =>
        {
            b["size"] = 2;
            b["size_param"] = "per_page";
            b["stop_on_short_page"] = true;
        }), handler, out var client);
        using var _ = client;

        var read = ReadAsync(partition);
        handler.Release(1, 2, 3);
        // Page 5 (past the real end) is issued and fails before page 4 is answered.
        await handler.Arrived(5);
        handler.Release(5);
        handler.Release(4);
        var log = await read.WaitAsync(Bound);

        Assert.Equal(Ids(1, 2, 3), log.Ids);
        Assert.DoesNotContain(handler.Pages, p => p > 6);
    }

    [Fact]
    public async Task Checkpoints_match_sequential()
    {
        async Task<ReadLog> Run(int? concurrency)
        {
            var handler = new PagedHandler(p => p <= 5 ? Full(p) : Empty);
            var options = Options(concurrency, b =>
            {
                b["size"] = 2;
                b["size_param"] = "per_page";
            });
            // A static param needing escaping, ahead of the paging params: the rebuilt query must be the
            // same string the page strategy produces.
            options["query"] = new Dictionary<string, object?> { ["filter"] = "a b/c&d" };
            var partition = Partition(options, handler, out var client);
            using var _ = client;
            var read = ReadAsync(partition);
            await InFlight(handler, concurrency ?? 1);
            return await read.WaitAsync(Bound);
        }

        var sequential = await Run(null);
        var readAhead = await Run(3);

        Assert.Equal(5, sequential.Checkpoints.Count);
        Assert.All(sequential.Checkpoints, t => Assert.Contains("per_page", t));
        Assert.Equal(sequential.Checkpoints, readAhead.Checkpoints);
        Assert.Equal(sequential.Ids, readAhead.Ids);
    }

    [Fact]
    public async Task Max_pages_never_issued_past()
    {
        var handler = new PagedHandler(Full);
        var partition = Partition(Options(3, maxPages: 4), handler, out var client);
        using var _ = client;

        var read = ReadAsync(partition);
        await InFlight(handler, 3);
        var log = await read.WaitAsync(Bound);

        Assert.Equal(Ids(1, 2, 3, 4), log.Ids);
        Assert.Equal([1, 2, 3, 4], handler.Pages.Order());
        Assert.True(partition.TruncatedWithMore);
    }

    [Fact]
    public async Task Speculative_error_past_end_ignored()
    {
        var handler = new PagedHandler(p => p <= 5 ? Full(p) : p == 6 ? Empty : ServerError);
        var partition = Partition(Options(3), handler, out var client);
        using var _ = client;

        var read = ReadAsync(partition);
        handler.Release(1, 2, 3, 4, 5, 7);
        // Page 7 is in flight (and answered 500) while page 6 is still held; page 8 is never answered.
        await handler.Arrived(7);
        handler.Release(6);
        var log = await read.WaitAsync(Bound);

        Assert.Equal(Ids(1, 2, 3, 4, 5), log.Ids);
    }

    [Fact]
    public async Task Error_surfaces_in_order()
    {
        var handler = new PagedHandler(p => p == 3 ? ServerError : Full(p));
        var partition = Partition(Options(3), handler, out var client);
        using var _ = client;
        var log = new ReadLog();

        var read = ReadAsync(partition, log);
        // Page 3 fails first; pages 1 and 2 still land before its error surfaces.
        handler.Release(3);
        await handler.Arrived(1);
        await handler.Arrived(2);
        handler.Release(2, 1);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => read.WaitAsync(Bound));

        Assert.Contains("500", ex.Message);
        Assert.Equal(Ids(1, 2), log.Ids);
        Assert.DoesNotContain(handler.Pages, p => p > 5);
    }

    [Fact]
    public async Task Held_guard_error_dropped_if_crawl_ended()
    {
        var ceiling = HttpPartition.UnboundedPageCeiling;
        HttpPartition.UnboundedPageCeiling = 4;
        try
        {
            // Page 4 ends the crawl; a 5th request would trip the ceiling, but that slot is never reached.
            var ended = new PagedHandler(p => p <= 3 ? Full(p) : Empty);
            var partition = Partition(Options(3), ended, out var client);
            using (client)
            {
                var read = ReadAsync(partition);
                await InFlight(ended, 3);
                var log = await read.WaitAsync(Bound);
                Assert.Equal(Ids(1, 2, 3), log.Ids);
                Assert.Equal([1, 2, 3, 4], ended.Pages.Order());
            }

            // The same guard is raised when the consumer does reach it, after the pages before it landed.
            var endless = new PagedHandler(Full);
            var failing = Partition(Options(3), endless, out var client2);
            using (client2)
            {
                var log = new ReadLog();
                var read = ReadAsync(failing, log);
                await InFlight(endless, 3);
                var ex = await Assert.ThrowsAsync<PzConnectorException>(() => read.WaitAsync(Bound));
                Assert.Contains("did not terminate within 4 pages", ex.Message);
                Assert.Equal(Ids(1, 2, 3, 4), log.Ids);
                Assert.Equal([1, 2, 3, 4], endless.Pages.Order());
            }
        }
        finally
        {
            HttpPartition.UnboundedPageCeiling = ceiling;
        }
    }

    [Fact]
    public async Task Body_page_read_ahead()
    {
        var handler = new PagedHandler(p => p <= 4 ? Full(p) : Empty);
        var options = Options(2, b =>
        {
            b["location"] = "body";
            b["param"] = "/page";
            b["size"] = 2;
            b["size_param"] = "/size";
        });
        options["method"] = "post";
        options["body"] = new Dictionary<string, object?> { ["q"] = "x" };
        var partition = Partition(options, handler, out var client);
        using var _ = client;

        var read = ReadAsync(partition);
        await Task.WhenAll(handler.Arrived(1), handler.Arrived(2));
        handler.Release(2);
        handler.Release(1);
        handler.ReleaseAll();
        var log = await read.WaitAsync(Bound);

        Assert.Equal(Ids(1, 2, 3, 4), log.Ids);
        Assert.Equal(2, handler.MaxInFlight);
        Assert.Equal([1, 2, 3, 4, 5], handler.Pages.Order().Take(5));
        Assert.All(handler.Bodies, b => Assert.Contains("\"q\":\"x\"", b));
        Assert.All(handler.Bodies, b => Assert.Contains("\"size\":2", b));
        Assert.DoesNotContain(handler.Pages, p => p > 6);
    }

    [Fact]
    public async Task Budget_hint_only_for_consumed_pages()
    {
        var handler = new PagedHandler(p => p <= 5 ? Full(p) : Empty) { BudgetHeaders = true };
        var gate = new CountingGate();
        var partition = Partition(Options(3), handler, out var client, gate);
        using var _ = client;

        var read = ReadAsync(partition);
        handler.Release(1, 2, 3, 4, 5, 7);
        // Pages 1-5 and the speculative page 7 have all been fetched (hint parsed) while page 6 is held.
        await gate.Finished(6);
        Assert.InRange(gate.Reported, 0, 5);
        handler.Release(6);
        var log = await read.WaitAsync(Bound);

        Assert.Equal(Ids(1, 2, 3, 4, 5), log.Ids);
        Assert.Equal(6, gate.Reported);
    }

    [Fact]
    public async Task Cancelled_read_ends_every_issued_request()
    {
        // Earlier tests' garbage (other classes' stub listeners) is finalized first, and only this read's
        // own failure kinds count, so the hook sees nothing but what this read could leave behind.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        var unobserved = new ConcurrentQueue<Exception>();
        EventHandler<UnobservedTaskExceptionEventArgs> hook = (_, e) =>
        {
            if (e.Exception.InnerExceptions.Any(x => x is PzConnectorException or OperationCanceledException
                    or HttpRequestException))
            {
                unobserved.Enqueue(e.Exception);
            }
        };
        TaskScheduler.UnobservedTaskException += hook;
        try
        {
            var handler = new PagedHandler(Full);
            var partition = Partition(Options(3), handler, out var client);
            using var _ = client;
            using var cts = new CancellationTokenSource();
            var pages = partition.PagesAsync(cts.Token).GetAsyncEnumerator();
            try
            {
                var firstPage = pages.MoveNextAsync().AsTask();
                await Task.WhenAll(handler.Arrived(1), handler.Arrived(2), handler.Arrived(3));
                handler.Release(1);
                Assert.True(await firstPage.WaitAsync(Bound));

                // Pages 2 and 3 are still blocked in the stub when the read is cancelled.
                await cts.CancelAsync();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(
                    () => pages.MoveNextAsync().AsTask().WaitAsync(Bound));
            }
            finally
            {
                await pages.DisposeAsync();
            }

            // Every issued fetch has ended, and nothing was issued on the cancelled token.
            Assert.Equal(0, handler.InFlight);
            Assert.Equal([1, 2, 3], handler.Pages.Order());
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            Assert.Empty(unobserved);
        }
        finally
        {
            TaskScheduler.UnobservedTaskException -= hook;
        }
    }
}
