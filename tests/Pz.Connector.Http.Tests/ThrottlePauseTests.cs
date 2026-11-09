using System.Globalization;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

/// <summary>The partition-wide pause a 429/503 <c>Retry-After</c> sets: every later request of the read
/// (its retry, the list's next page, sibling items) waits it out on the injected clock.</summary>
public sealed class ThrottlePauseTests : IAsyncLifetime
{
    private readonly StubHttpServer _server = new();

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync() => await _server.DisposeAsync();

    /// <summary>Retries a transient failure once, waiting its <c>Retry-After</c> on <paramref name="time"/>
    /// first when <paramref name="honourRetryAfter"/> (the engine gate's behaviour), or straight away
    /// otherwise (so only the shared pause can hold the retry back).</summary>
    private sealed class RetryOnceGate(TimeProvider time, bool honourRetryAfter) : IOperationGate
    {
        public async Task<T> ExecuteAsync<T>(string opLabel, bool idempotent, Func<CancellationToken, Task<T>> op,
            CancellationToken ct)
        {
            try
            {
                return await op(ct);
            }
            catch (PzConnectorException ex) when (ex.IsTransient)
            {
                if (honourRetryAfter && ex.RetryAfter is { } wait)
                {
                    await Task.Delay(wait, time, ct);
                }

                return await op(ct);
            }
        }

        public void ReportBudget(int remaining, DateTimeOffset resetAt)
        {
        }
    }

    private static Dictionary<string, object?> Map(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    private async Task<(long Rows, List<(string Path, DateTimeOffset At)> Requests)> ReadAsync(
        Dictionary<string, object?> options, ManualClock clock, IOperationGate gate,
        List<(string Path, DateTimeOffset At)> requests)
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(
            new ConnectorConfig(new Dictionary<string, object?> { ["base_url"] = _server.BaseUrl.ToString() }), errors);
        Assert.Empty(errors);
        await using var source = new HttpSource(connection!, clock);
        source.UseOperationGate(gate);
        var partitions = await source.PlanReadAsync(new DatasetSpec("s", "d", options), ReadHints.None,
            CancellationToken.None);
        long rows = 0;
        await foreach (var batch in Assert.Single(partitions).ReadAsync(new BatchOptions(), CancellationToken.None))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        return (rows, requests);
    }

    private static string Stamp(StubRequest request) => request.Url.PathAndQuery;

    /// <summary>The next timer's due delay; a bound, not a sleep, so a timer that never comes fails the
    /// test instead of hanging it.</summary>
    private static Task<TimeSpan> NextTimer(ManualClock clock)
        => clock.NextTimerAsync().WaitAsync(TimeSpan.FromSeconds(30));

    [Fact]
    public async Task Note_then_wait_blocks_until_time()
    {
        var clock = new ManualClock();
        var pause = new ThrottlePause(clock);

        pause.Note(TimeSpan.FromSeconds(5));
        var wait = pause.WaitAsync(CancellationToken.None);

        Assert.Equal(TimeSpan.FromSeconds(5), await NextTimer(clock));
        Assert.False(wait.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(4));
        Assert.False(wait.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(1));
        await wait;
        Assert.True(pause.WaitAsync(CancellationToken.None).IsCompletedSuccessfully);
    }

    [Fact]
    public async Task Max_of_hints_and_cap()
    {
        var clock = new ManualClock();
        var pause = new ThrottlePause(clock);
        pause.Note(TimeSpan.FromSeconds(2));
        pause.Note(TimeSpan.FromSeconds(10));
        pause.Note(TimeSpan.FromSeconds(3));
        _ = pause.WaitAsync(CancellationToken.None);
        Assert.Equal(TimeSpan.FromSeconds(10), await NextTimer(clock));

        var capped = new ThrottlePause(clock);
        capped.Note(TimeSpan.FromHours(2));
        _ = capped.WaitAsync(CancellationToken.None);
        Assert.Equal(TimeSpan.FromSeconds(600), await NextTimer(clock));
    }

    [Fact]
    public async Task Wait_is_cancellable()
    {
        var clock = new ManualClock();
        var pause = new ThrottlePause(clock);
        pause.Note(TimeSpan.FromSeconds(30));
        using var cts = new CancellationTokenSource();
        var wait = pause.WaitAsync(cts.Token);
        await NextTimer(clock);

        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => wait);
    }

    [Fact]
    public async Task Past_retry_after_no_pause()
    {
        var clock = new ManualClock();
        clock.Advance(DateTimeOffset.Parse("2026-11-09T12:00:00Z", CultureInfo.InvariantCulture) - clock.GetUtcNow());
        var requests = new List<(string Path, DateTimeOffset At)>();
        var hits = 0;
        _server.Map("/items", req =>
        {
            requests.Add((Stamp(req), clock.GetUtcNow()));
            return ++hits == 1
                ? new StubResponse(429, "slow down",
                    new Dictionary<string, string> { ["Retry-After"] = "Mon, 09 Nov 2026 11:59:00 GMT" })
                : new StubResponse(200, """[{"id":1}]""");
        });

        // The gate honours Retry-After too: a past date parses to zero, so neither it nor the pause may
        // start a timer, and a stalled read would never finish on a clock nobody advances.
        var (rows, _) = await ReadAsync(Map(("path", "/items")), clock, new RetryOnceGate(clock, honourRetryAfter: true),
            requests).WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(1, rows);
        Assert.Equal(2, requests.Count);
        Assert.All(requests, r => Assert.Equal(clock.GetUtcNow(), r.At));

        var pause = new ThrottlePause(clock);
        pause.Note(TimeSpan.Zero);
        Assert.True(pause.WaitAsync(CancellationToken.None).IsCompletedSuccessfully);
    }

    [Fact]
    public async Task Sibling_requests_wait_after_429()
    {
        var clock = new ManualClock();
        var start = clock.GetUtcNow();
        var requests = new List<(string Path, DateTimeOffset At)>();
        var o1Hits = 0;
        _server.Map("/orders", req =>
        {
            requests.Add((Stamp(req), clock.GetUtcNow()));
            return req.Url.Query.Contains("page=2")
                ? new StubResponse(200, """{"data":[{"id":"o2"}]}""")
                : new StubResponse(200, """{"data":[{"id":"o1"}]}""", new Dictionary<string, string>
                    { ["Link"] = $"<{_server.BaseUrl}orders?page=2>; rel=\"next\"" });
        });
        _server.MapPrefix("/orders/", req =>
        {
            requests.Add((Stamp(req), clock.GetUtcNow()));
            return req.Url.AbsolutePath == "/orders/o1" && ++o1Hits == 1
                ? new StubResponse(429, "slow down", new Dictionary<string, string> { ["Retry-After"] = "5" })
                : new StubResponse(200, """{"lines":[{"n":1}]}""");
        });

        var read = ReadAsync(Map(
            ("for_each", Map(("path", "/orders"), ("items", "/data"), ("fields", Map(("id", "/id"))),
                ("pagination", Map(("strategy", "link_header"))))),
            ("path", "/orders/{{ item.id }}"),
            ("items", "/lines")), clock, new RetryOnceGate(clock, honourRetryAfter: false), requests);

        // The gate retries at once; only the pause the 429 set holds that retry back.
        Assert.Equal(TimeSpan.FromSeconds(5), await NextTimer(clock));
        Assert.Equal(["/orders", "/orders/o1"], requests.Select(r => r.Path));
        clock.Advance(TimeSpan.FromSeconds(5));
        var (rows, _) = await read.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(2, rows);
        Assert.Equal(["/orders", "/orders/o1", "/orders/o1", "/orders?page=2", "/orders/o2"],
            requests.Select(r => r.Path));
        Assert.All(requests.Take(2), r => Assert.Equal(start, r.At));
        Assert.All(requests.Skip(2), r => Assert.Equal(start + TimeSpan.FromSeconds(5), r.At));
    }

    [Fact]
    public async Task Concurrency_one_unchanged()
    {
        var clock = new ManualClock();
        var requests = new List<(string Path, DateTimeOffset At)>();
        var page2Hits = 0;
        _server.Map("/items", req =>
        {
            requests.Add((Stamp(req), clock.GetUtcNow()));
            if (req.Url.Query.Contains("page=2"))
            {
                return ++page2Hits == 1
                    ? new StubResponse(429, "slow down", new Dictionary<string, string> { ["Retry-After"] = "3" })
                    : new StubResponse(200, """[{"id":2}]""");
            }

            return new StubResponse(200, """[{"id":1}]""", new Dictionary<string, string>
                { ["Link"] = $"<{_server.BaseUrl}items?page=2>; rel=\"next\"" });
        });

        var read = ReadAsync(Map(("path", "/items"), ("pagination", Map(("strategy", "link_header")))), clock,
            new RetryOnceGate(clock, honourRetryAfter: true), requests);

        // Only the gate's own Retry-After wait starts a timer; once it has elapsed, so has the pause.
        Assert.Equal(TimeSpan.FromSeconds(3), await NextTimer(clock));
        clock.Advance(TimeSpan.FromSeconds(3));
        var (rows, _) = await read.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(2, rows);
        Assert.Equal(["/items", "/items?page=2", "/items?page=2"], requests.Select(r => r.Path));
        Assert.False(clock.NextTimerAsync().IsCompleted);
    }
}
