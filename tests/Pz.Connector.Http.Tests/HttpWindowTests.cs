using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Xunit;

namespace Pz.Connector.Http.Tests;

public sealed class HttpWindowTests : IAsyncLifetime
{
    private StubHttpServer _server = null!;

    public Task InitializeAsync()
    {
        _server = new StubHttpServer();
        return Task.CompletedTask;
    }

    public async Task DisposeAsync() => await _server.DisposeAsync();

    private ConnectorConfig Config => new(new Dictionary<string, object?>
    {
        ["base_url"] = _server.BaseUrl.ToString(),
    });

    // Contract mode so 'id' is column 0; bounds are stamped on the spec directly (planner bypassed).
    private static DatasetSpec WindowedSpec(string path, string? cursorType, string lower, string? upper) =>
        new("stub", "win", new Dictionary<string, object?>
        {
            ["path"] = path,
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "page" },
            ["columns"] = new Dictionary<string, string> { ["id"] = cursorType ?? "bigint" },
            ["query"] = new Dictionary<string, object?>
            {
                ["after"] = "{{ watermark }}",
                ["before"] = "{{ window_upper }}",
            },
        })
        {
            WatermarkCursor = "id",
            WatermarkValue = lower,
            WatermarkUpperBound = upper,
        };

    // A page-aware endpoint returning id rows in (after, before] on page 1, empty on page 2.
    private void MapFiltering(string path) => _server.Map(path, req =>
    {
        var q = req.Url.Query;
        if (q.Contains("page=") && int.Parse(q.Split("page=")[1].Split('&')[0]) > 1)
        {
            return new StubResponse(200, "[]");
        }

        long after = q.Contains("after=") ? long.Parse(q.Split("after=")[1].Split('&')[0]) : -1;
        long before = q.Contains("before=") ? long.Parse(q.Split("before=")[1].Split('&')[0]) : 10;
        var rows = Enumerable.Range(0, 11).Where(i => i > after && i <= before)
            .Select(i => $$"""{"id":{{i}}}""");
        return new StubResponse(200, "[" + string.Join(',', rows) + "]");
    });

    private async Task ReadAll(DatasetSpec spec)
    {
        var connector = new HttpConnector();
        await using var source = await connector.OpenAsync(Config, CancellationToken.None);
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(new BatchOptions(), CancellationToken.None))
            {
                batch.Dispose();
            }
        }
    }

    [Fact]
    public async Task Upper_bound_is_stamped_into_the_query()
    {
        MapFiltering("/win");
        await ReadAll(WindowedSpec("/win", cursorType: "bigint", lower: "3", upper: "7"));

        var first = _server.Requests[0];
        Assert.Contains("after=3", first.Url.Query);
        Assert.Contains("before=7", first.Url.Query);
    }

    [Fact]
    public async Task Upper_bound_param_is_omitted_when_not_windowed()
    {
        MapFiltering("/win");
        await ReadAll(WindowedSpec("/win", cursorType: "bigint", lower: "3", upper: null));

        var first = _server.Requests[0];
        Assert.Contains("after=3", first.Url.Query);
        Assert.DoesNotContain("before=", first.Url.Query);
    }

    [Fact]
    public async Task Timestamp_upper_bound_is_iso_formatted()
    {
        _server.Map("/ts", req => req.Url.Query.Contains("page=2")
            ? new StubResponse(200, "[]")
            : new StubResponse(200, """[{"id":"2024-06-01T00:00:00Z"}]"""));

        await ReadAll(WindowedSpec("/ts", cursorType: "timestamp",
            lower: "2024-01-01T00:00:00Z", upper: "2024-07-01T00:00:00Z"));

        var q = _server.Requests[0].Url.Query;
        Assert.Contains("before=2024-07-01T00%3A00%3A00Z", q);
    }

    [Fact]
    public async Task Fractional_timestamp_bounds_floor_lower_and_ceil_upper()
    {
        _server.Map("/ts-frac", req => req.Url.Query.Contains("page=2")
            ? new StubResponse(200, "[]")
            : new StubResponse(200, """[{"id":"2024-06-01T00:00:00Z"}]"""));

        await ReadAll(WindowedSpec("/ts-frac", cursorType: "timestamp",
            lower: "2024-01-01T00:00:00.500000Z", upper: "2024-07-01T00:00:00.500000Z"));

        var q = _server.Requests[0].Url.Query;
        Assert.Contains("after=2024-01-01T00%3A00%3A00Z", q);
        Assert.Contains("before=2024-07-01T00%3A00%3A01Z", q);
    }

    [Fact]
    public async Task Consecutive_windows_return_their_own_slices()
    {
        MapFiltering("/seq");
        await ReadAll(WindowedSpec("/seq", cursorType: "bigint", lower: "0", upper: "3"));  // (0,3]
        await ReadAll(WindowedSpec("/seq", cursorType: "bigint", lower: "3", upper: "7"));  // (3,7]

        // Each ReadAll issues page 1 then a terminating empty page 2 -> requests [w1p1, w1p2, w2p1, w2p2].
        Assert.Contains("after=0", _server.Requests[0].Url.Query);
        Assert.Contains("before=3", _server.Requests[0].Url.Query);
        Assert.Contains("after=3", _server.Requests[2].Url.Query);
        Assert.Contains("before=7", _server.Requests[2].Url.Query);
    }
}
