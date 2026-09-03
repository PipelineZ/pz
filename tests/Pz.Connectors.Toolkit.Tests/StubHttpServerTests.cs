using Pz.Connectors.TestKit;

namespace Pz.Connectors.Toolkit.Tests;

public class StubHttpServerTests
{
    [Fact]
    public async Task Serves_mapped_route_captures_requests_and_404s_unmapped()
    {
        await using var server = new StubHttpServer();
        server.Map("/items", req => req.Url.Query.Contains("page=2")
            ? new StubResponse(200, """[]""")
            : new StubResponse(200, """[{"id":1}]""", new Dictionary<string, string>
                { ["Link"] = $"<{server.BaseUrl}items?page=2>; rel=\"next\"" }));

        using var client = new HttpClient();
        var first = await client.GetAsync(new Uri(server.BaseUrl, "items"));
        Assert.Equal(200, (int)first.StatusCode);
        Assert.Contains("rel=\"next\"", first.Headers.GetValues("Link").Single());
        Assert.Equal("""[{"id":1}]""", await first.Content.ReadAsStringAsync());

        var missing = await client.GetAsync(new Uri(server.BaseUrl, "nope"));
        Assert.Equal(404, (int)missing.StatusCode);

        Assert.Equal(2, server.Requests.Count);
        Assert.Equal("/items", server.Requests[0].Url.AbsolutePath);
    }

    [Fact]
    public async Task Loop_survives_handler_exception_and_returns_500_with_details()
    {
        await using var server = new StubHttpServer();
        server.Map("/boom", _ => throw new InvalidOperationException("boom"));
        server.Map("/ok", _ => new StubResponse(200, """{"ok":true}"""));

        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };

        var boom = await client.GetAsync(new Uri(server.BaseUrl, "boom"));
        Assert.Equal(500, (int)boom.StatusCode);
        var body = await boom.Content.ReadAsStringAsync();
        Assert.Contains("InvalidOperationException", body);
        Assert.Contains("boom", body);

        // Second request to healthy route: loop survived, still serving
        var ok = await client.GetAsync(new Uri(server.BaseUrl, "ok"));
        Assert.Equal(200, (int)ok.StatusCode);
        Assert.Equal("""{"ok":true}""", await ok.Content.ReadAsStringAsync());

        Assert.NotNull(server.HandlerError);
        Assert.IsType<InvalidOperationException>(server.HandlerError);
        Assert.Equal("boom", server.HandlerError.Message);
    }

    /// <summary>A server disposed before its serve loop has parked in GetContextAsync must still
    /// tear down. Stop() wakes only a waiter that has already registered: a loop whose
    /// listening check passed just before Stop() queued a waiter nothing would ever complete, and
    /// DisposeAsync awaited it forever -- a test that made no requests then hung until CI's
    /// blame-hang timeout. The construct-then-dispose window is microseconds wide, so this
    /// repeats it; the bounded wait is the hang guard, not a timing assertion.</summary>
    [Fact]
    public async Task Dispose_right_after_construction_never_hangs()
    {
        for (var i = 0; i < 2000; i++)
        {
            var server = new StubHttpServer();
            await server.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30));
        }
    }
}
