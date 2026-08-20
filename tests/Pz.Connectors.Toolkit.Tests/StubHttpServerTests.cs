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
}
