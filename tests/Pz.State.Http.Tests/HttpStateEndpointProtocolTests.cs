using Pz.Connectors.TestKit;
using Pz.Core.Validation;

namespace Pz.State.Http.Tests;

/// <summary>#95: protocol tolerance a real deployment (a reverse proxy in front of the state server, a
/// server that answers 200/412 instead of 204/201/409) needs. Drives a scripted <see cref="StubHttpServer"/>
/// directly rather than <see cref="FakeStateServer"/>, whose fake server always answers the exact status
/// codes pz's own store already expected -- the point here is proving the CLIENT tolerates the ones it
/// didn't.</summary>
public sealed class HttpStateEndpointProtocolTests
{
    private sealed record TestEntry(string Value);

    [Fact]
    public async Task A_weak_ETag_is_parsed_as_a_version()
    {
        // nginx/gzip in front of the state server commonly turns a strong ETag weak.
        await using var server = new StubHttpServer();
        server.Map("/state/watermarks/orders",
            _ => new StubResponse(200, """{"value":"1"}""", new Dictionary<string, string> { ["ETag"] = "W/\"3\"" }));
        using var endpoint = new HttpStateEndpoint($"{server.BaseUrl}state", null);

        var response = endpoint.Send(HttpMethod.Get, "watermarks", "orders");

        Assert.Equal(3, response.Version);
    }

    [Fact]
    public async Task A_412_on_Set_is_the_same_conflict_as_a_409()
    {
        await using var server = new StubHttpServer();
        server.Map("/state/watermarks/orders", _ => new StubResponse(412, "", new Dictionary<string, string> { ["ETag"] = "\"5\"" }));
        using var endpoint = new HttpStateEndpoint($"{server.BaseUrl}state", null);
        var store = new HttpKeyedStateStore<TestEntry>(endpoint, "watermarks",
            static e => new TestEntry(e.GetProperty("value").GetString()!),
            static (w, e) => w.WriteString("value", e.Value));

        var ex = Assert.Throws<PzConfigException>(() => store.Set("orders", new TestEntry("1")));

        Assert.Equal(PzErrorCode.StateConcurrencyConflict, ex.Error.Code);
    }

    [Fact]
    public async Task A_412_on_Remove_is_the_same_conflict_as_a_409()
    {
        await using var server = new StubHttpServer();
        server.Map("/state/watermarks/orders", _ => new StubResponse(412, ""));
        using var endpoint = new HttpStateEndpoint($"{server.BaseUrl}state", null);
        var store = new HttpKeyedStateStore<TestEntry>(endpoint, "watermarks",
            static e => new TestEntry(e.GetProperty("value").GetString()!),
            static (w, e) => w.WriteString("value", e.Value));

        var ex = Assert.Throws<PzConfigException>(() => store.Remove("orders"));

        Assert.Equal(PzErrorCode.StateConcurrencyConflict, ex.Error.Code);
    }

    [Fact]
    public async Task A_200_on_Set_is_success_not_unexpected()
    {
        await using var server = new StubHttpServer();
        server.Map("/state/watermarks/orders",
            _ => new StubResponse(200, """{"value":"1"}""", new Dictionary<string, string> { ["ETag"] = "\"1\"" }));
        using var endpoint = new HttpStateEndpoint($"{server.BaseUrl}state", null);
        var store = new HttpKeyedStateStore<TestEntry>(endpoint, "watermarks",
            static e => new TestEntry(e.GetProperty("value").GetString()!),
            static (w, e) => w.WriteString("value", e.Value));

        store.Set("orders", new TestEntry("1")); // must not throw
    }

    [Fact]
    public async Task A_200_on_Remove_is_success_not_unexpected()
    {
        await using var server = new StubHttpServer();
        server.Map("/state/watermarks/orders", _ => new StubResponse(200, ""));
        using var endpoint = new HttpStateEndpoint($"{server.BaseUrl}state", null);
        var store = new HttpKeyedStateStore<TestEntry>(endpoint, "watermarks",
            static e => new TestEntry(e.GetProperty("value").GetString()!),
            static (w, e) => w.WriteString("value", e.Value));

        store.Remove("orders"); // must not throw
    }

    [Fact]
    public async Task A_configured_timeout_aborts_a_slow_response_as_PZ0518()
    {
        await using var server = new StubHttpServer();
        server.Map("/state/watermarks/orders", _ =>
        {
            Thread.Sleep(500);
            return new StubResponse(200, """{"value":"1"}""");
        });
        using var endpoint = new HttpStateEndpoint(
            $"{server.BaseUrl}state", null, timeout: TimeSpan.FromMilliseconds(100));

        var ex = Assert.Throws<PzConfigException>(() => endpoint.Send(HttpMethod.Get, "watermarks", "orders"));

        Assert.Equal(PzErrorCode.StateStoreUnavailable, ex.Error.Code);
    }

    [Fact]
    public async Task A_cancelled_run_token_propagates_as_cancellation_not_PZ0518()
    {
        await using var server = new StubHttpServer();
        server.Map("/state/watermarks/orders", _ =>
        {
            Thread.Sleep(1000);
            return new StubResponse(200, """{"value":"1"}""");
        });
        using var cts = new CancellationTokenSource();
        using var endpoint = new HttpStateEndpoint($"{server.BaseUrl}state", null, ct: cts.Token);
        cts.CancelAfter(TimeSpan.FromMilliseconds(50));

        // TaskCanceledException (an OperationCanceledException) -- propagated uncaught, never wrapped
        // into a PzConfigException, so the dispatcher can tell cancellation apart from a genuine failure.
        Assert.ThrowsAny<OperationCanceledException>(() => endpoint.Send(HttpMethod.Get, "watermarks", "orders"));
    }
}
