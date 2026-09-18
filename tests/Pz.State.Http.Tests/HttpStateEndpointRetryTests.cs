using System.Net;
using Pz.Connectors.TestKit;

namespace Pz.State.Http.Tests;

/// <summary><see cref="HttpStateEndpoint.Send"/> retries a response the server itself marked as "not now"
/// up to its attempt budget and honours <c>Retry-After</c>. Drives a real <see cref="StubHttpServer"/>
/// rather than <see cref="FakeStateServer"/>: the fake state server models the wire CONTRACT, this
/// models the TRANSPORT's retryable-status behaviour, which needs a handler that answers differently
/// call to call. Delays go through a <see cref="RecordingTimeProvider"/>, so nothing here waits.</summary>
public sealed class HttpStateEndpointRetryTests
{
    [Fact]
    public async Task A_503_is_retried_and_the_eventual_200_succeeds()
    {
        await using var server = new StubHttpServer();
        var calls = 0;
        server.Map("/state/watermarks/orders", _ =>
        {
            calls++;
            return calls < 3
                ? new StubResponse(503, "")
                : new StubResponse(200, """{"value":"1"}""", Tag(1));
        });

        using var endpoint = new HttpStateEndpoint($"{server.BaseUrl}state", null, new RecordingTimeProvider(), maxAttempts: 3);

        var response = endpoint.Send(HttpMethod.Get, "watermarks", "orders");

        Assert.Equal(HttpStatusCode.OK, response.Status);
        Assert.Equal(3, calls);
    }

    [Fact]
    public async Task A_429_waits_for_the_servers_Retry_After_before_retrying()
    {
        await using var server = new StubHttpServer();
        var calls = 0;
        server.Map("/state/watermarks/orders", _ =>
        {
            calls++;
            return calls == 1
                ? new StubResponse(429, "", new Dictionary<string, string> { ["Retry-After"] = "7" })
                : new StubResponse(200, """{"value":"1"}""", Tag(1));
        });
        var time = new RecordingTimeProvider();
        using var endpoint = new HttpStateEndpoint($"{server.BaseUrl}state", null, time, maxAttempts: 3);

        var response = endpoint.Send(HttpMethod.Get, "watermarks", "orders");

        Assert.Equal(HttpStatusCode.OK, response.Status);
        Assert.Equal(2, calls);
        Assert.Equal([TimeSpan.FromSeconds(7)], time.Delays);
    }

    [Fact]
    public async Task A_Retry_After_longer_than_the_cap_is_cut_to_it()
    {
        await using var server = new StubHttpServer();
        var calls = 0;
        server.Map("/state/watermarks/orders", _ =>
        {
            calls++;
            return calls == 1
                ? new StubResponse(503, "", new Dictionary<string, string> { ["Retry-After"] = "600" })
                : new StubResponse(200, """{"value":"1"}""", Tag(1));
        });
        var time = new RecordingTimeProvider();
        using var endpoint = new HttpStateEndpoint($"{server.BaseUrl}state", null, time, maxAttempts: 3);

        endpoint.Send(HttpMethod.Get, "watermarks", "orders");

        Assert.Equal([TimeSpan.FromSeconds(30)], time.Delays);
    }

    [Fact]
    public async Task A_persistent_503_gives_up_after_the_attempt_budget()
    {
        await using var server = new StubHttpServer();
        var calls = 0;
        server.Map("/state/watermarks/orders", _ =>
        {
            calls++;
            return new StubResponse(503, "");
        });

        using var endpoint = new HttpStateEndpoint($"{server.BaseUrl}state", null, new RecordingTimeProvider(), maxAttempts: 2);

        var response = endpoint.Send(HttpMethod.Get, "watermarks", "orders");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.Status);
        Assert.Equal(2, calls); // exhausted the budget, not retried forever
    }

    // A gateway that gave up waiting says nothing about whether the server behind it applied the write.
    // Replaying a versioned PUT after that would find its own write and read it as another run's.
    [Theory]
    [InlineData(502)]
    [InlineData(504)]
    public async Task A_write_is_not_replayed_after_a_gateway_failure(int status)
    {
        await using var server = new StubHttpServer();
        var calls = 0;
        server.Map("/state/watermarks/orders", _ =>
        {
            calls++;
            return new StubResponse(status, "");
        });
        using var endpoint = new HttpStateEndpoint($"{server.BaseUrl}state", null, new RecordingTimeProvider(), maxAttempts: 3);

        var response = endpoint.Send(HttpMethod.Put, "watermarks", "orders", """{"value":"2"}""", ifMatch: 1);

        Assert.Equal((HttpStatusCode)status, response.Status);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task A_read_is_replayed_after_a_gateway_failure()
    {
        await using var server = new StubHttpServer();
        var calls = 0;
        server.Map("/state/watermarks/orders", _ =>
        {
            calls++;
            return calls == 1 ? new StubResponse(504, "") : new StubResponse(200, """{"value":"1"}""", Tag(1));
        });
        using var endpoint = new HttpStateEndpoint($"{server.BaseUrl}state", null, new RecordingTimeProvider(), maxAttempts: 3);

        Assert.Equal(HttpStatusCode.OK, endpoint.Send(HttpMethod.Get, "watermarks", "orders").Status);
        Assert.Equal(2, calls);
    }

    [Fact]
    public async Task A_run_cancelled_while_waiting_to_retry_stops_waiting()
    {
        await using var server = new StubHttpServer();
        using var cts = new CancellationTokenSource();
        server.Map("/state/watermarks/orders", _ => new StubResponse(503, ""));
        // A delay that never elapses on its own, and a run cancelled the moment the wait begins.
        var time = new RecordingTimeProvider(hold: true) { OnDelay = cts.Cancel };
        using var endpoint = new HttpStateEndpoint($"{server.BaseUrl}state", null, time, maxAttempts: 3, ct: cts.Token);

        // The guard only bounds a failing run: a wait that ignores the token would never return.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => Task.Run(() => endpoint.Send(HttpMethod.Get, "watermarks", "orders")).WaitAsync(TimeSpan.FromSeconds(10)));
    }

    private static Dictionary<string, string> Tag(int version) => new() { ["ETag"] = $"\"{version}\"" };
}
