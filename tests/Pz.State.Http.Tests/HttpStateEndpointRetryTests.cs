using System.Net;
using Pz.Connectors.TestKit;

namespace Pz.State.Http.Tests;

/// <summary>#92: <see cref="HttpStateEndpoint.Send"/> retries a 429/502/503/504 response up to its
/// attempt budget and honours <c>Retry-After</c>, instead of surfacing every one of those as an
/// unretried PZ0518. Drives a real <see cref="StubHttpServer"/> rather than <see cref="FakeStateServer"/>:
/// the fake state server models the wire CONTRACT, this models the TRANSPORT's retryable-status
/// behaviour, which needs a handler that answers differently call to call.</summary>
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

        using var endpoint = new HttpStateEndpoint($"{server.BaseUrl}state", null, TimeProvider.System, maxAttempts: 3);

        var response = endpoint.Send(HttpMethod.Get, "watermarks", "orders");

        Assert.Equal(HttpStatusCode.OK, response.Status);
        Assert.Equal(3, calls);
    }

    [Fact]
    public async Task A_429_honours_a_short_Retry_After_before_retrying()
    {
        await using var server = new StubHttpServer();
        var calls = 0;
        var first = DateTimeOffset.UtcNow;
        DateTimeOffset? secondAt = null;
        server.Map("/state/watermarks/orders", _ =>
        {
            calls++;
            if (calls == 1)
            {
                return new StubResponse(429, "", new Dictionary<string, string> { ["Retry-After"] = "1" });
            }

            secondAt = DateTimeOffset.UtcNow;
            return new StubResponse(200, """{"value":"1"}""", Tag(1));
        });

        using var endpoint = new HttpStateEndpoint($"{server.BaseUrl}state", null, TimeProvider.System, maxAttempts: 3);

        var response = endpoint.Send(HttpMethod.Get, "watermarks", "orders");

        Assert.Equal(HttpStatusCode.OK, response.Status);
        Assert.Equal(2, calls);
        Assert.True(secondAt - first >= TimeSpan.FromMilliseconds(900), "the server's Retry-After: 1 was not honoured");
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

        using var endpoint = new HttpStateEndpoint($"{server.BaseUrl}state", null, TimeProvider.System, maxAttempts: 2);

        var response = endpoint.Send(HttpMethod.Get, "watermarks", "orders");

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.Status);
        Assert.Equal(2, calls); // exhausted the budget, not retried forever
    }

    private static Dictionary<string, string> Tag(int version) => new() { ["ETag"] = $"\"{version}\"" };
}
