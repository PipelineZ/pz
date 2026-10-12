using System.Diagnostics;
using System.Net;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Sdk.Telemetry;

namespace Pz.Connectors.Sdk.Tests;

public sealed class RemoteHttpHandlerTests : IDisposable
{
    private readonly ActivitySource _source = new("Pz.Connector.test." + Guid.NewGuid().ToString("n"));
    private readonly List<Activity> _stopped = [];
    private readonly ActivityListener _listener;

    public RemoteHttpHandlerTests()
    {
        _listener = new ActivityListener
        {
            ShouldListenTo = s => ReferenceEquals(s, _source),
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = a => { lock (_stopped) { _stopped.Add(a); } },
        };
        ActivitySource.AddActivityListener(_listener);
    }

    public void Dispose()
    {
        _listener.Dispose();
        _source.Dispose();
    }

    private sealed class Stub(HttpStatusCode status, string body = "ok", bool throws = false) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            if (throws) throw new HttpRequestException("secret detail");
            return Task.FromResult(new HttpResponseMessage(status) { Content = new StringContent(body) });
        }
    }

    private async Task<ConnectorStatsAccumulator> SendAsync(HttpStatusCode status, HttpRequestMessage? request = null, bool throws = false)
    {
        var acc = new ConnectorStatsAccumulator();
        using var scope = OperationScope.Enter(acc, new RemoteSpanBudget());
        using var client = new HttpClient(new RemoteCalls(_source).HttpHandler(new Stub(status, throws: throws)));
        request ??= new HttpRequestMessage(HttpMethod.Get, "https://example.test:8443/secret/path?q=token");
        request.Headers.TryAddWithoutValidation("Authorization", "x");
        try
        {
            using var _ = await client.SendAsync(request);
        }
        catch (HttpRequestException) when (throws)
        {
        }

        return acc;
    }

    [Fact]
    public async Task Request_gets_a_remote_http_span_with_only_safe_tags()
    {
        var acc = await SendAsync(HttpStatusCode.OK);

        var span = Assert.Single(_stopped);
        Assert.Equal("remote.http", span.OperationName);
        Assert.Equal("GET", span.GetTagItem("http.request.method"));
        Assert.Equal("example.test", span.GetTagItem("server.address"));
        Assert.Equal(8443, span.GetTagItem("server.port"));
        Assert.Equal(200, span.GetTagItem("http.response.status_code"));
        foreach (var tag in span.TagObjects)
        {
            var value = tag.Value?.ToString() ?? "";
            Assert.DoesNotContain("secret", value);
            Assert.DoesNotContain("token", value);
            Assert.DoesNotContain("q=", value);
            Assert.DoesNotContain("Authorization", value);
            Assert.DoesNotContain("Authorization", tag.Key);
        }

        Assert.Equal(1, acc.Snapshot().Requests);
    }

    [Theory]
    [InlineData(HttpStatusCode.TooManyRequests, 1)]
    [InlineData(HttpStatusCode.ServiceUnavailable, 1)]
    [InlineData(HttpStatusCode.OK, 0)]
    public async Task Throttling_statuses_count_throttled(HttpStatusCode status, long expected)
    {
        var acc = await SendAsync(status);

        Assert.Equal(expected, acc.Snapshot().Throttled);
    }

    [Fact]
    public async Task Wire_bytes_are_request_plus_response_content_length()
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "https://example.test/x") { Content = new StringContent("12345") };

        var acc = await SendAsync(HttpStatusCode.OK, request);

        Assert.Equal(5 + 2, acc.Snapshot().WireBytes);
    }

    [Fact]
    public async Task A_failed_request_marks_the_span_failed_without_the_message()
    {
        var acc = await SendAsync(HttpStatusCode.OK, throws: true);

        var span = Assert.Single(_stopped);
        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal("System.Net.Http.HttpRequestException", span.GetTagItem("error.type"));
        Assert.DoesNotContain(span.TagObjects, t => t.Value?.ToString()?.Contains("secret") == true);
        Assert.Equal(1, acc.Snapshot().Requests);
    }
}
