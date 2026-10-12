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

    private sealed class Chunked(Stream stream) : HttpContent
    {
        protected override Task SerializeToStreamAsync(Stream target, TransportContext? context) => stream.CopyToAsync(target);

        protected override bool TryComputeLength(out long length)
        {
            length = 0;
            return false;
        }
    }

    private sealed class Reading(Func<HttpRequestMessage, HttpResponseMessage> respond) : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            if (request.Content is not null)
            {
                await request.Content.CopyToAsync(Stream.Null, ct);
            }

            return respond(request);
        }
    }

    private static HttpResponseMessage ChunkedResponse(int n) =>
        new(HttpStatusCode.OK) { Content = new StreamContent(new NonSeekable(new byte[n])) };

    private sealed class NonSeekable(byte[] data) : Stream
    {
        private readonly MemoryStream _inner = new(data);

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    [Fact]
    public async Task A_chunked_request_body_counts_the_bytes_sent()
    {
        var acc = new ConnectorStatsAccumulator();
        using var scope = OperationScope.Enter(acc, new RemoteSpanBudget());
        using var client = new HttpClient(new RemoteCalls(_source).HttpHandler(new Reading(_ => new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("") })));
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://example.test/x") { Content = new Chunked(new NonSeekable(new byte[1000])) };

        using var _ = await client.SendAsync(request);

        Assert.Equal(1000, acc.Snapshot().WireBytes);
    }

    [Fact]
    public async Task A_chunked_response_counts_its_length_once_read()
    {
        var acc = new ConnectorStatsAccumulator();
        using var scope = OperationScope.Enter(acc, new RemoteSpanBudget());
        using var client = new HttpClient(new RemoteCalls(_source).HttpHandler(new Reading(_ => ChunkedResponse(5000))));

        using var response = await client.GetAsync("https://example.test/x");
        var body = await response.Content.ReadAsByteArrayAsync();

        Assert.Equal(5000, body.Length);
        Assert.Equal(5000, acc.Snapshot().WireBytes);
    }

    [Fact]
    public async Task A_partially_read_response_counts_only_what_was_read()
    {
        var acc = new ConnectorStatsAccumulator();
        using var scope = OperationScope.Enter(acc, new RemoteSpanBudget());
        using var client = new HttpClient(new RemoteCalls(_source).HttpHandler(new Reading(_ => ChunkedResponse(5000))));

        using var response = await client.GetAsync("https://example.test/x", HttpCompletionOption.ResponseHeadersRead);
        await using var stream = await response.Content.ReadAsStreamAsync();
        var buffer = new byte[100];
        var read = await stream.ReadAsync(buffer);

        Assert.Equal(read, acc.Snapshot().WireBytes);
        Assert.True(read is > 0 and <= 100);
    }

    [Fact]
    public async Task Known_length_bodies_are_not_counted_twice()
    {
        var acc = new ConnectorStatsAccumulator();
        using var scope = OperationScope.Enter(acc, new RemoteSpanBudget());
        using var client = new HttpClient(new RemoteCalls(_source).HttpHandler(new Reading(_ => new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(new byte[40]) })));
        using var request = new HttpRequestMessage(HttpMethod.Post, "https://example.test/x") { Content = new ByteArrayContent(new byte[10]) };

        using var response = await client.SendAsync(request);
        _ = await response.Content.ReadAsByteArrayAsync();

        Assert.Equal(50, acc.Snapshot().WireBytes);
    }

    [Fact]
    public async Task A_public_remote_calls_counts_into_the_current_stats()
    {
        var acc = new ConnectorStatsAccumulator();
        using var scope = OperationScope.Enter(acc, new RemoteSpanBudget());
        using var source = new ActivitySource("t");
        using var client = new HttpClient(new RemoteCalls(source).HttpHandler(new Stub(HttpStatusCode.OK)));

        using var _ = await client.GetAsync("https://example.test/x");

        Assert.Equal(1, acc.Snapshot().Requests);
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
