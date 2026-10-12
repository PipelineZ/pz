namespace Pz.Connectors.Sdk;

/// <summary>Wraps each HTTP request in a <c>remote.http</c> span. Records the method, host, port and
/// status code; the URL path, query, headers and exception messages never reach a span. The span ends when the response headers arrive, so it times the wait for the
/// response, not the body download. Wire bytes come from <c>Content-Length</c> when it is known and from the
/// bytes actually streamed otherwise.</summary>
internal sealed class RemoteHttpHandler(RemoteCalls remote, HttpMessageHandler inner) : DelegatingHandler(inner)
{
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        using var call = remote.Start("http");
        call.Tag("http.request.method", request.Method.Method);
        if (request.RequestUri is { IsAbsoluteUri: true } uri)
        {
            call.Tag("server.address", uri.Host);
            call.Tag("server.port", uri.Port);
        }

        // A known length is counted once, up front; a body of unknown length (chunked) is counted as it
        // is serialized, so no byte is counted twice.
        if (request.Content is { } body)
        {
            if (body.Headers.ContentLength is { } sent)
            {
                call.WireBytes(sent);
            }
            else
            {
                request.Content = new CountingRequestContent(body, call);
            }
        }

        HttpResponseMessage? response = null;
        try
        {
            response = await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
            var status = (int)response.StatusCode;
            call.Tag("http.response.status_code", status);
            if (status is 429 or 503)
            {
                call.Throttled();
            }

            if (response.Content.Headers.ContentLength is { } received)
            {
                call.WireBytes(received);
            }
            else
            {
                // Counted as the caller reads, so it keeps adding after this span has ended.
                var original = response.Content;
                var stream = await original.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
                response.Content = new CountingResponseContent(original, stream, call);
            }

            return response;
        }
        catch (Exception ex)
        {
            call.Fail(ex);
            response?.Dispose();
            throw;
        }
    }
}
