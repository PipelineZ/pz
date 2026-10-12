namespace Pz.Connectors.Sdk;

/// <summary>Wraps each HTTP request in a <c>remote.http</c> span. Records the method, host, port and
/// status code; the URL path, query, headers and exception messages never reach a span. The span ends when the response headers arrive, so it times the wait for the
/// response, not the body download.</summary>
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

        if (request.Content?.Headers.ContentLength is { } sent)
        {
            call.WireBytes(sent);
        }

        try
        {
            var response = await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
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

            return response;
        }
        catch (Exception ex)
        {
            call.Fail(ex);
            throw;
        }
    }
}
