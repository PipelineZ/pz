using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Http;

namespace Pz.Connector.Http;

/// <summary>The first non-redirect response of a send, the post-auth URI that produced it (what every
/// error message redacts and reports) and the method that produced it. Disposing it disposes the
/// response.</summary>
internal sealed record SentResponse(HttpResponseMessage Response, Uri SendUri, HttpMethod Method) : IDisposable
{
    public void Dispose() => Response.Dispose();
}

/// <summary>The send path every HTTP read shares: gate routing, hand-walked redirects with an
/// allowed-target check on every hop, 401 handling, transport-failure and non-2xx classification, and
/// URL redaction for messages. Never retries, never sleeps.</summary>
/// <param name="bareClient">Carries no connection <c>headers:</c> and no base address; used for the
/// hops a send with <c>credentialsOnlyToOrigin</c> makes to a host other than the base URL's origin.</param>
/// <param name="redactWholeQuery">Replace the whole query string of a URL in messages instead of masking
/// the authenticator's secret params by name — for a URL whose query is itself a secret with a
/// server-defined name (a sync token, a signed download link).</param>
internal sealed class HttpRequestSender(HttpClient client, HttpClient bareClient, HttpConnectionConfig connection,
    HttpBody? body, IOperationGate? gate, TimeProvider time, string label, bool redactWholeQuery = false)
{
    private const int MaxRedirects = 5;

    /// <summary>Runs <paramref name="op"/> straight through without a gate, and as the gate's op
    /// callback otherwise. Classification into PzConnectorException happens inside the op, so the gate
    /// always sees fully-classified transient/permanent exceptions; only an <paramref name="idempotent"/>
    /// op may be retried by it.</summary>
    public Task<T> RunAsync<T>(string operation, bool idempotent, Func<CancellationToken, Task<T>> op,
        CancellationToken ct)
        => gate is null ? op(ct) : gate.ExecuteAsync(operation, idempotent, op, ct);

    /// <summary>Issues the request and walks any redirect chain by hand, because
    /// <see cref="HttpSource.CreateClient"/> turns the handler's own redirect following off. Each hop's
    /// target is resolved against the URL actually requested and then checked with
    /// <see cref="EnsureAllowedTarget"/>, so a redirect can never carry this connection's credentials
    /// to a host the endpoint picked; the chain is bounded so a redirect loop fails instead of
    /// spinning. Returns the first non-redirect response together with the URI that produced it —
    /// the post-auth URI, which is what every error message redacts and reports — and the method that
    /// produced it. Each hop sends a freshly built message: 307/308 keep the method and body, 301/302/303
    /// switch to GET without a body. The target of every hop is checked before credentials and the
    /// body go anywhere.
    ///
    /// <paramref name="streamBody"/> returns once the headers arrive, leaving the body on the wire
    /// (no <c>max_response_mb</c> buffer cap applies). <paramref name="credentialsOnlyToOrigin"/> sends
    /// every hop whose origin differs from <c>base_url</c>'s on the bare client with no authenticator,
    /// so neither the authenticator nor the connection's <c>headers:</c> reach that host.</summary>
    public async Task<SentResponse> SendAsync(HttpPageRequest first, bool streamBody, bool credentialsOnlyToOrigin,
        CancellationToken ct)
    {
        var completion = streamBody ? HttpCompletionOption.ResponseHeadersRead : HttpCompletionOption.ResponseContentRead;
        var current = first;
        for (var hop = 0; ; hop++)
        {
            using var request = current.ToMessage(body);
            var bare = credentialsOnlyToOrigin && !IsBaseOrigin(current.Url);
            var authenticator = bare ? null : connection.Authenticator;
            if (authenticator is not null)
            {
                await authenticator.ApplyAsync(request, ct).ConfigureAwait(false);
            }

            // Redact the POST-auth URI: an api_key-in-query authenticator just added the secret to
            // request.RequestUri, and that is the URL any error message must never echo unredacted.
            var sendUri = request.RequestUri ?? current.Url;
            var sent = current.Method;

            HttpResponseMessage response;
            try
            {
                response = await (bare ? bareClient : client).SendAsync(request, completion, ct).ConfigureAwait(false);
            }
            catch (HttpRequestException ex) when (ex.HttpRequestError == HttpRequestError.ConfigurationLimitExceeded)
            {
                // A buffered send (HttpCompletionOption.ResponseContentRead) reads the whole response
                // before SendAsync returns and refuses to grow the buffer past
                // MaxResponseContentBufferSize -- the runtime's own message never mentions the pz
                // option that set the limit, so callers see a bare "configured maximum buffer size"
                // with nothing to act on.
                throw new PzConnectorException(
                    $"{label}: response from {Target(sent, sendUri)} exceeds 'max_response_mb' " +
                    $"({connection.MaxResponseBytes / (1024 * 1024)} MiB)", isTransient: false,
                    innerException: ex);
            }
            catch (HttpRequestException ex)
            {
                throw new PzConnectorException($"{label}: request to {Target(sent, sendUri)} failed: {ex.Message}",
                    TransientClassifier.IsTransientException(ex), innerException: ex);
            }
            catch (TaskCanceledException ex) when (!ct.IsCancellationRequested)
            {
                throw new PzConnectorException($"{label}: request to {Target(sent, sendUri)} timed out",
                    isTransient: true, innerException: ex);
            }

            var redirectStatus = (int)response.StatusCode;
            if (!IsRedirect(redirectStatus))
            {
                try
                {
                    HttpAuth.ThrowIfTokenRejected(authenticator, request, redirectStatus, label);
                }
                catch
                {
                    response.Dispose();
                    throw;
                }

                return new SentResponse(response, sendUri, sent);
            }

            var location = response.Headers.Location;
            response.Dispose();

            if (location is null)
            {
                throw new PzConnectorException(
                    $"{label}: HTTP {redirectStatus} from {Target(sent, sendUri)} has no Location " +
                    "header (check the endpoint path)", isTransient: false);
            }

            if (hop >= MaxRedirects)
            {
                throw new PzConnectorException(
                    $"{label}: more than {MaxRedirects} redirects starting at {Target(first.Method, first.Url)}; " +
                    "point 'base_url'/'path' at the endpoint's final location", isTransient: false);
            }

            var target = new Uri(sendUri, location);
            EnsureAllowedTarget(target, "a redirect from the endpoint");
            current = current.RedirectedTo(target, redirectStatus);
        }
    }

    /// <summary>The generic non-2xx classification: 408/429/5xx are transient (with the response's
    /// <c>Retry-After</c>), everything else permanent with a redacted body snippet.</summary>
    public PzConnectorException StatusError(SentResponse sent, string bodyText)
    {
        var status = (int)sent.Response.StatusCode;
        if (TransientClassifier.IsTransientStatus(status))
        {
            var retryAfter = TransientClassifier.ParseRetryAfter(
                sent.Response.Headers.TryGetValues("Retry-After", out var values)
                    ? values.FirstOrDefault()
                    : null,
                time.GetUtcNow());
            return new PzConnectorException($"{label}: HTTP {status} from {Target(sent.Method, sent.SendUri)}",
                isTransient: true, retryAfter);
        }

        return new PzConnectorException(
            $"{label}: HTTP {status} from {Target(sent.Method, sent.SendUri)}: {RedactSnippet(bodyText)} " +
            "(check the endpoint path and auth config)", isTransient: false);
    }

    private bool IsBaseOrigin(Uri uri)
        => string.Equals(uri.Scheme, connection.BaseUrl.Scheme, StringComparison.OrdinalIgnoreCase)
            && string.Equals(uri.Host, connection.BaseUrl.Host, StringComparison.OrdinalIgnoreCase)
            && uri.Port == connection.BaseUrl.Port;

    private static bool IsRedirect(int status) => status is 301 or 302 or 303 or 307 or 308;

    /// <summary>Refuses any request target the far side chose that is not this connection's own origin
    /// (or an explicitly allow-listed host). Pagination links, redirect targets and stored resume
    /// tokens are all endpoint-controlled, and every request carries the connection's
    /// Authorization/api-key headers — so without this a single hostile response turns pz into a
    /// credential-exfiltration channel. Only the authority is named: the rest of the URL can embed the
    /// secret itself.</summary>
    public void EnsureAllowedTarget(Uri candidate, string what)
    {
        if (connection.IsAllowedTarget(candidate))
        {
            return;
        }

        throw new PzConnectorException(
            $"{label}: {what} points at '{candidate.GetLeftPart(UriPartial.Authority)}', which is not " +
            $"this connection's host '{connection.BaseUrl.GetLeftPart(UriPartial.Authority)}' — pz will " +
            "not send this connection's credentials to a host the endpoint chose; if this really is " +
            "part of the same API, add the host to 'allow_hosts' on the connection", isTransient: false);
    }

    // How a request is named in a message: the redacted URL, prefixed by the method when it is not
    // GET (GET messages stay exactly as they were). Never the body.
    public string Target(HttpMethod method, Uri uri)
        => method == HttpMethod.Get ? Redact(uri) : $"{method.Method} {Redact(uri)}";

    /// <summary>Redacts a URI before it is surfaced in an exception message. When the query is itself
    /// a secret with a server-defined name (e.g. Graph's `$deltatoken`), it is replaced wholesale
    /// rather than masked param-by-param; scheme/host/path are kept for diagnosability. Otherwise the
    /// authenticator's SecretQueryParams are masked.</summary>
    public string Redact(Uri uri) => redactWholeQuery ? RedactQuery(uri) : Redact(uri.ToString());

    private static string Snippet(string body)
    {
        var flat = body.ReplaceLineEndings(" ");
        return flat.Length <= 160 ? flat : flat[..160] + "…";
    }

    /// <summary>Redacts a permanent-error response body before it is surfaced in an exception message.
    /// A 4xx body can echo the request URL verbatim (e.g. a Graph-style 403/404 error payload naming the
    /// request), leaking a server-defined secret query param through the snippet even though it isn't
    /// one of the connector's configured <c>SecretQueryParams</c>. With whole-query redaction, strip the
    /// query string off any URL found in the text wholesale first (same approach as
    /// <see cref="RedactQuery"/> -- the param name is opaque, not maskable by name), then apply the
    /// param-based <see cref="Redact(string)"/> as a second pass for any configured auth secret the body
    /// might also echo.</summary>
    private string RedactSnippet(string text) =>
        Redact(Snippet(redactWholeQuery ? StripUrlQueries(text) : text));

    private static readonly System.Text.RegularExpressions.Regex UrlWithQuery =
        new(@"https?://[^\s""'?]+\?[^\s""']*", System.Text.RegularExpressions.RegexOptions.Compiled);

    internal static string StripUrlQueries(string text) =>
        UrlWithQuery.Replace(text, m =>
            // The match is regex-shaped, not RFC 3986-valid -- an upstream API's own error body can
            // embed "scheme://...?..." text (an example, a malformed hint) that the Uri constructor
            // refuses (e.g. an out-of-range port). This runs inside error-message formatting itself,
            // so it must never throw; a match Uri can't parse is masked wholesale instead.
            Uri.TryCreate(m.Value, UriKind.Absolute, out var uri) ? RedactQuery(uri) : "<redacted>");

    private static string RedactQuery(Uri uri)
    {
        var basePart = uri.GetLeftPart(UriPartial.Path);
        return uri.Query.Length > 0 ? $"{basePart}?<redacted>" : basePart;
    }

    /// <summary>Masks any authenticator secret-query-param value found in arbitrary text — the
    /// same rule applied to request URIs, reused here for 4xx response body snippets (a server
    /// echoing the request URL, e.g. in a 403 body, must not leak an api_key-in-query secret).</summary>
    private string Redact(string text) => connection.RedactSecretParams(text);
}
