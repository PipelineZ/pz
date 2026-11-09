using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Http;

namespace Pz.Connector.Http;

/// <summary>The first non-redirect response of a send, the post-auth URI that produced it (what every
/// error message redacts and reports) and the method that produced it. Disposing it disposes the
/// response.</summary>
internal sealed record SentResponse(HttpResponseMessage Response, Uri SendUri, HttpMethod Method) : IDisposable
{
    /// <summary>A <c>credentialsOnlyToOrigin</c> send whose final hop is a redirect target off
    /// <c>base_url</c>'s origin: the server picked that URL, and its query can be a credential (a signed
    /// link), so messages drop the query whole.</summary>
    public bool ServerChosen { get; init; }

    public void Dispose() => Response.Dispose();
}

/// <summary>The send path every HTTP read shares: gate routing, hand-walked redirects with an
/// allowed-target check on every hop, 401 handling, transport-failure and non-2xx classification, and
/// URL redaction for messages. Never retries; the only wait is the read's shared <c>Retry-After</c>
/// pause.</summary>
/// <param name="bareClient">Carries no connection <c>headers:</c> and no base address; used for the
/// hops a send with <c>credentialsOnlyToOrigin</c> makes to a host other than the base URL's origin.</param>
/// <param name="redactWholeQuery">Replace the whole query string of a URL in messages instead of masking
/// the authenticator's secret params by name — for a URL whose query is itself a secret with a
/// server-defined name (a sync token, a signed download link).</param>
/// <param name="pause">The read's shared <c>Retry-After</c> pause: every hop waits it out, and a 429/503
/// with a <c>Retry-After</c> moves it forward. Null means this sender neither waits nor notes.</param>
/// <param name="rules">The request's <c>on_status</c> rules, tried in order before the built-in
/// classification of a non-2xx response. Null or empty leaves the built-in classification alone.</param>
internal sealed class HttpRequestSender(HttpClient client, HttpClient bareClient, HttpConnectionConfig connection,
    HttpBody? body, IOperationGate? gate, TimeProvider time, string label, bool redactWholeQuery = false,
    ThrottlePause? pause = null, IReadOnlyList<HttpStatusRule>? rules = null)
{
    /// <summary>True when some rule needs the response body to decide.</summary>
    public bool RulesReadBody => rules?.Any(r => r.BodyContains is not null) == true;

    /// <summary>True when some rule names <paramref name="status"/>, whatever its <c>body_contains</c>.</summary>
    public bool RulesCover(int status) => rules?.Any(r => r.Covers(status)) == true;

    /// <summary>The first <c>on_status</c> rule matching the response, or null. <paramref name="bodyText"/>
    /// is the raw body (or its head), never shown.</summary>
    public HttpStatusRule? MatchRule(SentResponse sent, string bodyText)
        => HttpStatusRule.FirstMatch(rules, (int)sent.Response.StatusCode, bodyText);

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
    /// so neither the authenticator nor the connection's <c>headers:</c> reach that host, and names
    /// such a redirect target in messages without its query. <paramref name="stage"/>, when set, follows
    /// the label in every message (<c>export poll</c>).</summary>
    public Task<SentResponse> SendAsync(HttpPageRequest first, bool streamBody, bool credentialsOnlyToOrigin,
        CancellationToken ct)
        => SendAsync(first, streamBody, credentialsOnlyToOrigin, stage: null, ct);

    /// <inheritdoc cref="SendAsync(HttpPageRequest, bool, bool, CancellationToken)"/>
    public async Task<SentResponse> SendAsync(HttpPageRequest first, bool streamBody, bool credentialsOnlyToOrigin,
        string? stage, CancellationToken ct)
    {
        var prefix = Prefix(stage);
        var completion = streamBody ? HttpCompletionOption.ResponseHeadersRead : HttpCompletionOption.ResponseContentRead;
        var current = first;
        for (var hop = 0; ; hop++)
        {
            // Every hop is a request the throttled API sees, so each one waits out the read's pause. This
            // runs inside the gated op, whose pacing token is already spent: requests released together
            // by a pause are shaped by the gate's bucket only from their next request on.
            if (pause is not null)
            {
                await pause.WaitAsync(ct).ConfigureAwait(false);
            }

            using var request = current.ToMessage(body);
            var bare = credentialsOnlyToOrigin && !IsBaseOrigin(current.Url);
            var authenticator = bare ? null : connection.Authenticator;
            if (authenticator is not null)
            {
                await authenticator.ApplyAsync(request, ct).ConfigureAwait(false);
            }

            // The page token header is set before auth, so a second value there is the authenticator's.
            if (current.PageHeader is { } pageHeader && request.Headers.TryGetValues(pageHeader.Name, out var sentValues)
                && sentValues.Skip(1).Any())
            {
                throw new PzConnectorException(
                    $"{prefix}: pagination header '{pageHeader.Name}' is also set by the connection's auth; the page " +
                    "token header must be sent by pagination alone (use another 'auth.header')", isTransient: false);
            }

            // Redact the POST-auth URI: an api_key-in-query authenticator just added the secret to
            // request.RequestUri, and that is the URL any error message must never echo unredacted.
            var sendUri = request.RequestUri ?? current.Url;
            var sent = current.Method;
            var serverChosen = credentialsOnlyToOrigin && hop > 0 && !IsBaseOrigin(current.Url);

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
                    $"{prefix}: response from {Target(sent, sendUri, serverChosen)} exceeds 'max_response_mb' " +
                    $"({connection.MaxResponseBytes / (1024 * 1024)} MiB)", isTransient: false,
                    innerException: ex);
            }
            catch (HttpRequestException ex) when (HttpHandlerFactory.Describe(ex, connection, sendUri) is { } failure)
            {
                throw new PzConnectorException(
                    $"{prefix}: request to {Target(sent, sendUri, serverChosen)} failed: {failure.Detail}",
                    failure.Transient, innerException: ex);
            }
            catch (HttpRequestException ex)
            {
                throw new PzConnectorException(
                    $"{prefix}: request to {Target(sent, sendUri, serverChosen)} failed: {ex.Message}",
                    TransientClassifier.IsTransientException(ex), innerException: ex);
            }
            catch (TaskCanceledException ex) when (!ct.IsCancellationRequested)
            {
                throw new PzConnectorException($"{prefix}: request to {Target(sent, sendUri, serverChosen)} timed out",
                    isTransient: true, innerException: ex);
            }

            var redirectStatus = (int)response.StatusCode;
            if (redirectStatus == 407)
            {
                // A proxy's answer to a plain-http request (an https one fails its tunnel instead): retrying
                // with the same credentials cannot succeed.
                response.Dispose();
                throw new PzConnectorException(
                    $"{prefix}: request to {Target(sent, sendUri, serverChosen)} failed: {HttpHandlerFactory.ProxyAuthenticationFailed}",
                    isTransient: false);
            }

            if (!IsRedirect(redirectStatus))
            {
                try
                {
                    HttpAuth.ThrowIfTokenRejected(authenticator, request, redirectStatus, prefix);
                }
                catch
                {
                    response.Dispose();
                    throw;
                }

                return new SentResponse(response, sendUri, sent) { ServerChosen = serverChosen };
            }

            var location = response.Headers.Location;
            response.Dispose();

            if (location is null)
            {
                throw new PzConnectorException(
                    $"{prefix}: HTTP {redirectStatus} from {Target(sent, sendUri, serverChosen)} has no Location " +
                    "header (check the endpoint path)", isTransient: false);
            }

            if (hop >= MaxRedirects)
            {
                throw new PzConnectorException(
                    $"{prefix}: more than {MaxRedirects} redirects starting at {Target(first.Method, first.Url)}; " +
                    "point 'base_url'/'path' at the endpoint's final location", isTransient: false);
            }

            var target = new Uri(sendUri, location);
            EnsureAllowedTarget(target, "a redirect from the endpoint", stage);
            current = current.RedirectedTo(target, redirectStatus);
        }
    }

    /// <summary>The non-2xx classification: the first matching <c>on_status</c> rule, else the built-in
    /// one -- 408/429/5xx are transient (with the response's <c>Retry-After</c>, which on a 429/503 also moves
    /// the read's shared pause), everything else permanent with a redacted body snippet.</summary>
    public PzConnectorException StatusError(SentResponse sent, string bodyText, string? stage = null)
        => StatusError(sent, bodyText, stage, MatchRule(sent, bodyText));

    /// <summary>As <see cref="StatusError(SentResponse, string, string?)"/>, with the rule the caller
    /// already matched (on text it may since have redacted) instead of matching again.</summary>
    public PzConnectorException StatusError(SentResponse sent, string bodyText, string? stage, HttpStatusRule? rule)
    {
        var prefix = Prefix(stage);
        var status = (int)sent.Response.StatusCode;
        if (rule is not null)
        {
            return RuleError(rule, sent, bodyText, prefix, status);
        }

        if (TransientClassifier.IsTransientStatus(status))
        {
            var throttled = status is 429 or 503;
            var retryAfter = RetryAfter(sent.Response, configuredFallback: throttled, out var note);
            // A throttle (429) or overload (503) hint is noted here, inside the gated operation and before
            // the exception reaches the gate, so the gate's retry of this request and every sibling
            // request of the read wait it out.
            if (throttled && retryAfter is { } hint)
            {
                pause?.Note(hint);
            }

            return new PzConnectorException($"{prefix}: HTTP {status} from {Target(sent)}{note}",
                isTransient: true, retryAfter);
        }

        return new PzConnectorException(
            $"{prefix}: HTTP {status} from {Target(sent)}: {RedactSnippet(bodyText, sent.ServerChosen)} " +
            "(check the endpoint path and auth config)", isTransient: false);
    }

    /// <summary>What a matched rule makes of the response. A <c>skip</c> rule only gets here for a request
    /// that may not be skipped (the caller handles a skippable one), so it fails the read naming the rule:
    /// falling through to another classification would hide that the rule matched where it cannot act.</summary>
    private PzConnectorException RuleError(HttpStatusRule rule, SentResponse sent, string bodyText, string prefix,
        int status)
    {
        switch (rule.Action)
        {
            case StatusAction.Transient:
            {
                // The server's own timing first, then the connection's header, then the rule's constant. A
                // rule is how a user says "this is a throttle", so its hint moves the shared pause too.
                var retryAfter = RetryAfter(sent.Response, configuredFallback: true, out var note) ?? rule.RetryAfter;
                if (retryAfter is { } hint)
                {
                    pause?.Note(hint);
                }

                return new PzConnectorException($"{prefix}: HTTP {status} from {Target(sent)} ({rule.Label}: transient){note}",
                    isTransient: true, retryAfter);
            }

            case StatusAction.Permanent:
                return new PzConnectorException(
                    $"{prefix}: HTTP {status} from {Target(sent)}: {RedactSnippet(bodyText, sent.ServerChosen)} " +
                    $"({rule.Label}: permanent)", isTransient: false);
            default:
                return new PzConnectorException(
                    $"{prefix}: HTTP {status} from {Target(sent)}: {RedactSnippet(bodyText, sent.ServerChosen)} " +
                    $"matched {rule.Label} (skip), but skip applies only to the first request of a read or of a " +
                    "for_each item, and this was a later request; narrow the rule's status/body_contains or " +
                    "use action: permanent", isTransient: false);
        }
    }

    /// <summary>A <c>skip</c> rule that matched the first request of a read carrying a watermark (a cursor,
    /// a value or an upper bound): validation could not see the incremental (a raw-mode SQL-declared one),
    /// but the read is one, and a skipped incremental read can let the watermark move past rows never read.</summary>
    public PzConnectorException SkipOnIncremental(SentResponse sent, string bodyText, string? stage, HttpStatusRule rule)
        => new($"{Prefix(stage)}: HTTP {(int)sent.Response.StatusCode} from {Target(sent)}: " +
            $"{RedactSnippet(bodyText, sent.ServerChosen)} matched {rule.Label} (skip), but skip is refused on " +
            "incremental reads, and this read carries a watermark: a skipped incremental read can let the watermark " +
            "move past rows never read; use action: permanent (fail) or transient (retry)", isTransient: false);

    /// <summary>True when the read carries any watermark the engine stamped.</summary>
    public static bool CarriesWatermark(DatasetSpec spec)
        => spec.WatermarkCursor is not null || spec.WatermarkValue is not null || spec.WatermarkUpperBound is not null;

    /// <summary>The response's <c>Retry-After</c>; when it has none usable and
    /// <paramref name="configuredFallback"/>, the wait the connection's <c>retry_after</c> header states.
    /// <paramref name="note"/> says when that header was present but unreadable.</summary>
    private TimeSpan? RetryAfter(HttpResponseMessage response, bool configuredFallback, out string note)
    {
        note = "";
        var now = time.GetUtcNow();
        var standard = TransientClassifier.ParseRetryAfter(
            response.Headers.TryGetValues("Retry-After", out var values) ? values.FirstOrDefault() : null, now);
        return standard is null && configuredFallback && connection.RetryAfter is { } configured
            ? configured.Read(response, now, out note)
            : standard;
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
    public void EnsureAllowedTarget(Uri candidate, string what, string? stage = null)
    {
        if (connection.IsAllowedTarget(candidate))
        {
            return;
        }

        throw new PzConnectorException(
            $"{Prefix(stage)}: {what} points at '{candidate.GetLeftPart(UriPartial.Authority)}', which is not " +
            $"this connection's host '{connection.BaseUrl.GetLeftPart(UriPartial.Authority)}' — pz will " +
            "not send this connection's credentials to a host the endpoint chose; if this really is " +
            "part of the same API, add the host to 'allow_hosts' on the connection", isTransient: false);
    }

    // How a request is named in a message: the redacted URL, prefixed by the method when it is not
    // GET (GET messages stay exactly as they were). Never the body.
    public string Target(HttpMethod method, Uri uri) => Target(method, uri, wholeQuery: false);

    /// <summary>How the request that produced <paramref name="sent"/> is named: a server-chosen URL
    /// loses its whole query.</summary>
    public string Target(SentResponse sent) => Target(sent.Method, sent.SendUri, sent.ServerChosen);

    private string Target(HttpMethod method, Uri uri, bool wholeQuery)
    {
        var named = wholeQuery ? RedactQuery(uri) : Redact(uri);
        return method == HttpMethod.Get ? named : $"{method.Method} {named}";
    }

    private string Prefix(string? stage) => stage is null ? label : $"{label}: {stage}";

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
    internal string RedactSnippet(string text, bool wholeQuery = false) =>
        Redact(Snippet(redactWholeQuery || wholeQuery ? StripUrlQueries(text) : text));

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
