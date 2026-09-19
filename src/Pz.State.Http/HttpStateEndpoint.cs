using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using Pz.Core.Validation;

namespace Pz.State.Http;

/// <summary>One HTTP round trip, already read to completion: status, body, and the version the
/// server reported in its <c>ETag</c> (null when it sent none). Returning this rather than an
/// <see cref="HttpResponseMessage"/> keeps every disposable inside
/// <see cref="HttpStateEndpoint.Send"/>, so <see cref="HttpKeyedStateStore{T}"/> never touches
/// <c>System.Net.Http</c> types.</summary>
public sealed record StateResponse(HttpStatusCode Status, string Body, int? Version);

/// <summary>The transport half of the HTTP state
/// backend. Holds the run-scoped base URL the agent handed us plus the optional bearer token, builds
/// <c>{base}/{scope}[/{key}]</c>, and maps every transport-level failure onto PZ0518 ("never got a
/// response") or a received-but-unexpected response onto PZ0529 ("reached it, the request failed") --
/// the same division of labour <see cref="Pz.State.SqlServer"/> splits between
/// <c>SqlStateConnection.Unavailable</c>/<c>QueryFailed</c>.
///
/// **The base URL is supplied whole, never composed.** It already carries the server's run id
/// (<c>/api/agents/runs/{id}/state</c>), which resolves (project, environment) server-side. pz's own
/// run id is a different identifier entirely, so pz cannot build that path -- the agent passes it in
/// via <c>PZ_STATE_URL</c>.
///
/// **Requests are synchronous on purpose.** <see cref="HttpClient.Send(HttpRequestMessage)"/> is a
/// real blocking send (not <c>GetAwaiter().GetResult()</c> over the async path), so
/// <see cref="Pz.Engine.State.IKeyedStateStore{T}"/> stays synchronous exactly as the SQL
/// backend's blocking <c>ExecuteReader</c> does.
///
/// **Retry.** A <c>429</c>/<c>503</c> response -- and, for anything but a versioned PUT, a
/// <c>502</c>/<c>504</c> -- is retried up to
/// <see cref="_maxAttempts"/> times, honouring a server <c>Retry-After</c> up to
/// <see cref="MaxRetryDelay"/> (never longer -- a server asking for minutes is a reason to give up and
/// let the caller decide, not to block a run's node indefinitely), through the constructor's
/// <see cref="TimeProvider"/> so a test can prove the retry count and delay deterministically. The
/// wait ends early when the run is cancelled.
///
/// Secret hygiene: the token travels in an <c>Authorization</c> header and never reaches an error
/// message; failures name the host and the path only.</summary>
public sealed class HttpStateEndpoint : IDisposable
{
    /// <summary>The server itself refused to start the request, so nothing was applied.</summary>
    private static readonly HashSet<HttpStatusCode> NotAppliedStatuses =
    [
        (HttpStatusCode)429, HttpStatusCode.ServiceUnavailable,
    ];

    /// <summary>A gateway in between gave up; whether the server behind it applied the request is
    /// unknown.</summary>
    private static readonly HashSet<HttpStatusCode> OutcomeUnknownStatuses =
    [
        HttpStatusCode.BadGateway, HttpStatusCode.GatewayTimeout,
    ];

    private static readonly TimeSpan RetryBaseDelay = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan MaxRetryDelay = TimeSpan.FromSeconds(30);

    private readonly string _root;
    private readonly Uri _rootUri;
    private readonly HttpClient _client = new();
    private readonly TimeProvider _time;
    private readonly int _maxAttempts;
    private readonly CancellationToken _ct;

    /// <summary><paramref name="timeout"/> null keeps <see cref="HttpClient"/>'s own 100s default
    /// (<c>state.timeout_seconds</c> absent -- unchanged behaviour). <paramref name="ct"/> is this
    /// endpoint's run's cancellation token, captured once: one instance serves one run (the class doc's
    /// "lives exactly as long as the process that built it"), so there is no per-call token to thread
    /// through <see cref="Pz.Engine.State.IKeyedStateStore{T}"/>'s synchronous, cancellation-unaware
    /// surface -- this is what lets Ctrl-C actually abort a state request instead of the fixed 100s
    /// timeout being the only way out.</summary>
    public HttpStateEndpoint(string url, string? token, TimeProvider? timeProvider = null, int maxAttempts = 3,
        TimeSpan? timeout = null, CancellationToken ct = default)
    {
        _root = url.TrimEnd('/');
        _rootUri = new Uri(_root, UriKind.Absolute);
        _time = timeProvider ?? TimeProvider.System;
        _maxAttempts = maxAttempts;
        _ct = ct;

        if (timeout is { } t)
        {
            _client.Timeout = t;
        }

        if (!string.IsNullOrWhiteSpace(token))
        {
            // Optional by design: a server may serve these endpoints unauthenticated and ignore the
            // header, so an absent token must not be an error -- but the header is sent the moment one
            // is configured, which keeps adding authentication a server-side change only.
            _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
        }
    }

    /// <summary>One logical request, transparently retried while the response keeps coming back
    /// retryable. <paramref name="key"/> null addresses the whole scope (the list endpoint); otherwise
    /// it is percent-encoded into a single path segment -- the wire contract forbids a raw <c>/</c> in a
    /// key and requires <c>%</c> and <c>#</c> to be encoded.</summary>
    public StateResponse Send(HttpMethod method, string scope, string? key,
        string? payload = null, int? ifMatch = null)
    {
        var path = key is null ? $"{_root}/{scope}" : $"{_root}/{scope}/{Uri.EscapeDataString(key)}";

        for (var attempt = 1; ; attempt++)
        {
            try
            {
                using var request = new HttpRequestMessage(method, path);
                if (payload is not null)
                {
                    // Explicitly BOM-free UTF-8: the contract promises byte-exact round-tripping, and pz's
                    // KeyedJsonStateStore has a byte-stability contract with golden files.
                    request.Content = new StringContent(payload, new UTF8Encoding(false), "application/json");
                }

                if (ifMatch is { } expected)
                {
                    request.Headers.TryAddWithoutValidation("If-Match", Tag(expected));
                }

                using var response = _client.Send(request, HttpCompletionOption.ResponseContentRead, _ct);

                if (IsRetryable(method, response.StatusCode) && attempt < _maxAttempts)
                {
                    Delay(attempt, response);
                    continue;
                }

                using var reader = new StreamReader(response.Content.ReadAsStream(), new UTF8Encoding(false));
                return new StateResponse(response.StatusCode, reader.ReadToEnd(), ReadVersion(response));
            }
            catch (OperationCanceledException) when (_ct.IsCancellationRequested)
            {
                // The run's own cancellation, not a request timeout: propagates uncaught, exactly like a
                // cancellation from anywhere else in the run (KindDispatchingExecutor's contract) --
                // never wrapped into a PzConfigException the dispatcher would otherwise have to unwrap
                // to tell "cancelled" apart from "genuinely failed".
                throw;
            }
            catch (TaskCanceledException)
            {
                // Not the run's token (handled above), so this is the client's own timeout expiring.
                throw TimedOut();
            }
            catch (Exception ex) when (ex is HttpRequestException or IOException or InvalidOperationException)
            {
                throw Unavailable(ex.GetType().Name);
            }
        }
    }

    /// <summary>The transport worked -- a response came back -- but it is not one the contract expects
    /// (or the URL points at something that is not this run's state resource). PZ0529: the store IS
    /// reachable, only this request failed, unlike <see cref="Unavailable"/>.</summary>
    public PzConfigException Unexpected(HttpStatusCode status, string scope, string? key)
    {
        var what = key is null ? $"scope '{scope}'" : $"key '{key}' (scope '{scope}')";
        var hint = status is HttpStatusCode.NotFound
            ? "check PZ_STATE_URL / state.url -- it must be the run-scoped state URL " +
              "(.../api/agents/runs/{id}/state) for a run the server knows"
            : "check PZ_STATE_URL / state.url and PZ_STATE_TOKEN, and that the server is healthy";

        return new PzConfigException(new PzError(PzErrorCode.StateQueryFailed,
            $"the state store at '{Host}' answered {(int)status} {status} for {what}.",
            "project.yml", null, hint));
    }

    public PzConfigException Unavailable(string cause) =>
        new(new PzError(PzErrorCode.StateStoreUnavailable,
            $"cannot reach the state store at '{Host}': {cause}.",
            "project.yml", null,
            "check PZ_STATE_URL / state.url, and that the server is reachable from this host"));

    /// <summary>A versioned PUT is the one request that is not safe to replay blind: had the first one
    /// been applied, the replay would meet its own write and read it as another run's (PZ0520). A read
    /// and a tombstoning delete give the same answer however often they are sent.</summary>
    private static bool IsRetryable(HttpMethod method, HttpStatusCode status) =>
        NotAppliedStatuses.Contains(status) ||
        (OutcomeUnknownStatuses.Contains(status) && method != HttpMethod.Put);

    private PzConfigException TimedOut() =>
        new(new PzError(PzErrorCode.StateStoreUnavailable,
            string.Create(CultureInfo.InvariantCulture,
                $"the state store at '{Host}' timed out after {_client.Timeout.TotalSeconds:0.###}s."),
            "project.yml", null,
            "check that the server is healthy and reachable from this host, or raise state.timeout_seconds / " +
            "PZ_STATE_TIMEOUT_SECONDS if it is only slow"));

    private void Delay(int attempt, HttpResponseMessage response)
    {
        var retryAfter = ParseRetryAfter(response) is { } serverDelay
            ? serverDelay
            : RetryBaseDelay * attempt;
        var bounded = retryAfter > MaxRetryDelay ? MaxRetryDelay : retryAfter;
        Task.Delay(bounded, _time, _ct).GetAwaiter().GetResult();
    }

    /// <summary>Delta-seconds only (<c>Retry-After: 3</c>) -- the HTTP-date form exists for browser
    /// redirect targets, not the state server's own retry hint, so it is not worth the extra parsing
    /// surface here. A missing or unparsable header (or a negative value) falls back to the base delay
    /// in <see cref="Delay"/>.</summary>
    private static TimeSpan? ParseRetryAfter(HttpResponseMessage response) =>
        response.Headers.RetryAfter?.Delta is { } delta && delta > TimeSpan.Zero ? delta : null;

    private string Host => _rootUri.GetLeftPart(UriPartial.Authority);

    private static string Tag(int version) =>
        string.Create(CultureInfo.InvariantCulture, $"\"{version}\"");

    /// <summary>The server's <c>ETag</c> holds a 1-based version. Read leniently: a missing or
    /// unparseable tag is null, which downgrades the next write to insert-if-absent rather than sending
    /// a fabricated expected version. A weak tag (<c>W/"3"</c> -- common once a reverse proxy's gzip
    /// layer sits in front of the state server) carries the same version as the strong form; the
    /// weakness marker means only "byte-for-byte identity is not guaranteed", irrelevant to a version
    /// counter pz never compares byte-for-byte. Stripped before parsing rather than rejected, which used
    /// to read as null and spuriously downgrade every write behind such a proxy to insert-if-absent --
    /// PZ0520 on every run.</summary>
    private static int? ReadVersion(HttpResponseMessage response)
    {
        if (!response.Headers.TryGetValues("ETag", out var values))
        {
            return null;
        }

        var raw = values.FirstOrDefault()?.Trim();
        if (raw is null)
        {
            return null;
        }

        if (raw.StartsWith("W/", StringComparison.Ordinal))
        {
            raw = raw[2..];
        }

        if (raw.Length < 3 || raw[0] != '"' || raw[^1] != '"')
        {
            return null;
        }

        return int.TryParse(raw[1..^1], NumberStyles.None, CultureInfo.InvariantCulture, out var version)
            && version > 0
            ? version
            : null;
    }

    public void Dispose() => _client.Dispose();
}
