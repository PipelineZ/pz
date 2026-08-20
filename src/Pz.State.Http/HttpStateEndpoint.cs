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
/// <c>{base}/{scope}[/{key}]</c>, and maps every transport-level failure onto PZ0518 -- the same
/// division of labour <see cref="Pz.State.SqlServer"/> splits between <c>SqlStateConnection</c> and
/// <c>SqlKeyedStateStore</c>.
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
/// Secret hygiene: the token travels in an <c>Authorization</c> header and never reaches an error
/// message; failures name the host and the path only.</summary>
public sealed class HttpStateEndpoint : IDisposable
{
    private readonly string _root;
    private readonly Uri _rootUri;
    private readonly HttpClient _client = new();

    public HttpStateEndpoint(string url, string? token)
    {
        _root = url.TrimEnd('/');
        _rootUri = new Uri(_root, UriKind.Absolute);

        if (!string.IsNullOrWhiteSpace(token))
        {
            // Optional by design: a server may serve these endpoints unauthenticated and ignore the
            // header, so an absent token must not be an error -- but the header is sent the moment one
            // is configured, which keeps adding authentication a server-side change only.
            _client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
        }
    }

    /// <summary>One request. <paramref name="key"/> null addresses the whole scope (the list
    /// endpoint); otherwise it is percent-encoded into a single path segment -- the wire contract
    /// forbids a raw <c>/</c> in a key and requires <c>%</c> and <c>#</c> to be encoded.</summary>
    public StateResponse Send(HttpMethod method, string scope, string? key,
        string? payload = null, int? ifMatch = null)
    {
        var path = key is null ? $"{_root}/{scope}" : $"{_root}/{scope}/{Uri.EscapeDataString(key)}";

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

            using var response = _client.Send(request, HttpCompletionOption.ResponseContentRead);
            using var reader = new StreamReader(response.Content.ReadAsStream(), new UTF8Encoding(false));
            return new StateResponse(response.StatusCode, reader.ReadToEnd(), ReadVersion(response));
        }
        catch (Exception ex) when (ex is HttpRequestException or IOException
            or TaskCanceledException or InvalidOperationException)
        {
            throw Unavailable(ex.GetType().Name);
        }
    }

    /// <summary>The transport worked but said something the contract does not allow (or the URL points
    /// at something that is not this run's state resource). PZ0518 for the same reason the SQL backend
    /// uses it: the state store did not answer, so nothing downstream may assume "no watermark".</summary>
    public PzConfigException Unexpected(HttpStatusCode status, string scope, string? key)
    {
        var what = key is null ? $"scope '{scope}'" : $"key '{key}' (scope '{scope}')";
        var hint = status is HttpStatusCode.NotFound
            ? "check PZ_STATE_URL / state.url -- it must be the run-scoped state URL " +
              "(.../api/agents/runs/{id}/state) for a run the server knows"
            : "check PZ_STATE_URL / state.url and PZ_STATE_TOKEN, and that the server is healthy";

        return new PzConfigException(new PzError(PzErrorCode.StateStoreUnavailable,
            $"the state store at '{Host}' answered {(int)status} {status} for {what}.",
            "project.yml", null, hint));
    }

    public PzConfigException Unavailable(string cause) =>
        new(new PzError(PzErrorCode.StateStoreUnavailable,
            $"cannot reach the state store at '{Host}': {cause}.",
            "project.yml", null,
            "check PZ_STATE_URL / state.url, and that the server is reachable from this host"));

    private string Host => _rootUri.GetLeftPart(UriPartial.Authority);

    private static string Tag(int version) =>
        string.Create(CultureInfo.InvariantCulture, $"\"{version}\"");

    /// <summary>The server's <c>ETag</c> is a strong tag holding a 1-based version. Read leniently:
    /// a missing or unparseable tag is null, which downgrades the next write to insert-if-absent
    /// rather than sending a fabricated expected version.</summary>
    private static int? ReadVersion(HttpResponseMessage response)
    {
        if (!response.Headers.TryGetValues("ETag", out var values))
        {
            return null;
        }

        var raw = values.FirstOrDefault()?.Trim();
        if (raw is null || raw.Length < 3 || raw[0] != '"' || raw[^1] != '"')
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
