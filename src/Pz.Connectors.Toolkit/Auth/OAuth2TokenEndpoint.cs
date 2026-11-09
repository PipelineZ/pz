using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Authentication;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Http;

namespace Pz.Connectors.Toolkit.Auth;

/// <summary>The YAML field rules every OAuth2 grant shares: scalar-only values, the https token URL, the
/// client-authentication style and the extra form <c>params</c>.</summary>
internal static class OAuth2Fields
{
    /// <summary>The token URL must be https (plain http only on loopback) because the request carries a
    /// client secret or a refresh token; credentials in the URL itself are refused.</summary>
    public static Uri? TokenUrl(IReadOnlyDictionary<string, object?> auth, List<string> errors)
    {
        if (RequiredText(auth, "token_url", errors) is not { } tokenUrl)
        {
            return null;
        }

        if (!Uri.TryCreate(tokenUrl, UriKind.Absolute, out var url)
            || !(url.Scheme == Uri.UriSchemeHttps || (url.Scheme == Uri.UriSchemeHttp && url.IsLoopback)))
        {
            errors.Add("auth.token_url must be an https URL (http is allowed only for localhost)");
            return null;
        }

        if (url.UserInfo.Length > 0)
        {
            errors.Add("auth.token_url must not embed credentials; put them in client_id/client_secret");
            return null;
        }

        return url;
    }

    /// <summary>True for <c>client_auth: basic</c>; false for <c>body</c> (the default) and for an error.</summary>
    public static bool BasicClientAuth(IReadOnlyDictionary<string, object?> auth, List<string> errors)
    {
        switch (OptionalText(auth, "client_auth", errors))
        {
            case null or "body":
                return false;
            case "basic":
                return true;
            default:
                errors.Add("auth.client_auth must be one of: body, basic");
                return false;
        }
    }

    /// <summary>The extra form fields; a <paramref name="reserved"/> protocol field cannot be set here.</summary>
    public static Dictionary<string, string> Params(IReadOnlyDictionary<string, object?> auth, IReadOnlyCollection<string> reserved,
        List<string> errors)
    {
        var extra = new Dictionary<string, string>(StringComparer.Ordinal);
        if (!auth.TryGetValue("params", out var paramsRaw) || paramsRaw is null)
        {
            return extra;
        }

        if (paramsRaw is not IReadOnlyDictionary<string, object?> map)
        {
            errors.Add("auth.params must be a map of names to scalar values");
            return extra;
        }

        var refused = map.Keys.Where(reserved.Contains).ToList();
        if (refused.Count > 0)
        {
            errors.Add($"auth.params cannot set reserved names: {string.Join(", ", refused)}");
        }

        foreach (var (key, value) in map.Where(kv => !reserved.Contains(kv.Key)))
        {
            if (NonScalarKind(value) is { } kind)
            {
                errors.Add($"auth.params.{key} must be a single value, not a {kind}");
            }
            else if (ScalarText(value) is { } text)
            {
                extra[key] = text;
            }
            else
            {
                errors.Add($"auth.params.{key} must be a scalar value");
            }
        }

        return extra;
    }

    public static string? RequiredText(IReadOnlyDictionary<string, object?> auth, string key, List<string> errors)
    {
        auth.TryGetValue(key, out var raw);
        if (NonScalarKind(raw) is { } kind)
        {
            errors.Add($"auth.{key} must be a single value, not a {kind}");
            return null;
        }

        var text = ScalarText(raw);
        if (string.IsNullOrWhiteSpace(text))
        {
            errors.Add($"auth.{key} is required");
            return null;
        }

        return text;
    }

    /// <summary>Null when absent or blank; a list or map is an error rather than a stringified CLR type name.</summary>
    public static string? OptionalText(IReadOnlyDictionary<string, object?> auth, string key, List<string> errors)
    {
        auth.TryGetValue(key, out var raw);
        if (NonScalarKind(raw) is { } kind)
        {
            errors.Add($"auth.{key} must be a single value, not a {kind}");
            return null;
        }

        return ScalarText(raw) is { } text && !string.IsNullOrWhiteSpace(text) ? text : null;
    }

    private static string? NonScalarKind(object? value) => value switch
    {
        System.Collections.IDictionary or IReadOnlyDictionary<string, object?> => "map",
        System.Collections.IEnumerable and not string => "list",
        _ => null,
    };

    // YAML values arrive typed; they go on the wire as text, so bools are lowercase and numbers invariant.
    private static string? ScalarText(object? value) => value switch
    {
        null => null,
        string s => s,
        bool b => b ? "true" : "false",
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => null,
    };
}

/// <summary>One OAuth2 token endpoint: posts a grant's form with the client's credentials and turns the
/// answer into a bearer token or a classified <see cref="PzConnectorException"/>. Owns its HttpClient (no
/// redirects: the secret must not follow a Location). Failure messages carry only the endpoint's
/// scheme://authority, the status and the server's OAuth error fields, never a secret or token.</summary>
internal sealed class OAuth2TokenEndpoint : IDisposable
{
    private const int MaxDescription = 200;
    private const long MaxExpiresInSeconds = 10 * 365 * 24 * 3600L;

    private readonly Uri _tokenUrl;
    private readonly string _clientId;
    private readonly string? _clientSecret;
    private readonly bool _basic;
    private readonly string? _scope;
    private readonly TimeProvider _time;
    private readonly HttpClient _http;

    /// <param name="handler">The transport (proxy, trust anchors, client certificate); owned and disposed here.
    /// It must not follow redirects. Null builds the default handler, which follows none.</param>
    public OAuth2TokenEndpoint(Uri tokenUrl, string clientId, string? clientSecret, bool basic, string? scope,
        TimeSpan timeout, TimeProvider time, HttpMessageHandler? handler)
    {
        _tokenUrl = tokenUrl;
        _clientId = clientId;
        _clientSecret = clientSecret;
        _basic = basic;
        _scope = scope;
        _time = time;
        Endpoint = tokenUrl.GetComponents(UriComponents.SchemeAndServer, UriFormat.UriEscaped);
        _http = new HttpClient(handler ?? new SocketsHttpHandler { AllowAutoRedirect = false }) { Timeout = timeout };
    }

    /// <summary>The endpoint's scheme://authority: the only part of the URL a message names.</summary>
    public string Endpoint { get; }

    public void Dispose() => _http.Dispose();

    /// <summary>Posts <paramref name="grant"/> (its first field must be <c>grant_type</c>; <c>scope</c> is added when set) with the client credentials. On a
    /// 2xx returns the bearer token and the whole response object, for grant-specific checks.
    /// <paramref name="errorHints"/> appends a next step to a non-2xx answer with that OAuth <c>error</c>.</summary>
    /// <param name="terminalErrors">OAuth <c>error</c> values that mean the credential is refused for good; the
    /// exception is marked terminal (<see cref="CachedTokenAuthenticator.MarkTerminal"/>).</param>
    /// <param name="secrets">Grant values (a refresh token) removed from any server text put in a message,
    /// together with the client secret.</param>
    public async ValueTask<(FetchedToken Token, JsonObject Response)> RequestAsync(
        IEnumerable<KeyValuePair<string, string>> grant, IReadOnlyDictionary<string, string>? errorHints, CancellationToken ct,
        IReadOnlyCollection<string>? terminalErrors = null, IReadOnlyCollection<string>? secrets = null)
    {
        // grant_type first, then scope, then the grant's other fields: the order the wire has always had.
        var fields = grant.ToList();
        var form = new List<KeyValuePair<string, string>> { fields[0] };
        if (_scope is { } scope)
        {
            form.Add(new("scope", scope));
        }

        form.AddRange(fields.Skip(1));

        using var request = new HttpRequestMessage(HttpMethod.Post, _tokenUrl);
        if (_basic)
        {
            var credentials = $"{WebUtility.UrlEncode(_clientId)}:{WebUtility.UrlEncode(_clientSecret ?? "")}";
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes(credentials)));
        }
        else
        {
            form.Add(new("client_id", _clientId));
            if (_clientSecret is { } secret)
            {
                form.Add(new("client_secret", secret));
            }
        }

        request.Content = new FormUrlEncodedContent(form);
        try
        {
            using var response = await _http.SendAsync(request, ct).ConfigureAwait(false);
            var body = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            return Interpret(response, body, errorHints, terminalErrors, secrets ?? []);
        }
        catch (Exception ex) when (ex is HttpRequestException or IOException or TaskCanceledException && !ct.IsCancellationRequested)
        {
            throw Unreachable(ex);
        }
    }

    // A TLS verdict (an AuthenticationException anywhere in the chain) and a proxy's explicit 407 do not
    // change on retry, so they are permanent; plain I/O, even during a handshake, stays transient.
    private PzConnectorException Unreachable(Exception ex)
    {
        for (var current = ex; current is not null; current = current.InnerException)
        {
            if (current is AuthenticationException)
            {
                return new PzConnectorException($"token endpoint {Endpoint}: TLS failed (check tls.ca_file / tls.client_certificate)",
                    isTransient: false, innerException: ex);
            }
        }

        if (ex is HttpRequestException { StatusCode: HttpStatusCode.ProxyAuthenticationRequired })
        {
            return new PzConnectorException($"token endpoint {Endpoint}: proxy authentication failed (check proxy.user/proxy.password)",
                isTransient: false, innerException: ex);
        }

        return new PzConnectorException($"token endpoint {Endpoint} could not be reached ({ex.GetType().Name})",
            isTransient: true, innerException: ex);
    }

    private (FetchedToken, JsonObject) Interpret(HttpResponseMessage response, string body,
        IReadOnlyDictionary<string, string>? errorHints, IReadOnlyCollection<string>? terminalErrors,
        IReadOnlyCollection<string> secrets)
    {
        var status = (int)response.StatusCode;
        if (status is >= 200 and < 300)
        {
            return ParseToken(body);
        }

        if (status is >= 300 and < 400)
        {
            throw new PzConnectorException(
                $"token endpoint {Endpoint} redirected (HTTP {status}); redirects are not followed", isTransient: false);
        }

        var message = new StringBuilder($"token endpoint {Endpoint} responded HTTP {status}");
        var (error, description) = ReadOAuthError(body);
        var known = error;
        error = Scrub(error, secrets);
        description = Scrub(description, secrets);
        if (error is not null)
        {
            message.Append(": ").Append(error);
            if (description is not null)
            {
                message.Append(" - ").Append(description);
            }
        }

        if (error == "invalid_scope" && _scope is { } scope)
        {
            message.Append(" (configured scope: ").Append(scope).Append(')');
        }

        if (error is not null && errorHints is not null && errorHints.TryGetValue(error, out var hint))
        {
            message.Append("; ").Append(hint);
        }

        // Every 5xx from the token endpoint is retryable, not only the shared classifier's subset.
        var transient = status is 408 or 429 or (>= 500 and <= 599);
        TimeSpan? retryAfter = null;
        if (transient && response.Headers.TryGetValues("Retry-After", out var values))
        {
            retryAfter = TransientClassifier.ParseRetryAfter(values.FirstOrDefault(), _time.GetUtcNow());
        }

        var failure = new PzConnectorException(message.ToString(), transient, retryAfter);
        throw known is not null && !transient && terminalErrors is not null && terminalErrors.Contains(known)
            ? CachedTokenAuthenticator.MarkTerminal(failure)
            : failure;
    }

    // A provider may echo what it was sent ("refresh token 'abc' is invalid"); the secrets go before the text does.
    private string? Scrub(string? text, IReadOnlyCollection<string> secrets)
    {
        if (text is null)
        {
            return null;
        }

        foreach (var secret in secrets.Append(_clientSecret))
        {
            if (!string.IsNullOrEmpty(secret))
            {
                text = text.Replace(secret, "***", StringComparison.Ordinal)
                    .Replace(Uri.EscapeDataString(secret), "***", StringComparison.Ordinal)
                    .Replace(WebUtility.UrlEncode(secret), "***", StringComparison.Ordinal);
            }
        }

        return text;
    }

    private static (string? Error, string? Description) ReadOAuthError(string body)
    {
        try
        {
            if (JsonNode.Parse(body) is JsonObject obj)
            {
                var error = obj["error"] is JsonValue e && e.TryGetValue<string>(out var es) ? es : null;
                var description = obj["error_description"] is JsonValue d && d.TryGetValue<string>(out var ds) ? ds : null;
                if (description is { Length: > MaxDescription })
                {
                    description = description[..MaxDescription];
                }

                return (error, description);
            }
        }
        catch (Exception ex) when (ex is JsonException or ArgumentException or InvalidOperationException)
        {
            // not JSON, or JSON with duplicate keys: no OAuth error fields to report
        }

        return (null, null);
    }

    private (FetchedToken, JsonObject) ParseToken(string body)
    {
        try
        {
            return ParseTokenFields(body);
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException)
        {
            // A JSON object with duplicate keys throws only when a property is read.
            throw new PzConnectorException(
                $"token endpoint {Endpoint} answered 200 with a JSON response that could not be read (duplicate keys)",
                isTransient: false);
        }
    }

    private (FetchedToken, JsonObject) ParseTokenFields(string body)
    {
        JsonObject? obj;
        try
        {
            obj = JsonNode.Parse(body) as JsonObject;
        }
        catch (JsonException)
        {
            obj = null;
        }

        var token = obj?["access_token"] is JsonValue t && t.TryGetValue<string>(out var ts) ? ts : null;
        if (string.IsNullOrEmpty(token))
        {
            throw new PzConnectorException(
                $"token endpoint {Endpoint} answered 200 without a usable access_token", isTransient: false);
        }

        if (obj!["token_type"] is JsonValue tt && tt.TryGetValue<string>(out var type)
            && !string.Equals(type, "Bearer", StringComparison.OrdinalIgnoreCase))
        {
            throw new PzConnectorException(
                $"token endpoint {Endpoint} issued token_type '{type}'; only Bearer is supported", isTransient: false);
        }

        DateTimeOffset? expiresOn = null;
        if (ExpiresInSeconds(obj["expires_in"]) is { } seconds and > 0)
        {
            expiresOn = _time.GetUtcNow() + TimeSpan.FromSeconds(Math.Min(seconds, MaxExpiresInSeconds));
        }

        return (new FetchedToken(token, expiresOn), obj);
    }

    /// <summary>A lifetime in seconds from a JSON number or a numeric string; null for anything else.</summary>
    internal static double? ExpiresInSeconds(JsonNode? node)
    {
        if (node is not JsonValue v)
        {
            return null;
        }

        if (v.TryGetValue<double>(out var d))
        {
            return d;
        }

        if (v.TryGetValue<string>(out var s)
            && double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed;
        }

        return null;
    }
}
