using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Http;

namespace Pz.Connectors.Toolkit.Auth;

/// <summary>OAuth2 client-credentials settings (RFC 6749 section 4.4). <see cref="Params"/> are extra
/// token-request form fields (e.g. <c>audience</c>); the reserved protocol fields cannot be set there.</summary>
public sealed record OAuth2ClientCredentialsOptions(
    Uri TokenUrl,
    string ClientId,
    string ClientSecret,
    string? Scope,
    bool BasicClientAuth,
    IReadOnlyDictionary<string, string> Params)
{
    private static readonly string[] ReservedParams = ["grant_type", "client_id", "client_secret", "scope"];

    /// <summary>Parses the YAML <c>auth:</c> map. Every problem is appended to <paramref name="errors"/>;
    /// the result is null when any was found. The token URL must be https (plain http only on loopback)
    /// because the client secret travels in the request.</summary>
    public static OAuth2ClientCredentialsOptions? TryParse(IReadOnlyDictionary<string, object?> auth, List<string> errors)
    {
        var before = errors.Count;

        var tokenUrl = RequiredText(auth, "token_url", errors);
        var clientId = RequiredText(auth, "client_id", errors);
        var clientSecret = RequiredText(auth, "client_secret", errors);
        var scope = OptionalText(auth, "scope", errors);
        if (string.IsNullOrWhiteSpace(scope))
        {
            scope = null;
        }

        Uri? url = null;
        if (tokenUrl is not null)
        {
            if (!Uri.TryCreate(tokenUrl, UriKind.Absolute, out url)
                || !(url.Scheme == Uri.UriSchemeHttps || (url.Scheme == Uri.UriSchemeHttp && url.IsLoopback)))
            {
                errors.Add("auth.token_url must be an https URL (http is allowed only for localhost)");
                url = null;
            }
            else if (url.UserInfo.Length > 0)
            {
                errors.Add("auth.token_url must not embed credentials; put them in client_id/client_secret");
                url = null;
            }
        }

        var basic = false;
        if (OptionalText(auth, "client_auth", errors) is { } clientAuth)
        {
            switch (clientAuth)
            {
                case "body":
                    break;
                case "basic":
                    basic = true;
                    break;
                default:
                    errors.Add("auth.client_auth must be one of: body, basic");
                    break;
            }
        }

        var extra = new Dictionary<string, string>(StringComparer.Ordinal);
        if (auth.TryGetValue("params", out var paramsRaw) && paramsRaw is not null)
        {
            if (paramsRaw is IReadOnlyDictionary<string, object?> map)
            {
                var reserved = map.Keys.Where(k => ReservedParams.Contains(k)).ToList();
                if (reserved.Count > 0)
                {
                    errors.Add($"auth.params cannot set reserved names: {string.Join(", ", reserved)}");
                }

                foreach (var (key, value) in map.Where(kv => !ReservedParams.Contains(kv.Key)))
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
            }
            else
            {
                errors.Add("auth.params must be a map of names to scalar values");
            }
        }

        return errors.Count > before ? null : new OAuth2ClientCredentialsOptions(url!, clientId!, clientSecret!, scope, basic, extra);
    }

    private static string? RequiredText(IReadOnlyDictionary<string, object?> auth, string key, List<string> errors)
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

    // Null when absent or blank; a list or map is an error rather than a stringified CLR type name.
    private static string? OptionalText(IReadOnlyDictionary<string, object?> auth, string key, List<string> errors)
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

/// <summary>Fetches bearer tokens with the OAuth2 client-credentials grant. Owns its HttpClient (no
/// redirects: the secret must not follow a Location). Failure messages carry only the endpoint's
/// scheme://authority, the status and the server's OAuth error fields, never the secret or token.</summary>
public sealed class OAuth2ClientCredentialsAuthenticator : CachedTokenAuthenticator
{
    private const int MaxDescription = 200;
    private const long MaxExpiresInSeconds = 10 * 365 * 24 * 3600L;

    private readonly OAuth2ClientCredentialsOptions _options;
    private readonly TimeProvider _time;
    private readonly HttpClient _http;
    private readonly string _endpoint;

    public OAuth2ClientCredentialsAuthenticator(OAuth2ClientCredentialsOptions options, TimeSpan timeout, TimeProvider time)
        : base(time)
    {
        _options = options;
        _time = time;
        _endpoint = options.TokenUrl.GetComponents(UriComponents.SchemeAndServer, UriFormat.UriEscaped);
        _http = new HttpClient(new SocketsHttpHandler { AllowAutoRedirect = false }) { Timeout = timeout };
    }

    public override void Dispose()
    {
        base.Dispose();
        _http.Dispose();
    }

    protected override async ValueTask<FetchedToken> FetchTokenAsync(CancellationToken ct)
    {
        var form = new List<KeyValuePair<string, string>> { new("grant_type", "client_credentials") };
        if (_options.Scope is { } scope)
        {
            form.Add(new("scope", scope));
        }

        form.AddRange(_options.Params);

        using var request = new HttpRequestMessage(HttpMethod.Post, _options.TokenUrl);
        if (_options.BasicClientAuth)
        {
            var credentials = $"{WebUtility.UrlEncode(_options.ClientId)}:{WebUtility.UrlEncode(_options.ClientSecret)}";
            request.Headers.Authorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes(credentials)));
        }
        else
        {
            form.Add(new("client_id", _options.ClientId));
            form.Add(new("client_secret", _options.ClientSecret));
        }

        request.Content = new FormUrlEncodedContent(form);

        HttpResponseMessage response;
        string body;
        try
        {
            response = await _http.SendAsync(request, ct).ConfigureAwait(false);
            using (response)
            {
                body = await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
                return Interpret(response, body);
            }
        }
        catch (Exception ex) when (ex is HttpRequestException or IOException or TaskCanceledException && !ct.IsCancellationRequested)
        {
            throw new PzConnectorException($"token endpoint {_endpoint} could not be reached ({ex.GetType().Name})", isTransient: true);
        }
    }

    private FetchedToken Interpret(HttpResponseMessage response, string body)
    {
        var status = (int)response.StatusCode;
        if (status is >= 200 and < 300)
        {
            return ParseToken(body);
        }

        if (status is >= 300 and < 400)
        {
            throw new PzConnectorException(
                $"token endpoint {_endpoint} redirected (HTTP {status}); redirects are not followed", isTransient: false);
        }

        var message = new StringBuilder($"token endpoint {_endpoint} responded HTTP {status}");
        var (error, description) = ReadOAuthError(body);
        if (error is not null)
        {
            message.Append(": ").Append(error);
            if (description is not null)
            {
                message.Append(" - ").Append(description);
            }
        }

        if (error == "invalid_scope" && _options.Scope is { } scope)
        {
            message.Append(" (configured scope: ").Append(scope).Append(')');
        }

        // Every 5xx from the token endpoint is retryable, not only the shared classifier's subset.
        var transient = status is 408 or 429 or (>= 500 and <= 599);
        TimeSpan? retryAfter = null;
        if (transient && response.Headers.TryGetValues("Retry-After", out var values))
        {
            retryAfter = TransientClassifier.ParseRetryAfter(values.FirstOrDefault(), _time.GetUtcNow());
        }

        throw new PzConnectorException(message.ToString(), transient, retryAfter);
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
        catch (JsonException)
        {
            // not JSON: no OAuth error fields to report
        }

        return (null, null);
    }

    private FetchedToken ParseToken(string body)
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
                $"token endpoint {_endpoint} answered 200 without a usable access_token", isTransient: false);
        }

        if (obj!["token_type"] is JsonValue tt && tt.TryGetValue<string>(out var type)
            && !string.Equals(type, "Bearer", StringComparison.OrdinalIgnoreCase))
        {
            throw new PzConnectorException(
                $"token endpoint {_endpoint} issued token_type '{type}'; only Bearer is supported", isTransient: false);
        }

        DateTimeOffset? expiresOn = null;
        if (ExpiresInSeconds(obj["expires_in"]) is { } seconds and > 0)
        {
            expiresOn = _time.GetUtcNow() + TimeSpan.FromSeconds(Math.Min(seconds, MaxExpiresInSeconds));
        }

        return new FetchedToken(token, expiresOn);
    }

    private static double? ExpiresInSeconds(JsonNode? node)
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
