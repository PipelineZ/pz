using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Authentication;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Auth;
using Pz.Connectors.Toolkit.Http;
using Pz.Connectors.Toolkit.Json;
using Pz.Connectors.Toolkit.Paging;

namespace Pz.Connector.Http;

/// <summary>Where a session token goes on each API request: <c>Authorization: Bearer</c> (both null), a named
/// header with an optional prefix, or a query parameter.</summary>
internal sealed record SessionInject(string? Header, string Prefix, string? Query);

/// <summary><c>auth.type: session</c>: a login request whose JSON answer holds a token. The body is literal
/// (no bindings) and rendered once here; it holds the password, so nothing in it ever reaches a message.</summary>
internal sealed record SessionOptions(
    Uri LoginUrl,
    HttpMethod Method,
    string? Body,
    string? MediaType,
    IReadOnlyList<KeyValuePair<string, string>> Headers,
    string TokenPointer,
    SessionInject Inject,
    TimeSpan? Ttl,
    string? ExpiresInPointer,
    IReadOnlySet<int> ReauthStatus,
    Uri? BaseUrl)
{
    /// <summary>The largest login response read; a session answer is a few hundred bytes.</summary>
    public const int MaxLoginResponseBytes = 1024 * 1024;

    private static readonly TimeSpan MinTtl = TimeSpan.FromSeconds(1);

    /// <summary>The longest <c>ttl</c>: a session that outlives a month is a fixed credential in disguise.</summary>
    public static readonly TimeSpan MaxTtl = TimeSpan.FromDays(30);

    /// <summary>The statuses that mean "this session expired" when <c>reauth_status</c> is not set.</summary>
    public static readonly IReadOnlySet<int> DefaultReauthStatus = new HashSet<int> { 401 };

    /// <summary>Parses the YAML <c>auth:</c> map. Every problem is appended to <paramref name="errors"/>; the
    /// result is null when any was found, or when <paramref name="baseUrl"/> is null and <c>login.path</c>
    /// needs it (the missing base_url is reported on its own). Whether the login host is allowed is checked by
    /// the caller, once <c>allow_hosts</c> is known.</summary>
    public static SessionOptions? TryParse(IReadOnlyDictionary<string, object?> auth, Uri? baseUrl, List<string> errors)
    {
        var before = errors.Count;

        Uri? loginUrl = null;
        var method = HttpMethod.Post;
        string? body = null;
        string? mediaType = null;
        var headers = new List<KeyValuePair<string, string>>();
        auth.TryGetValue("login", out var rawLogin);
        if (rawLogin is null)
        {
            errors.Add("auth.login is required (a map with 'path' or 'url', and usually 'body')");
        }
        else if (rawLogin is not IReadOnlyDictionary<string, object?> login)
        {
            errors.Add("auth.login must be a map (path or url, method, body, body_format, content_type, headers)");
        }
        else
        {
            var known = new[] { "path", "url", "method", "body", "body_format", "content_type", "headers" };
            foreach (var key in login.Keys.Where(k => !known.Contains(k)))
            {
                errors.Add($"auth.login.{key} is not a login option (accepted: {string.Join(", ", known)})");
            }

            loginUrl = ParseLoginUrl(login, baseUrl, errors);
            method = ParseMethod(login, errors);
            (body, mediaType) = ParseBody(login, method, errors);
            headers = ParseHeaders(login, errors);
        }

        var tokenPointer = Text(auth, "token_pointer", errors, out var tokenRefused);
        if (tokenPointer is not null)
        {
            Pointer(tokenPointer, "token_pointer", errors);
        }
        else if (!tokenRefused)
        {
            errors.Add("auth.token_pointer is required (a JSON pointer to the token in the login response, e.g. /token)");
        }

        var inject = ParseInject(auth, errors);

        var ttlText = Text(auth, "ttl", errors, out var ttlRefused);
        var expiresPointer = Text(auth, "expires_in_pointer", errors, out var expiresRefused);
        TimeSpan? ttl = null;
        if ((ttlText is null) == (expiresPointer is null) && !ttlRefused && !expiresRefused)
        {
            errors.Add("auth needs exactly one of ttl (e.g. 30m) or expires_in_pointer (a JSON pointer to the lifetime in seconds)");
        }

        if (ttlText is not null)
        {
            if (TryParseDuration(ttlText, out var parsed) && parsed >= MinTtl && parsed <= MaxTtl)
            {
                ttl = parsed;
            }
            else
            {
                errors.Add($"auth.ttl must be a duration from 1s to 30d like 90s, 30m, 8h or 1d, got '{ttlText}'");
            }
        }

        if (expiresPointer is not null)
        {
            Pointer(expiresPointer, "expires_in_pointer", errors);
        }

        var reauth = ParseReauthStatus(auth, errors);

        return errors.Count > before || loginUrl is null
            ? null
            : new SessionOptions(loginUrl, method, body, mediaType, headers, tokenPointer!, inject!, ttl, expiresPointer, reauth,
                baseUrl);
    }

    // Statuses that, on a session that had worked, mean it expired. 407 is the proxy's and 429 is throttling: neither
    // is about the session.
    private static IReadOnlySet<int> ParseReauthStatus(IReadOnlyDictionary<string, object?> auth, List<string> errors)
    {
        if (!auth.TryGetValue("reauth_status", out var raw) || raw is null)
        {
            return DefaultReauthStatus;
        }

        if (raw is string || YamlScalarText.NonScalarKind(raw) != "list")
        {
            errors.Add("auth.reauth_status must be a list of HTTP statuses, e.g. [401, 419, 440]");
            return DefaultReauthStatus;
        }

        // 401 always means the session was refused; a custom list adds statuses to it.
        var statuses = new HashSet<int> { 401 };
        foreach (var entry in (System.Collections.IEnumerable)raw)
        {
            if (int.TryParse(YamlScalarText.Of(entry), NumberStyles.None, CultureInfo.InvariantCulture, out var status)
                && status is >= 400 and <= 499 and not 407 and not 429)
            {
                statuses.Add(status);
            }
            else
            {
                errors.Add($"auth.reauth_status entries must be 4xx statuses other than 407 and 429, got '{YamlScalarText.Of(entry)}'");
            }
        }

        return statuses;
    }

    private static Uri? ParseLoginUrl(IReadOnlyDictionary<string, object?> login, Uri? baseUrl, List<string> errors)
    {
        var path = LoginText(login, "path", errors);
        var url = LoginText(login, "url", errors);
        if ((path is null) == (url is null))
        {
            errors.Add("auth.login needs exactly one of path (relative to base_url) or url (absolute)");
            return null;
        }

        if (path is not null)
        {
            if (path.Contains("://", StringComparison.Ordinal))
            {
                errors.Add("auth.login.path is relative to base_url; put an absolute address in auth.login.url");
                return null;
            }

            if (baseUrl is null)
            {
                return null;
            }

            // Resolved the way dataset paths are: against the slash-terminated base, so a base_url path prefix stays.
            // A scheme-like prefix ('c:login', 'javascript:x') either fails to resolve or leaves http(s): both are refused.
            if (!Uri.TryCreate(baseUrl, path.TrimStart('/'), out var resolved) || resolved.Scheme is not ("http" or "https"))
            {
                errors.Add($"auth.login.path '{path}' is not a valid path relative to base_url");
                return null;
            }

            return resolved;
        }

        if (!Uri.TryCreate(url, UriKind.Absolute, out var absolute) || absolute.Scheme is not ("http" or "https"))
        {
            errors.Add("auth.login.url must be an absolute http(s) URL");
            return null;
        }

        if (absolute.UserInfo.Length > 0)
        {
            errors.Add("auth.login.url must not embed credentials; put them in auth.login.body");
            return null;
        }

        return absolute;
    }

    private static HttpMethod ParseMethod(IReadOnlyDictionary<string, object?> login, List<string> errors)
    {
        switch (LoginText(login, "method", errors)?.ToLowerInvariant())
        {
            case null or "post":
                return HttpMethod.Post;
            case "get":
                return HttpMethod.Get;
            default:
                errors.Add("auth.login.method must be one of: post, get");
                return HttpMethod.Post;
        }
    }

    private static (string? Body, string? MediaType) ParseBody(IReadOnlyDictionary<string, object?> login, HttpMethod method,
        List<string> errors)
    {
        login.TryGetValue("body", out var raw);
        var format = LoginText(login, "body_format", errors);
        var contentType = LoginText(login, "content_type", errors);
        if (format is not (null or "json" or "form"))
        {
            errors.Add("auth.login.body_format must be one of: json, form");
            format = null;
        }

        if (raw is null)
        {
            if (format is not null || contentType is not null)
            {
                errors.Add("auth.login.body_format and auth.login.content_type need an auth.login.body");
            }

            return (null, null);
        }

        if (method == HttpMethod.Get)
        {
            errors.Add("auth.login.body cannot be sent with method get; use method post");
            return (null, null);
        }

        var form = format == "form";
        var mediaType = contentType ?? (form ? "application/x-www-form-urlencoded" : "application/json");
        if (!MediaTypeHeaderValue.TryParse(mediaType, out _))
        {
            errors.Add($"auth.login.content_type is not a valid media type: '{mediaType}'");
        }

        if (raw is string text)
        {
            if (form)
            {
                errors.Add("auth.login.body_format form needs auth.login.body to be a flat map, not a string");
            }

            return (text, mediaType);
        }

        if (YamlScalarText.NonScalarKind(raw) != "map")
        {
            errors.Add("auth.login.body must be a map or a string");
            return (null, null);
        }

        var json = ToJson(raw);
        if (!form)
        {
            return (json!.ToJsonString(), mediaType);
        }

        var pairs = new List<string>();
        foreach (var (key, value) in (JsonObject)json!)
        {
            switch (value)
            {
                case null:
                    // An empty ${VAR} arrives as null; dropping the field would turn it into a confusing refused login.
                    errors.Add($"auth.login.body.{key} is empty (null); a form field needs a value");
                    break;
                case JsonValue v:
                    var scalar = v.TryGetValue<string>(out var s) ? s : v.ToJsonString();
                    pairs.Add($"{Uri.EscapeDataString(key)}={Uri.EscapeDataString(scalar)}");
                    break;
                default:
                    errors.Add($"auth.login.body.{key}: body_format form needs scalar values, not a nested map or list");
                    break;
            }
        }

        return (string.Join('&', pairs), mediaType);
    }

    private static List<KeyValuePair<string, string>> ParseHeaders(IReadOnlyDictionary<string, object?> login, List<string> errors)
    {
        var headers = new List<KeyValuePair<string, string>>();
        if (!login.TryGetValue("headers", out var raw) || raw is null)
        {
            return headers;
        }

        if (raw is not IReadOnlyDictionary<string, object?> map)
        {
            errors.Add("auth.login.headers must be a map of header names to values");
            return headers;
        }

        foreach (var (name, value) in map)
        {
            if (YamlScalarText.NonScalarKind(value) is { } kind)
            {
                errors.Add($"auth.login.headers.{name} must be a single value, not a {kind}");
            }
            else if (IsContentHeader(name))
            {
                errors.Add($"auth.login.headers.{name}: set the body's type with auth.login.content_type");
            }
            else if (!IsRequestHeaderName(name))
            {
                errors.Add($"auth.login.headers has an invalid header name '{name}'");
            }
            else
            {
                headers.Add(new(name, YamlScalarText.Of(value) ?? ""));
            }
        }

        return headers;
    }

    private static SessionInject? ParseInject(IReadOnlyDictionary<string, object?> auth, List<string> errors)
    {
        auth.TryGetValue("inject", out var raw);
        switch (raw)
        {
            case null or "bearer":
                return new SessionInject(null, "", null);
            case string other:
                errors.Add($"auth.inject must be 'bearer', {{ header: <name> }} or {{ query: <name> }}, got '{other}'");
                return null;
        }

        if (raw is not IReadOnlyDictionary<string, object?> map)
        {
            errors.Add("auth.inject must be 'bearer', { header: <name> } or { query: <name> }");
            return null;
        }

        var before = errors.Count;
        foreach (var key in map.Keys.Where(k => k is not ("header" or "prefix" or "query")))
        {
            errors.Add($"auth.inject.{key} is not an inject option (accepted: header, prefix, query)");
        }

        var header = InjectText(map, "header", errors);
        var query = InjectText(map, "query", errors);
        var prefix = map.TryGetValue("prefix", out var p) && p is not null ? YamlScalarText.Of(p) : null;
        if (map.ContainsKey("prefix") && YamlScalarText.NonScalarKind(p) is { } kind)
        {
            errors.Add($"auth.inject.prefix must be a single value, not a {kind}");
            prefix = null;
        }

        if ((header is null) == (query is null))
        {
            errors.Add("auth.inject needs exactly one of header or query");
        }

        if (prefix is not null && header is null)
        {
            errors.Add("auth.inject.prefix applies only with auth.inject.header");
        }

        if (header is not null && (IsContentHeader(header) || !IsRequestHeaderName(header)))
        {
            errors.Add($"auth.inject.header '{header}' is not a request header name");
        }

        return errors.Count > before ? null : new SessionInject(header, prefix ?? "", query);
    }

    private static string? InjectText(IReadOnlyDictionary<string, object?> map, string key, List<string> errors)
    {
        map.TryGetValue(key, out var raw);
        if (YamlScalarText.NonScalarKind(raw) is { } kind)
        {
            errors.Add($"auth.inject.{key} must be a single value, not a {kind}");
            return null;
        }

        return YamlScalarText.Of(raw) is { } text && !string.IsNullOrWhiteSpace(text) ? text : null;
    }

    private static bool IsContentHeader(string name)
        => name.StartsWith("Content-", StringComparison.OrdinalIgnoreCase);

    private static bool IsRequestHeaderName(string name)
    {
        using var probe = new HttpRequestMessage();
        return name.Length > 0 && probe.Headers.TryAddWithoutValidation(name, "x");
    }

    private static string? LoginText(IReadOnlyDictionary<string, object?> login, string key, List<string> errors)
    {
        login.TryGetValue(key, out var raw);
        if (YamlScalarText.NonScalarKind(raw) is { } kind)
        {
            errors.Add($"auth.login.{key} must be a single value, not a {kind}");
            return null;
        }

        return YamlScalarText.Of(raw) is { } text && !string.IsNullOrWhiteSpace(text) ? text : null;
    }

    private static string? Text(IReadOnlyDictionary<string, object?> auth, string key, List<string> errors, out bool refused)
    {
        auth.TryGetValue(key, out var raw);
        refused = YamlScalarText.NonScalarKind(raw) is not null;
        if (YamlScalarText.NonScalarKind(raw) is { } kind)
        {
            errors.Add($"auth.{key} must be a single value, not a {kind}");
            return null;
        }

        return YamlScalarText.Of(raw) is { } text && !string.IsNullOrWhiteSpace(text) ? text : null;
    }

    private static void Pointer(string pointer, string key, List<string> errors)
    {
        try
        {
            JsonPointer.TryResolve(JsonNode.Parse("{}"), pointer, out _);
        }
        catch (ArgumentException)
        {
            errors.Add($"auth.{key} is not a valid JSON pointer: '{pointer}' (it must start with '/')");
        }
    }

    /// <summary>pz's one duration grammar for config: a non-negative integer and one of <c>ms|s|m|h|d</c>,
    /// lowercase, no spaces, signs, fractions or compounds.</summary>
    internal static bool TryParseDuration(string text, out TimeSpan value)
    {
        value = default;
        var unitStart = 0;
        while (unitStart < text.Length && char.IsAsciiDigit(text[unitStart]))
        {
            unitStart++;
        }

        if (unitStart == 0
            || !long.TryParse(text.AsSpan(0, unitStart), NumberStyles.None, CultureInfo.InvariantCulture, out var magnitude))
        {
            return false;
        }

        try
        {
            switch (text[unitStart..])
            {
                case "ms": value = TimeSpan.FromMilliseconds(magnitude); return true;
                case "s": value = TimeSpan.FromSeconds(magnitude); return true;
                case "m": value = TimeSpan.FromMinutes(magnitude); return true;
                case "h": value = TimeSpan.FromHours(magnitude); return true;
                case "d": value = TimeSpan.FromDays(magnitude); return true;
                default: return false;
            }
        }
        catch (OverflowException)
        {
            return false;
        }
    }

    private static JsonNode? ToJson(object? value)
    {
        switch (value)
        {
            case null:
                return null;
            case string s:
                return JsonValue.Create(s);
            case bool b:
                return JsonValue.Create(b);
            case int or long or short or byte or sbyte or ushort or uint:
                return JsonValue.Create(Convert.ToInt64(value, CultureInfo.InvariantCulture));
            case ulong ul:
                return JsonValue.Create(ul);
            case float or double:
                return JsonValue.Create(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            case decimal d:
                return JsonValue.Create(d);
        }

        if (YamlScalarText.NonScalarKind(value) == "map")
        {
            var obj = new JsonObject();
            var entries = value is IReadOnlyDictionary<string, object?> ro
                ? ro.Select(kv => (kv.Key, kv.Value))
                : ((System.Collections.IDictionary)value).Cast<System.Collections.DictionaryEntry>()
                    .Select(e => (e.Key.ToString() ?? "", e.Value));
            foreach (var (k, v) in entries)
            {
                obj[k] = ToJson(v);
            }

            return obj;
        }

        if (value is System.Collections.IEnumerable items)
        {
            var arr = new JsonArray();
            foreach (var item in items)
            {
                arr.Add(ToJson(item));
            }

            return arr;
        }

        return JsonValue.Create(YamlScalarText.Of(value));
    }
}

/// <summary>Signs in with the session login request and puts its token on every API request. A refused login
/// (401/403) is permanent and remembered for the process under a fingerprint of the connection and the login
/// (<see cref="CachedTokenAuthenticator.SignInKey"/>), so one run makes at most one refused login per connection
/// however many nodes use it. A status in <c>reauth_status</c> on a session that had worked drops it, like a 401
/// does for every token. Messages name the login
/// endpoint's scheme://authority and the status only: the request body holds the password, and the response
/// may echo it.</summary>
internal sealed class SessionAuthenticator : CachedTokenAuthenticator
{
    private const long MaxLifetimeSeconds = 10 * 365 * 24 * 3600L;

    private readonly SessionOptions _options;
    private readonly TimeProvider _time;
    private readonly HttpClient _http;
    private readonly string _endpoint;
    private readonly string _signInKey;

    /// <param name="handler">The login transport (the connection's proxy and TLS); owned here. It must not
    /// follow redirects. Null builds the default handler, which follows none.</param>
    public SessionAuthenticator(SessionOptions options, TimeSpan timeout, TimeProvider time, HttpMessageHandler? handler)
        : base(time)
    {
        _options = options;
        _time = time;
        _endpoint = options.LoginUrl.GetComponents(UriComponents.SchemeAndServer, UriFormat.UriEscaped);
        _http = new HttpClient(handler ?? new HttpClientHandler
        {
            AllowAutoRedirect = false,
            AutomaticDecompression = DecompressionMethods.All,
        })
        {
            Timeout = timeout,
            MaxResponseContentBufferSize = SessionOptions.MaxLoginResponseBytes,
        };
        _http.DefaultRequestHeaders.UserAgent.ParseAdd(HttpSource.UserAgent);
        _signInKey = Fingerprint(
            [
                "session", options.BaseUrl?.AbsoluteUri, options.LoginUrl.AbsoluteUri, options.Method.Method, options.MediaType,
                options.Body, .. options.Headers.SelectMany(h => new[] { h.Key, h.Value }),
            ]);
    }

    protected override string? SignInKey => _signInKey;

    protected override bool IsTokenRejection(int status) => _options.ReauthStatus.Contains(status);

    private string Label => $"session login {_endpoint}";

    public override IReadOnlyCollection<string> SecretQueryParams
        => _options.Inject.Query is { } query ? [query] : [];

    /// <summary>The request header the token is sent in (`auth.inject.header`); null for bearer and query.</summary>
    public string? InjectedHeader => _options.Inject.Header;

    public override void Dispose()
    {
        base.Dispose();
        _http.Dispose();
    }

    protected override void ApplyToken(HttpRequestMessage request, string token)
    {
        var inject = _options.Inject;
        if (inject.Query is { } query)
        {
            request.RequestUri = QueryString.With(request.RequestUri!, query, token);
        }
        else if (inject.Header is { } header)
        {
            request.Headers.Remove(header);
            request.Headers.TryAddWithoutValidation(header, inject.Prefix + token);
        }
        else
        {
            base.ApplyToken(request, token);
        }
    }

    protected override async ValueTask<FetchedToken> FetchTokenAsync(CancellationToken ct)
    {
        using var request = new HttpRequestMessage(_options.Method, _options.LoginUrl);
        foreach (var (name, value) in _options.Headers)
        {
            request.Headers.TryAddWithoutValidation(name, value);
        }

        if (_options.Body is { } body)
        {
            var content = new StringContent(body, Encoding.UTF8);
            var type = MediaTypeHeaderValue.Parse(_options.MediaType!);
            type.CharSet ??= "utf-8";
            content.Headers.ContentType = type;
            request.Content = content;
        }

        try
        {
            using var response = await _http.SendAsync(request, ct).ConfigureAwait(false);
            var status = (int)response.StatusCode;
            if (status is < 200 or >= 300)
            {
                throw Refused(response, status);
            }

            return Interpret(await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false));
        }
        catch (HttpRequestException ex) when (ex.HttpRequestError == HttpRequestError.ConfigurationLimitExceeded)
        {
            throw new PzConnectorException(
                $"{Label}: the login response is larger than {SessionOptions.MaxLoginResponseBytes / 1024 / 1024} MiB",
                isTransient: false, innerException: ex);
        }
        catch (Exception ex) when (ex is HttpRequestException or IOException or TaskCanceledException && !ct.IsCancellationRequested)
        {
            throw Unreachable(ex);
        }
    }

    private PzConnectorException Refused(HttpResponseMessage response, int status)
    {
        if (status is 401 or 403)
        {
            return Terminal(new PzConnectorException(
                $"{Label} was refused (HTTP {status}); check the credentials in auth.login. pz makes one refused login "
                + "per connection per run: later reads and nodes in this run fail without trying again",
                isTransient: false));
        }

        if (status is >= 300 and < 400)
        {
            return new PzConnectorException(
                $"{Label} redirected (HTTP {status}); redirects are not followed (the login carries credentials), so point "
                + "auth.login at the final address", isTransient: false);
        }

        if (status == 407)
        {
            return new PzConnectorException($"{Label}: {HttpHandlerFactory.ProxyAuthenticationFailed}", isTransient: false);
        }

        if (status is 408 or 429 or (>= 500 and <= 599))
        {
            TimeSpan? retryAfter = response.Headers.TryGetValues("Retry-After", out var values)
                ? TransientClassifier.ParseRetryAfter(values.FirstOrDefault(), _time.GetUtcNow())
                : null;
            return new PzConnectorException($"{Label} failed temporarily (HTTP {status}); it will be retried",
                isTransient: true, retryAfter);
        }

        return new PzConnectorException(
            $"{Label} failed (HTTP {status}); check auth.login (path or url, method, body and headers)", isTransient: false);
    }

    // A TLS verdict and a proxy's explicit 407 do not change on retry; plain I/O and timeouts do.
    private PzConnectorException Unreachable(Exception ex)
    {
        for (var current = ex; current is not null; current = current.InnerException)
        {
            if (current is AuthenticationException)
            {
                return new PzConnectorException($"{Label}: TLS failed (check tls.ca_file / tls.client_certificate)",
                    isTransient: false, innerException: ex);
            }
        }

        if (ex is HttpRequestException { StatusCode: HttpStatusCode.ProxyAuthenticationRequired })
        {
            return new PzConnectorException($"{Label}: {HttpHandlerFactory.ProxyAuthenticationFailed}",
                isTransient: false, innerException: ex);
        }

        return ex is TaskCanceledException
            ? new PzConnectorException($"{Label} timed out", isTransient: true, innerException: ex)
            : new PzConnectorException($"{Label} could not be reached ({ex.GetType().Name})", isTransient: true, innerException: ex);
    }

    private FetchedToken Interpret(string body)
    {
        try
        {
            return ReadToken(body);
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or FormatException)
        {
            // A JSON object with duplicate keys throws only when a property is read.
            throw new PzConnectorException(
                $"{Label}: the login response could not be read at auth.token_pointer '{_options.TokenPointer}' "
                + "(duplicate keys or malformed JSON)", isTransient: false);
        }
    }

    private FetchedToken ReadToken(string body)
    {
        JsonNode? root;
        try
        {
            root = JsonNode.Parse(body);
        }
        catch (JsonException)
        {
            throw new PzConnectorException($"{Label}: the login response is not JSON, so auth.token_pointer cannot be read",
                isTransient: false);
        }

        JsonPointer.TryResolve(root, _options.TokenPointer, out var node);
        var token = node is JsonValue value
            ? value.GetValueKind() switch
            {
                JsonValueKind.String => value.GetValue<string>(),
                JsonValueKind.Number => value.ToJsonString(),
                _ => null,
            }
            : null;
        if (string.IsNullOrEmpty(token))
        {
            throw new PzConnectorException(
                $"{Label}: the login response has no token (a non-empty string or number) at auth.token_pointer "
                + $"'{_options.TokenPointer}'", isTransient: false);
        }

        if (token.Any(char.IsControl))
        {
            throw new PzConnectorException(
                $"{Label}: the token at auth.token_pointer '{_options.TokenPointer}' contains control characters and "
                + "cannot be sent in a header or URL", isTransient: false);
        }

        // A header carries printable ASCII only; a query parameter escapes anything else.
        if (_options.Inject.Query is null && token.Any(c => c is < ' ' or > '~'))
        {
            throw new PzConnectorException(
                $"{Label}: the token at auth.token_pointer '{_options.TokenPointer}' has characters outside printable "
                + "ASCII, which a header cannot carry; check auth.token_pointer, or send it with auth.inject.query",
                isTransient: false);
        }

        var now = _time.GetUtcNow();
        if (_options.Ttl is { } ttl)
        {
            return new FetchedToken(token, now + ttl);
        }

        JsonPointer.TryResolve(root, _options.ExpiresInPointer!, out var expires);
        if (Seconds(expires) is not { } seconds || seconds <= 0)
        {
            throw new PzConnectorException(
                $"{Label}: the login response has no positive number of seconds at auth.expires_in_pointer "
                + $"'{_options.ExpiresInPointer}' (set auth.ttl instead if the API does not report a lifetime)",
                isTransient: false);
        }

        return new FetchedToken(token, now + TimeSpan.FromSeconds(Math.Min(seconds, MaxLifetimeSeconds)));
    }

    private static double? Seconds(JsonNode? node)
    {
        if (node is not JsonValue v)
        {
            return null;
        }

        return v.GetValueKind() switch
        {
            JsonValueKind.Number => v.GetValue<double>(),
            JsonValueKind.String when double.TryParse(v.GetValue<string>(), NumberStyles.Float, CultureInfo.InvariantCulture,
                out var parsed) && double.IsFinite(parsed) => parsed,
            _ => null,
        };
    }
}
