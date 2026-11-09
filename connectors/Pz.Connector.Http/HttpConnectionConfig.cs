using System.Globalization;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Auth;

namespace Pz.Connector.Http;

internal sealed record HttpConnectionConfig(Uri BaseUrl, IRequestAuthenticator? Authenticator,
    IReadOnlyDictionary<string, string> Headers, string? CheckPath, TimeSpan Timeout,
    long MaxResponseBytes, IReadOnlyList<string> AllowedHosts,
    Func<TimeProvider, Func<Uri, HttpMessageHandler>?, IRequestAuthenticator>? AuthFactory = null, string? CertificatePathInProject = null,
    IReadOnlyList<string>? AuthWarnings = null)
{
    /// <summary>HttpClient's own default is 100 seconds — far too long for one page of JSON, and it
    /// is per attempt, so a black-holed endpoint holds a run for minutes before the engine even sees
    /// a transient failure. 30s matches the retry policy's 30s max delay; raise it per connection
    /// with 'timeout_seconds' for a genuinely slow endpoint.</summary>
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

    /// <summary>A page is buffered whole and then parsed into a JsonNode DOM, so peak memory is a
    /// multiple of the wire size. Without a cap the only backstop is the process OOM-ing on a
    /// response the endpoint chooses the size of. 256 MiB is far above any real page of JSON.</summary>
    public const long DefaultMaxResponseBytes = 256L * 1024 * 1024;

    /// <summary>Cap on a downloaded file body on disk ('max_download_mb'); file-mode datasets only.</summary>
    public const long DefaultMaxDownloadBytes = 4096L * 1024 * 1024;

    /// <summary>Highest legal 'max_download_mb' (1048576 MiB = 1 TiB).</summary>
    public const long MaxDownloadMbCap = 1048576;

    public long MaxDownloadBytes { get; init; } = DefaultMaxDownloadBytes;

    /// <summary>The 'proxy:' and 'tls:' options; <see cref="HttpTransportOptions.Default"/> when neither is set.</summary>
    public HttpTransportOptions Transport { get; init; } = HttpTransportOptions.Default;

    /// <summary>Highest legal 'max_response_mb'. HttpClient.MaxResponseContentBufferSize refuses any
    /// value above int.MaxValue bytes (2147483647); 2048 MiB (2147483648 bytes) already overflows
    /// that ceiling, so 2047 is the largest whole-MiB value that stays under it.</summary>
    public const long MaxResponseMbCap = 2047;

    /// <summary>Masks the value of every authenticator secret query param found in
    /// <paramref name="text"/>. Anything the endpoint sends back can echo the request URL, so
    /// response-derived text goes through this before it reaches an exception message.</summary>
    public string RedactSecretParams(string text)
    {
        foreach (var param in Authenticator?.SecretQueryParams ?? [])
        {
            text = System.Text.RegularExpressions.Regex.Replace(
                text, $"(?<=[?&]){System.Text.RegularExpressions.Regex.Escape(param)}=[^&]*",
                $"{param}=***");
        }

        return text;
    }

    /// <summary>True when <paramref name="candidate"/> may be requested with this connection's
    /// credentials attached: the base URL's own origin (scheme + host + port), or a host the author
    /// explicitly listed in 'allow_hosts'. Pagination links, redirect targets and stored resume
    /// tokens are all chosen by the far side, so each is checked against this before it is followed —
    /// otherwise a compromised or malicious endpoint can name any host it likes and pz will send the
    /// connection's Authorization/api-key headers there. An allow-listed host must be reached over
    /// https unless the base URL is itself http: otherwise a far-side link to 'http://files.x' would
    /// carry credentials in cleartext that the author only ever meant to send encrypted.</summary>
    public bool IsAllowedTarget(Uri candidate)
    {
        if (candidate.Scheme is not ("http" or "https"))
        {
            return false;
        }

        if (string.Equals(candidate.Scheme, BaseUrl.Scheme, StringComparison.OrdinalIgnoreCase)
            && string.Equals(candidate.Host, BaseUrl.Host, StringComparison.OrdinalIgnoreCase)
            && candidate.Port == BaseUrl.Port)
        {
            return true;
        }

        if (candidate.Scheme == Uri.UriSchemeHttp && BaseUrl.Scheme == Uri.UriSchemeHttps)
        {
            return false;
        }

        foreach (var host in AllowedHosts)
        {
            if (string.Equals(candidate.Host, host, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>True when the connection sends credentials: an authenticator (built or still to be
    /// built by <see cref="AuthFactory"/>) or an Authorization header.</summary>
    public bool HasCredentials => Authenticator is not null || AuthFactory is not null
        || Headers.Keys.Any(k => string.Equals(k, "Authorization", StringComparison.OrdinalIgnoreCase));

    /// <summary>The live copy: token-fetching authenticators are built here, not while parsing, so
    /// validation never creates a credential or touches the network. The caller owns the result's
    /// authenticator when it came from <see cref="AuthFactory"/> and disposes it if it is
    /// <see cref="IDisposable"/>.</summary>
    /// <remarks>Binding also loads the 'tls:' files into <see cref="Bound"/>, which the caller owns and
    /// disposes with the clients built from this copy.</remarks>
    public HttpConnectionConfig Bind(TimeProvider time) => Bind(time, BoundTransport.Load(Transport));

    /// <summary>Binds with TLS material the caller already loaded; the authenticator gets the handler source
    /// for its token endpoint. On success the caller keeps owning <paramref name="bound"/>; if binding throws
    /// (anything), <paramref name="bound"/> is disposed before the exception propagates.</summary>
    internal HttpConnectionConfig Bind(TimeProvider time, BoundTransport? bound)
    {
        try
        {
            return this with
            {
                Bound = bound,
                Authenticator = AuthFactory?.Invoke(time, HttpHandlerFactory.TokenEndpointHandlers(this, bound)) ?? Authenticator,
            };
        }
        catch
        {
            bound?.Dispose();
            throw;
        }
    }

    /// <summary>The loaded 'tls:' material of a bound copy; null before binding and when neither 'proxy'
    /// nor 'tls' is set.</summary>
    public BoundTransport? Bound { get; init; }

    /// <summary>Non-blocking validation diagnostics. Credentials (an 'auth:' block or an
    /// Authorization header) sent to a cleartext http base URL travel unencrypted; that is legal —
    /// some internal APIs are http-only — but never silent. Loopback is exempt: it never leaves the
    /// machine.</summary>
    public IReadOnlyList<string> Warnings()
    {
        var warnings = new List<string>();
        if (BaseUrl.Scheme == Uri.UriSchemeHttp && HasCredentials && !BaseUrl.IsLoopback)
        {
            warnings.Add($"'base_url' is http://{BaseUrl.Authority}, so this connection's credentials are sent " +
                "unencrypted on every request; use an https:// base_url unless this API really is http-only");
        }

        if (CertificatePathInProject is { } certificate)
        {
            warnings.Add($"auth.certificate_path '{certificate}' is inside the project directory; keep the " +
                "private key outside the project (or out of version control) so it is not committed with it");
        }

        warnings.AddRange(AuthWarnings ?? []);
        return warnings;
    }

    public static HttpConnectionConfig? Parse(ConnectorConfig config, List<string> errors)
    {
        var baseUrl = config.GetString("base_url");
        Uri? baseUri = null;
        if (string.IsNullOrEmpty(baseUrl))
        {
            errors.Add("missing required connection field 'base_url'");
        }
        else if (!Uri.TryCreate(baseUrl, UriKind.Absolute, out baseUri)
            || baseUri.Scheme is not ("http" or "https"))
        {
            errors.Add($"'base_url' must be an absolute http(s) URL, got '{baseUrl}'");
            baseUri = null;
        }
        else if (!baseUri.AbsoluteUri.EndsWith('/'))
        {
            // Relative-URI resolution (new Uri(base, relative)) treats everything after the last
            // '/' in base as a filename to replace. Without a trailing slash, a path prefix like
            // '/api/v2' is silently dropped when a dataset's relative path resolves against it.
            // Normalizing here means every consumer (dataset paths, check_path) can safely treat
            // base_url as a directory and combine with a relative (leading-slash-stripped) segment.
            baseUri = new Uri(baseUri.AbsoluteUri + "/", UriKind.Absolute);
        }

        var baseDir = config.GetString("base_dir");
        var headers = new Dictionary<string, string>();
        if (config.Values.TryGetValue("headers", out var h) && h is IReadOnlyDictionary<string, object?> map)
        {
            foreach (var (name, value) in map)
            {
                headers[name] = YamlScalarText.Of(value) ?? "";
            }
        }

        var timeout = DefaultTimeout;
        if (config.Values.TryGetValue("timeout_seconds", out var t) && t is not null)
        {
            if (double.TryParse(t.ToString(), NumberStyles.Float, CultureInfo.InvariantCulture, out var secs)
                && secs > 0 && secs <= 3600)
            {
                timeout = TimeSpan.FromSeconds(secs);
            }
            else
            {
                errors.Add($"'timeout_seconds' must be a positive number of seconds up to 3600, got '{t}'");
            }
        }

        var transport = HttpTransportOptions.Parse(config.Values, baseDir, errors);
        var hasTransport = !transport.IsDefault;

        HttpAuth.Parsed auth = new(null, null, null);
        var authBlock = config.Values.TryGetValue("auth", out var a)
            ? a as IReadOnlyDictionary<string, object?>
            : null;
        if (config.Values.TryGetValue("auth", out var rawAuth) && rawAuth is not null && authBlock is null)
        {
            errors.Add("'auth' must be a map (type + fields)");
        }
        else
        {
            auth = HttpAuth.Parse(authBlock, baseDir, timeout, errors, hasTransport, baseUri);
        }

        var maxResponseBytes = DefaultMaxResponseBytes;
        if (config.Values.TryGetValue("max_response_mb", out var m) && m is not null)
        {
            if (long.TryParse(m.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var mb)
                && mb > 0 && mb <= MaxResponseMbCap)
            {
                maxResponseBytes = mb * 1024 * 1024;
            }
            else
            {
                errors.Add($"'max_response_mb' must be a positive whole number of MiB up to " +
                    $"{MaxResponseMbCap}, got '{m}'");
            }
        }

        var maxDownloadBytes = DefaultMaxDownloadBytes;
        if (config.Values.TryGetValue("max_download_mb", out var dl) && dl is not null)
        {
            if (long.TryParse(Pz.Connector.Http.YamlScalarText.Of(dl), NumberStyles.Integer,
                    CultureInfo.InvariantCulture, out var dmb) && dmb > 0 && dmb <= MaxDownloadMbCap)
            {
                maxDownloadBytes = dmb * 1024 * 1024;
            }
            else
            {
                errors.Add($"'max_download_mb' must be a positive whole number of MiB up to " +
                    $"{MaxDownloadMbCap}, got '{dl}'");
            }
        }

        var allowedHosts = new List<string>();
        if (config.Values.TryGetValue("allow_hosts", out var ah) && ah is not null)
        {
            if (ah is System.Collections.IEnumerable list and not string)
            {
                foreach (var entry in list)
                {
                    var host = entry?.ToString();
                    if (string.IsNullOrWhiteSpace(host))
                    {
                        errors.Add("'allow_hosts' entries must be non-empty host names");
                    }
                    else if (host.Contains('/', StringComparison.Ordinal))
                    {
                        errors.Add($"'allow_hosts' entries are bare host names, not URLs, got '{host}'");
                    }
                    else
                    {
                        allowedHosts.Add(host);
                    }
                }
            }
            else
            {
                errors.Add("'allow_hosts' must be a list of host names");
            }
        }

        if (baseUri is null)
        {
            return null;
        }

        var connection = new HttpConnectionConfig(baseUri, auth.Static, headers, config.GetString("check_path"),
            timeout, maxResponseBytes, allowedHosts, auth.Factory, CertificateInProject(auth.CertificatePath, baseDir), auth.Warnings)
        {
            MaxDownloadBytes = maxDownloadBytes,
            Transport = transport,
        };

        // The login carries the credentials, so it follows the rule every credentialed request follows.
        if (auth.LoginUrl is { } login && !connection.IsAllowedTarget(login))
        {
            errors.Add($"auth.login points at '{login.GetLeftPart(UriPartial.Authority)}', which is not base_url's origin " +
                $"'{baseUri.GetLeftPart(UriPartial.Authority)}' or an 'allow_hosts' host reached over https; add the host " +
                "to 'allow_hosts' if the login really belongs to this API");
        }

        return connection;
    }

    // A relative certificate_path resolves under base_dir; a path still inside it is easy to commit by accident.
    private static string? CertificateInProject(string? certificatePath, string? baseDir)
    {
        if (certificatePath is null || string.IsNullOrEmpty(baseDir))
        {
            return null;
        }

        var root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(baseDir)) + Path.DirectorySeparatorChar;
        var comparison = OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        return Path.GetFullPath(certificatePath).StartsWith(root, comparison) ? certificatePath : null;
    }
}
