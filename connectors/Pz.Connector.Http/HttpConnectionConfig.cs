using System.Globalization;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Auth;

namespace Pz.Connector.Http;

internal sealed record HttpConnectionConfig(Uri BaseUrl, IRequestAuthenticator? Authenticator,
    IReadOnlyDictionary<string, string> Headers, string? CheckPath, TimeSpan Timeout,
    long MaxResponseBytes, IReadOnlyList<string> AllowedHosts)
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

    /// <summary>True when <paramref name="candidate"/> may be requested with this connection's
    /// credentials attached: the base URL's own origin (scheme + host + port), or a host the author
    /// explicitly listed in 'allow_hosts'. Pagination links, redirect targets and stored resume
    /// tokens are all chosen by the far side, so each is checked against this before it is followed —
    /// otherwise a compromised or malicious endpoint can name any host it likes and pz will send the
    /// connection's Authorization/api-key headers there.</summary>
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

        foreach (var host in AllowedHosts)
        {
            if (string.Equals(candidate.Host, host, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
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

        IRequestAuthenticator? authenticator = null;
        var authBlock = config.Values.TryGetValue("auth", out var a)
            ? a as IReadOnlyDictionary<string, object?>
            : null;
        if (config.Values.TryGetValue("auth", out var rawAuth) && rawAuth is not null && authBlock is null)
        {
            errors.Add("'auth' must be a map (type + fields)");
        }
        else if (!Authenticators.TryCreate(authBlock, out authenticator, out var authError))
        {
            errors.Add(authError!);
        }

        var headers = new Dictionary<string, string>();
        if (config.Values.TryGetValue("headers", out var h) && h is IReadOnlyDictionary<string, object?> map)
        {
            foreach (var (name, value) in map)
            {
                headers[name] = value?.ToString() ?? "";
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

        var maxResponseBytes = DefaultMaxResponseBytes;
        if (config.Values.TryGetValue("max_response_mb", out var m) && m is not null)
        {
            if (long.TryParse(m.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var mb)
                && mb > 0 && mb <= 4096)
            {
                maxResponseBytes = mb * 1024 * 1024;
            }
            else
            {
                errors.Add($"'max_response_mb' must be a positive whole number of MiB up to 4096, got '{m}'");
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

        return baseUri is null
            ? null
            : new HttpConnectionConfig(baseUri, authenticator, headers, config.GetString("check_path"),
                timeout, maxResponseBytes, allowedHosts);
    }
}
