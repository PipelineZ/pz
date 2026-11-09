using System.Text;
using Pz.Connectors.Toolkit;

namespace Pz.Connector.Http;

internal enum ProxyMode
{
    /// <summary>No 'proxy' key: the platform default proxy, exactly as before the option existed.</summary>
    Default,

    /// <summary>'proxy: none': never use a proxy, not even the environment's.</summary>
    None,

    /// <summary>'proxy: { url: ... }'.</summary>
    Explicit,
}

/// <summary>The connection's 'proxy:' and 'tls:' options, parsed and validated; nothing here touches
/// the network or the files named (existence is checked when the transport is built). Paths are
/// absolute by the time they land here.</summary>
internal sealed record HttpTransportOptions(ProxyMode Mode, Uri? ProxyUrl, string? ProxyUser,
    string? ProxyPassword, IReadOnlyList<string> Bypass, string? CaFile, string? CertPath, string? KeyPath,
    string? CertPassword)
{
    private static readonly string[] PemExtensions = [".pem", ".crt", ".cer"];
    private static readonly string[] Pkcs12Extensions = [".pfx", ".p12"];

    public static HttpTransportOptions Default { get; } =
        new(ProxyMode.Default, null, null, null, [], null, null, null, null);

    /// <summary>True when neither a proxy nor any TLS option is set: the handler must then stay exactly
    /// what it was before these options existed.</summary>
    public bool IsDefault => Mode == ProxyMode.Default && CaFile is null && CertPath is null;

    // A record's generated ToString prints every member, and two of them are passwords.
    private bool PrintMembers(StringBuilder builder)
    {
        builder.Append("Mode = ").Append(Mode);
        return true;
    }

    /// <summary>An exact entry matches only that host; '*.suffix' matches any host with one or more
    /// labels in front of the suffix, never the bare suffix or a longer look-alike. Case-insensitive.</summary>
    public static bool BypassMatches(string pattern, string host)
    {
        if (pattern.StartsWith("*.", StringComparison.Ordinal))
        {
            var suffix = pattern[1..];
            return host.Length > suffix.Length && host.EndsWith(suffix, StringComparison.OrdinalIgnoreCase);
        }

        return string.Equals(pattern, host, StringComparison.OrdinalIgnoreCase);
    }

    public static HttpTransportOptions Parse(IReadOnlyDictionary<string, object?> values, string? baseDir, List<string> errors)
    {
        var (mode, url, user, password, bypass) = ParseProxy(values, errors);
        var (ca, certPath, keyPath, certPassword) = ParseTls(values, baseDir, errors);
        return new HttpTransportOptions(mode, url, user, password, bypass, ca, certPath, keyPath, certPassword);
    }

    private static (ProxyMode, Uri?, string?, string?, IReadOnlyList<string>) ParseProxy(
        IReadOnlyDictionary<string, object?> values, List<string> errors)
    {
        if (!values.TryGetValue("proxy", out var raw) || raw is null)
        {
            return (ProxyMode.Default, null, null, null, []);
        }

        if (raw is string text)
        {
            if (text == "none")
            {
                return (ProxyMode.None, null, null, null, []);
            }

            errors.Add("'proxy' must be 'none' or a map with 'url'");
            return (ProxyMode.Default, null, null, null, []);
        }

        if (raw is not IReadOnlyDictionary<string, object?> map)
        {
            errors.Add("'proxy' must be 'none' or a map with 'url'");
            return (ProxyMode.Default, null, null, null, []);
        }

        RejectUnknown(map, "proxy", ["url", "user", "password", "bypass"], errors);
        var url = ParseProxyUrl(map, errors);
        var user = Scalar(map, "user", "proxy", errors);
        var password = Scalar(map, "password", "proxy", errors);
        if ((user is null) != (password is null))
        {
            errors.Add("proxy.user and proxy.password must be set together");
        }

        var bypass = new List<string>();
        if (map.TryGetValue("bypass", out var rawBypass) && rawBypass is not null)
        {
            if (rawBypass is System.Collections.IEnumerable list and not string
                && YamlScalarText.NonScalarKind(rawBypass) == "list")
            {
                foreach (var entry in list)
                {
                    var host = YamlScalarText.NonScalarKind(entry) is null ? YamlScalarText.Of(entry) : null;
                    if (host is not null && IsBypassEntry(host))
                    {
                        bypass.Add(host);
                    }
                    else
                    {
                        errors.Add("proxy.bypass entries must be a host name (letters, digits, '-' and '.'), " +
                            "optionally prefixed with '*.'");
                    }
                }
            }
            else
            {
                errors.Add("proxy.bypass must be a list of host names");
            }
        }

        return (ProxyMode.Explicit, url, user, password, bypass);
    }

    // The URL is never echoed: it may carry the proxy password in its userinfo.
    private static Uri? ParseProxyUrl(IReadOnlyDictionary<string, object?> map, List<string> errors)
    {
        var text = Scalar(map, "url", "proxy", errors, out var refused);
        if (text is null)
        {
            if (!refused)
            {
                errors.Add("proxy.url is required");
            }

            return null;
        }

        if (!Uri.TryCreate(text, UriKind.Absolute, out var url) || url.Scheme != Uri.UriSchemeHttp)
        {
            // A TLS connection to the proxy would be handed the target's TLS settings (client certificate,
            // ca_file trust), so only a plain-http proxy is accepted; CONNECT still tunnels TLS end to end.
            errors.Add("proxy.url must be an http:// URL (the TLS connection to the target still runs end to end through the proxy)");
            return null;
        }

        if (url.UserInfo.Length > 0)
        {
            errors.Add("proxy.url must not contain userinfo (credentials); set proxy.user and proxy.password instead");
            return null;
        }

        if (url.AbsolutePath != "/" || url.Query.Length > 0 || url.Fragment.Length > 0
            || text.Contains('?', StringComparison.Ordinal) || text.Contains('#', StringComparison.Ordinal))
        {
            errors.Add("proxy.url must be only a scheme, host and optional port (no path, query or fragment)");
            return null;
        }

        return url;
    }

    private static bool IsBypassEntry(string entry)
    {
        var host = entry.StartsWith("*.", StringComparison.Ordinal) ? entry[2..] : entry;
        return host.Length > 0 && host.Split('.').All(label => label.Length > 0
            && label.All(c => c is (>= 'a' and <= 'z') or (>= 'A' and <= 'Z') or (>= '0' and <= '9') or '-'));
    }

    private static (string?, string?, string?, string?) ParseTls(
        IReadOnlyDictionary<string, object?> values, string? baseDir, List<string> errors)
    {
        if (!values.TryGetValue("tls", out var raw) || raw is null)
        {
            return (null, null, null, null);
        }

        if (raw is not IReadOnlyDictionary<string, object?> tls)
        {
            errors.Add("'tls' must be a map (ca_file, client_certificate)");
            return (null, null, null, null);
        }

        RejectUnknown(tls, "tls", ["ca_file", "client_certificate"], errors);
        var ca = ResolvePath(Scalar(tls, "ca_file", "tls", errors), baseDir);

        string? certPath = null, keyPath = null, certPassword = null;
        if (tls.TryGetValue("client_certificate", out var rawCert) && rawCert is not null)
        {
            if (rawCert is not IReadOnlyDictionary<string, object?> cert)
            {
                errors.Add("tls.client_certificate must be a map (path, key_path, password)");
            }
            else
            {
                RejectUnknown(cert, "tls.client_certificate", ["path", "key_path", "password"], errors);
                var path = Scalar(cert, "path", "tls.client_certificate", errors, out var pathRefused);
                var key = Scalar(cert, "key_path", "tls.client_certificate", errors);
                certPassword = Scalar(cert, "password", "tls.client_certificate", errors);
                if (path is null)
                {
                    if (!pathRefused)
                    {
                        errors.Add("tls.client_certificate.path is required when tls.client_certificate is set");
                    }
                }
                else if (HasExtension(path, Pkcs12Extensions))
                {
                    if (key is not null)
                    {
                        errors.Add("tls.client_certificate.key_path applies only to a PEM certificate (.pem, .crt, .cer); a .pfx/.p12 file holds its own key");
                    }
                }
                else if (HasExtension(path, PemExtensions))
                {
                    if (key is null)
                    {
                        errors.Add("tls.client_certificate.key_path is required with a PEM certificate");
                    }
                }
                else
                {
                    errors.Add("tls.client_certificate.path must end in .pfx or .p12 (PKCS#12), or .pem, .crt or .cer (PEM with key_path)");
                }

                certPath = ResolvePath(path, baseDir);
                keyPath = ResolvePath(key, baseDir);
            }
        }

        return (ca, certPath, keyPath, certPassword);
    }

    private static bool HasExtension(string path, string[] extensions)
        => extensions.Contains(Path.GetExtension(path), StringComparer.OrdinalIgnoreCase);

    private static string? ResolvePath(string? path, string? baseDir)
        => ProjectRelativePath.Resolve(HomePath.Expand(path), baseDir);

    private static void RejectUnknown(IReadOnlyDictionary<string, object?> map, string block, string[] known, List<string> errors)
    {
        foreach (var key in map.Keys.Where(k => !known.Contains(k)))
        {
            errors.Add($"unknown option '{block}.{key}' (accepted: {string.Join(", ", known)})");
        }
    }

    private static string? Scalar(IReadOnlyDictionary<string, object?> map, string key, string block, List<string> errors)
        => Scalar(map, key, block, errors, out _);

    private static string? Scalar(IReadOnlyDictionary<string, object?> map, string key, string block,
        List<string> errors, out bool refused)
    {
        refused = false;
        map.TryGetValue(key, out var raw);
        if (YamlScalarText.NonScalarKind(raw) is { } kind)
        {
            errors.Add($"{block}.{key} must be a single value, not a {kind}");
            refused = true;
            return null;
        }

        return YamlScalarText.Of(raw) is { } text && !string.IsNullOrWhiteSpace(text) ? text : null;
    }
}
