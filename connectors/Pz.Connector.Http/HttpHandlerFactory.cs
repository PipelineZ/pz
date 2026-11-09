using System.Net;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Http;

namespace Pz.Connector.Http;

/// <summary>The TLS material a connection's 'tls:' names, loaded once per bound connection and owned by the
/// source, sink or check that bound it: disposing it releases every certificate. Null (never loaded) when
/// the connection sets neither 'proxy' nor 'tls'.</summary>
internal sealed class BoundTransport : IDisposable
{
    private BoundTransport(X509Certificate2Collection? caCerts, X509Certificate2? clientCert)
    {
        CaCerts = caCerts;
        ClientCert = clientCert;
    }

    /// <summary>The trust anchors 'tls.ca_file' adds; null when it is not set.</summary>
    public X509Certificate2Collection? CaCerts { get; }

    /// <summary>'tls.client_certificate' with its private key; null when it is not set.</summary>
    public X509Certificate2? ClientCert { get; }

    // The 'ca_file' trust callback's last verdict per authority, so a failed handshake can name tls.ca_file
    // without reading the runtime's (possibly localized) exception text.
    private readonly System.Collections.Concurrent.ConcurrentDictionary<string, bool> _refused = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Notes the trust callback's verdict for the request's authority and returns it.</summary>
    public bool Record(Uri? request, bool trusted)
    {
        if (request is not null)
        {
            _refused[request.Authority] = !trusted;
        }

        return trusted;
    }

    /// <summary>True when the trust callback refused <paramref name="target"/>'s certificate at its last
    /// handshake; null when it never judged that authority (no 'ca_file', or the handshake failed before).</summary>
    public bool? WasRefused(Uri target) => _refused.TryGetValue(target.Authority, out var refused) ? refused : null;

    /// <summary>Loads the files <paramref name="options"/> names. Every failure is a permanent error that
    /// names the setting and the path, never a password.</summary>
    public static BoundTransport? Load(HttpTransportOptions options)
    {
        if (options.IsDefault)
        {
            return null;
        }

        var ca = options.CaFile is { } caFile ? LoadCaFile(caFile) : null;
        try
        {
            var client = options.CertPath is { } path ? LoadClientCertificate(path, options.KeyPath, options.CertPassword) : null;
            return new BoundTransport(ca, client);
        }
        catch
        {
            Dispose(ca);
            throw;
        }
    }

    public void Dispose()
    {
        Dispose(CaCerts);
        ClientCert?.Dispose();
    }

    private static void Dispose(X509Certificate2Collection? certificates)
    {
        foreach (var certificate in certificates ?? [])
        {
            certificate.Dispose();
        }
    }

    // Every '-----BEGIN CERTIFICATE-----' block is imported; text between blocks (comments, openssl's
    // 'subject='/'issuer=' lines) and non-certificate PEM items are skipped.
    private static X509Certificate2Collection LoadCaFile(string path)
    {
        if (!File.Exists(path))
        {
            throw Permanent($"tls.ca_file '{path}' was not found");
        }

        var certificates = new X509Certificate2Collection();
        try
        {
            certificates.ImportFromPem(File.ReadAllText(path));
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw Permanent($"tls.ca_file '{path}' could not be read");
        }
        catch (CryptographicException)
        {
            Dispose(certificates);
            throw Permanent($"tls.ca_file '{path}' holds a malformed PEM certificate");
        }

        return certificates.Count > 0 ? certificates : throw Permanent($"tls.ca_file '{path}' has no PEM certificates");
    }

    // The key must be a persisted one, never EphemeralKeySet: Windows SChannel cannot present a certificate
    // whose key lives only in memory. A PEM certificate and key are therefore re-imported through PKCS#12.
    private static X509Certificate2 LoadClientCertificate(string path, string? keyPath, string? password)
    {
        if (!File.Exists(path))
        {
            throw Permanent($"tls.client_certificate.path '{path}' was not found");
        }

        if (keyPath is not null && !File.Exists(keyPath))
        {
            throw Permanent($"tls.client_certificate.key_path '{keyPath}' was not found");
        }

        X509Certificate2 certificate;
        try
        {
            if (keyPath is null)
            {
                certificate = X509CertificateLoader.LoadPkcs12FromFile(path, password, X509KeyStorageFlags.DefaultKeySet);
            }
            else
            {
                using var pem = password is null
                    ? X509Certificate2.CreateFromPemFile(path, keyPath)
                    : X509Certificate2.CreateFromEncryptedPemFile(path, password, keyPath);
                certificate = X509CertificateLoader.LoadPkcs12(pem.Export(X509ContentType.Pkcs12), null,
                    X509KeyStorageFlags.DefaultKeySet);
            }
        }
        catch (Exception ex) when (ex is CryptographicException or IOException or UnauthorizedAccessException or ArgumentException)
        {
            // The framework text can echo key details; only the setting and the path are named.
            throw Permanent($"tls.client_certificate '{path}' could not be loaded (an unreadable or malformed file, " +
                "a wrong tls.client_certificate.password, or a key_path that does not match the certificate)");
        }

        if (!certificate.HasPrivateKey)
        {
            certificate.Dispose();
            throw Permanent($"tls.client_certificate '{path}' has no private key (a .pfx/.p12 file must hold the key; " +
                "a PEM certificate takes it from key_path)");
        }

        return certificate;
    }

    private static PzConnectorException Permanent(string message) => new(message, isTransient: false);
}

/// <summary>Builds every connector <see cref="HttpClientHandler"/> from the connection's 'proxy:' and 'tls:'
/// options, and maps the transport failures those options cause to errors naming the setting to fix.</summary>
internal static class HttpHandlerFactory
{
    public const string ClientCertificateHint = "(if the server requires a client certificate, check tls.client_certificate)";

    public const string ProxyAuthenticationFailed =
        "the proxy refused authentication (HTTP 407); check proxy.user/proxy.password";

    /// <summary>Redirects are always off: <see cref="HttpRequestSender"/> follows them by hand so a redirect
    /// can never carry credentials to a host the endpoint picked. With neither 'proxy' nor 'tls' the handler
    /// is exactly the one connections always had (default proxy, system trust, no client certificate).
    /// <paramref name="withClientCertificate"/> is false for the bare (cross-origin) client and for a token
    /// endpoint on another origin: the certificate identifies pz to the API, not to whatever host it names.</summary>
    public static HttpClientHandler Create(HttpConnectionConfig connection, BoundTransport? bound, bool withClientCertificate)
    {
        var handler = new HttpClientHandler
        {
            AllowAutoRedirect = false,
            AutomaticDecompression = System.Net.DecompressionMethods.All,
        };
        var options = connection.Transport;
        if (options.IsDefault)
        {
            return handler;
        }

        if (bound is null)
        {
            handler.Dispose();
            throw new InvalidOperationException("a connection with 'proxy' or 'tls' must be bound before a client is built");
        }

        switch (options.Mode)
        {
            case ProxyMode.None:
                // Also opts out of the environment's and the OS's proxy settings.
                handler.UseProxy = false;
                break;
            case ProxyMode.Explicit:
                handler.UseProxy = true;
                handler.Proxy = new ExplicitProxy(options.ProxyUrl!,
                    options.ProxyUser is { } user ? new NetworkCredential(user, options.ProxyPassword) : null, options.Bypass);
                break;
        }

        if (bound.CaCerts is { } ca)
        {
            handler.ServerCertificateCustomValidationCallback = (request, certificate, chain, errors)
                => bound.Record(request.RequestUri, Trusts(ca, certificate, chain, errors));
        }

        if (withClientCertificate && bound.ClientCert is { } client)
        {
            handler.ClientCertificateOptions = ClientCertificateOption.Manual;
            handler.ClientCertificates.Add(client);
        }

        return handler;
    }

    /// <summary>The handler source a token-fetching authenticator uses for its token endpoint: the client
    /// certificate is presented only when the endpoint has <c>base_url</c>'s origin (scheme, host and port,
    /// the rule credentials follow everywhere else). Null when the connection sets neither 'proxy' nor
    /// 'tls', so authenticators keep their own default transport.</summary>
    public static Func<Uri, HttpMessageHandler>? TokenEndpointHandlers(HttpConnectionConfig connection, BoundTransport? bound)
        => bound is null
            ? null
            : endpoint => Create(connection, bound, withClientCertificate: IsSameOrigin(endpoint, connection.BaseUrl));

    private static bool IsSameOrigin(Uri a, Uri b)
        => string.Equals(a.Scheme, b.Scheme, StringComparison.OrdinalIgnoreCase)
            && string.Equals(a.Host, b.Host, StringComparison.OrdinalIgnoreCase)
            && a.Port == b.Port;

    /// <summary>'tls.ca_file' adds trust anchors and relaxes nothing else. A certificate the system already
    /// trusts is accepted. Otherwise the only acceptable fault is an untrusted or incomplete chain: a name
    /// mismatch, a missing certificate or chain, or any other chain status (expiry included) is refused, and
    /// the certificate must then chain, as a server certificate (serverAuth), to an anchor in the file (a
    /// root or an intermediate). Revocation is not checked, matching the runtime's default for
    /// system-trusted certificates (a private CA rarely publishes a reachable CRL or OCSP responder).</summary>
    internal static bool Trusts(X509Certificate2Collection ca, X509Certificate2? certificate, X509Chain? presented,
        SslPolicyErrors errors)
    {
        if (errors == SslPolicyErrors.None)
        {
            return true;
        }

        if (certificate is null || presented is null || errors != SslPolicyErrors.RemoteCertificateChainErrors)
        {
            return false;
        }

        if (presented.ChainStatus.Any(s => !IsAnchorFault(s.Status)))
        {
            return false;
        }

        using var chain = new X509Chain();
        chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        chain.ChainPolicy.CustomTrustStore.AddRange(ca);
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.DisableCertificateDownloads = true;
        chain.ChainPolicy.ApplicationPolicy.Add(new Oid(ServerAuthentication));
        // The certificates the server sent bridge to the anchor; they belong to the runtime, which disposes them.
        chain.ChainPolicy.ExtraStore.AddRange(presented.ChainPolicy.ExtraStore);
        try
        {
            if (chain.Build(certificate))
            {
                return true;
            }

            // An intermediate in the file is an anchor too. Some chain engines (OpenSSL on Linux) accept only a
            // self-signed anchor and report the chain as partial or untrusted at the file's certificate, so the
            // built chain is checked by hand: the chain may carry no fault but those two, every certificate
            // below the anchor must be clean (validity, signature, constraints, usage), and the anchor itself
            // may carry only the partial/untrusted flags.
            if (chain.ChainStatus.Any(s => !IsAnchorFault(s.Status)))
            {
                return false;
            }

            var elements = chain.ChainElements;
            for (var i = 1; i < elements.Count; i++)
            {
                if (!ca.Any(anchor => anchor.RawDataMemory.Span.SequenceEqual(elements[i].Certificate.RawDataMemory.Span)))
                {
                    continue;
                }

                return elements.Take(i).All(e => e.ChainElementStatus.All(s => s.Status == X509ChainStatusFlags.NoError))
                    && elements[i].ChainElementStatus.All(s => IsAnchorFault(s.Status));
            }

            return false;
        }
        finally
        {
            foreach (var element in chain.ChainElements)
            {
                element.Certificate.Dispose();
            }
        }
    }

    private const string ServerAuthentication = "1.3.6.1.5.5.7.3.1";

    private static bool IsAnchorFault(X509ChainStatusFlags status)
        => status is X509ChainStatusFlags.NoError or X509ChainStatusFlags.UntrustedRoot or X509ChainStatusFlags.PartialChain;

    /// <summary>The failures 'proxy' and 'tls' explain, as a message detail and a transient flag; null for
    /// any other failure, which keeps its existing classification.
    /// <list type="bullet">
    /// <item>A TLS authentication failure (an <see cref="AuthenticationException"/>) is permanent: naming
    /// tls.ca_file when the server's certificate was refused (recorded by the 'ca_file' trust callback for
    /// <paramref name="target"/>; without one, the runtime's refusal text is the fallback) and
    /// tls.client_certificate otherwise.</item>
    /// <item>A proxy's explicit 407 to the tunnel request is permanent and names the proxy credentials; any
    /// other refused tunnel keeps its status's class.</item>
    /// <item>An I/O failure (a reset during or after the handshake) keeps its class; with a client
    /// certificate configured it gains a hint, since a server refusing the certificate after a TLS 1.3
    /// handshake looks exactly like a reset.</item>
    /// </list></summary>
    internal static (string Detail, bool Transient)? Describe(HttpRequestException ex, HttpConnectionConfig connection, Uri target)
    {
        var chain = Chain(ex).ToList();
        if (chain.OfType<AuthenticationException>().Any())
        {
            var cause = chain[^1].Message;
            var serverRefused = connection.Bound?.WasRefused(target)
                ?? chain.OfType<AuthenticationException>().Any(a => a.Message.Contains("remote certificate", StringComparison.OrdinalIgnoreCase));
            return serverRefused
                ? ($"the server's TLS certificate was refused ({cause}); if the server uses a private CA, set tls.ca_file " +
                    "to its CA certificate (an expired certificate or a host name mismatch is always refused)", false)
                : ($"the TLS handshake failed ({cause}); if the server requires a client certificate, check tls.client_certificate", false);
        }

        if (ex.HttpRequestError == HttpRequestError.ProxyTunnelError && ex.StatusCode is { } status)
        {
            return status == HttpStatusCode.ProxyAuthenticationRequired
                ? (ProxyAuthenticationFailed, false)
                : ($"the proxy refused the tunnel (HTTP {(int)status})", TransientClassifier.IsTransientStatus((int)status));
        }

        if (connection.Transport.CertPath is not null
            && (ex.HttpRequestError == HttpRequestError.SecureConnectionError || chain.OfType<IOException>().Any()))
        {
            return ($"{ex.Message} {ClientCertificateHint}", TransientClassifier.IsTransientException(ex));
        }

        return null;
    }

    private static IEnumerable<Exception> Chain(Exception ex)
    {
        for (Exception? current = ex; current is not null; current = current.InnerException)
        {
            yield return current;
        }
    }

    /// <summary>'proxy.url' for every request except loopback and the 'bypass' hosts, matched by
    /// <see cref="HttpTransportOptions.BypassMatches"/>; user text never becomes a regular expression.</summary>
    private sealed class ExplicitProxy(Uri url, ICredentials? credentials, IReadOnlyList<string> bypass) : IWebProxy
    {
        public ICredentials? Credentials { get; set; } = credentials;

        public Uri? GetProxy(Uri destination) => IsBypassed(destination) ? destination : url;

        public bool IsBypassed(Uri host)
            => host.IsLoopback || bypass.Any(pattern => HttpTransportOptions.BypassMatches(pattern, host.IdnHost));
    }
}
