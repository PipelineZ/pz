using System.Collections.Concurrent;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace Pz.Connector.Http.Tests;

/// <summary>A throwaway PKI: root → intermediate → server leaves, and a client certificate issued by the
/// root. Every certificate is round-tripped through PKCS#12 so its private key is a persisted one (what
/// SslStream on every platform can use). Built once per process: RSA key generation is the slow part.</summary>
internal sealed class TestPki
{
    public const string ClientPassword = "client-secret-pw";
    private static readonly Lazy<TestPki> Shared = new(() => new TestPki());

    public static TestPki Instance => Shared.Value;

    public X509Certificate2 Root { get; }
    public X509Certificate2 Intermediate { get; }

    /// <summary>For localhost, 127.0.0.1 and api.test (the name a test reaches through the proxy).</summary>
    public X509Certificate2 Leaf { get; }
    public X509Certificate2 ExpiredLeaf { get; }

    /// <summary>Valid and chained to the root, but only for 'other.test'.</summary>
    public X509Certificate2 WrongNameLeaf { get; }
    /// <summary>For localhost, chained to the root, but a client certificate (clientAuth only).</summary>
    public X509Certificate2 WrongUsageLeaf { get; }

    /// <summary>For localhost, signed by <see cref="Leaf"/>, which is not a CA.</summary>
    public X509Certificate2 SignedByLeaf { get; }
    public X509Certificate2 Client { get; }
    private readonly RSA _clientKey;

    private TestPki()
    {
        var now = DateTimeOffset.UtcNow;
        using var rootKey = RSA.Create(2048);
        Root = Persist(CreateCa("CN=pz test root", rootKey, null, null, now.AddDays(-60), now.AddYears(2)));
        using var intermediateKey = RSA.Create(2048);
        Intermediate = Persist(CreateCa("CN=pz test intermediate", intermediateKey, Root, rootKey,
            now.AddDays(-60), now.AddYears(1)));
        Leaf = Persist(CreateLeaf("CN=localhost", ["localhost", "api.test"], Intermediate, now.AddDays(-1), now.AddDays(90),
            serverAuth: true));
        ExpiredLeaf = Persist(CreateLeaf("CN=localhost", ["localhost", "api.test"], Intermediate, now.AddDays(-30),
            now.AddDays(-1), serverAuth: true));
        WrongNameLeaf = Persist(CreateLeaf("CN=other.test", ["other.test"], Intermediate, now.AddDays(-1), now.AddDays(90),
            serverAuth: true));
        WrongUsageLeaf = Persist(CreateLeaf("CN=localhost", ["localhost"], Intermediate, now.AddDays(-1), now.AddDays(90),
            serverAuth: false));
        SignedByLeaf = Persist(CreateLeaf("CN=localhost", ["localhost"], Leaf, now.AddDays(-1), now.AddDays(80),
            serverAuth: true));
        _clientKey = RSA.Create(2048);
        Client = Persist(CreateLeaf("CN=pz client", [], Root, now.AddDays(-1), now.AddDays(90), serverAuth: false,
            key: _clientKey));
    }

    /// <summary>The server's chain for <see cref="SslStreamCertificateContext"/>: what it sends after the leaf.</summary>
    public X509Certificate2Collection ServerChain(bool withIntermediate = true, bool withRoot = false)
    {
        var chain = new X509Certificate2Collection();
        if (withIntermediate)
        {
            chain.Add(Intermediate);
        }

        if (withRoot)
        {
            chain.Add(Root);
        }

        return chain;
    }

    /// <summary>Writes the files a connection config names into <paramref name="dir"/>: PEM bundles (with
    /// leading text before the first BEGIN, as real bundles have), the client certificate as a
    /// password-protected PKCS#12, and as PEM with a plain and an encrypted key.</summary>
    public void WriteFiles(string dir)
    {
        File.WriteAllText(Path.Combine(dir, "root.pem"), "# pz test root\nsubject=CN=pz test root\n" + Root.ExportCertificatePem() + "\n");
        File.WriteAllText(Path.Combine(dir, "intermediate.pem"), "intermediate only\n" + Intermediate.ExportCertificatePem() + "\n");
        File.WriteAllText(Path.Combine(dir, "no-certs.pem"), "# nothing here\n" + _clientKey.ExportPkcs8PrivateKeyPem() + "\n");
        File.WriteAllBytes(Path.Combine(dir, "client.pfx"), Client.Export(X509ContentType.Pkcs12, ClientPassword));
        File.WriteAllBytes(Path.Combine(dir, "client-nokey.pfx"),
            X509CertificateLoader.LoadCertificate(Client.RawData).Export(X509ContentType.Pkcs12, ClientPassword));
        File.WriteAllText(Path.Combine(dir, "client.pem"), Client.ExportCertificatePem());
        File.WriteAllText(Path.Combine(dir, "client.key"), _clientKey.ExportPkcs8PrivateKeyPem());
        File.WriteAllText(Path.Combine(dir, "client-enc.key"), _clientKey.ExportEncryptedPkcs8PrivateKeyPem(
            ClientPassword, new PbeParameters(PbeEncryptionAlgorithm.Aes256Cbc, HashAlgorithmName.SHA256, 10_000)));
    }

    private static X509Certificate2 Persist(X509Certificate2 created)
    {
        using (created)
        {
            return X509CertificateLoader.LoadPkcs12(created.Export(X509ContentType.Pkcs12, "x"), "x",
                X509KeyStorageFlags.Exportable);
        }
    }

    private static X509Certificate2 CreateCa(string subject, RSA key, X509Certificate2? issuer, RSA? issuerKey,
        DateTimeOffset notBefore, DateTimeOffset notAfter)
    {
        var request = new CertificateRequest(subject, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(
            X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
        if (issuer is null)
        {
            return request.CreateSelfSigned(notBefore, notAfter);
        }

        request.CertificateExtensions.Add(X509AuthorityKeyIdentifierExtension.CreateFromCertificate(issuer, true, false));
        using var signed = request.Create(issuer.SubjectName, X509SignatureGenerator.CreateForRSA(issuerKey!, RSASignaturePadding.Pkcs1),
            notBefore, notAfter, Serial());
        return signed.CopyWithPrivateKey(key);
    }

    private static X509Certificate2 CreateLeaf(string subject, string[] dnsNames, X509Certificate2 issuer,
        DateTimeOffset notBefore, DateTimeOffset notAfter, bool serverAuth, RSA? key = null)
    {
        using var generated = key is null ? RSA.Create(2048) : null;
        var leafKey = key ?? generated!;
        var request = new CertificateRequest(subject, leafKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(
            X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            [serverAuth ? new Oid("1.3.6.1.5.5.7.3.1") : new Oid("1.3.6.1.5.5.7.3.2")], false));
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
        request.CertificateExtensions.Add(X509AuthorityKeyIdentifierExtension.CreateFromCertificate(issuer, true, false));
        if (dnsNames.Length > 0)
        {
            var san = new SubjectAlternativeNameBuilder();
            foreach (var name in dnsNames)
            {
                san.AddDnsName(name);
            }

            if (dnsNames.Contains("localhost"))
            {
                san.AddIpAddress(IPAddress.Loopback);
            }

            request.CertificateExtensions.Add(san.Build());
        }

        // A child's validity must sit inside the issuer's, or the chain fails for that reason instead.
        var after = notAfter < issuer.NotAfter ? notAfter : new DateTimeOffset(issuer.NotAfter.ToUniversalTime());
        // The generator overload signs with any key: one test needs a certificate issued by a non-CA leaf.
        using var issuerKey = issuer.GetRSAPrivateKey()!;
        using var signed = request.Create(issuer.SubjectName, X509SignatureGenerator.CreateForRSA(issuerKey, RSASignaturePadding.Pkcs1),
            notBefore, after, Serial());
        return signed.CopyWithPrivateKey(leafKey);
    }

    private static byte[] Serial() => RandomNumberGenerator.GetBytes(12);
}

/// <summary>A one-port HTTPS server on 127.0.0.1: TLS through <see cref="SslStream"/>, then one HTTP/1.1
/// request per connection answered with a fixed 200 JSON array. Records whether the client presented a
/// certificate. Any Host is accepted, so it also stands behind <see cref="TestProxy"/> as 'api.test'. With
/// no certificate it serves plain http. Dispose it at the end of each test.</summary>
internal sealed class TestTlsServer : IAsyncDisposable
{
    public const string Body = """[{"id":1},{"id":2}]""";
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _accepting;
    private readonly SslStreamCertificateContext? _context;
    private readonly bool _requestClientCertificate;
    private readonly bool _requireClientCertificate;
    private readonly bool _rejectClientCertificates;
    private readonly SslProtocols _protocols;
    private readonly Func<string, string> _respond;

    /// <param name="requestClientCertificate">Ask for a client certificate but accept a handshake without one.</param>
    /// <param name="requireClientCertificate">Refuse a handshake without a client certificate.</param>
    /// <param name="rejectClientCertificates">Refuse every client certificate (a server trusting another CA).</param>
    /// <param name="respond">The JSON body for a request line; <see cref="Body"/> for every request when null.</param>
    /// <remarks>SslStream validates the client certificate once its own handshake is complete, so a refusal
    /// reaches the client as a closed connection, never as a handshake alert, whatever the TLS version.</remarks>
    public TestTlsServer(X509Certificate2? leaf, X509Certificate2Collection? chain = null,
        bool requestClientCertificate = false, bool requireClientCertificate = false,
        SslProtocols protocols = SslProtocols.None, bool rejectClientCertificates = false, Func<string, string>? respond = null)
    {
        _respond = respond ?? (_ => Body);
        _rejectClientCertificates = rejectClientCertificates;
        _context = leaf is null ? null : SslStreamCertificateContext.Create(leaf, chain ?? [], offline: true);
        _requestClientCertificate = requestClientCertificate || requireClientCertificate;
        _requireClientCertificate = requireClientCertificate;
        _protocols = protocols;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        _accepting = AcceptAsync();
    }

    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    public Uri Url(string host = "localhost") => new($"{(_context is null ? "http" : "https")}://{host}:{Port}/");

    /// <summary>One entry per completed handshake: the client certificate's thumbprint, or null when none.</summary>
    public ConcurrentQueue<string?> ClientCertificates { get; } = new();

    /// <summary>The request lines served.</summary>
    public ConcurrentQueue<string> Requests { get; } = new();

    private async Task AcceptAsync()
    {
        var connections = new List<Task>();
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                var socket = await _listener.AcceptSocketAsync(_stop.Token);
                connections.Add(ServeAsync(socket));
            }
        }
        catch (Exception) when (_stop.IsCancellationRequested)
        {
        }

        await Task.WhenAll(connections);
    }

    private async Task ServeAsync(Socket socket)
    {
        using var _ = socket;
        try
        {
            await using var network = new NetworkStream(socket, ownsSocket: false);
            if (_context is null)
            {
                await AnswerAsync(network);
                return;
            }

            await using var ssl = new SslStream(network);
            await ssl.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
            {
                ServerCertificateContext = _context,
                ClientCertificateRequired = _requestClientCertificate,
                EnabledSslProtocols = _protocols,
                // The test inspects what was presented; refusing only a missing certificate when one is
                // required, or any certificate when told to.
                RemoteCertificateValidationCallback = (_, certificate, _, _)
                    => (!_requireClientCertificate || certificate is not null) && !(_rejectClientCertificates && certificate is not null),
            }, _stop.Token);
            ClientCertificates.Enqueue(ssl.RemoteCertificate is { } presented
                ? X509CertificateLoader.LoadCertificate(presented.GetRawCertData()).Thumbprint
                : null);
            await AnswerAsync(ssl);
            await ssl.ShutdownAsync();
        }
        catch (Exception ex) when (ex is IOException or AuthenticationException or OperationCanceledException
            or SocketException or ObjectDisposedException)
        {
            // A refused handshake is the point of several tests; the client side reports it.
        }
    }

    private async Task AnswerAsync(Stream stream)
    {
        var head = await TestHttp.ReadHeadAsync(stream, _stop.Token);
        if (head is null)
        {
            return;
        }

        await TestHttp.ReadBodyAsync(stream, head, _stop.Token);
        var line = head.Split("\r\n")[0];
        Requests.Enqueue(line);
        var bytes = Encoding.UTF8.GetBytes(_respond(line));
        var response = $"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {bytes.Length}\r\n" +
            "Connection: close\r\n\r\n";
        await stream.WriteAsync(Encoding.ASCII.GetBytes(response), _stop.Token);
        await stream.WriteAsync(bytes, _stop.Token);
        await stream.FlushAsync(_stop.Token);
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        _listener.Stop();
        await _accepting.WaitAsync(TimeSpan.FromSeconds(30));
        _stop.Dispose();
    }
}

/// <summary>Minimal HTTP/1.1 framing for the test servers: a header block, then a Content-Length body.</summary>
internal static class TestHttp
{
    public static async Task<string?> ReadHeadAsync(Stream stream, CancellationToken ct)
    {
        var buffer = new List<byte>();
        var one = new byte[1];
        while (true)
        {
            if (await stream.ReadAsync(one, ct) == 0)
            {
                return null;
            }

            buffer.Add(one[0]);
            if (buffer.Count >= 4 && buffer[^4] == '\r' && buffer[^3] == '\n' && buffer[^2] == '\r' && buffer[^1] == '\n')
            {
                return Encoding.ASCII.GetString([.. buffer]);
            }
        }
    }

    public static string? Header(string head, string name)
        => head.Split("\r\n").Skip(1)
            .Select(line => line.Split(':', 2))
            .Where(parts => parts.Length == 2 && parts[0].Trim().Equals(name, StringComparison.OrdinalIgnoreCase))
            .Select(parts => parts[1].Trim())
            .FirstOrDefault();

    public static async Task<byte[]> ReadBodyAsync(Stream stream, string head, CancellationToken ct)
    {
        var length = int.TryParse(Header(head, "Content-Length"), out var n) ? n : 0;
        var body = new byte[length];
        await stream.ReadExactlyAsync(body, ct);
        return body;
    }
}
