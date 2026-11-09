using System.Security.Authentication;
using Azure.Core;
using Azure.Core.Pipeline;
using Azure.Identity;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

/// <summary>'tls:' end to end against a real TLS server and a throwaway PKI: ca_file trust (and what it
/// never relaxes), client certificates in both file formats, which client presents one, and the
/// lifetime of the loaded material.</summary>
public sealed class HttpTlsTests : IDisposable
{
    internal static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);
    private readonly string _dir = Directory.CreateTempSubdirectory("pz-tls-").FullName;
    private readonly TestPki _pki = TestPki.Instance;

    public HttpTlsTests()
    {
        try
        {
            _pki.WriteFiles(_dir);
        }
        catch
        {
            // xunit never disposes an instance whose constructor threw.
            Directory.Delete(_dir, recursive: true);
            throw;
        }
    }

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private string File(string name) => Path.Combine(_dir, name);

    internal static Dictionary<string, object?> Map(params (string Key, object? Value)[] pairs)
        => pairs.ToDictionary(p => p.Key, p => p.Value);

    private Dictionary<string, object?> Tls(string? ca = "root.pem", Dictionary<string, object?>? client = null)
    {
        var tls = new Dictionary<string, object?>();
        if (ca is not null)
        {
            tls["ca_file"] = File(ca);
        }

        if (client is not null)
        {
            tls["client_certificate"] = client;
        }

        return tls;
    }

    private Dictionary<string, object?> Pfx(string name = "client.pfx", string password = TestPki.ClientPassword)
        => Map(("path", File(name)), ("password", password));

    internal static ConnectorConfig Config(Uri baseUrl, Dictionary<string, object?>? tls = null, object? proxy = null,
        params (string Key, object? Value)[] extra)
    {
        var values = new Dictionary<string, object?> { ["base_url"] = baseUrl.ToString() };
        if (tls is not null)
        {
            values["tls"] = tls;
        }

        if (proxy is not null)
        {
            values["proxy"] = proxy;
        }

        foreach (var (k, v) in extra)
        {
            values[k] = v;
        }

        return new ConnectorConfig(values);
    }

    internal static HttpConnectionConfig Parse(ConnectorConfig config)
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(config, errors);
        Assert.Empty(errors);
        return connection!;
    }

    internal static async Task<long> ReadAsync(ISource source)
    {
        var partitions = await source.PlanReadAsync(new DatasetSpec("s", "d", Map(("path", "/items"))),
            ReadHints.None, CancellationToken.None);
        long rows = 0;
        await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        return rows;
    }

    internal static async Task<long> ReadAsync(ConnectorConfig config)
    {
        await using var source = await new HttpConnector().OpenAsync(config, CancellationToken.None);
        return await ReadAsync(source).WaitAsync(Bound);
    }

    internal static async Task<PzConnectorException> ReadFailsAsync(ConnectorConfig config)
        => await Assert.ThrowsAsync<PzConnectorException>(() => ReadAsync(config));

    [Fact]
    public async Task Private_ca_refused_then_trusted()
    {
        await using var server = new TestTlsServer(_pki.Leaf, _pki.ServerChain());

        var refused = await ReadFailsAsync(Config(server.Url()));
        Assert.False(refused.IsTransient);
        Assert.Contains("tls.ca_file", refused.Message, StringComparison.Ordinal);

        Assert.Equal(2, await ReadAsync(Config(server.Url(), Tls())));
        Assert.Equal(2, await ReadAsync(Config(server.Url("127.0.0.1"), Tls())));
    }

    [Fact]
    public async Task Expired_and_wrong_name_still_refused()
    {
        await using (var expired = new TestTlsServer(_pki.ExpiredLeaf, _pki.ServerChain()))
        {
            var ex = await ReadFailsAsync(Config(expired.Url(), Tls()));
            Assert.False(ex.IsTransient);
            Assert.Contains("tls.ca_file", ex.Message, StringComparison.Ordinal);
        }

        await using (var wrongName = new TestTlsServer(_pki.WrongNameLeaf, _pki.ServerChain()))
        {
            var ex = await ReadFailsAsync(Config(wrongName.Url(), Tls()));
            Assert.False(ex.IsTransient);
            Assert.Contains("tls.ca_file", ex.Message, StringComparison.Ordinal);
        }

        // The rule itself: chain errors over a trusted anchor are never enough with any other flag set.
        Assert.False(HttpHandlerFactory.Trusts([_pki.Root], _pki.Leaf, null,
            System.Net.Security.SslPolicyErrors.RemoteCertificateChainErrors | System.Net.Security.SslPolicyErrors.RemoteCertificateNameMismatch));
        Assert.False(HttpHandlerFactory.Trusts([_pki.Root], null, null,
            System.Net.Security.SslPolicyErrors.RemoteCertificateNotAvailable));
    }

    [Fact]
    public async Task Intermediate_in_ca_file()
    {
        // The server sends its intermediate: an intermediate-only ca_file is an anchor for it.
        await using (var full = new TestTlsServer(_pki.Leaf, _pki.ServerChain()))
        {
            Assert.Equal(2, await ReadAsync(Config(full.Url(), Tls("intermediate.pem"))));
        }

        // ... including when the server also sends the (untrusted) root.
        await using (var withRoot = new TestTlsServer(_pki.Leaf, _pki.ServerChain(withRoot: true)))
        {
            Assert.Equal(2, await ReadAsync(Config(withRoot.Url(), Tls("intermediate.pem"))));
        }

        // A root-only file cannot bridge a missing intermediate.
        await using var partial = new TestTlsServer(_pki.Leaf, _pki.ServerChain(withIntermediate: false));
        var ex = await ReadFailsAsync(Config(partial.Url(), Tls()));
        Assert.False(ex.IsTransient);
        Assert.Contains("tls.ca_file", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Client_certificate_pfx_and_pem()
    {
        // No certificate configured, server requires one. A server that refuses inside the handshake (a TLS 1.2
        // alert) gives a permanent error naming tls.client_certificate; one that refuses after it (TLS 1.3, or
        // SslStream on any version) closes the connection, which is indistinguishable from a reset and stays
        // transient. Either is honest; nothing else is.
        foreach (var protocols in new[] { SslProtocols.Tls12, SslProtocols.None })
        {
            await using var refusing = new TestTlsServer(_pki.Leaf, _pki.ServerChain(), requireClientCertificate: true,
                protocols: protocols);
            var none = await ReadFailsAsync(Config(refusing.Url(), Tls()));
            Assert.True(none.IsTransient || none.Message.Contains("tls.client_certificate", StringComparison.Ordinal), none.Message);
        }

        // A configured certificate the server refuses: whichever way the refusal arrives, the setting is named.
        await using (var rejecting = new TestTlsServer(_pki.Leaf, _pki.ServerChain(), requireClientCertificate: true,
            rejectClientCertificates: true))
        {
            var rejected = await ReadFailsAsync(Config(rejecting.Url(), Tls(client: Pfx())));
            Assert.Contains("tls.client_certificate", rejected.Message, StringComparison.Ordinal);
            if (rejected.IsTransient)
            {
                Assert.EndsWith(HttpHandlerFactory.ClientCertificateHint, rejected.Message, StringComparison.Ordinal);
            }
        }

        await using var server = new TestTlsServer(_pki.Leaf, _pki.ServerChain(), requireClientCertificate: true);
        await using var tls12 = new TestTlsServer(_pki.Leaf, _pki.ServerChain(), requireClientCertificate: true,
            protocols: SslProtocols.Tls12);
        Assert.Equal(2, await ReadAsync(Config(tls12.Url(), Tls(client: Pfx()))));
        Assert.Equal(2, await ReadAsync(Config(server.Url(), Tls(client: Pfx()))));
        Assert.Equal(2, await ReadAsync(Config(server.Url(), Tls(client: Map(("path", File("client.pem")), ("key_path", File("client.key")))))));
        Assert.Equal(2, await ReadAsync(Config(server.Url(), Tls(client: Map(("path", File("client.pem")),
            ("key_path", File("client-enc.key")), ("password", TestPki.ClientPassword))))));
        Assert.Equal(Enumerable.Repeat<string?>(_pki.Client.Thumbprint, 3), server.ClientCertificates.ToArray());
        Assert.Equal(new string?[] { _pki.Client.Thumbprint }, tls12.ClientCertificates.ToArray());
    }

    [Fact]
    public void Transport_failures_are_classified()
    {
        var bound = Parse(Config(new Uri("https://localhost/"), Tls(client: Pfx()))).Bind(TimeProvider.System);
        using var _ = bound.Bound;

        // A refusal after the handshake, with a certificate configured: transient, naming the setting.
        var reset = HttpHandlerFactory.Describe(
            new HttpRequestException(HttpRequestError.ResponseEnded, "ended", new IOException("reset")), bound.Transport);
        Assert.NotNull(reset);
        Assert.True(reset.Value.Transient);
        Assert.EndsWith(HttpHandlerFactory.ClientCertificateHint, reset.Value.Detail, StringComparison.Ordinal);

        // Without a certificate configured, the same failure keeps today's classification.
        Assert.Null(HttpHandlerFactory.Describe(
            new HttpRequestException(HttpRequestError.ResponseEnded, "ended", new IOException("reset")), HttpTransportOptions.Default));

        // A handshake alert (a server refusing inside a TLS 1.2 handshake): permanent, naming the certificate.
        var alert = HttpHandlerFactory.Describe(new HttpRequestException(HttpRequestError.SecureConnectionError,
            "The SSL connection could not be established, see inner exception.",
            new AuthenticationException("Authentication failed, see inner exception.", new IOException("handshake failure"))),
            HttpTransportOptions.Default);
        Assert.False(alert!.Value.Transient);
        Assert.Contains("tls.client_certificate", alert.Value.Detail, StringComparison.Ordinal);
        Assert.DoesNotContain("tls.ca_file", alert.Value.Detail, StringComparison.Ordinal);

        // A refused tunnel with another status keeps that status's class; a 407 names the credentials.
        var badGateway = HttpHandlerFactory.Describe(new HttpRequestException(HttpRequestError.ProxyTunnelError, "tunnel",
            null, System.Net.HttpStatusCode.BadGateway), HttpTransportOptions.Default);
        Assert.True(badGateway!.Value.Transient);
        var auth = HttpHandlerFactory.Describe(new HttpRequestException(HttpRequestError.ProxyTunnelError, "tunnel",
            null, System.Net.HttpStatusCode.ProxyAuthenticationRequired), HttpTransportOptions.Default);
        Assert.Equal((HttpHandlerFactory.ProxyAuthenticationFailed, false), auth);
    }

    [Fact]
    public async Task Bare_client_presents_no_certificate()
    {
        await using var server = new TestTlsServer(_pki.Leaf, _pki.ServerChain(), requestClientCertificate: true);
        var bound = Parse(Config(server.Url(), Tls(client: Pfx()))).Bind(TimeProvider.System);
        using var transport = bound.Bound;

        using (var main = HttpSource.CreateClient(bound))
        {
            (await main.GetAsync("items").WaitAsync(Bound)).Dispose();
        }

        using (var bare = HttpSource.CreateBareClient(bound))
        {
            (await bare.GetAsync(new Uri(server.Url(), "items")).WaitAsync(Bound)).Dispose();
        }

        Assert.Equal(new string?[] { _pki.Client.Thumbprint, null }, server.ClientCertificates.ToArray());
    }

    [Fact]
    public async Task Plain_http_with_client_certificate_config()
    {
        await using var server = new TestTlsServer(null);
        Assert.Equal(2, await ReadAsync(Config(server.Url("127.0.0.1"), Tls(ca: null, client: Pfx()))));
        Assert.Empty(server.ClientCertificates);
    }

    [Fact]
    public async Task Wrong_password_bind_error_secret_free()
    {
        var url = new Uri("https://localhost/");
        foreach (var client in new[]
        {
            Pfx(password: "wrong-pw-1234"),
            Map(("path", File("client.pem")), ("key_path", File("client-enc.key")), ("password", "wrong-pw-1234")),
        })
        {
            var ex = Assert.Throws<PzConnectorException>(() => Parse(Config(url, Tls(client: client))).Bind(TimeProvider.System));
            Assert.False(ex.IsTransient);
            Assert.Contains("tls.client_certificate", ex.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("wrong-pw-1234", ex.Message, StringComparison.Ordinal);
            Assert.DoesNotContain(TestPki.ClientPassword, ex.Message, StringComparison.Ordinal);

            var check = await new HttpConnector().CheckConnectionAsync(Config(url, Tls(client: client)), CancellationToken.None);
            Assert.False(check.Ok);
            Assert.Contains("tls.client_certificate", check.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("wrong-pw-1234", check.Message!, StringComparison.Ordinal);
            Assert.DoesNotContain("sign-in", check.Message!, StringComparison.Ordinal);
        }

        var noKey = Assert.Throws<PzConnectorException>(
            () => Parse(Config(url, Tls(client: Pfx("client-nokey.pfx")))).Bind(TimeProvider.System));
        Assert.Contains("private key", noKey.Message, StringComparison.Ordinal);
        Assert.Contains("client-nokey.pfx", noKey.Message, StringComparison.Ordinal);

        var missing = Assert.Throws<PzConnectorException>(
            () => Parse(Config(url, Tls(client: Pfx("absent.pfx")))).Bind(TimeProvider.System));
        Assert.Contains("was not found", missing.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Pem_bundle_parse_helper()
    {
        // Leading text before '-----BEGIN' (a comment, an openssl 'subject=' line) is skipped.
        using var root = BoundTransport.Load(Parse(Config(new Uri("https://x.test/"), Tls())).Transport)!;
        Assert.Equal(_pki.Root.Thumbprint, Assert.Single(root.CaCerts!).Thumbprint);

        var url = new Uri("https://x.test/");
        var none = Assert.Throws<PzConnectorException>(() => BoundTransport.Load(Parse(Config(url, Tls("no-certs.pem"))).Transport));
        Assert.Equal($"tls.ca_file '{File("no-certs.pem")}' has no PEM certificates", none.Message);
        Assert.False(none.IsTransient);

        var missing = Assert.Throws<PzConnectorException>(() => BoundTransport.Load(Parse(Config(url, Tls("absent.pem"))).Transport));
        Assert.Equal($"tls.ca_file '{File("absent.pem")}' was not found", missing.Message);

        Assert.Null(BoundTransport.Load(HttpTransportOptions.Default));
    }

    [Fact]
    public async Task Certificates_disposed_with_source()
    {
        await using var server = new TestTlsServer(_pki.Leaf, _pki.ServerChain(), requireClientCertificate: true);
        var connection = Parse(Config(server.Url(), Tls(client: Pfx())));

        BoundTransport first;
        await using (var source = new HttpSource(connection))
        {
            Assert.Equal(2, await ReadAsync(source).WaitAsync(Bound));
            first = source.Connection.Bound!;
        }

        Assert.Equal(IntPtr.Zero, first.ClientCert!.Handle);
        Assert.All(first.CaCerts!, c => Assert.Equal(IntPtr.Zero, c.Handle));

        // A second read in the same process loads its own copies.
        await using (var second = new HttpSource(connection))
        {
            Assert.Equal(2, await ReadAsync(second).WaitAsync(Bound));
        }

        var sink = new HttpSink(connection);
        var sinkTransport = sink.Connection.Bound!;
        await sink.DisposeAsync();
        Assert.Equal(IntPtr.Zero, sinkTransport.ClientCert!.Handle);
        Assert.Equal(new string?[] { _pki.Client.Thumbprint, _pki.Client.Thumbprint }, server.ClientCertificates.ToArray());
    }

    [Fact]
    public async Task Azure_sp_transport_and_chain_refused()
    {
        await using var server = new TestTlsServer(_pki.Leaf, _pki.ServerChain());
        var connection = Parse(Config(server.Url(), Tls()));
        using var bound = BoundTransport.Load(connection.Transport);
        var handlers = HttpHandlerFactory.TokenEndpointHandlers(connection, bound);
        Assert.NotNull(handlers);

        var sp = new AzureAdOptions("service_principal", "t", "c", "s", null, null, "api://x/.default", server.Url());
        var options = sp.CreateCredentialOptions(TimeSpan.FromSeconds(7), handlers);
        Assert.IsType<HttpClientTransport>(options.Transport);
        Assert.NotSame(HttpClientTransport.Shared, options.Transport);

        // The transport carries the connection's trust: a request through it reaches the private-CA server.
        var pipeline = HttpPipelineBuilder.Build(new ProbeOptions { Transport = options.Transport });
        using var message = pipeline.CreateMessage();
        message.Request.Uri.Reset(new Uri(server.Url(), "items"));
        await pipeline.SendAsync(message, CancellationToken.None).AsTask().WaitAsync(Bound);
        Assert.Equal(200, message.Response.Status);
        (options.Transport as IDisposable)?.Dispose();

        var identity = new AzureAdOptions("managed_identity", null, null, null, null, null, "api://x/.default", null)
            .CreateCredentialOptions(TimeSpan.FromSeconds(7), handlers);
        Assert.Same(HttpClientTransport.Shared, identity.Transport);

        var errors = new List<string>();
        HttpConnectionConfig.Parse(Config(server.Url(), Tls(), extra: ("auth", Map(("type", "azure_ad"),
            ("method", "credential_chain"), ("scope", "api://x/.default")))), errors);
        Assert.Contains("auth.method credential_chain cannot use proxy or tls: its developer-tool credentials run outside pz", errors);
    }

    private sealed class ProbeOptions : ClientOptions
    {
    }
}
