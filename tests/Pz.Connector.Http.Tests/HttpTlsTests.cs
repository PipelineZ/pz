using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
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

        // An intermediate anchor relaxes nothing below it: an expired leaf is still refused.
        await using (var expired = new TestTlsServer(_pki.ExpiredLeaf, _pki.ServerChain()))
        {
            var refused = await ReadFailsAsync(Config(expired.Url(), Tls("intermediate.pem")));
            Assert.False(refused.IsTransient);
            Assert.Contains("tls.ca_file", refused.Message, StringComparison.Ordinal);
        }

        // A root-only file cannot bridge a missing intermediate.
        await using var partial = new TestTlsServer(_pki.Leaf, _pki.ServerChain(withIntermediate: false));
        var diag = OperatingSystem.IsWindows() ? await DiagnoseAsync(partial) : "";
        PzConnectorException ex;
        try
        {
            ex = await ReadFailsAsync(Config(partial.Url(), Tls()));
        }
        catch (Exception failure)
        {
            throw new Xunit.Sdk.XunitException("DIAG " + diag + "\n" + failure.Message);
        }

        Assert.False(ex.IsTransient);
        Assert.Contains("tls.ca_file", ex.Message, StringComparison.Ordinal);
    }

    // DIAGNOSTIC (temporary, Windows CI): what the trust callback sees for a server that omits its intermediate.
    private async Task<string> DiagnoseAsync(TestTlsServer server)
    {
        var log = new System.Text.StringBuilder();
        log.Append($"serverSends=[{string.Join("; ", server.SentIntermediates.Select(c => c.Subject))}] ");
        foreach (var location in new[] { StoreLocation.CurrentUser, StoreLocation.LocalMachine })
        {
            using var store = new X509Store(StoreName.CertificateAuthority, location);
            store.Open(OpenFlags.ReadOnly);
            log.Append($"{location}\\CA has intermediate={store.Certificates.Contains(_pki.Intermediate)} ");
        }

        foreach (var location in new[] { StoreLocation.CurrentUser, StoreLocation.LocalMachine })
        {
            using var store = new X509Store(StoreName.Root, location);
            store.Open(OpenFlags.ReadOnly);
            log.Append($"{location}\\Root has root={store.Certificates.Contains(_pki.Root)} ");
        }

        using var handler = new HttpClientHandler();
        handler.ServerCertificateCustomValidationCallback = (_, certificate, chain, errors) =>
        {
            log.Append($"errors={errors} ");
            if (chain is not null)
            {
                log.Append($"chainStatus=[{string.Join(",", chain.ChainStatus.Select(s => s.Status))}] ");
                foreach (var element in chain.ChainElements)
                {
                    log.Append($"element {element.Certificate.Subject}: [{string.Join(",", element.ChainElementStatus.Select(s => s.Status))}] ");
                }

                log.Append($"extraStore=[{string.Join("; ", chain.ChainPolicy.ExtraStore.Select(c => c.Subject))}] ");
            }

            var verdict = HttpHandlerFactory.Trusts([_pki.Root], certificate, chain, errors);
            log.Append($"trusts={verdict} ");
            return verdict;
        };
        using var client = new HttpClient(handler);
        try
        {
            using var response = await client.GetAsync(server.Url());
            log.Append($"status={(int)response.StatusCode}");
        }
        catch (Exception e)
        {
            log.Append($"failed={e.GetType().Name}");
        }

        return log.ToString();
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

    /// <summary>The system chain the runtime hands the trust callback: built with system trust over what the
    /// server sent.</summary>
    private static X509Chain Presented(X509Certificate2 leaf, params X509Certificate2[] sent)
    {
        var chain = new X509Chain();
        chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        chain.ChainPolicy.DisableCertificateDownloads = true;
        chain.ChainPolicy.ExtraStore.AddRange(sent);
        chain.Build(leaf);
        return chain;
    }

    [Fact]
    public void Trust_rule_refuses_wrong_usage_and_a_leaf_used_as_ca()
    {
        const SslPolicyErrors chainErrors = SslPolicyErrors.RemoteCertificateChainErrors;
        foreach (var anchor in new[] { _pki.Root, _pki.Intermediate })
        {
            X509Certificate2Collection ca = [anchor];
            using (var good = Presented(_pki.Leaf, _pki.Intermediate))
            {
                Assert.True(HttpHandlerFactory.Trusts(ca, _pki.Leaf, good, chainErrors), anchor.Subject);
            }

            using (var wrongUsage = Presented(_pki.WrongUsageLeaf, _pki.Intermediate))
            {
                Assert.False(HttpHandlerFactory.Trusts(ca, _pki.WrongUsageLeaf, wrongUsage, chainErrors), anchor.Subject);
            }

            using (var leafAsCa = Presented(_pki.SignedByLeaf, _pki.Leaf, _pki.Intermediate))
            {
                Assert.False(HttpHandlerFactory.Trusts(ca, _pki.SignedByLeaf, leafAsCa, chainErrors), anchor.Subject);
            }

            // Without the runtime's chain there is nothing to judge.
            Assert.False(HttpHandlerFactory.Trusts(ca, _pki.Leaf, null, chainErrors));
        }
    }

    [Fact]
    public void Trust_rule_bridges_only_with_sent_certificates_never_an_os_store()
    {
        // On Windows, SslStreamCertificateContext puts a server's intermediates into the machine (or user)
        // CA store, and every chain engine there searches that store; .NET on Linux searches the user's CA
        // store too. An intermediate the server did not send must not bridge a leaf to a root-only ca_file
        // just because some store on this machine happens to hold it.
        const SslPolicyErrors chainErrors = SslPolicyErrors.RemoteCertificateChainErrors;
        X509Certificate2Collection rootOnly = [_pki.Root];
        using var store = new X509Store(StoreName.CertificateAuthority, StoreLocation.CurrentUser);
        store.Open(OpenFlags.ReadWrite);
        var added = !store.Certificates.Contains(_pki.Intermediate);
        try
        {
            store.Add(_pki.Intermediate);
        }
        catch (System.Security.Cryptography.CryptographicException)
        {
            // Some accounts may not write the user store (a CI runner on Windows); the rule must hold
            // either way, and there the server tests have already put the intermediate in a CA store.
            added = false;
        }

        try
        {
            using (var omitted = Presented(_pki.Leaf))
            {
                Assert.False(HttpHandlerFactory.Trusts(rootOnly, _pki.Leaf, omitted, chainErrors));
            }

            using var sent = Presented(_pki.Leaf, _pki.Intermediate);
            Assert.True(HttpHandlerFactory.Trusts(rootOnly, _pki.Leaf, sent, chainErrors));
        }
        finally
        {
            if (added)
            {
                store.Remove(_pki.Intermediate);
            }
        }
    }

    [Fact]
    public void Transport_failures_are_classified()
    {
        var target = new Uri("https://localhost/");
        var bound = Parse(Config(target, Tls(client: Pfx()))).Bind(TimeProvider.System);
        using var _ = bound.Bound;
        var plain = Parse(Config(target));

        // A reset during the handshake keeps today's (transient) class; with a certificate it gains the hint.
        HttpRequestException HandshakeReset() => new(HttpRequestError.SecureConnectionError,
            "The SSL connection could not be established, see inner exception.", new IOException("reset"));
        Assert.Null(HttpHandlerFactory.Describe(HandshakeReset(), plain, target));
        Assert.True(Pz.Connectors.Toolkit.Http.TransientClassifier.IsTransientException(HandshakeReset()));
        var handshakeHint = HttpHandlerFactory.Describe(HandshakeReset(), bound, target);
        Assert.True(handshakeHint!.Value.Transient);
        Assert.EndsWith(HttpHandlerFactory.ClientCertificateHint, handshakeHint.Value.Detail, StringComparison.Ordinal);

        // The trust callback's own refusal names tls.ca_file, whatever the runtime's text says.
        var alertText = new HttpRequestException(HttpRequestError.SecureConnectionError, "ssl",
            new AuthenticationException("Authentication failed, see inner exception."));
        Assert.Contains("tls.client_certificate", HttpHandlerFactory.Describe(alertText, bound, target)!.Value.Detail, StringComparison.Ordinal);
        bound.Bound!.Record(target, trusted: false);
        Assert.Contains("tls.ca_file", HttpHandlerFactory.Describe(alertText, bound, target)!.Value.Detail, StringComparison.Ordinal);
        bound.Bound.Record(target, trusted: true);
        Assert.Contains("tls.client_certificate", HttpHandlerFactory.Describe(alertText, bound, target)!.Value.Detail, StringComparison.Ordinal);

        // A refusal after the handshake, with a certificate configured: transient, naming the setting.
        var reset = HttpHandlerFactory.Describe(
            new HttpRequestException(HttpRequestError.ResponseEnded, "ended", new IOException("reset")), bound, target);
        Assert.NotNull(reset);
        Assert.True(reset.Value.Transient);
        Assert.EndsWith(HttpHandlerFactory.ClientCertificateHint, reset.Value.Detail, StringComparison.Ordinal);

        // Without a certificate configured, the same failure keeps today's classification.
        Assert.Null(HttpHandlerFactory.Describe(
            new HttpRequestException(HttpRequestError.ResponseEnded, "ended", new IOException("reset")), plain, target));

        // A handshake alert (a server refusing inside a TLS 1.2 handshake): permanent, naming the certificate.
        var alert = HttpHandlerFactory.Describe(new HttpRequestException(HttpRequestError.SecureConnectionError,
            "The SSL connection could not be established, see inner exception.",
            new AuthenticationException("Authentication failed, see inner exception.", new IOException("handshake failure"))),
            plain, target);
        Assert.False(alert!.Value.Transient);
        Assert.Contains("tls.client_certificate", alert.Value.Detail, StringComparison.Ordinal);
        Assert.DoesNotContain("tls.ca_file", alert.Value.Detail, StringComparison.Ordinal);

        // A refused tunnel with another status keeps that status's class; only an explicit 407 names the
        // credentials, and a tunnel failure without a status keeps today's classification.
        var badGateway = HttpHandlerFactory.Describe(new HttpRequestException(HttpRequestError.ProxyTunnelError, "tunnel",
            null, System.Net.HttpStatusCode.BadGateway), plain, target);
        Assert.True(badGateway!.Value.Transient);
        var auth = HttpHandlerFactory.Describe(new HttpRequestException(HttpRequestError.ProxyTunnelError, "tunnel",
            null, System.Net.HttpStatusCode.ProxyAuthenticationRequired), plain, target);
        Assert.Equal((HttpHandlerFactory.ProxyAuthenticationFailed, false), auth);
        Assert.Null(HttpHandlerFactory.Describe(new HttpRequestException(HttpRequestError.ProxyTunnelError, "tunnel"), plain, target));
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
            Assert.Contains("or the account cannot store private keys (Windows: the user profile must be loaded)",
                ex.Message, StringComparison.Ordinal);
            Assert.Contains("(cause: CryptographicException", ex.Message, StringComparison.Ordinal);
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
    public void Failed_bind_disposes_the_loaded_transport()
    {
        var connection = Parse(Config(new Uri("https://localhost/"), Tls(client: Pfx())))
            with { AuthFactory = (_, _) => throw new InvalidOperationException("boom") };
        var transport = BoundTransport.Load(connection.Transport);
        Assert.NotNull(transport?.ClientCert);
        Assert.NotEqual(IntPtr.Zero, transport.ClientCert.Handle);

        Assert.Throws<InvalidOperationException>(() => connection.Bind(TimeProvider.System, transport));
        Assert.Equal(IntPtr.Zero, transport.ClientCert.Handle);
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

    /// <summary>A token for <c>POST /token</c>, the rows for anything else.</summary>
    private static string TokenOrRows(string requestLine) => requestLine.StartsWith("POST /token ", StringComparison.Ordinal)
        ? """{"access_token":"tok","token_type":"Bearer","expires_in":3600}"""
        : TestTlsServer.Body;

    private static (string, object?) OAuth(Uri tokenUrl) => ("auth", Map(("type", "oauth2_client_credentials"),
        ("token_url", new Uri(tokenUrl, "token").ToString()), ("client_id", "c"), ("client_secret", "s3cret")));

    [Fact]
    public async Task Token_endpoint_trusts_ca_file_and_uses_proxy()
    {
        // api.test is reachable only through the proxy, and its certificate only through tls.ca_file.
        await using var proxy = new TestProxy();
        await using var server = new TestTlsServer(_pki.Leaf, _pki.ServerChain(), respond: TokenOrRows);
        var url = server.Url("api.test");
        var proxyMap = Map(("url", proxy.Url.ToString()));

        Assert.Equal(2, await ReadAsync(Config(url, Tls(), proxyMap, OAuth(url))));
        Assert.Equal(["POST /token HTTP/1.1", "GET /items HTTP/1.1"], server.Requests.ToArray());
        Assert.Equal([$"api.test:{server.Port}", $"api.test:{server.Port}"], proxy.Targets.ToArray());

        // Without the ca_file the token endpoint's certificate is refused, permanently, naming the setting.
        var untrusted = await ReadFailsAsync(Config(url, null, proxyMap, OAuth(url)));
        Assert.False(untrusted.IsTransient);
        Assert.Contains("tls.ca_file", untrusted.Message, StringComparison.Ordinal);

        // A proxy refusing the credentials is permanent and names them, never the password.
        await using var guarded = new TestProxy("u", "right-pw");
        var refused = await ReadFailsAsync(Config(url, Tls(),
            Map(("url", guarded.Url.ToString()), ("user", "u"), ("password", "wrong-pw-1234")), OAuth(url)));
        Assert.False(refused.IsTransient);
        Assert.Contains("proxy.user/proxy.password", refused.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("wrong-pw-1234", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Token_endpoint_cert_only_same_origin()
    {
        // Same origin as base_url: the token request presents the client certificate too.
        await using (var same = new TestTlsServer(_pki.Leaf, _pki.ServerChain(), requestClientCertificate: true, respond: TokenOrRows))
        {
            Assert.Equal(2, await ReadAsync(Config(same.Url(), Tls(client: Pfx()), extra: OAuth(same.Url()))));
            Assert.Equal(["POST /token HTTP/1.1", "GET /items HTTP/1.1"], same.Requests.ToArray());
            Assert.Equal(new string?[] { _pki.Client.Thumbprint, _pki.Client.Thumbprint }, same.ClientCertificates.ToArray());
        }

        // Another origin (here another port on the same host): trusted through ca_file, but no certificate.
        await using var token = new TestTlsServer(_pki.Leaf, _pki.ServerChain(), requestClientCertificate: true, respond: TokenOrRows);
        await using var api = new TestTlsServer(_pki.Leaf, _pki.ServerChain(), requestClientCertificate: true);
        Assert.Equal(2, await ReadAsync(Config(api.Url(), Tls(client: Pfx()), extra: OAuth(token.Url()))));
        Assert.Equal(["POST /token HTTP/1.1"], token.Requests.ToArray());
        Assert.Equal(new string?[] { null }, token.ClientCertificates.ToArray());
        Assert.Equal(new string?[] { _pki.Client.Thumbprint }, api.ClientCertificates.ToArray());
    }

    private sealed class ProbeOptions : ClientOptions
    {
    }
}
