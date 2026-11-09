using System.Net;
using System.Net.Sockets;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.TestKit;
using static Pz.Connector.Http.Tests.HttpTlsTests;

namespace Pz.Connector.Http.Tests;

/// <summary>'proxy:' end to end: an explicit proxy for https (CONNECT) and plain http (absolute URI), its
/// basic auth and failure classes, the bypass rule, 'none', and the untouched default handler. The test
/// proxy dials every target on 127.0.0.1, so 'api.test' is reachable only through it.</summary>
public sealed class HttpProxyTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("pz-proxy-").FullName;
    private readonly TestPki _pki = TestPki.Instance;

    public HttpProxyTests()
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

    private Dictionary<string, object?> Trust() => Map(("ca_file", Path.Combine(_dir, "root.pem")));

    private static Dictionary<string, object?> Proxy(TestProxy proxy, string? user = null, string? password = null,
        params string[] bypass)
    {
        var map = Map(("url", proxy.Url.ToString()));
        if (user is not null)
        {
            map["user"] = user;
            map["password"] = password;
        }

        if (bypass.Length > 0)
        {
            map["bypass"] = bypass.ToList();
        }

        return map;
    }

    [Fact]
    public async Task Requests_go_through_explicit_proxy()
    {
        await using var proxy = new TestProxy();
        await using var tls = new TestTlsServer(_pki.Leaf, _pki.ServerChain());
        await using var plain = new TestTlsServer(null);

        Assert.Equal(2, await ReadAsync(Config(tls.Url("api.test"), Trust(), Proxy(proxy))));
        Assert.Equal(2, await ReadAsync(Config(plain.Url("api.test"), null, Proxy(proxy))));
        Assert.Equal([$"api.test:{tls.Port}", $"api.test:{plain.Port}"], proxy.Targets.ToArray());
        Assert.Equal("GET /items HTTP/1.1", Assert.Single(plain.Requests));

        // Loopback never goes through the proxy.
        Assert.Equal(2, await ReadAsync(Config(tls.Url(), Trust(), Proxy(proxy))));
        Assert.Equal(2, proxy.Targets.Count);

        // A bypass entry is matched by HttpTransportOptions.BypassMatches, never a regex over user text.
        var connection = Parse(Config(tls.Url("api.test"), null, Proxy(proxy, bypass: ["*.corp.example", "files.test"])));
        using var transport = BoundTransport.Load(connection.Transport);
        using var handler = HttpHandlerFactory.Create(connection, transport, withClientCertificate: true);
        var webProxy = handler.Proxy!;
        Assert.True(handler.UseProxy);
        Assert.True(webProxy.IsBypassed(new Uri("https://a.corp.example/x")));
        Assert.True(webProxy.IsBypassed(new Uri("http://FILES.test/")));
        Assert.True(webProxy.IsBypassed(new Uri("http://127.0.0.1:9/")));
        Assert.False(webProxy.IsBypassed(new Uri("https://corp.example/")));
        Assert.False(webProxy.IsBypassed(new Uri("https://evilcorp.example/")));
        Assert.False(webProxy.IsBypassed(new Uri("https://api.test/")));
        Assert.Equal(proxy.Url, webProxy.GetProxy(new Uri("https://api.test/")));
    }

    [Fact]
    public async Task Proxy_407_is_permanent()
    {
        await using var proxy = new TestProxy("alice", "proxy-pw-123");
        await using var tls = new TestTlsServer(_pki.Leaf, _pki.ServerChain());
        await using var plain = new TestTlsServer(null);

        foreach (var (user, password) in new (string?, string?)[] { (null, null), ("alice", "wrong-pw-456") })
        {
            // https: the CONNECT is refused.
            var tunnel = await ReadFailsAsync(Config(tls.Url("api.test"), Trust(), Proxy(proxy, user, password)));
            Assert.False(tunnel.IsTransient);
            Assert.Contains("proxy.user/proxy.password", tunnel.Message, StringComparison.Ordinal);

            // http: the proxy's 407 arrives as a response.
            var response = await ReadFailsAsync(Config(plain.Url("api.test"), null, Proxy(proxy, user, password)));
            Assert.False(response.IsTransient);
            Assert.Contains("proxy.user/proxy.password", response.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("wrong-pw-456", tunnel.Message + response.Message, StringComparison.Ordinal);
        }

        Assert.Equal(2, await ReadAsync(Config(tls.Url("api.test"), Trust(), Proxy(proxy, "alice", "proxy-pw-123"))));
        Assert.Equal(2, await ReadAsync(Config(plain.Url("api.test"), null, Proxy(proxy, "alice", "proxy-pw-123"))));
    }

    [Fact]
    public async Task Proxy_unreachable_is_transient()
    {
        int port;
        using (var closed = new TcpListener(IPAddress.Loopback, 0))
        {
            closed.Start();
            port = ((IPEndPoint)closed.LocalEndpoint).Port;
        }

        var ex = await ReadFailsAsync(Config(new Uri("https://api.test/"), null,
            Map(("url", $"http://127.0.0.1:{port}"))));
        Assert.True(ex.IsTransient, ex.Message);
    }

    [Fact]
    public async Task Proxy_none_disables_proxy()
    {
        var connection = Parse(Config(new Uri("https://api.test/"), null, "none"));
        using var transport = BoundTransport.Load(connection.Transport);
        using var handler = HttpHandlerFactory.Create(connection, transport, withClientCertificate: true);
        Assert.False(handler.UseProxy);
        Assert.False(handler.AllowAutoRedirect);
        Assert.Null(handler.ServerCertificateCustomValidationCallback);

        await using var plain = new TestTlsServer(null);
        Assert.Equal(2, await ReadAsync(Config(plain.Url("127.0.0.1"), null, "none")));
    }

    [Fact]
    public void Default_handler_unchanged()
    {
        var connection = Parse(Config(new Uri("https://api.test/")));
        var bound = connection.Bind(TimeProvider.System);
        Assert.Null(bound.Bound);

        using var today = new HttpClientHandler { AllowAutoRedirect = false };
        foreach (var withCertificate in new[] { true, false })
        {
            using var handler = HttpHandlerFactory.Create(bound, null, withCertificate);
            Assert.False(handler.AllowAutoRedirect);
            Assert.Equal(today.UseProxy, handler.UseProxy);
            Assert.True(handler.UseProxy);
            Assert.Null(handler.Proxy);
            Assert.Null(handler.ServerCertificateCustomValidationCallback);
            Assert.Equal(today.ClientCertificateOptions, handler.ClientCertificateOptions);
            Assert.Empty(handler.ClientCertificates);
            Assert.Equal(today.CheckCertificateRevocationList, handler.CheckCertificateRevocationList);
        }

        // An unbound connection with transport options is a programming error, never silently default.
        var withTls = Parse(Config(new Uri("https://api.test/"), Trust()));
        Assert.Throws<InvalidOperationException>(() => HttpSource.CreateClient(withTls));
    }

    [Fact]
    public async Task Sink_and_check_use_settings()
    {
        await using var proxy = new TestProxy();
        await using var tls = new TestTlsServer(_pki.Leaf, _pki.ServerChain());
        var config = Config(tls.Url("api.test"), Trust(), Proxy(proxy), ("check_path", "health"));

        var check = await new HttpConnector().CheckConnectionAsync(config, CancellationToken.None).AsTask().WaitAsync(Bound);
        Assert.True(check.Ok, check.Message);

        var untrusted = await new HttpConnector().CheckConnectionAsync(Config(tls.Url("api.test"), null, Proxy(proxy)),
            CancellationToken.None).AsTask().WaitAsync(Bound);
        Assert.False(untrusted.Ok);
        Assert.Contains("tls.ca_file", untrusted.Message, StringComparison.Ordinal);

        var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);
        ISinkConnector connector = new HttpConnector();
        await using (var sink = await connector.OpenAsync(config, CancellationToken.None))
        {
            await using var session = await sink.BeginWriteAsync(
                new OutputSpec("api", "out", "append", "fail_on_change", Map(("path", "/ingest"))), schema, CancellationToken.None);
            var builder = new ArrowBatchBuilder(schema);
            builder.AppendRow([1L]);
            using (var batch = builder.Flush()!)
            {
                await session.WriteBatchAsync(batch, CancellationToken.None).AsTask().WaitAsync(Bound);
            }

            Assert.Equal(1, (await session.CommitAsync(CancellationToken.None)).RowsWritten);
        }

        Assert.Equal(["GET /health HTTP/1.1", "POST /ingest HTTP/1.1"], tls.Requests.ToArray());
        Assert.Equal(3, proxy.Targets.Count(t => t == $"api.test:{tls.Port}"));
    }
}
