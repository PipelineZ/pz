using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

public class HttpRequestSenderTests
{
    private sealed class RecordingGate : IOperationGate
    {
        public List<(string Label, bool Idempotent)> Calls { get; } = [];

        public Task<T> ExecuteAsync<T>(string opLabel, bool idempotent, Func<CancellationToken, Task<T>> op,
            CancellationToken ct)
        {
            Calls.Add((opLabel, idempotent));
            return op(ct);
        }

        public void ReportBudget(int remaining, DateTimeOffset resetAt)
        {
        }
    }

    private static HttpConnectionConfig Connection(Uri baseUrl, int? maxResponseMb = null, params string[] allowHosts)
    {
        var values = new Dictionary<string, object?>
        {
            ["base_url"] = baseUrl.ToString(),
            ["auth"] = new Dictionary<string, object?> { ["type"] = "bearer", ["token"] = "secret-token" },
            ["headers"] = new Dictionary<string, object?> { ["X-Api-Key"] = "header-secret" },
            ["allow_hosts"] = allowHosts.ToList(),
        };
        if (maxResponseMb is { } mb)
        {
            values["max_response_mb"] = mb;
        }

        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(values), errors);
        Assert.Empty(errors);
        return connection!.Bind(TimeProvider.System);
    }

    private static async Task<SentResponse> SendAsync(HttpConnectionConfig connection, Uri url, bool streamBody,
        bool credentialsOnlyToOrigin)
    {
        using var client = HttpSource.CreateClient(connection);
        using var bare = HttpSource.CreateBareClient(connection);
        var sender = new HttpRequestSender(client, bare, connection, null, null, TimeProvider.System, "test");
        return await sender.SendAsync(new HttpPageRequest(HttpMethod.Get, url, null, null), streamBody,
            credentialsOnlyToOrigin, CancellationToken.None);
    }

    [Fact]
    public async Task Stream_mode_returns_before_the_body_is_read()
    {
        await using var server = new StubHttpServer();
        server.Map("/big", _ => new StubResponse(200, "") { RawBody = new byte[3 * 1024 * 1024] });
        var connection = Connection(server.BaseUrl, maxResponseMb: 1);
        var url = new Uri(server.BaseUrl, "/big");

        using (var sent = await SendAsync(connection, url, streamBody: true, credentialsOnlyToOrigin: false))
        {
            Assert.Equal(200, (int)sent.Response.StatusCode);
        }

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => SendAsync(connection, url, streamBody: false, credentialsOnlyToOrigin: false));
        Assert.Contains("max_response_mb", ex.Message);
    }

    [Fact]
    public async Task Cross_origin_hop_is_bare_when_stripping()
    {
        await using var files = new StubHttpServer();
        files.Map("/blob", _ => new StubResponse(200, "ok"));
        await using var api = new StubHttpServer();
        api.Map("/download", _ => new StubResponse(302, "",
            new Dictionary<string, string> { ["Location"] = new Uri(files.BaseUrl, "/blob").ToString() }));
        var connection = Connection(api.BaseUrl, null, files.BaseUrl.Host);

        using var sent = await SendAsync(connection, new Uri(api.BaseUrl, "/download"), streamBody: true,
            credentialsOnlyToOrigin: true);

        Assert.Equal(200, (int)sent.Response.StatusCode);
        Assert.Equal("Bearer secret-token", Assert.Single(api.Requests).Headers["Authorization"]);
        var hop = Assert.Single(files.Requests);
        Assert.False(hop.Headers.ContainsKey("Authorization"));
        Assert.False(hop.Headers.ContainsKey("X-Api-Key"));
    }

    [Fact]
    public async Task Same_origin_keeps_credentials_when_stripping()
    {
        await using var api = new StubHttpServer();
        api.Map("/download", _ => new StubResponse(302, "",
            new Dictionary<string, string> { ["Location"] = "/blob" }));
        api.Map("/blob", _ => new StubResponse(200, "ok"));
        var connection = Connection(api.BaseUrl);

        using var sent = await SendAsync(connection, new Uri(api.BaseUrl, "/download"), streamBody: true,
            credentialsOnlyToOrigin: true);

        Assert.Equal(200, (int)sent.Response.StatusCode);
        var hop = api.Requests.Single(r => r.Url.AbsolutePath == "/blob");
        Assert.Equal("Bearer secret-token", hop.Headers["Authorization"]);
        Assert.Equal("header-secret", hop.Headers["X-Api-Key"]);
    }

    [Fact]
    public async Task Without_stripping_allow_listed_hop_keeps_credentials()
    {
        await using var files = new StubHttpServer();
        files.Map("/blob", _ => new StubResponse(200, "ok"));
        await using var api = new StubHttpServer();
        api.Map("/download", _ => new StubResponse(302, "",
            new Dictionary<string, string> { ["Location"] = new Uri(files.BaseUrl, "/blob").ToString() }));
        var connection = Connection(api.BaseUrl, null, files.BaseUrl.Host);

        using var sent = await SendAsync(connection, new Uri(api.BaseUrl, "/download"), streamBody: false,
            credentialsOnlyToOrigin: false);

        var hop = Assert.Single(files.Requests);
        Assert.Equal("Bearer secret-token", hop.Headers["Authorization"]);
        Assert.Equal("header-secret", hop.Headers["X-Api-Key"]);
    }

    [Fact]
    public async Task Non_idempotent_op_is_passed_to_the_gate()
    {
        var connection = Connection(new Uri("http://127.0.0.1:1/"));
        using var client = HttpSource.CreateClient(connection);
        using var bare = HttpSource.CreateBareClient(connection);
        var gate = new RecordingGate();
        var sender = new HttpRequestSender(client, bare, connection, null, gate, TimeProvider.System, "test");

        var result = await sender.RunAsync("http.export_start", idempotent: false,
            _ => Task.FromResult(42), CancellationToken.None);

        Assert.Equal(42, result);
        Assert.Equal(("http.export_start", false), Assert.Single(gate.Calls));
    }
}
