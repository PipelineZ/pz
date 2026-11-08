using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

/// <summary>An oauth2_client_credentials connection against one stub server that serves both the
/// token endpoint and the API: sign-in count, bearer headers, 401 classification and the check.</summary>
public sealed class HttpOAuthEndToEndTests : IAsyncLifetime
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    private static readonly Schema SinkSchema = new(
        [new Field("id", Int64Type.Default, nullable: false)], null);

    private readonly StubHttpServer _server = new();
    private readonly ManualClock _clock = new();

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync() => await _server.DisposeAsync();

    private ConnectorConfig Config(Dictionary<string, object?>? extra = null)
    {
        var values = new Dictionary<string, object?>
        {
            ["base_url"] = _server.BaseUrl.ToString(),
            ["auth"] = new Dictionary<string, object?>
            {
                ["type"] = "oauth2_client_credentials",
                ["token_url"] = new Uri(_server.BaseUrl, "token").ToString(),
                ["client_id"] = "c",
                ["client_secret"] = "s3cret",
            },
        };
        foreach (var (k, v) in extra ?? []) values[k] = v;
        return new ConnectorConfig(values);
    }

    private HttpSource OpenSource() => new(Parse(Config()), _clock);

    private HttpSink OpenSink() => new(Parse(Config()), _clock);

    private static HttpConnectionConfig Parse(ConnectorConfig config)
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(config, errors);
        Assert.Empty(errors);
        return connection!;
    }

    private int TokenRequests => _server.Requests.Count(r => r.Url.AbsolutePath == "/token");

    private void MapToken(int expiresIn = 3600, Func<StubRequest, StubResponse?>? before = null)
    {
        var issued = 0;
        _server.Map("/token", req =>
        {
            if (before?.Invoke(req) is { } custom) return custom;
            issued++;
            return new StubResponse(200,
                $$"""{"access_token":"t{{issued}}","token_type":"Bearer","expires_in":{{expiresIn}}}""");
        });
    }

    private static DatasetSpec Items(string path = "/items")
        => new("s", "d", new Dictionary<string, object?>
        {
            ["path"] = path,
            ["items"] = "/data",
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "link_header" },
        });

    private static async Task<long> ReadAsync(IDatasetPartition partition)
    {
        long rows = 0;
        await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        return rows;
    }

    private static async Task<IDatasetPartition> PlanAsync(ISource source, DatasetSpec spec)
        => (await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None))[0];

    private string PageLink(int page) => $"<{new Uri(_server.BaseUrl, $"items?page={page}")}>; rel=\"next\"";

    private static IReadOnlyDictionary<string, string> Link(string value)
        => new Dictionary<string, string> { ["Link"] = value };

    [Fact]
    public async Task A_paged_read_signs_in_once_and_sends_the_bearer_token()
    {
        MapToken();
        _server.Map("/items", req => req.Url.Query switch
        {
            "?page=2" => new StubResponse(200, """{"data":[{"id":2}]}""", Link(PageLink(3))),
            "?page=3" => new StubResponse(200, """{"data":[{"id":3}]}"""),
            _ => new StubResponse(200, """{"data":[{"id":1}]}""", Link(PageLink(2))),
        });

        await using var source = OpenSource();
        var rows = await ReadAsync(await PlanAsync(source, Items())).WaitAsync(Bound);

        Assert.Equal(3, rows);
        Assert.Equal(1, TokenRequests);
        var api = _server.Requests.Where(r => r.Url.AbsolutePath == "/items").ToList();
        Assert.Equal(3, api.Count);
        Assert.All(api, r => Assert.Equal("Bearer t1", r.Headers["Authorization"]));
    }

    [Fact]
    public async Task A_read_longer_than_the_token_lifetime_signs_in_again_without_a_401()
    {
        MapToken(expiresIn: 600);
        var unauthorized = 0;
        _server.Map("/items", req =>
        {
            if (req.Headers["Authorization"] != (TokenRequests == 1 ? "Bearer t1" : "Bearer t2"))
            {
                unauthorized++;
                return new StubResponse(401, "{}");
            }

            if (req.Url.Query == "?page=2")
            {
                return new StubResponse(200, """{"data":[{"id":2}]}""");
            }

            _clock.Advance(TimeSpan.FromMinutes(6));
            return new StubResponse(200, """{"data":[{"id":1}]}""", Link(PageLink(2)));
        });

        await using var source = OpenSource();
        var rows = await ReadAsync(await PlanAsync(source, Items())).WaitAsync(Bound);

        Assert.Equal(2, rows);
        Assert.Equal(2, TokenRequests);
        Assert.Equal(0, unauthorized);
    }

    [Fact]
    public async Task A_401_after_success_is_transient_and_the_retry_signs_in_again()
    {
        MapToken();
        var served401 = false;
        _server.Map("/items", req =>
        {
            if (req.Url.Query == "?page=2" && !served401)
            {
                served401 = true;
                return new StubResponse(401, "{}");
            }

            return req.Url.Query == "?page=2"
                ? new StubResponse(200, """{"data":[{"id":2}]}""")
                : new StubResponse(200, """{"data":[{"id":1}]}""", Link(PageLink(2)));
        });

        await using var source = OpenSource();
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await ReadAsync(await PlanAsync(source, Items())).WaitAsync(Bound));

        Assert.True(ex.IsTransient);
        Assert.Contains("signing in again", ex.Message);
        Assert.Equal(1, TokenRequests);

        var rows = await ReadAsync(await PlanAsync(source, Items())).WaitAsync(Bound);
        Assert.Equal(2, rows);
        Assert.Equal(2, TokenRequests);
        Assert.Equal("Bearer t2", _server.Requests.Last(r => r.Url.AbsolutePath == "/items").Headers["Authorization"]);
    }

    [Fact]
    public async Task A_401_on_the_first_request_is_permanent_with_the_scope_hint()
    {
        MapToken();
        _server.Map("/items", _ => new StubResponse(401, "{}"));

        await using var source = OpenSource();
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await ReadAsync(await PlanAsync(source, Items())).WaitAsync(Bound));

        Assert.False(ex.IsTransient);
        Assert.Contains("scope", ex.Message);
        Assert.DoesNotContain("t1", ex.Message);
    }

    [Fact]
    public async Task A_403_is_not_treated_as_a_token_problem()
    {
        MapToken();
        _server.Map("/items", _ => new StubResponse(403, "{}"));

        await using var source = OpenSource();
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await ReadAsync(await PlanAsync(source, Items())).WaitAsync(Bound));

        Assert.False(ex.IsTransient);
        Assert.Contains("HTTP 403", ex.Message);
        Assert.DoesNotContain("token", ex.Message);
        Assert.Equal(1, TokenRequests);
    }

    [Fact]
    public async Task The_sink_signs_its_writes_and_refreshes_after_expiry()
    {
        MapToken(expiresIn: 600);
        _server.Map("/out", _ => new StubResponse(200, "{}"));

        await using var sink = OpenSink();
        var spec = new OutputSpec("api", "out", "append", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "/out" });
        await using var session = await sink.BeginWriteAsync(spec, SinkSchema, CancellationToken.None);

        for (var i = 0; i < 2; i++)
        {
            var builder = new ArrowBatchBuilder(SinkSchema);
            builder.AppendRow([(long)i]);
            using var batch = builder.Flush()!;
            await session.WriteBatchAsync(batch, CancellationToken.None).AsTask().WaitAsync(Bound);
            _clock.Advance(TimeSpan.FromMinutes(6));
        }

        Assert.Equal(2, TokenRequests);
        var posts = _server.Requests.Where(r => r.Url.AbsolutePath == "/out").ToList();
        Assert.Equal(["Bearer t1", "Bearer t2"], posts.Select(r => r.Headers["Authorization"]));
    }

    [Fact]
    public async Task Check_connection_reports_a_sign_in_failure_as_such()
    {
        _server.Map("/token", _ => new StubResponse(401, """{"error":"invalid_client"}"""));

        var check = await new HttpConnector().CheckConnectionAsync(Config(), CancellationToken.None)
            .AsTask().WaitAsync(Bound);

        Assert.False(check.Ok);
        Assert.StartsWith("sign-in failed", check.Message);
        Assert.DoesNotContain("s3cret", check.Message);
    }

    [Fact]
    public async Task Check_connection_reports_an_api_failure_separately()
    {
        MapToken();
        _server.Map("/check", _ => new StubResponse(404, "{}"));

        var check = await new HttpConnector()
            .CheckConnectionAsync(Config(new() { ["check_path"] = "/check" }), CancellationToken.None)
            .AsTask().WaitAsync(Bound);

        Assert.False(check.Ok);
        Assert.Contains("returned HTTP 404", check.Message);
        Assert.DoesNotContain("sign-in", check.Message);
        Assert.Equal("Bearer t1", _server.Requests.Single(r => r.Url.AbsolutePath == "/check").Headers["Authorization"]);
    }

    [Fact]
    public async Task The_token_is_not_sent_to_a_foreign_next_link()
    {
        MapToken();
        _server.Map("/items", _ => new StubResponse(200, """{"data":[{"id":1}]}""",
            Link("<https://foreign.example.invalid/items>; rel=\"next\"")));

        await using var source = OpenSource();
        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await ReadAsync(await PlanAsync(source, Items())).WaitAsync(Bound));

        Assert.False(ex.IsTransient);
        Assert.Contains("allow_hosts", ex.Message);
        Assert.Equal(2, _server.Requests.Count);
        Assert.Equal(1, TokenRequests);
        Assert.Single(_server.Requests, r => r.Url.AbsolutePath == "/items");
    }
}
