using System.Text.Json;
using Json.Schema;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.TestKit;
using Pz.Connectors.Toolkit.Auth;

namespace Pz.Connector.Http.Tests;

/// <summary>An oauth2_refresh_token connection against one stub server serving the token endpoint and the API.</summary>
public sealed class HttpRefreshTokenTests : IAsyncLifetime
{
    private const string Refresh = "1//refresh-Tok3n-xyz";
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    private readonly StubHttpServer _server = new();
    private readonly ManualClock _clock = new();

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync() => await _server.DisposeAsync();

    private Dictionary<string, object?> Auth() => new()
    {
        ["type"] = "oauth2_refresh_token",
        ["token_url"] = new Uri(_server.BaseUrl, "token").ToString(),
        ["client_id"] = "c",
        ["client_secret"] = "s3cret",
        ["refresh_token"] = Refresh,
    };

    private ConnectorConfig Config(Dictionary<string, object?>? auth = null)
        => new(new Dictionary<string, object?> { ["base_url"] = _server.BaseUrl.ToString(), ["auth"] = auth ?? Auth() });

    private HttpSource OpenSource()
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(Config(), errors);
        Assert.Empty(errors);
        return new HttpSource(connection!, _clock);
    }

    private int TokenRequests => _server.Requests.Count(r => r.Url.AbsolutePath == "/token");

    private static DatasetSpec Items()
        => new("s", "d", new Dictionary<string, object?> { ["path"] = "/items", ["items"] = "/data" });

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

    private static async Task<long> ReadAsync(ISource source)
        => await ReadAsync((await source.PlanReadAsync(Items(), ReadHints.None, CancellationToken.None))[0]);

    [Fact]
    public async Task Validate_accepts_the_type_and_reports_its_errors_together()
    {
        var ok = await new HttpConnector().ValidateAsync(Config(), CancellationToken.None);
        Assert.True(ok.IsValid, string.Join("; ", ok.Errors));

        var bad = Auth();
        bad.Remove("refresh_token");
        bad["token_url"] = "http://login.example.com/token";
        var result = await new HttpConnector().ValidateAsync(Config(bad), CancellationToken.None);
        Assert.Equal(2, result.Errors.Count);
        Assert.Contains("auth.refresh_token is required", result.Errors);
    }

    [Fact]
    public void Bind_builds_the_refresh_token_authenticator()
    {
        var errors = new List<string>();
        var parsed = HttpConnectionConfig.Parse(Config(), errors)!;
        Assert.Null(parsed.Authenticator);
        Assert.True(parsed.HasCredentials);
        using var bound = Assert.IsType<OAuth2RefreshTokenAuthenticator>(parsed.Bind(TimeProvider.System).Authenticator);
    }

    [Fact]
    public async Task A_read_exchanges_the_refresh_token_once_and_sends_the_access_token()
    {
        _server.Map("/token", _ => new StubResponse(200, """{"access_token":"acc1","token_type":"Bearer","expires_in":3600}"""));
        _server.Map("/items", _ => new StubResponse(200, """{"data":[{"id":1},{"id":2}]}"""));

        await using var source = OpenSource();
        Assert.Equal(2, await ReadAsync(source).WaitAsync(Bound));

        Assert.Equal(1, TokenRequests);
        var form = System.Web.HttpUtility.ParseQueryString(_server.Requests.Single(r => r.Url.AbsolutePath == "/token").Body);
        Assert.Equal("refresh_token", form["grant_type"]);
        Assert.Equal(Refresh, form["refresh_token"]);
        Assert.Equal("Bearer acc1", _server.Requests.Single(r => r.Url.AbsolutePath == "/items").Headers["Authorization"]);
    }

    [Fact]
    public async Task Rotation_fails_the_read_permanently_before_any_api_request()
    {
        _server.Map("/token", _ => new StubResponse(200,
            """{"access_token":"acc1","token_type":"Bearer","expires_in":3600,"refresh_token":"rotated-R2"}"""));
        _server.Map("/items", _ => new StubResponse(200, """{"data":[{"id":1}]}"""));

        await using var source = OpenSource();
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await ReadAsync(source).WaitAsync(Bound));

        Assert.False(ex.IsTransient);
        Assert.Contains("PZ0366", ex.Message);
        Assert.DoesNotContain("rotated-R2", ex.Message);
        Assert.DoesNotContain(Refresh, ex.Message);
        Assert.DoesNotContain(_server.Requests, r => r.Url.AbsolutePath == "/items");
    }

    [Fact]
    public async Task A_401_after_success_is_transient_and_the_retry_refreshes_again()
    {
        var issued = 0;
        _server.Map("/token", _ => new StubResponse(200, $$"""{"access_token":"acc{{++issued}}","expires_in":3600}"""));
        var calls = 0;
        _server.Map("/items", _ => ++calls == 2
            ? new StubResponse(401, "{}")
            : new StubResponse(200, """{"data":[{"id":1}]}"""));

        await using var source = OpenSource();
        Assert.Equal(1, await ReadAsync(source).WaitAsync(Bound));
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await ReadAsync(source).WaitAsync(Bound));
        Assert.True(ex.IsTransient);
        Assert.Equal(1, await ReadAsync(source).WaitAsync(Bound));

        Assert.Equal(2, TokenRequests);
        Assert.Equal("Bearer acc2", _server.Requests.Last(r => r.Url.AbsolutePath == "/items").Headers["Authorization"]);
    }

    [Fact]
    public async Task Check_connection_reports_a_revoked_refresh_token_as_a_sign_in_failure()
    {
        _server.Map("/token", _ => new StubResponse(400, """{"error":"invalid_grant"}"""));

        var check = await new HttpConnector().CheckConnectionAsync(Config(), CancellationToken.None).AsTask().WaitAsync(Bound);

        Assert.False(check.Ok);
        Assert.StartsWith("sign-in failed", check.Message);
        Assert.Contains("auth.refresh_token", check.Message);
        Assert.DoesNotContain(Refresh, check.Message);
        Assert.DoesNotContain("s3cret", check.Message);
    }

    [Fact]
    public void Schema_accepts_the_type_and_marks_the_refresh_token_writeOnly()
    {
        var text = new HttpConnector().ConnectionConfigSchema;
        var doc = JsonSerializer.Deserialize<JsonElement>("""
            {"base_url":"https://api.example.com","auth":{"type":"oauth2_refresh_token",
             "token_url":"https://login.example.com/token","client_id":"c","refresh_token":"r",
             "client_auth":"body","scope":"x","params":{"audience":"a"},"rotation":"ignore"}}
            """);
        Assert.True(JsonSchema.FromText(text).Evaluate(doc).IsValid);

        using var parsed = JsonDocument.Parse(text);
        var props = parsed.RootElement.GetProperty("properties").GetProperty("auth").GetProperty("properties");
        Assert.True(props.GetProperty("refresh_token").GetProperty("writeOnly").GetBoolean());
    }

    private static readonly Apache.Arrow.Schema SinkSchema = new(
        [new Apache.Arrow.Field("id", Apache.Arrow.Types.Int64Type.Default, nullable: false)], null);

    private HttpSink OpenSink(Dictionary<string, object?> auth)
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(Config(auth), errors);
        Assert.Empty(errors);
        return new HttpSink(connection!, _clock);
    }

    private static async Task WriteOneAsync(ISink sink)
    {
        var spec = new OutputSpec("api", "out", "append", "fail_on_change", new Dictionary<string, object?> { ["path"] = "/out" });
        await using var session = await sink.BeginWriteAsync(spec, SinkSchema, CancellationToken.None);
        var builder = new ArrowBatchBuilder(SinkSchema);
        builder.AppendRow([1L]);
        using var batch = builder.Flush()!;
        await session.WriteBatchAsync(batch, CancellationToken.None).AsTask().WaitAsync(Bound);
    }

    [Fact]
    public async Task Rotation_ignore_reads_and_writes_and_reports_a_notice_through_source_and_sink()
    {
        _server.Map("/token", _ => new StubResponse(200, """{"access_token":"acc","expires_in":3600,"refresh_token":"rotated-R2"}"""));
        _server.Map("/items", _ => new StubResponse(200, """{"data":[{"id":1}]}"""));
        _server.Map("/out", _ => new StubResponse(200, "{}"));
        var auth = Auth();
        auth["rotation"] = "ignore";

        var notices = new List<string>();
        await using (var source = (HttpSource)await new HttpConnector().OpenAsync(Config(auth), CancellationToken.None))
        {
            source.UseNotice(notices.Add);
            Assert.Equal(1, await ReadAsync(source).WaitAsync(Bound));
        }

        await using (var sink = OpenSink(auth))
        {
            sink.UseNotice(notices.Add);
            await WriteOneAsync(sink);
        }

        Assert.Equal("Bearer acc", _server.Requests.Single(r => r.Url.AbsolutePath == "/out").Headers["Authorization"]);
        Assert.Equal(2, notices.Count);
        Assert.Contains("auth.rotation is ignore", notices[0]);
        Assert.All(_server.Requests.Where(r => r.Url.AbsolutePath == "/token"),
            r => Assert.Equal(Refresh, System.Web.HttpUtility.ParseQueryString(r.Body)["refresh_token"]));
    }

    [Fact]
    public async Task A_rotation_refused_on_one_node_is_not_replayed_by_the_next_node_or_the_sink()
    {
        _server.Map("/token", _ => new StubResponse(200, """{"access_token":"acc","refresh_token":"rotated-R2"}"""));
        _server.Map("/out", _ => new StubResponse(200, "{}"));

        await using (var source = OpenSource())
        {
            Assert.Contains("PZ0366", (await Assert.ThrowsAsync<PzConnectorException>(async () => await ReadAsync(source))).Message);
        }

        await using (var again = OpenSource())
        {
            var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await ReadAsync(again));
            Assert.Contains("PZ0366", ex.Message);
            Assert.Contains("pz did not try again", ex.Message);
        }

        await using (var sink = OpenSink(Auth()))
        {
            var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await WriteOneAsync(sink));
            Assert.False(ex.IsTransient);
            Assert.Contains("PZ0366", ex.Message);
        }

        Assert.Equal(1, TokenRequests);
        Assert.DoesNotContain(_server.Requests, r => r.Url.AbsolutePath is "/items" or "/out");
    }

    [Fact]
    public async Task The_sink_signs_its_writes_with_the_refreshed_token_and_signs_in_again_after_a_401()
    {
        var issued = 0;
        _server.Map("/token", _ => new StubResponse(200, $$"""{"access_token":"acc{{++issued}}","expires_in":3600}"""));
        var posts = 0;
        _server.Map("/out", _ => ++posts == 2 ? new StubResponse(401, "{}") : new StubResponse(200, "{}"));

        await using var sink = OpenSink(Auth());
        await WriteOneAsync(sink);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await WriteOneAsync(sink));
        Assert.True(ex.IsTransient);
        await WriteOneAsync(sink);

        Assert.Equal(2, TokenRequests);
        Assert.Equal(["Bearer acc1", "Bearer acc1", "Bearer acc2"],
            _server.Requests.Where(r => r.Url.AbsolutePath == "/out").Select(r => r.Headers["Authorization"]));
    }
}
