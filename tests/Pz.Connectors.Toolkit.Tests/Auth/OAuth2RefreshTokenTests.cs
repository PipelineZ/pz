using System.Text;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Pz.Connectors.Toolkit.Auth;

namespace Pz.Connectors.Toolkit.Tests.Auth;

public class OAuth2RefreshTokenTests
{
    private const string Secret = "S3cr3t-value-123";
    private const string Refresh = "1//refresh-Tok3n-xyz";
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private static Dictionary<string, object?> Block(string tokenUrl, Action<Dictionary<string, object?>>? edit = null)
    {
        var d = new Dictionary<string, object?>
        {
            ["token_url"] = tokenUrl,
            ["client_id"] = "my-id",
            ["client_secret"] = Secret,
            ["refresh_token"] = Refresh,
        };
        edit?.Invoke(d);
        return d;
    }

    private static OAuth2RefreshTokenOptions Parse(Dictionary<string, object?> auth)
    {
        var errors = new List<string>();
        var options = OAuth2RefreshTokenOptions.TryParse(auth, errors);
        Assert.Empty(errors);
        return Assert.IsType<OAuth2RefreshTokenOptions>(options);
    }

    private static List<string> Errors(Dictionary<string, object?> auth)
    {
        var errors = new List<string>();
        Assert.Null(OAuth2RefreshTokenOptions.TryParse(auth, errors));
        return errors;
    }

    private static async Task<PzConnectorException> FailureAsync(StubHttpServer server,
        Action<Dictionary<string, object?>>? edit = null)
    {
        using var auth = new OAuth2RefreshTokenAuthenticator(Parse(Block(server.BaseUrl + "token", edit)), Timeout, new ManualClock());
        using var request = new HttpRequestMessage(HttpMethod.Get, server.BaseUrl + "api");
        return await Assert.ThrowsAsync<PzConnectorException>(
            async () => await auth.ApplyAsync(request, CancellationToken.None).AsTask().WaitAsync(Timeout));
    }

    [Fact]
    public void Parses_a_full_block()
    {
        var o = Parse(Block("https://login.example.com/token", d =>
        {
            d["scope"] = "read";
            d["client_auth"] = "basic";
            d["params"] = new Dictionary<string, object?> { ["audience"] = "a" };
        }));
        Assert.Equal(new Uri("https://login.example.com/token"), o.TokenUrl);
        Assert.Equal("my-id", o.ClientId);
        Assert.Equal(Secret, o.ClientSecret);
        Assert.Equal(Refresh, o.RefreshToken);
        Assert.Equal("read", o.Scope);
        Assert.True(o.BasicClientAuth);
        Assert.Equal("a", o.Params["audience"]);
    }

    [Fact]
    public void A_public_client_needs_no_secret()
    {
        var o = Parse(Block("https://login.example.com/token", d => d.Remove("client_secret")));
        Assert.Null(o.ClientSecret);
    }

    [Fact]
    public void Basic_client_auth_without_a_secret_is_refused()
    {
        var error = Assert.Single(Errors(Block("https://login.example.com/token", d =>
        {
            d.Remove("client_secret");
            d["client_auth"] = "basic";
        })));
        Assert.Contains("client_secret", error);
        Assert.Contains("basic", error);
    }

    [Fact]
    public void Reports_every_missing_field_at_once()
    {
        var errors = Errors(new Dictionary<string, object?>());
        Assert.Equal(3, errors.Count);
        Assert.Contains("auth.token_url is required", errors);
        Assert.Contains("auth.client_id is required", errors);
        Assert.Contains("auth.refresh_token is required", errors);
    }

    [Fact]
    public void Aggregates_unrelated_errors()
    {
        var errors = Errors(Block("http://login.example.com/token", d =>
        {
            d["client_auth"] = "header";
            d["refresh_token"] = new List<object?> { "a" };
            d["params"] = new Dictionary<string, object?> { ["refresh_token"] = "x", ["grant_type"] = "y" };
        }));
        Assert.Equal(4, errors.Count);
        Assert.Contains(errors, e => e.Contains("token_url") && e.Contains("https"));
        Assert.Contains("auth.client_auth must be one of: body, basic", errors);
        Assert.Contains("auth.refresh_token must be a single value, not a list", errors);
        Assert.Contains(errors, e => e.Contains("reserved") && e.Contains("refresh_token") && e.Contains("grant_type"));
    }

    [Fact]
    public async Task Posts_the_refresh_grant_with_body_client_auth()
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(200, """{"access_token":"tok","token_type":"Bearer","expires_in":3600}"""));
        using var auth = new OAuth2RefreshTokenAuthenticator(
            Parse(Block(server.BaseUrl + "token", d =>
            {
                d["scope"] = "read";
                d["params"] = new Dictionary<string, object?> { ["audience"] = "aud" };
            })), Timeout, new ManualClock());
        using var api = new HttpRequestMessage(HttpMethod.Get, server.BaseUrl + "api");
        await auth.ApplyAsync(api, CancellationToken.None).AsTask().WaitAsync(Timeout);

        Assert.Equal("Bearer tok", api.Headers.Authorization!.ToString());
        var req = Assert.Single(server.Requests);
        Assert.Equal("POST", req.Method);
        Assert.False(req.Headers.ContainsKey("Authorization"));
        var form = System.Web.HttpUtility.ParseQueryString(req.Body);
        Assert.Equal("refresh_token", form["grant_type"]);
        Assert.Equal(Refresh, form["refresh_token"]);
        Assert.Equal("my-id", form["client_id"]);
        Assert.Equal(Secret, form["client_secret"]);
        Assert.Equal("read", form["scope"]);
        Assert.Equal("aud", form["audience"]);
    }

    [Fact]
    public async Task A_public_client_sends_its_id_and_no_secret()
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(200, """{"access_token":"tok"}"""));
        using var auth = new OAuth2RefreshTokenAuthenticator(
            Parse(Block(server.BaseUrl + "token", d => d.Remove("client_secret"))), Timeout, new ManualClock());
        using var api = new HttpRequestMessage(HttpMethod.Get, server.BaseUrl + "api");
        await auth.ApplyAsync(api, CancellationToken.None).AsTask().WaitAsync(Timeout);

        var form = System.Web.HttpUtility.ParseQueryString(Assert.Single(server.Requests).Body);
        Assert.Equal("my-id", form["client_id"]);
        Assert.Null(form["client_secret"]);
    }

    [Fact]
    public async Task Basic_client_auth_uses_the_basic_header()
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(200, """{"access_token":"tok"}"""));
        using var auth = new OAuth2RefreshTokenAuthenticator(
            Parse(Block(server.BaseUrl + "token", d => d["client_auth"] = "basic")), Timeout, new ManualClock());
        using var api = new HttpRequestMessage(HttpMethod.Get, server.BaseUrl + "api");
        await auth.ApplyAsync(api, CancellationToken.None).AsTask().WaitAsync(Timeout);

        var req = Assert.Single(server.Requests);
        Assert.Equal("Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes($"my-id:{Secret}")), req.Headers["Authorization"]);
        var form = System.Web.HttpUtility.ParseQueryString(req.Body);
        Assert.Null(form["client_secret"]);
        Assert.Equal(Refresh, form["refresh_token"]);
    }

    [Theory]
    [InlineData("")]
    [InlineData(",\"refresh_token\":\"" + Refresh + "\"")]
    [InlineData(",\"refresh_token\":\"\"")]
    [InlineData(",\"refresh_token\":null")]
    public async Task A_missing_or_identical_refresh_token_is_the_non_rotating_case(string extra)
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(200, "{\"access_token\":\"tok\",\"expires_in\":600" + extra + "}"));
        var clock = new ManualClock();
        using var auth = new OAuth2RefreshTokenAuthenticator(Parse(Block(server.BaseUrl + "token")), Timeout, clock);
        foreach (var advance in new[] { TimeSpan.Zero, TimeSpan.FromMinutes(6) })
        {
            clock.Advance(advance);
            using var api = new HttpRequestMessage(HttpMethod.Get, server.BaseUrl + "api");
            await auth.ApplyAsync(api, CancellationToken.None).AsTask().WaitAsync(Timeout);
            Assert.Equal("Bearer tok", api.Headers.Authorization!.ToString());
        }

        // The token expired after its lifetime and was refreshed with the same configured refresh token.
        Assert.Equal(2, server.Requests.Count);
        Assert.All(server.Requests, r => Assert.Equal(Refresh, System.Web.HttpUtility.ParseQueryString(r.Body)["refresh_token"]));
    }

    [Fact]
    public async Task A_rotated_refresh_token_is_a_permanent_coded_error_and_the_access_token_is_not_used()
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(200,
            """{"access_token":"NewAccess-999","token_type":"Bearer","expires_in":3600,"refresh_token":"Rotated-R3fresh-777"}"""));
        using var auth = new OAuth2RefreshTokenAuthenticator(Parse(Block(server.BaseUrl + "token")), Timeout, new ManualClock());
        for (var attempt = 0; attempt < 2; attempt++)
        {
            using var api = new HttpRequestMessage(HttpMethod.Get, server.BaseUrl + "api");
            var ex = await Assert.ThrowsAsync<PzConnectorException>(
                async () => await auth.ApplyAsync(api, CancellationToken.None).AsTask().WaitAsync(Timeout));
            Assert.False(ex.IsTransient);
            Assert.StartsWith("PZ0366: ", ex.Message);
            Assert.Contains("rotates refresh tokens", ex.Message);
            Assert.Contains("auth.refresh_token", ex.Message);
            Assert.DoesNotContain("Rotated-R3fresh-777", ex.Message);
            Assert.DoesNotContain("NewAccess-999", ex.Message);
            Assert.DoesNotContain(Refresh, ex.Message);
            Assert.Null(api.Headers.Authorization);
            Assert.Equal(attempt == 1, ex.Message.Contains("pz did not try again", StringComparison.Ordinal));
        }

        // The refusal is remembered: the second attempt sent nothing, so the rotated token family is not replayed.
        Assert.Single(server.Requests);
    }

    private static async Task<PzConnectorException> ApplyFailsAsync(OAuth2RefreshTokenAuthenticator auth, StubHttpServer server)
    {
        using var api = new HttpRequestMessage(HttpMethod.Get, server.BaseUrl + "api");
        return await Assert.ThrowsAsync<PzConnectorException>(
            async () => await auth.ApplyAsync(api, CancellationToken.None).AsTask().WaitAsync(Timeout));
    }

    [Fact]
    public async Task A_refused_refresh_token_is_never_replayed_by_any_authenticator_in_the_process()
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(400, """{"error":"invalid_grant"}"""));
        var options = Parse(Block(server.BaseUrl + "token"));

        // Two nodes, each with its own authenticator, starting together.
        using var first = new OAuth2RefreshTokenAuthenticator(options, Timeout, new ManualClock());
        using var second = new OAuth2RefreshTokenAuthenticator(options, Timeout, new ManualClock());
        var failures = await Task.WhenAll(ApplyFailsAsync(first, server), ApplyFailsAsync(second, server));
        using var third = new OAuth2RefreshTokenAuthenticator(options, Timeout, new ManualClock());
        var later = await ApplyFailsAsync(third, server);

        Assert.Single(server.Requests);
        Assert.All(failures.Append(later), ex =>
        {
            Assert.False(ex.IsTransient);
            Assert.Contains("invalid_grant", ex.Message);
        });
        Assert.Contains("pz did not try again", later.Message);

        // A new refresh token is a new credential and is tried.
        server.Map("/token", _ => new StubResponse(200, """{"access_token":"tok"}"""));
        using var renewed = new OAuth2RefreshTokenAuthenticator(Parse(Block(server.BaseUrl + "token", d => d["refresh_token"] = "new-one")),
            Timeout, new ManualClock());
        using var api = new HttpRequestMessage(HttpMethod.Get, server.BaseUrl + "api");
        await renewed.ApplyAsync(api, CancellationToken.None).AsTask().WaitAsync(Timeout);
        Assert.Equal(2, server.Requests.Count);
    }

    [Fact]
    public async Task A_transient_failure_is_not_remembered()
    {
        await using var server = new StubHttpServer();
        var calls = 0;
        server.Map("/token", _ => ++calls == 1 ? new StubResponse(503, "{}") : new StubResponse(200, """{"access_token":"tok"}"""));
        using var auth = new OAuth2RefreshTokenAuthenticator(Parse(Block(server.BaseUrl + "token")), Timeout, new ManualClock());
        Assert.True((await ApplyFailsAsync(auth, server)).IsTransient);
        using var api = new HttpRequestMessage(HttpMethod.Get, server.BaseUrl + "api");
        await auth.ApplyAsync(api, CancellationToken.None).AsTask().WaitAsync(Timeout);
        Assert.Equal(2, server.Requests.Count);
    }

    [Fact]
    public async Task Rotation_ignore_uses_the_token_keeps_the_configured_refresh_token_and_says_so()
    {
        await using var server = new StubHttpServer();
        var issued = 0;
        server.Map("/token", _ => new StubResponse(200,
            $$$"""{"access_token":"acc{{{++issued}}}","expires_in":600,"refresh_token":"Rotated-{{{issued}}}"}"""));
        var clock = new ManualClock();
        var notices = new List<string>();
        using var auth = new OAuth2RefreshTokenAuthenticator(Parse(Block(server.BaseUrl + "token", d => d["rotation"] = "ignore")),
            Timeout, clock) { Notice = notices.Add };
        foreach (var advance in new[] { TimeSpan.Zero, TimeSpan.FromMinutes(6) })
        {
            clock.Advance(advance);
            using var api = new HttpRequestMessage(HttpMethod.Get, server.BaseUrl + "api");
            await auth.ApplyAsync(api, CancellationToken.None).AsTask().WaitAsync(Timeout);
        }

        Assert.Equal(2, server.Requests.Count);
        Assert.All(server.Requests, r => Assert.Equal(Refresh, System.Web.HttpUtility.ParseQueryString(r.Body)["refresh_token"]));
        // The same text each time: the engine shows it once per run.
        Assert.Equal(2, notices.Count);
        Assert.Single(notices.Distinct());
        Assert.Contains("auth.rotation is ignore", notices[0]);
        Assert.DoesNotContain("Rotated-", notices[0]);
        Assert.DoesNotContain(Refresh, notices[0]);
    }

    [Fact]
    public void Rotation_accepts_only_refuse_or_ignore()
    {
        Assert.False(Parse(Block("https://x/token", d => d["rotation"] = "refuse")).IgnoreRotation);
        Assert.True(Parse(Block("https://x/token", d => d["rotation"] = "ignore")).IgnoreRotation);
        Assert.Equal(["auth.rotation must be one of: refuse, ignore"], Errors(Block("https://x/token", d => d["rotation"] = "store")));
    }

    [Fact]
    public async Task Provider_text_never_carries_the_refresh_token_or_the_secret()
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(400,
            $$"""{"error":"invalid_request","error_description":"refresh token '{{Refresh}}' for secret {{Uri.EscapeDataString(Secret)}} / {{Secret}} is bad"}"""));
        var ex = await FailureAsync(server);
        Assert.DoesNotContain(Refresh, ex.Message);
        Assert.DoesNotContain(Secret, ex.Message);
        Assert.DoesNotContain(Uri.EscapeDataString(Secret), ex.Message);
        Assert.Contains("refresh token '***' for secret *** / *** is bad", ex.Message);
    }

    [Theory]
    [InlineData("""{"access_token":"a","access_token":"b"}""")]
    [InlineData("""{"access_token":"a","refresh_token":"x","refresh_token":"y"}""")]
    public async Task A_token_response_with_duplicate_keys_is_a_permanent_connector_error(string body)
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(200, body));
        var ex = await FailureAsync(server);
        Assert.False(ex.IsTransient);
        Assert.Contains("duplicate keys", ex.Message);
    }

    [Fact]
    public async Task Invalid_grant_is_permanent_and_says_to_authorize_again()
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(400, """{"error":"invalid_grant","error_description":"Token has been expired or revoked."}"""));
        var ex = await FailureAsync(server);
        Assert.False(ex.IsTransient);
        Assert.Contains("invalid_grant", ex.Message);
        Assert.Contains("expired or revoked", ex.Message);
        Assert.Contains("update auth.refresh_token", ex.Message);
    }

    [Theory]
    [InlineData(429)]
    [InlineData(503)]
    public async Task Throttling_and_server_errors_are_transient(int status)
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(status, "{}", new Dictionary<string, string> { ["Retry-After"] = "9" }));
        var ex = await FailureAsync(server);
        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(9), ex.RetryAfter);
    }

    [Fact]
    public async Task No_message_ever_contains_a_secret()
    {
        var cases = new (int Status, string Body)[]
        {
            (200, """{"access_token":"a","refresh_token":"other"}"""),
            (200, """{"access_token":"a","token_type":"mac"}"""),
            (200, "{}"),
            (400, """{"error":"invalid_grant"}"""),
            (401, """{"error":"invalid_client"}"""),
            (503, "{}"),
            (302, ""),
        };
        foreach (var (status, body) in cases)
        {
            await using var server = new StubHttpServer();
            server.Map("/token", _ => new StubResponse(status, body));
            var ex = await FailureAsync(server);
            Assert.DoesNotContain(Secret, ex.Message);
            Assert.DoesNotContain(Refresh, ex.Message);
        }
    }

    [Fact]
    public async Task The_401_rule_is_shared_with_client_credentials()
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(200, """{"access_token":"tok"}"""));
        using var auth = new OAuth2RefreshTokenAuthenticator(Parse(Block(server.BaseUrl + "token")), Timeout, new ManualClock());
        using var first = new HttpRequestMessage(HttpMethod.Get, server.BaseUrl + "api");
        await auth.ApplyAsync(first, CancellationToken.None).AsTask().WaitAsync(Timeout);
        Assert.Equal(AuthResponseOutcome.FreshTokenRejected, auth.OnResponse(first, 401));
    }
}
