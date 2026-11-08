using System.Text;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Pz.Connectors.Toolkit.Auth;

namespace Pz.Connectors.Toolkit.Tests.Auth;

public class OAuth2ClientCredentialsTests
{
    private const string Secret = "S3cr3t-value-123";
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    private static Dictionary<string, object?> Block(string tokenUrl, Action<Dictionary<string, object?>>? edit = null)
    {
        var d = new Dictionary<string, object?>
        {
            ["token_url"] = tokenUrl,
            ["client_id"] = "my-id",
            ["client_secret"] = Secret,
            ["scope"] = "read write",
            ["params"] = new Dictionary<string, object?> { ["audience"] = "https://api.x/" },
        };
        edit?.Invoke(d);
        return d;
    }

    private static OAuth2ClientCredentialsOptions Parse(Dictionary<string, object?> auth)
    {
        var errors = new List<string>();
        var options = OAuth2ClientCredentialsOptions.TryParse(auth, errors);
        Assert.Empty(errors);
        return Assert.IsType<OAuth2ClientCredentialsOptions>(options);
    }

    private static async Task<PzConnectorException> FailureAsync(
        StubHttpServer server, Action<Dictionary<string, object?>>? edit = null, ManualClock? clock = null)
    {
        using var auth = new OAuth2ClientCredentialsAuthenticator(
            Parse(Block(server.BaseUrl + "token", edit)), TimeSpan.FromSeconds(10), clock ?? new ManualClock());
        using var request = new HttpRequestMessage(HttpMethod.Get, server.BaseUrl + "api");
        return await Assert.ThrowsAsync<PzConnectorException>(
            async () => await auth.ApplyAsync(request, CancellationToken.None).AsTask().WaitAsync(Timeout));
    }

    [Fact]
    public void Parses_a_full_block()
    {
        var o = Parse(Block("https://login.example.com/token"));
        Assert.Equal(new Uri("https://login.example.com/token"), o.TokenUrl);
        Assert.Equal("my-id", o.ClientId);
        Assert.Equal(Secret, o.ClientSecret);
        Assert.Equal("read write", o.Scope);
        Assert.False(o.BasicClientAuth);
        Assert.Equal("https://api.x/", o.Params["audience"]);
    }

    [Fact]
    public void Reports_every_missing_field_at_once()
    {
        var errors = new List<string>();
        Assert.Null(OAuth2ClientCredentialsOptions.TryParse(new Dictionary<string, object?>(), errors));
        Assert.Equal(3, errors.Count);
        Assert.Contains(errors, e => e.Contains("token_url"));
        Assert.Contains(errors, e => e.Contains("client_id"));
        Assert.Contains(errors, e => e.Contains("client_secret"));
    }

    [Theory]
    [InlineData("http://login.example.com/token")]
    [InlineData("ftp://x/token")]
    [InlineData("not a url")]
    public void Refuses_a_token_url_that_is_not_https(string url)
    {
        var errors = new List<string>();
        Assert.Null(OAuth2ClientCredentialsOptions.TryParse(Block(url), errors));
        var error = Assert.Single(errors);
        Assert.Contains("token_url", error);
        Assert.Contains("https", error);
    }

    [Theory]
    [InlineData("http://localhost:9000/token")]
    [InlineData("http://127.0.0.1/token")]
    public void Allows_http_on_loopback(string url) => Parse(Block(url));

    [Fact]
    public void Refuses_reserved_names_in_params()
    {
        var errors = new List<string>();
        var auth = Block("https://x/token", d => d["params"] = new Dictionary<string, object?>
        {
            ["client_secret"] = "x",
            ["scope"] = "y",
        });
        Assert.Null(OAuth2ClientCredentialsOptions.TryParse(auth, errors));
        var error = Assert.Single(errors);
        Assert.Contains("client_secret", error);
        Assert.Contains("scope", error);
    }

    [Fact]
    public void Refuses_an_unknown_client_auth()
    {
        var errors = new List<string>();
        Assert.Null(OAuth2ClientCredentialsOptions.TryParse(Block("https://x/token", d => d["client_auth"] = "header"), errors));
        var error = Assert.Single(errors);
        Assert.Contains("body", error);
        Assert.Contains("basic", error);
    }

    [Fact]
    public void Renders_scalars_the_way_the_author_wrote_them()
    {
        var o = Parse(Block("https://x/token", d => d["params"] = new Dictionary<string, object?>
        {
            ["flag"] = true,
            ["n"] = 5L,
            ["f"] = 1.5,
        }));
        Assert.Equal("true", o.Params["flag"]);
        Assert.Equal("5", o.Params["n"]);
        Assert.Equal("1.5", o.Params["f"]);
    }

    [Fact]
    public async Task Body_client_auth_posts_the_form()
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(200, """{"access_token":"tok","token_type":"Bearer","expires_in":3600}"""));
        using var auth = new OAuth2ClientCredentialsAuthenticator(
            Parse(Block(server.BaseUrl + "token")), TimeSpan.FromSeconds(10), new ManualClock());
        using var api = new HttpRequestMessage(HttpMethod.Get, server.BaseUrl + "api");
        await auth.ApplyAsync(api, CancellationToken.None).AsTask().WaitAsync(Timeout);

        Assert.Equal("Bearer tok", api.Headers.Authorization!.ToString());
        var req = Assert.Single(server.Requests);
        Assert.Equal("POST", req.Method);
        Assert.StartsWith("application/x-www-form-urlencoded", req.Headers["Content-Type"]);
        Assert.False(req.Headers.ContainsKey("Authorization"));
        var form = System.Web.HttpUtility.ParseQueryString(req.Body);
        Assert.Equal("client_credentials", form["grant_type"]);
        Assert.Equal("my-id", form["client_id"]);
        Assert.Equal(Secret, form["client_secret"]);
        Assert.Equal("read write", form["scope"]);
        Assert.Equal("https://api.x/", form["audience"]);
    }

    [Fact]
    public async Task Basic_client_auth_uses_the_basic_header()
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(200, """{"access_token":"tok","token_type":"Bearer"}"""));
        using var auth = new OAuth2ClientCredentialsAuthenticator(
            Parse(Block(server.BaseUrl + "token", d =>
            {
                d["client_auth"] = "basic";
                d["client_id"] = "my id";
            })), TimeSpan.FromSeconds(10), new ManualClock());
        using var api = new HttpRequestMessage(HttpMethod.Get, server.BaseUrl + "api");
        await auth.ApplyAsync(api, CancellationToken.None).AsTask().WaitAsync(Timeout);

        var req = Assert.Single(server.Requests);
        var expected = Convert.ToBase64String(Encoding.UTF8.GetBytes($"my+id:{Secret}"));
        Assert.Equal($"Basic {expected}", req.Headers["Authorization"]);
        var form = System.Web.HttpUtility.ParseQueryString(req.Body);
        Assert.Equal("client_credentials", form["grant_type"]);
        Assert.Equal("read write", form["scope"]);
        Assert.Null(form["client_secret"]);
    }

    private static async Task<int> FetchesAfterAsync(string tokenBody, params TimeSpan[] advances)
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(200, tokenBody));
        var clock = new ManualClock();
        using var auth = new OAuth2ClientCredentialsAuthenticator(
            Parse(Block(server.BaseUrl + "token")), TimeSpan.FromSeconds(10), clock);
        foreach (var advance in advances.Prepend(TimeSpan.Zero))
        {
            clock.Advance(advance);
            using var api = new HttpRequestMessage(HttpMethod.Get, server.BaseUrl + "api");
            await auth.ApplyAsync(api, CancellationToken.None).AsTask().WaitAsync(Timeout);
        }

        return server.Requests.Count;
    }

    [Fact]
    public async Task Expiry_comes_from_expires_in()
    {
        const string body = """{"access_token":"a","token_type":"Bearer","expires_in":3600}""";
        // refresh is due 5 minutes before now + 3600s
        Assert.Equal(1, await FetchesAfterAsync(body, TimeSpan.FromSeconds(3299)));
        Assert.Equal(2, await FetchesAfterAsync(body, TimeSpan.FromSeconds(3300)));
    }

    [Theory]
    [InlineData("")]
    [InlineData(",\"expires_in\":0")]
    [InlineData(",\"expires_in\":-5")]
    public async Task Missing_or_non_positive_expires_in_means_no_expiry(string extra)
        => Assert.Equal(1, await FetchesAfterAsync(
            "{\"access_token\":\"a\",\"token_type\":\"Bearer\"" + extra + "}", TimeSpan.FromDays(30)));

    [Fact]
    public async Task A_numeric_string_expires_in_is_accepted()
    {
        const string body = """{"access_token":"a","token_type":"Bearer","expires_in":"3600"}""";
        Assert.Equal(2, await FetchesAfterAsync(body, TimeSpan.FromSeconds(3300)));
    }

    [Fact]
    public async Task A_non_bearer_token_type_is_permanent()
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(200, """{"access_token":"a","token_type":"mac"}"""));
        var ex = await FailureAsync(server);
        Assert.False(ex.IsTransient);
        Assert.Contains("mac", ex.Message);
    }

    [Theory]
    [InlineData(429)]
    [InlineData(503)]
    public async Task Throttling_and_server_errors_are_transient(int status)
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(status, "{}", new Dictionary<string, string> { ["Retry-After"] = "7" }));
        var ex = await FailureAsync(server);
        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(7), ex.RetryAfter);
    }

    [Theory]
    [InlineData("invalid_client")]
    [InlineData("unauthorized_client")]
    public async Task Bad_client_credentials_are_permanent(string error)
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(401, $$"""{"error":"{{error}}","error_description":"client rejected"}"""));
        var ex = await FailureAsync(server);
        Assert.False(ex.IsTransient);
        Assert.Contains(error, ex.Message);
        Assert.Contains("client rejected", ex.Message);
    }

    [Fact]
    public async Task An_invalid_scope_names_the_scope()
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(400, """{"error":"invalid_scope"}"""));
        var ex = await FailureAsync(server, d => d["scope"] = "orders.read");
        Assert.False(ex.IsTransient);
        Assert.Contains("orders.read", ex.Message);
    }

    [Fact]
    public async Task A_redirect_from_the_token_endpoint_is_permanent()
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(302, "", new Dictionary<string, string> { ["Location"] = "http://elsewhere/" }));
        var ex = await FailureAsync(server);
        Assert.False(ex.IsTransient);
        Assert.Single(server.Requests);
    }

    [Theory]
    [InlineData("<html>login</html>")]
    [InlineData("{}")]
    [InlineData("{\"access_token\":\"\"}")]
    public async Task A_200_without_a_usable_token_is_permanent(string body)
    {
        await using var server = new StubHttpServer();
        server.Map("/token", _ => new StubResponse(200, body));
        var ex = await FailureAsync(server);
        Assert.False(ex.IsTransient);
        Assert.Contains(server.BaseUrl.Authority, ex.Message);
    }

    [Fact]
    public async Task No_message_ever_contains_the_secret()
    {
        var cases = new (int Status, string Body)[]
        {
            (200, """{"access_token":"a","token_type":"mac"}"""),
            (200, "<html>login</html>"),
            (200, "{}"),
            (429, "{}"),
            (503, "{}"),
            (401, """{"error":"invalid_client","error_description":"bad"}"""),
            (400, """{"error":"invalid_scope"}"""),
            (302, ""),
        };
        foreach (var (status, body) in cases)
        {
            await using var server = new StubHttpServer();
            server.Map("/token", _ => new StubResponse(status, body));
            var ex = await FailureAsync(server);
            Assert.DoesNotContain(Secret, ex.Message);
            Assert.DoesNotContain(Convert.ToBase64String(Encoding.UTF8.GetBytes($"my-id:{Secret}")), ex.Message);
        }
    }
}
