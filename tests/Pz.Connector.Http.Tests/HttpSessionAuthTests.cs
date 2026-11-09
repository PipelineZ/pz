using System.Text.Json;
using Json.Schema;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

/// <summary><c>auth.type: session</c>: offline validation, and a stub server playing both the login endpoint
/// and the API.</summary>
public sealed class HttpSessionAuthTests : IAsyncLifetime
{
    private const string Password = "P@ss-w0rd-{{not-a-binding}}";
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    private readonly StubHttpServer _server = new();
    private readonly ManualClock _clock = new();

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync() => await _server.DisposeAsync();

    private static Dictionary<string, object?> Session(Action<Dictionary<string, object?>>? edit = null)
    {
        var auth = new Dictionary<string, object?>
        {
            ["type"] = "session",
            ["login"] = new Dictionary<string, object?>
            {
                ["path"] = "/auth/login",
                ["body"] = new Dictionary<string, object?> { ["user"] = "me", ["password"] = Password },
            },
            ["token_pointer"] = "/session/token",
            ["ttl"] = "30m",
        };
        edit?.Invoke(auth);
        return auth;
    }

    private static Dictionary<string, object?> Login(Dictionary<string, object?> auth)
        => (Dictionary<string, object?>)auth["login"]!;

    private static ConnectorConfig Config(string baseUrl, Dictionary<string, object?> auth,
        Dictionary<string, object?>? extra = null)
    {
        var values = new Dictionary<string, object?> { ["base_url"] = baseUrl, ["auth"] = auth };
        foreach (var (k, v) in extra ?? []) values[k] = v;
        return new ConnectorConfig(values);
    }

    private static List<string> Errors(Dictionary<string, object?> auth, string baseUrl = "https://api.example.com/v1/",
        Dictionary<string, object?>? extra = null)
    {
        var errors = new List<string>();
        HttpConnectionConfig.Parse(Config(baseUrl, auth, extra), errors);
        return errors;
    }

    private static SessionOptions Options(Dictionary<string, object?> auth, string baseUrl = "https://api.example.com/v1/")
    {
        var errors = new List<string>();
        var options = SessionOptions.TryParse(auth, new Uri(baseUrl), errors);
        Assert.Empty(errors);
        return options!;
    }

    // ---- validation ----

    [Fact]
    public void A_full_block_parses()
    {
        var o = Options(Session(a =>
        {
            Login(a)["headers"] = new Dictionary<string, object?> { ["X-Tenant"] = "acme" };
            a["inject"] = new Dictionary<string, object?> { ["header"] = "X-Session-Token", ["prefix"] = "Token " };
        }));
        Assert.Equal(new Uri("https://api.example.com/v1/auth/login"), o.LoginUrl);
        Assert.Equal(HttpMethod.Post, o.Method);
        Assert.Equal("application/json", o.MediaType);
        Assert.Equal("""{"user":"me","password":"P@ss-w0rd-{{not-a-binding}}"}""", o.Body);
        Assert.Equal("acme", Assert.Single(o.Headers).Value);
        Assert.Equal("/session/token", o.TokenPointer);
        Assert.Equal(new SessionInject("X-Session-Token", "Token ", null), o.Inject);
        Assert.Equal(TimeSpan.FromMinutes(30), o.Ttl);
        Assert.Null(o.ExpiresInPointer);
    }

    [Fact]
    public void Validate_accepts_session_and_bind_builds_its_authenticator()
    {
        var errors = new List<string>();
        var parsed = HttpConnectionConfig.Parse(Config("https://api.example.com/", Session()), errors)!;
        Assert.Empty(errors);
        Assert.True(parsed.HasCredentials);
        using var bound = Assert.IsType<SessionAuthenticator>(parsed.Bind(TimeProvider.System).Authenticator);
    }

    [Fact]
    public void A_form_body_is_url_encoded_and_a_string_body_is_kept_verbatim()
    {
        var form = Options(Session(a =>
        {
            Login(a)["body_format"] = "form";
            Login(a)["body"] = new Dictionary<string, object?> { ["user"] = "me x", ["n"] = 5L, ["flag"] = true };
        }));
        Assert.Equal("user=me%20x&n=5&flag=true", form.Body);
        Assert.Equal("application/x-www-form-urlencoded", form.MediaType);

        var text = Options(Session(a =>
        {
            Login(a)["body"] = "<login user=\"me\"/>";
            Login(a)["content_type"] = "application/xml";
        }));
        Assert.Equal("<login user=\"me\"/>", text.Body);
        Assert.Equal("application/xml", text.MediaType);
    }

    [Fact]
    public void Every_error_is_reported_at_once()
    {
        var errors = Errors(new Dictionary<string, object?>
        {
            ["type"] = "session",
            ["login"] = new Dictionary<string, object?>
            {
                ["method"] = "put",
                ["body_format"] = "xml",
                ["headers"] = new Dictionary<string, object?> { ["Content-Type"] = "x", ["bad name"] = "y" },
                ["verb"] = "x",
            },
            ["token_pointer"] = "token",
            ["inject"] = new Dictionary<string, object?> { ["query"] = "s", ["prefix"] = "x" },
            ["ttl"] = "5m2s",
            ["expires_in_pointer"] = "/exp",
        });
        Assert.Contains("auth.login.verb is not a login option (accepted: path, url, method, body, body_format, content_type, headers)", errors);
        Assert.Contains("auth.login needs exactly one of path (relative to base_url) or url (absolute)", errors);
        Assert.Contains("auth.login.method must be one of: post, get", errors);
        Assert.Contains("auth.login.body_format must be one of: json, form", errors);
        Assert.Contains("auth.login.headers.Content-Type: set the body's type with auth.login.content_type", errors);
        Assert.Contains("auth.login.headers has an invalid header name 'bad name'", errors);
        Assert.Contains("auth.token_pointer is not a valid JSON pointer: 'token' (it must start with '/')", errors);
        Assert.Contains("auth.inject.prefix applies only with auth.inject.header", errors);
        Assert.Contains("auth needs exactly one of ttl (e.g. 30m) or expires_in_pointer (a JSON pointer to the lifetime in seconds)", errors);
        Assert.Contains("auth.ttl must be a duration from 1s to 30d like 90s, 30m, 8h or 1d, got '5m2s'", errors);
        Assert.Equal(10, errors.Count);
    }

    [Fact]
    public void Missing_pieces_are_named()
    {
        var errors = Errors(new Dictionary<string, object?> { ["type"] = "session" });
        Assert.Equal(3, errors.Count);
        Assert.Contains(errors, e => e.StartsWith("auth.login is required", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.StartsWith("auth.token_pointer is required", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.StartsWith("auth needs exactly one of ttl", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("0s")]
    [InlineData("500ms")]
    [InlineData("30 m")]
    [InlineData("1.5h")]
    [InlineData("-5m")]
    [InlineData("30M")]
    public void A_ttl_outside_the_duration_grammar_or_under_a_second_is_refused(string ttl)
        => Assert.Contains($"auth.ttl must be a duration from 1s to 30d like 90s, 30m, 8h or 1d, got '{ttl}'",
            Errors(Session(a => a["ttl"] = ttl)));

    [Fact]
    public void A_ttl_and_an_expires_in_pointer_together_are_refused()
        => Assert.Equal(["auth needs exactly one of ttl (e.g. 30m) or expires_in_pointer (a JSON pointer to the lifetime in seconds)"],
            Errors(Session(a => a["expires_in_pointer"] = "/expires_in")));

    [Fact]
    public void A_get_login_cannot_carry_a_body()
        => Assert.Contains("auth.login.body cannot be sent with method get; use method post",
            Errors(Session(a => Login(a)["method"] = "get")));

    [Fact]
    public void A_form_body_must_be_flat()
    {
        Assert.Contains("auth.login.body.nested: body_format form needs scalar values, not a nested map or list",
            Errors(Session(a =>
            {
                Login(a)["body_format"] = "form";
                Login(a)["body"] = new Dictionary<string, object?> { ["nested"] = new Dictionary<string, object?> { ["a"] = 1L } };
            })));
        Assert.Contains("auth.login.body_format form needs auth.login.body to be a flat map, not a string",
            Errors(Session(a =>
            {
                Login(a)["body_format"] = "form";
                Login(a)["body"] = "user=me";
            })));
    }

    [Theory]
    [InlineData("cookie")]
    [InlineData("Bearer")]
    public void An_unknown_inject_word_is_refused(string inject)
        => Assert.Contains(Errors(Session(a => a["inject"] = inject)), e => e.StartsWith("auth.inject must be 'bearer'", StringComparison.Ordinal));

    [Fact]
    public void Inject_needs_exactly_one_target_and_a_request_header_name()
    {
        Assert.Contains("auth.inject needs exactly one of header or query",
            Errors(Session(a => a["inject"] = new Dictionary<string, object?> { ["header"] = "X", ["query"] = "q" })));
        Assert.Contains("auth.inject.header 'Content-Type' is not a request header name",
            Errors(Session(a => a["inject"] = new Dictionary<string, object?> { ["header"] = "Content-Type" })));
        Assert.Contains("auth.inject.header 'X Session' is not a request header name",
            Errors(Session(a => a["inject"] = new Dictionary<string, object?> { ["header"] = "X Session" })));
    }

    [Fact]
    public void The_login_must_reach_an_allowed_host()
    {
        var foreign = Errors(Session(a =>
        {
            Login(a).Remove("path");
            Login(a)["url"] = "https://login.example.org/session";
        }));
        var error = Assert.Single(foreign);
        Assert.Contains("'https://login.example.org'", error);
        Assert.Contains("allow_hosts", error);

        Assert.Empty(Errors(Session(a =>
        {
            Login(a).Remove("path");
            Login(a)["url"] = "https://login.example.org/session";
        }), extra: new() { ["allow_hosts"] = new List<object?> { "login.example.org" } }));

        // Credentials never drop to cleartext: an allow-listed host must still be https under an https base_url.
        Assert.Single(Errors(Session(a =>
        {
            Login(a).Remove("path");
            Login(a)["url"] = "http://login.example.org/session";
        }), extra: new() { ["allow_hosts"] = new List<object?> { "login.example.org" } }));
    }

    [Fact]
    public void A_login_url_with_credentials_or_a_path_with_a_scheme_is_refused()
    {
        var withUser = Errors(Session(a =>
        {
            Login(a).Remove("path");
            Login(a)["url"] = "https://me:Secr3t@api.example.com/login";
        }));
        Assert.Equal(["auth.login.url must not embed credentials; put them in auth.login.body"], withUser);
        Assert.Equal(["auth.login.path is relative to base_url; put an absolute address in auth.login.url"],
            Errors(Session(a => Login(a)["path"] = "https://api.example.com/login")));
    }

    [Fact]
    public void Errors_never_echo_the_login_body()
    {
        var errors = Errors(Session(a =>
        {
            Login(a)["method"] = "get";
            a["inject"] = "nope";
        }));
        Assert.NotEmpty(errors);
        Assert.DoesNotContain(errors, e => e.Contains(Password, StringComparison.Ordinal));
    }

    [Fact]
    public void Schema_accepts_a_session_block_and_marks_the_login_body_writeOnly()
    {
        var text = new HttpConnector().ConnectionConfigSchema;
        var schema = JsonSchema.FromText(text);
        var ok = JsonSerializer.Deserialize<JsonElement>("""
            {"base_url":"https://api.example.com","auth":{"type":"session",
             "login":{"path":"/auth/login","method":"post","body":{"user":"u","password":"p"},"body_format":"json",
                      "content_type":"application/json","headers":{"X-Tenant":"a"}},
             "token_pointer":"/token","inject":{"header":"X-Session","prefix":"Token "},"ttl":"30m"}}
            """);
        Assert.True(schema.Evaluate(ok).IsValid);
        var bearer = JsonSerializer.Deserialize<JsonElement>("""
            {"base_url":"https://api.example.com","auth":{"type":"session","login":{"url":"https://api.example.com/l"},
             "token_pointer":"/token","inject":"bearer","expires_in_pointer":"/expires_in"}}
            """);
        Assert.True(schema.Evaluate(bearer).IsValid);

        using var doc = JsonDocument.Parse(text);
        var login = doc.RootElement.GetProperty("properties").GetProperty("auth").GetProperty("properties").GetProperty("login");
        Assert.True(login.GetProperty("properties").GetProperty("body").GetProperty("writeOnly").GetBoolean());
    }

    // ---- against a server ----

    private int Logins => _server.Requests.Count(r => r.Url.AbsolutePath == "/auth/login");

    private void MapLogin(Func<int, StubResponse>? respond = null)
    {
        var issued = 0;
        _server.Map("/auth/login", _ =>
        {
            issued++;
            return respond?.Invoke(issued) ?? new StubResponse(200, $$$"""{"session":{"token":"sess{{{issued}}}"}}""");
        });
    }

    private HttpSource Open(Dictionary<string, object?> auth, Dictionary<string, object?>? extra = null)
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(Config(_server.BaseUrl.ToString(), auth, extra), errors);
        Assert.Empty(errors);
        return new HttpSource(connection!, _clock);
    }

    private static DatasetSpec Items(Dictionary<string, object?>? extra = null)
    {
        var options = new Dictionary<string, object?> { ["path"] = "/items", ["items"] = "/data" };
        foreach (var (k, v) in extra ?? []) options[k] = v;
        return new DatasetSpec("s", "d", options);
    }

    private static async Task<long> ReadAsync(ISource source, DatasetSpec? spec = null)
    {
        var partition = (await source.PlanReadAsync(spec ?? Items(), ReadHints.None, CancellationToken.None))[0];
        long rows = 0;
        await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        return rows;
    }

    private static async Task<PzConnectorException> FailsAsync(ISource source)
        => await Assert.ThrowsAsync<PzConnectorException>(async () => await ReadAsync(source).WaitAsync(Bound));

    private StubRequest Api => _server.Requests.Last(r => r.Url.AbsolutePath == "/items");

    [Fact]
    public async Task A_read_logs_in_once_posts_the_literal_body_and_sends_the_bearer_token()
    {
        MapLogin();
        _server.Map("/items", _ => new StubResponse(200, """{"data":[{"id":1},{"id":2}]}"""));

        await using var source = Open(Session(a => Login(a)["headers"] = new Dictionary<string, object?> { ["X-Tenant"] = "acme" }),
            extra: new() { ["headers"] = new Dictionary<string, object?> { ["X-Api-Version"] = "7" } });
        Assert.Equal(2, await ReadAsync(source).WaitAsync(Bound));
        Assert.Equal(2, await ReadAsync(source).WaitAsync(Bound));

        Assert.Equal(1, Logins);
        var login = _server.Requests.Single(r => r.Url.AbsolutePath == "/auth/login");
        Assert.Equal("POST", login.Method);
        Assert.StartsWith("application/json", login.Headers["Content-Type"]);
        Assert.Equal("""{"user":"me","password":"P@ss-w0rd-{{not-a-binding}}"}""", login.Body);
        Assert.Equal("acme", login.Headers["X-Tenant"]);
        Assert.False(login.Headers.ContainsKey("X-Api-Version"), "connection headers are not sent on the login");
        Assert.False(login.Headers.ContainsKey("Authorization"));
        Assert.Equal("Bearer sess1", Api.Headers["Authorization"]);
        Assert.Equal("7", Api.Headers["X-Api-Version"]);
    }

    [Fact]
    public async Task A_named_header_with_a_prefix_carries_the_token()
    {
        MapLogin();
        _server.Map("/items", _ => new StubResponse(200, """{"data":[{"id":1}]}"""));

        await using var source = Open(Session(a =>
            a["inject"] = new Dictionary<string, object?> { ["header"] = "X-Session-Token", ["prefix"] = "Token " }));
        Assert.Equal(1, await ReadAsync(source).WaitAsync(Bound));

        Assert.Equal("Token sess1", Api.Headers["X-Session-Token"]);
        Assert.False(Api.Headers.ContainsKey("Authorization"));
    }

    [Fact]
    public async Task A_query_token_is_added_to_every_request_and_redacted_from_errors()
    {
        MapLogin();
        _server.Map("/items", req => req.Url.Query.Contains("page=2", StringComparison.Ordinal)
            ? new StubResponse(404, $$"""{"error":"no such page {{req.Url}}"}""")
            : new StubResponse(200, """{"data":[{"id":1}]}""",
                new Dictionary<string, string> { ["Link"] = $"<{new Uri(_server.BaseUrl, "items?page=2")}>; rel=\"next\"" }));

        await using var source = Open(Session(a => a["inject"] = new Dictionary<string, object?> { ["query"] = "sid" }));
        var spec = Items(new() { ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "link_header" } });
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await ReadAsync(source, spec).WaitAsync(Bound));

        var api = _server.Requests.Where(r => r.Url.AbsolutePath == "/items").ToList();
        Assert.Equal(2, api.Count);
        Assert.All(api, r => Assert.Contains("sid=sess1", r.Url.Query, StringComparison.Ordinal));
        Assert.All(api, r => Assert.False(r.Headers.ContainsKey("Authorization")));
        Assert.Contains("HTTP 404", ex.Message);
        Assert.DoesNotContain("sess1", ex.Message);
    }

    [Fact]
    public async Task A_numeric_token_is_sent_as_its_text()
    {
        MapLogin(_ => new StubResponse(200, """{"session":{"token":123456}}"""));
        _server.Map("/items", _ => new StubResponse(200, """{"data":[]}"""));

        await using var source = Open(Session());
        await ReadAsync(source).WaitAsync(Bound);
        Assert.Equal("Bearer 123456", Api.Headers["Authorization"]);
    }

    [Fact]
    public async Task The_ttl_decides_when_to_log_in_again()
    {
        MapLogin();
        _server.Map("/items", _ => new StubResponse(200, """{"data":[{"id":1}]}"""));

        await using var source = Open(Session());
        await ReadAsync(source).WaitAsync(Bound);
        _clock.Advance(TimeSpan.FromMinutes(24));
        await ReadAsync(source).WaitAsync(Bound);
        Assert.Equal(1, Logins);

        // 30m ttl, refreshed 5 minutes early.
        _clock.Advance(TimeSpan.FromMinutes(1));
        await ReadAsync(source).WaitAsync(Bound);
        Assert.Equal(2, Logins);
        Assert.Equal("Bearer sess2", Api.Headers["Authorization"]);
    }

    [Theory]
    [InlineData("600")]
    [InlineData("\"600\"")]
    public async Task The_lifetime_can_come_from_the_login_response(string expiresIn)
    {
        MapLogin(n => new StubResponse(200, $$$"""{"session":{"token":"sess{{{n}}}"},"meta":{"expires_in":{{{expiresIn}}}}}"""));
        _server.Map("/items", _ => new StubResponse(200, """{"data":[{"id":1}]}"""));

        await using var source = Open(Session(a =>
        {
            a.Remove("ttl");
            a["expires_in_pointer"] = "/meta/expires_in";
        }));
        await ReadAsync(source).WaitAsync(Bound);
        _clock.Advance(TimeSpan.FromMinutes(4));
        await ReadAsync(source).WaitAsync(Bound);
        Assert.Equal(1, Logins);
        _clock.Advance(TimeSpan.FromMinutes(2));
        await ReadAsync(source).WaitAsync(Bound);
        Assert.Equal(2, Logins);
    }

    [Theory]
    [InlineData("""{"session":{"token":"t"}}""")]
    [InlineData("""{"session":{"token":"t"},"meta":{"expires_in":0}}""")]
    [InlineData("""{"session":{"token":"t"},"meta":{"expires_in":"soon"}}""")]
    public async Task A_missing_or_unusable_lifetime_is_permanent(string body)
    {
        MapLogin(_ => new StubResponse(200, body));
        await using var source = Open(Session(a =>
        {
            a.Remove("ttl");
            a["expires_in_pointer"] = "/meta/expires_in";
        }));
        var ex = await FailsAsync(source);
        Assert.False(ex.IsTransient);
        Assert.Contains("auth.expires_in_pointer '/meta/expires_in'", ex.Message);
        Assert.DoesNotContain(_server.Requests, r => r.Url.AbsolutePath == "/items");
    }

    [Theory]
    [InlineData(401)]
    [InlineData(403)]
    public async Task A_refused_login_is_permanent_and_tried_once(int status)
    {
        MapLogin(_ => new StubResponse(status, $$"""{"error":"bad password {{Password}}"}"""));
        await using var source = Open(Session());

        var ex = await FailsAsync(source);
        Assert.False(ex.IsTransient);
        Assert.Contains($"HTTP {status}", ex.Message);
        Assert.Contains("auth.login", ex.Message);
        Assert.Contains("one refused login per connection per run", ex.Message);
        Assert.DoesNotContain(Password, ex.Message);
        Assert.Equal(1, Logins);
        Assert.DoesNotContain(_server.Requests, r => r.Url.AbsolutePath == "/items");
    }

    [Theory]
    [InlineData(429)]
    [InlineData(503)]
    [InlineData(408)]
    public async Task A_busy_login_endpoint_is_transient_with_retry_after(int status)
    {
        MapLogin(_ => new StubResponse(status, "{}", new Dictionary<string, string> { ["Retry-After"] = "12" }));
        await using var source = Open(Session());

        var ex = await FailsAsync(source);
        Assert.True(ex.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(12), ex.RetryAfter);
    }

    [Theory]
    [InlineData(429, 30)]
    [InlineData(503, 30)]
    [InlineData(408, null)]
    public async Task A_throttled_login_falls_back_to_the_connections_retry_after_header(int status, int? seconds)
    {
        // Like any other request: the configured header applies to a 429/503 without a usable Retry-After.
        MapLogin(_ => new StubResponse(status, "{}", new Dictionary<string, string> { ["X-RateLimit-Reset"] = "30" }));
        await using var source = Open(Session(), extra: new()
        {
            ["retry_after"] = new Dictionary<string, object?> { ["header"] = "X-RateLimit-Reset", ["kind"] = "seconds" },
        });

        var ex = await FailsAsync(source);
        Assert.True(ex.IsTransient);
        Assert.Equal(seconds is { } s ? TimeSpan.FromSeconds(s) : null, ex.RetryAfter);
    }

    [Fact]
    public async Task A_throttled_login_prefers_its_own_retry_after_and_notes_an_unreadable_configured_header()
    {
        MapLogin(issued => issued == 1
            ? new StubResponse(429, "{}", new Dictionary<string, string> { ["Retry-After"] = "12", ["X-Reset"] = "30" })
            : new StubResponse(429, "{}", new Dictionary<string, string> { ["X-Reset"] = "soon" }));
        await using var first = Open(Session(), extra: new()
        {
            ["retry_after"] = new Dictionary<string, object?> { ["header"] = "X-Reset", ["kind"] = "seconds" },
        });

        var standard = await FailsAsync(first);
        Assert.Equal(TimeSpan.FromSeconds(12), standard.RetryAfter);
        var unreadable = await FailsAsync(first);
        Assert.Null(unreadable.RetryAfter);
        Assert.Contains("retry_after header 'X-Reset' value is not a non-negative number; ignored", unreadable.Message);
    }

    [Fact]
    public async Task A_redirected_login_is_permanent_and_not_followed()
    {
        MapLogin(_ => new StubResponse(302, "", new Dictionary<string, string> { ["Location"] = "/elsewhere" }));
        await using var source = Open(Session());

        var ex = await FailsAsync(source);
        Assert.False(ex.IsTransient);
        Assert.Contains("redirect", ex.Message);
        Assert.Single(_server.Requests);
    }

    [Fact]
    public async Task Another_client_error_is_permanent()
    {
        MapLogin(_ => new StubResponse(400, "{}"));
        await using var source = Open(Session());
        var ex = await FailsAsync(source);
        Assert.False(ex.IsTransient);
        Assert.Contains("HTTP 400", ex.Message);
    }

    [Theory]
    [InlineData("<html>login page</html>", "not JSON")]
    [InlineData("""{"session":{}}""", "no token")]
    [InlineData("""{"session":{"token":""}}""", "no token")]
    [InlineData("""{"session":{"token":null}}""", "no token")]
    [InlineData("""{"session":{"token":{"v":"x"}}}""", "no token")]
    [InlineData("""{"session":{"token":"a\nb"}}""", "control characters")]
    public async Task A_login_answer_without_a_usable_token_is_permanent(string body, string expected)
    {
        MapLogin(_ => new StubResponse(200, body));
        await using var source = Open(Session());

        var ex = await FailsAsync(source);
        Assert.False(ex.IsTransient);
        Assert.Contains(expected, ex.Message);
        Assert.DoesNotContain("login page", ex.Message);
    }

    [Fact]
    public async Task An_oversized_login_answer_is_permanent()
    {
        MapLogin(_ => new StubResponse(200, "{\"session\":{\"token\":\"" + new string('x', SessionOptions.MaxLoginResponseBytes) + "\"}}"));
        await using var source = Open(Session());

        var ex = await FailsAsync(source);
        Assert.False(ex.IsTransient);
        Assert.Contains("larger than 1 MiB", ex.Message);
    }

    [Fact]
    public async Task A_401_on_a_session_that_worked_is_transient_and_the_retry_logs_in_again()
    {
        MapLogin();
        var calls = 0;
        _server.Map("/items", _ => ++calls == 2
            ? new StubResponse(401, "{}")
            : new StubResponse(200, """{"data":[{"id":1}]}"""));

        await using var source = Open(Session());
        await ReadAsync(source).WaitAsync(Bound);
        var ex = await FailsAsync(source);
        Assert.True(ex.IsTransient);
        await ReadAsync(source).WaitAsync(Bound);

        Assert.Equal(2, Logins);
        Assert.Equal("Bearer sess2", Api.Headers["Authorization"]);
    }

    [Fact]
    public async Task A_401_on_a_fresh_session_is_permanent_and_points_at_inject()
    {
        MapLogin();
        _server.Map("/items", _ => new StubResponse(401, "{}"));

        await using var source = Open(Session());
        var ex = await FailsAsync(source);
        Assert.False(ex.IsTransient);
        Assert.Contains("auth.inject", ex.Message);
        Assert.Contains("auth.token_pointer", ex.Message);
        Assert.DoesNotContain("sess1", ex.Message);
        Assert.Equal(1, Logins);
    }

    [Fact]
    public async Task Check_connection_reports_a_refused_login_as_a_sign_in_failure()
    {
        MapLogin(_ => new StubResponse(401, "{}"));

        var check = await new HttpConnector().CheckConnectionAsync(Config(_server.BaseUrl.ToString(), Session()), CancellationToken.None)
            .AsTask().WaitAsync(Bound);

        Assert.False(check.Ok);
        Assert.StartsWith("sign-in failed", check.Message);
        Assert.DoesNotContain(Password, check.Message);
    }

    [Fact]
    public async Task Check_connection_reports_a_foreign_login_host_offline()
    {
        var check = await new HttpConnector().CheckConnectionAsync(Config(_server.BaseUrl.ToString(), Session(a =>
        {
            Login(a).Remove("path");
            Login(a)["url"] = "https://login.example.invalid/session";
        })), CancellationToken.None).AsTask().WaitAsync(Bound);

        Assert.False(check.Ok);
        Assert.Contains("allow_hosts", check.Message);
        Assert.Empty(_server.Requests);
    }

    // ---- fix round ----

    [Fact]
    public async Task Two_runs_in_one_process_each_get_one_login_attempt()
    {
        // A long-lived pz mcp server: an admin unlocks the account between runs, and the next run must try.
        MapLogin(_ => new StubResponse(401, "{}"));
        var outcomes = new List<PzConnectorException>();
        foreach (var run in new[] { "run-a", "run-b" })
        {
            foreach (var node in new[] { "n1", "n2" })
            {
                await using var source = Open(Session());
                ((IRunContextAware)source).UseRunContext(new ConnectorRunContext(run, node));
                outcomes.Add(await FailsAsync(source));
            }
        }

        Assert.Equal(2, Logins);
        Assert.DoesNotContain("pz did not try again", outcomes[0].Message);
        Assert.Contains("pz did not try again", outcomes[1].Message);
        Assert.DoesNotContain("pz did not try again", outcomes[2].Message);
        Assert.Contains("pz did not try again", outcomes[3].Message);
    }

    [Fact]
    public async Task Each_connection_check_gets_its_own_login_attempt()
    {
        // pz validate --connect in a long-lived pz mcp server: a refused check must not stop the next one.
        MapLogin(_ => new StubResponse(401, "{}"));
        var config = Config(_server.BaseUrl.ToString(), Session(), null);

        var first = await new HttpConnector().CheckConnectionAsync(config, CancellationToken.None);
        var second = await new HttpConnector().CheckConnectionAsync(config, CancellationToken.None);

        Assert.False(first.Ok);
        Assert.False(second.Ok);
        Assert.DoesNotContain("pz did not try again", second.Message);
        Assert.Equal(2, Logins);
    }

    [Fact]
    public async Task A_check_and_a_read_in_one_validation_pass_share_one_login_attempt()
    {
        MapLogin(_ => new StubResponse(401, "{}"));
        var pass = new ConnectorRunContext("validate-1", null);

        var check = await new HttpConnector().CheckConnectionAsync(Config(_server.BaseUrl.ToString(), Session(), null), pass,
            CancellationToken.None);
        await using var source = Open(Session());
        ((IRunContextAware)source).UseRunContext(pass);
        var read = await FailsAsync(source);

        Assert.False(check.Ok);
        Assert.Contains("pz did not try again", read.Message);
        Assert.Equal(1, Logins);
    }

    [Fact]
    public async Task A_refused_login_is_not_repeated_by_later_reads_or_other_nodes()
    {
        MapLogin(_ => new StubResponse(401, "{}"));
        await using var first = Open(Session());
        await using var second = Open(Session());

        // Two nodes starting together, then a later read on each.
        var together = await Task.WhenAll(FailsAsync(first), FailsAsync(second));
        var later = await FailsAsync(first);

        Assert.Equal(1, Logins);
        Assert.All(together.Append(later), ex => Assert.False(ex.IsTransient));
        Assert.Contains("pz did not try again", later.Message);
        Assert.DoesNotContain(_server.Requests, r => r.Url.AbsolutePath == "/items");

        // Another password is another credential, and it is tried.
        MapLogin();
        _server.Map("/items", _ => new StubResponse(200, """{"data":[]}"""));
        await using var fixedSource = Open(Session(a => Login(a)["body"] = new Dictionary<string, object?> { ["user"] = "me", ["password"] = "new" }));
        await ReadAsync(fixedSource).WaitAsync(Bound);
        Assert.Equal(2, Logins);
    }

    [Fact]
    public async Task A_login_that_failed_temporarily_is_tried_again()
    {
        MapLogin(n => n == 1 ? new StubResponse(503, "{}") : new StubResponse(200, """{"session":{"token":"ok"}}"""));
        _server.Map("/items", _ => new StubResponse(200, """{"data":[]}"""));
        await using var source = Open(Session());
        Assert.True((await FailsAsync(source)).IsTransient);
        await ReadAsync(source).WaitAsync(Bound);
        Assert.Equal(2, Logins);
    }

    [Theory]
    [InlineData("c:login")]
    [InlineData("a:b:c")]
    [InlineData("javascript:alert(1)")]
    [InlineData("http:api.example.com/login")]
    public void A_malformed_login_path_is_a_validation_error_not_a_crash(string path)
    {
        var errors = Errors(Session(a => Login(a)["path"] = path));
        Assert.Contains($"auth.login.path '{path}' is not a valid path relative to base_url", errors);
    }

    [Fact]
    public async Task Validate_lists_a_malformed_login_path_with_the_other_errors()
    {
        var auth = Session(a =>
        {
            Login(a)["path"] = "c:login";
            a["ttl"] = "forever";
        });
        var result = await new HttpConnector().ValidateAsync(
            Config("https://api.example.com/", auth, new() { ["timeout_seconds"] = -1 }), CancellationToken.None);

        Assert.False(result.IsValid);
        Assert.Equal(3, result.Errors.Count);
        Assert.Contains("auth.login.path 'c:login' is not a valid path relative to base_url", result.Errors);
        Assert.Contains(result.Errors, e => e.StartsWith("auth.ttl must be", StringComparison.Ordinal));
        Assert.Contains(result.Errors, e => e.StartsWith("'timeout_seconds'", StringComparison.Ordinal));
    }

    [Fact]
    public void Ttl_is_bounded_to_thirty_days()
    {
        Assert.Equal(TimeSpan.FromDays(30), Options(Session(a => a["ttl"] = "30d")).Ttl);
        Assert.Contains("auth.ttl must be a duration from 1s to 30d like 90s, 30m, 8h or 1d, got '31d'",
            Errors(Session(a => a["ttl"] = "31d")));
        Assert.Contains("auth.ttl must be a duration from 1s to 30d like 90s, 30m, 8h or 1d, got '5000000d'",
            Errors(Session(a => a["ttl"] = "5000000d")));
    }

    [Theory]
    [InlineData("""{"session":{"token":"a","token":"b"}}""")]
    [InlineData("""{"session":{"token":"a"},"session":{"token":"b"}}""")]
    public async Task A_login_response_with_duplicate_keys_is_permanent_and_names_the_pointer(string body)
    {
        MapLogin(_ => new StubResponse(200, body));
        await using var source = Open(Session());
        var ex = await FailsAsync(source);
        Assert.False(ex.IsTransient);
        Assert.Contains("auth.token_pointer '/session/token'", ex.Message);
        Assert.Contains("duplicate keys", ex.Message);
    }

    [Fact]
    public async Task A_non_ascii_token_is_refused_for_a_header_and_accepted_for_a_query()
    {
        MapLogin(_ => new StubResponse(200, """{"session":{"token":"tök"}}"""));
        _server.Map("/items", _ => new StubResponse(200, """{"data":[]}"""));

        await using (var bearer = Open(Session()))
        {
            var ex = await FailsAsync(bearer);
            Assert.False(ex.IsTransient);
            Assert.Contains("auth.token_pointer", ex.Message);
            Assert.Contains("printable ASCII", ex.Message);
        }

        await using var query = Open(Session(a => a["inject"] = new Dictionary<string, object?> { ["query"] = "sid" }));
        await ReadAsync(query).WaitAsync(Bound);
        Assert.Contains("sid=t%C3%B6k", Api.Url.Query, StringComparison.Ordinal);
    }

    [Fact]
    public void A_null_form_field_is_a_validation_error()
        => Assert.Contains("auth.login.body.password is empty (null); a form field needs a value", Errors(Session(a =>
        {
            Login(a)["body_format"] = "form";
            Login(a)["body"] = new Dictionary<string, object?> { ["user"] = "me", ["password"] = null };
        })));

    [Fact]
    public async Task The_login_sends_the_connector_user_agent()
    {
        MapLogin();
        _server.Map("/items", _ => new StubResponse(200, """{"data":[]}"""));
        await using var source = Open(Session());
        await ReadAsync(source).WaitAsync(Bound);
        Assert.Equal("pz-http/0.1", _server.Requests.Single(r => r.Url.AbsolutePath == "/auth/login").Headers["User-Agent"]);
    }

    [Fact]
    public void Reauth_status_is_validated()
    {
        Assert.Equal(new HashSet<int> { 401 }, Options(Session()).ReauthStatus);
        Assert.Equal(new HashSet<int> { 401, 419, 440 },
            Options(Session(a => a["reauth_status"] = new List<object?> { 401L, 419L, "440" })).ReauthStatus);

        var errors = Errors(Session(a => a["reauth_status"] = new List<object?> { 500L, 407L, 429L, "x" }));
        Assert.Contains("auth.reauth_status entries must be 4xx statuses other than 407 and 429, got '500'", errors);
        Assert.Contains("auth.reauth_status entries must be 4xx statuses other than 407 and 429, got '407'", errors);
        Assert.Contains("auth.reauth_status entries must be 4xx statuses other than 407 and 429, got '429'", errors);
        Assert.Contains("auth.reauth_status entries must be 4xx statuses other than 407 and 429, got 'x'", errors);
        Assert.Equal(4, errors.Count);

        // 401 is always in the list: a custom list adds to it.
        Assert.Equal(new HashSet<int> { 401, 419 }, Options(Session(a => a["reauth_status"] = new List<object?> { 419L })).ReauthStatus);
        Assert.Equal(["auth.reauth_status must be a list of HTTP statuses, e.g. [401, 419, 440]"],
            Errors(Session(a => a["reauth_status"] = 419L)));
    }

    [Theory]
    [InlineData(419)]
    [InlineData(440)]
    public async Task A_reauth_status_on_a_session_that_worked_logs_in_again(int status)
    {
        MapLogin();
        var calls = 0;
        _server.Map("/items", _ => ++calls == 2 ? new StubResponse(status, "{}") : new StubResponse(200, """{"data":[{"id":1}]}"""));

        await using var source = Open(Session(a => a["reauth_status"] = new List<object?> { 401L, 419L, 440L }));
        await ReadAsync(source).WaitAsync(Bound);
        var ex = await FailsAsync(source);
        Assert.True(ex.IsTransient);
        Assert.Contains($"HTTP {status}", ex.Message);
        await ReadAsync(source).WaitAsync(Bound);

        Assert.Equal(2, Logins);
        Assert.Equal("Bearer sess2", Api.Headers["Authorization"]);
    }

    [Fact]
    public async Task A_custom_reauth_status_list_keeps_401()
    {
        MapLogin();
        var calls = 0;
        _server.Map("/items", _ => ++calls == 2 ? new StubResponse(401, "{}") : new StubResponse(200, """{"data":[{"id":1}]}"""));

        await using var source = Open(Session(a => a["reauth_status"] = new List<object?> { 403L }));
        await ReadAsync(source).WaitAsync(Bound);
        Assert.True((await FailsAsync(source)).IsTransient);
        await ReadAsync(source).WaitAsync(Bound);
        Assert.Equal(2, Logins);
    }

    [Fact]
    public async Task Without_reauth_status_a_403_on_a_session_that_worked_stays_permanent()
    {
        MapLogin();
        var calls = 0;
        _server.Map("/items", _ => ++calls == 2 ? new StubResponse(403, "{}") : new StubResponse(200, """{"data":[{"id":1}]}"""));

        await using var source = Open(Session());
        await ReadAsync(source).WaitAsync(Bound);
        var ex = await FailsAsync(source);
        Assert.False(ex.IsTransient);
        Assert.Contains("HTTP 403", ex.Message);
        Assert.Equal(1, Logins);
    }

    [Fact]
    public async Task A_reauth_status_on_a_fresh_session_is_permanent_and_points_at_inject()
    {
        MapLogin();
        _server.Map("/items", _ => new StubResponse(419, "{}"));

        await using var source = Open(Session(a => a["reauth_status"] = new List<object?> { 419L }));
        var ex = await FailsAsync(source);
        Assert.False(ex.IsTransient);
        Assert.Contains("(HTTP 419)", ex.Message);
        Assert.Contains("auth.inject", ex.Message);
    }

    private static readonly Apache.Arrow.Schema SinkSchema = new(
        [new Apache.Arrow.Field("id", Apache.Arrow.Types.Int64Type.Default, nullable: false)], null);

    private async Task WriteOneAsync(ISink sink)
    {
        var spec = new OutputSpec("api", "out", "append", "fail_on_change", new Dictionary<string, object?> { ["path"] = "/out" });
        await using var session = await sink.BeginWriteAsync(spec, SinkSchema, CancellationToken.None);
        var builder = new ArrowBatchBuilder(SinkSchema);
        builder.AppendRow([1L]);
        using var batch = builder.Flush()!;
        await session.WriteBatchAsync(batch, CancellationToken.None).AsTask().WaitAsync(Bound);
    }

    [Fact]
    public async Task The_sink_writes_with_the_session_and_logs_in_again_after_a_401()
    {
        MapLogin();
        var posts = 0;
        _server.Map("/out", _ => ++posts == 2 ? new StubResponse(401, "{}") : new StubResponse(200, "{}"));
        var errors = new List<string>();
        await using var sink = new HttpSink(HttpConnectionConfig.Parse(
            Config(_server.BaseUrl.ToString(), Session(a => a["inject"] = new Dictionary<string, object?> { ["header"] = "X-Session" })),
            errors)!, _clock);
        Assert.Empty(errors);

        await WriteOneAsync(sink);
        Assert.True((await Assert.ThrowsAsync<PzConnectorException>(() => WriteOneAsync(sink))).IsTransient);
        await WriteOneAsync(sink);

        Assert.Equal(2, Logins);
        Assert.Equal(["sess1", "sess1", "sess2"],
            _server.Requests.Where(r => r.Url.AbsolutePath == "/out").Select(r => r.Headers["X-Session"]));
    }

    [Fact]
    public async Task A_parallel_for_each_read_shares_one_login_and_a_mid_read_401_logs_in_again_on_retry()
    {
        MapLogin();
        _server.Map("/orders", _ => new StubResponse(200, """{"data":[{"id":"a"},{"id":"b"},{"id":"c"},{"id":"d"}]}"""));
        _server.MapPrefix("/orders/", req => req.Url.AbsolutePath == "/orders/b/lines" && req.Headers["Authorization"] == "Bearer sess1"
            ? new StubResponse(401, "{}")
            : new StubResponse(200, """{"lines":[{"sku":"x"}]}"""));
        var spec = Items(new()
        {
            ["path"] = "/orders/{{ item.id }}/lines",
            ["items"] = "/lines",
            ["for_each"] = new Dictionary<string, object?>
            {
                ["path"] = "/orders", ["items"] = "/data", ["concurrency"] = 4L,
                ["fields"] = new Dictionary<string, object?> { ["id"] = "/id" },
            },
        });

        await using var source = Open(Session());
        // The list page succeeded with sess1, so the item's 401 is on a session that had worked: transient.
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await ReadAsync(source, spec).WaitAsync(Bound));
        Assert.True(ex.IsTransient);
        // One login served all four items; a sibling still running when the 401 dropped the session may already
        // have started the next login, which the retry then reuses.
        Assert.InRange(Logins, 1, 2);

        Assert.Equal(4, await ReadAsync(source, spec).WaitAsync(Bound));
        Assert.Equal(2, Logins);
    }

    [Theory]
    [InlineData("sid")]
    [InlineData("SID")]
    [InlineData("%73id")]
    [InlineData("%53Id")]
    public async Task A_query_session_token_is_never_stored_in_a_checkpoint_and_is_added_back_on_resume(string echoedName)
    {
        MapLogin();
        // The API echoes the session id into its next links, as such APIs do.
        _server.Map("/items", req =>
        {
            var sid = System.Web.HttpUtility.ParseQueryString(req.Url.Query)["sid"];
            return req.Url.Query.Contains("page=2", StringComparison.Ordinal)
                ? new StubResponse(200, """{"data":[{"id":3}]}""")
                : new StubResponse(200, """{"data":[{"id":1},{"id":2}]}""", new Dictionary<string, string>
                {
                    ["Link"] = $"<{new Uri(_server.BaseUrl, $"items?{echoedName}={sid}&page=2")}>; rel=\"next\"",
                });
        });
        var auth = Session(a => a["inject"] = new Dictionary<string, object?> { ["query"] = "sid" });
        var spec = Items(new() { ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "link_header" } });

        string? token = null;
        await using (var source = Open(auth))
        {
            var partition = Assert.IsAssignableFrom<ICheckpointingPartition>(
                (await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None))[0]);
            await foreach (var batch in partition.ReadAsync(new BatchOptions(MaxRowsPerBatch: 2), CancellationToken.None))
            {
                batch.Dispose();
                if (partition.TryGetCheckpoint(out var offered) && offered is not null)
                {
                    token = offered;
                    break;
                }
            }
        }

        Assert.NotNull(token);
        Assert.DoesNotContain("sess1", token);
        Assert.DoesNotContain("sid", token, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(echoedName, token, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("page=2", token);

        // A later run resumes from the token with its own session.
        await using var resumed = Open(auth);
        var resume = Assert.IsAssignableFrom<ICheckpointingPartition>(
            (await resumed.PlanReadAsync(spec, ReadHints.None, CancellationToken.None))[0]);
        Assert.True(resume.TryResumeFrom(token!));
        long rows = 0;
        await foreach (var batch in resume.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        Assert.Equal(1, rows);
        Assert.Equal(2, Logins);
        Assert.Contains("sid=sess2", Api.Url.Query, StringComparison.Ordinal);
        Assert.Contains("page=2", Api.Url.Query, StringComparison.Ordinal);
    }
}
