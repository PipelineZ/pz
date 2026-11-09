using Pz.Mcp.Handlers;

namespace Pz.Mcp.Tests;

/// <summary>Realistic connection and entity configs, each with the paths the guard must refuse (none for a config it
/// must accept). Keys that merely contain "key" or "pass" (merge <c>keys</c>, <c>primary_key</c>, proxy
/// <c>bypass</c>, <c>idempotency_key_header</c>) are not secrets; templated values whose literal part is structure
/// (<c>session=${S}</c>) are fine; literal secrets in URLs, bodies and numbers are caught.</summary>
public class CredentialGuardTableTests
{
    private static Dictionary<string, object?> Map(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    private static List<object?> List(params object?[] items) => [.. items];

    public static TheoryData<string, Dictionary<string, object?>, string[]> Connections => new()
    {
        { "http bearer from env", Map(("base_url", "https://api.example.com/"),
            ("auth", Map(("type", "bearer"), ("token", "${API_TOKEN}")))), [] },
        { "proxy bypass list", Map(("base_url", "https://api.example.com/"),
            ("proxy", Map(("url", "http://proxy:8080"), ("bypass", List("localhost", "*.corp"))))), [] },
        { "cookie with a templated session", Map(("headers", Map(("Cookie", "session=${SESSION}"), ("Accept", "*/*")))), [] },
        { "cookie with two templated pairs", Map(("headers", Map(("Cookie", "a=${A}; b=${B}")))), [] },
        { "authorization scheme plus reference", Map(("headers", Map(("Authorization", "Bearer ${T}")))), [] },
        { "api_key auth in a header from env", Map(("auth", Map(("type", "api_key"), ("key", "${K}"), ("header", "X-Api-Key")))), [] },
        { "tls key and cert paths", Map(("tls", Map(("client_certificate", Map(("path", "c.pem"), ("key_path", "c.key")))))), [] },
        { "oauth2 from env", Map(("auth", Map(("type", "oauth2_client_credentials"), ("token_url", "https://idp/token"),
            ("client_id", "app"), ("client_secret", "${S}"), ("token_pointer", "/access_token")))), [] },
        { "sas base_url from env", Map(("base_url", "https://acct.blob.core.windows.net/c?sv=2022&sig=${SIG}")), [] },
        { "port and timeouts are numbers", Map(("host", "db"), ("port", 5432L), ("timeout_seconds", 30L)), [] },
        { "literal token", Map(("auth", Map(("type", "bearer"), ("token", "hunter2")))), ["auth.token"] },
        { "literal cookie", Map(("headers", Map(("Cookie", "session=abc123")))), ["headers.Cookie"] },
        { "literal X-Auth header", Map(("headers", Map(("X-Auth", "hunter2")))), ["headers.X-Auth"] },
        { "numeric password", Map(("password", 123456L)), ["password"] },
        { "inline service-account key", Map(("key_json", "{\"type\":\"service_account\"}")), ["key_json"] },
        { "camelCase client secret", Map(("clientSecret", "abc")), ["clientSecret"] },
        { "api keys list", Map(("api_keys", List("${K1}", "abc"))), ["api_keys[1]"] },
        { "userinfo in an endpoint", Map(("endpoint", "https://user:hunter2@host:9000")), ["endpoint"] },
        { "SAS signature in base_url", Map(("base_url", "https://acct.blob.core.windows.net/c?sv=2022&sig=abcdef")), ["base_url"] },
        { "client secret in a token_url", Map(("auth", Map(("token_url", "https://idp/token?client_secret=abc")))), ["auth.token_url"] },
    };

    public static TheoryData<string, Dictionary<string, object?>, string[]> Entities => new()
    {
        { "merge keys list", Map(("strategy", "merge"), ("keys", List("id", "region"))), [] },
        { "merge keys scalar", Map(("strategy", "merge"), ("keys", "id")), [] },
        { "primary key", Map(("primary_key", "id")), [] },
        { "export idempotency header", Map(("export", Map(("idempotency_key_header", "Idempotency-Key")))), [] },
        { "query from env", Map(("query", Map(("api_key", "${K}"), ("page_size", 100L)))), [] },
        { "path with a templated key", Map(("path", "/r?api_key=${K}&page=1")), [] },
        { "body string with a templated password", Map(("method", "post"), ("body", "user=me&password=${P}")), [] },
        { "literal key in a path query", Map(("path", "/r?api_key=abc&page=1")), ["entities.e.read.path"] },
        { "literal query param", Map(("query", Map(("api_key", "abc")))), ["entities.e.read.query.api_key"] },
        { "literal form body", Map(("method", "post"), ("body", "user=me&password=hunter2")),
            ["entities.e.read.body.password"] },
        { "literal json body string", Map(("method", "post"), ("body", "{\"user\":\"me\",\"token\":\"hunter2\"}")),
            ["entities.e.read.body.token"] },
        { "literal body map field", Map(("body", Map(("secret", "x")))), ["entities.e.read.body.secret"] },
    };

    [Theory]
    [MemberData(nameof(Connections))]
    public void Connection_configs(string name, Dictionary<string, object?> connection, string[] refused)
    {
        _ = name;
        var paths = CredentialGuard.FindLiteralCredentials(connection)
            .Select(e => e.Message.Split('\'')[1]).ToArray();

        Assert.Equal(refused, paths);
    }

    [Theory]
    [MemberData(nameof(Entities))]
    public void Entity_configs(string name, Dictionary<string, object?> read, string[] refused)
    {
        _ = name;
        var paths = CredentialGuard.FindLiteralCredentialsUnder(read, "entities.e.read")
            .Select(e => e.Message.Split('\'')[1]).ToArray();

        Assert.Equal(refused, paths);
    }
}
