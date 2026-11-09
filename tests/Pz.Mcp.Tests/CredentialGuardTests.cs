using Pz.Mcp.Handlers;

namespace Pz.Mcp.Tests;

/// <summary>The literal-credential guard over a whole proposed connection block: nested maps (an http
/// <c>auth:</c>, <c>proxy:</c>, <c>tls:</c>, <c>headers:</c>), lists, and a session login body. Errors name the
/// dotted path of the key, never the value.</summary>
public class CredentialGuardTests
{
    private static Dictionary<string, object?> Map(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    private static List<string> Flagged(Dictionary<string, object?> connection)
        => [.. CredentialGuard.FindLiteralCredentials(connection).Select(e => e.Message)];

    [Theory]
    [InlineData("token")]
    [InlineData("password")]
    [InlineData("client_secret")]
    [InlineData("refresh_token")]
    [InlineData("key")]
    public void A_literal_secret_nested_under_auth_is_refused(string key)
    {
        var errors = CredentialGuard.FindLiteralCredentials(
            Map(("base_url", "https://api.example.com"), ("auth", Map(("type", "x"), (key, "hunter2")))));

        var error = Assert.Single(errors);
        Assert.Equal("PZ0601", error.Code);
        Assert.Contains($"'auth.{key}'", error.Message);
        Assert.DoesNotContain("hunter2", error.Message + error.Hint);
    }

    [Fact]
    public void Login_body_fields_proxy_tls_and_auth_headers_are_checked_at_any_depth()
    {
        var flagged = Flagged(Map(
            ("auth", Map(
                ("type", "session"),
                ("login", Map(("path", "/login"), ("body", Map(("user", "me"), ("pass", "hunter2"))),
                    ("headers", Map(("X-Api-Key", "k-123"))))),
                ("token_pointer", "/token"))),
            ("proxy", Map(("url", "http://proxy:8080"), ("password", "p"))),
            ("tls", Map(("client_certificate", Map(("path", "c.pfx"), ("password", "p"))))),
            ("headers", Map(("Authorization", "Bearer abc"), ("Accept", "application/json"))),
            ("allow_hosts", new List<object?> { "files.example.com" })));

        Assert.Equal(
        [
            "connection option 'auth.login.body.pass' looks like a literal credential typed directly into YAML.",
            "connection option 'auth.login.headers.X-Api-Key' looks like a literal credential typed directly into YAML.",
            "connection option 'proxy.password' looks like a literal credential typed directly into YAML.",
            "connection option 'tls.client_certificate.password' looks like a literal credential typed directly into YAML.",
            "connection option 'headers.Authorization' looks like a literal credential typed directly into YAML.",
        ], flagged);
    }

    [Fact]
    public void A_form_login_body_typed_without_any_variable_is_refused()
    {
        Assert.Single(Flagged(Map(("auth", Map(("login", Map(("body", "user=me&pass=hunter2"))))))));
        Assert.Empty(Flagged(Map(("auth", Map(("login", Map(("body", "user=me&pass=${LOGIN_PASSWORD}"))))))));
    }

    [Fact]
    public void Env_references_paths_urls_and_pointers_are_accepted()
    {
        Assert.Empty(Flagged(Map(
            ("auth", Map(
                ("type", "oauth2_refresh_token"),
                ("token_url", "https://login.example.com/token"),
                ("client_id", "my-app"),
                ("client_secret", "${CLIENT_SECRET}"),
                ("refresh_token", "${REFRESH_TOKEN}"),
                ("token_pointer", "/access_token"))),
            ("tls", Map(("client_certificate", Map(("path", "c.pem"), ("key_path", "c.key"))))),
            ("headers", Map(("Authorization", "${API_AUTH}"))),
            ("key_file", "keys/service.json"))));
    }

    [Fact]
    public void Lists_of_maps_are_walked_with_their_index()
    {
        var flagged = Flagged(Map(("accounts", new List<object?> { Map(("name", "a")), Map(("secret", "s")) })));

        Assert.Equal(["connection option 'accounts[1].secret' looks like a literal credential typed directly into YAML."],
            flagged);
    }

    [Theory]
    [InlineData("Bearer ${API_TOKEN}")]
    [InlineData("Basic ${CREDS}")]
    [InlineData("Token ${T}")]
    [InlineData("${USER}:${PASSWORD}")]
    [InlineData("  ${A}${B} ")]
    public void A_value_of_only_references_or_a_scheme_word_and_references_is_accepted(string value)
    {
        Assert.Empty(Flagged(Map(("headers", Map(("Authorization", value))), ("auth", Map(("token", value))))));
    }

    [Theory]
    [InlineData("Bearer abc123")]
    [InlineData("Bearer ${X} abc123")]
    [InlineData("${PREFIX}hunter2")]
    public void A_value_with_literal_text_beside_its_references_is_refused(string value)
    {
        Assert.Single(Flagged(Map(("headers", Map(("Authorization", value))))));
    }

    [Theory]
    [InlineData("user=${U}&password=hunter2", "auth.login.body.password")]
    [InlineData("user=me&pass%77ord=hunter2", "auth.login.body.password")]
    [InlineData("{\"user\":\"${U}\",\"secret\":\"hunter2\"}", "auth.login.body.secret")]
    public void A_literal_credential_inside_a_form_or_json_login_body_string_is_refused(string body, string path)
    {
        var flagged = Flagged(Map(("auth", Map(("login", Map(("body", body)))))));

        Assert.Equal([$"connection option '{path}' looks like a literal credential typed directly into YAML."], flagged);
    }

    [Theory]
    [InlineData("user=${U}&password=${P}")]
    [InlineData("grant_type=client_credentials&user=me")]
    [InlineData("{\"user\":\"me\",\"password\":\"${P}\"}")]
    public void A_login_body_string_without_a_literal_credential_is_accepted(string body)
    {
        Assert.Empty(Flagged(Map(("auth", Map(("login", Map(("body", body))))))));
    }

    [Theory]
    [InlineData("base_url", "https://user:hunter2@api.example.com/", "base_url")]
    [InlineData("token_url", "https://idp.example.com/token?client_secret=abc", "token_url")]
    [InlineData("url", "http://u:p@proxy:8080", "proxy.url")]
    public void A_url_carrying_a_password_or_a_secret_query_parameter_is_refused(string key, string url, string path)
    {
        var connection = key == "url" ? Map(("proxy", Map((key, url)))) : Map((key, url));

        Assert.Equal([$"connection option '{path}' looks like a literal credential typed directly into YAML."],
            Flagged(connection));
    }

    [Theory]
    [InlineData("https://${USER}:${PASSWORD}@api.example.com/")]
    [InlineData("https://idp.example.com/token?client_secret=${SECRET}&scope=read")]
    [InlineData("https://user@api.example.com/?page=1")]
    public void A_url_with_referenced_or_no_secrets_is_accepted(string url)
    {
        Assert.Empty(Flagged(Map(("base_url", url))));
    }

    [Fact]
    public void Strings_in_a_list_under_a_credential_shaped_key_are_checked()
    {
        Assert.Equal(["connection option 'api_keys[1]' looks like a literal credential typed directly into YAML."],
            Flagged(Map(("api_keys", new List<object?> { "${K1}", "abc" }), ("allow_hosts", new List<object?> { "x" }))));
    }

    [Fact]
    public void Entity_options_are_checked_under_their_own_path()
    {
        var errors = CredentialGuard.FindLiteralCredentialsUnder(
            Map(("query", Map(("api_key", "abc"))), ("body", Map(("password", "${P}")))), "entities.orders.read");

        var error = Assert.Single(errors);
        Assert.Equal("option 'entities.orders.read.query.api_key' looks like a literal credential typed directly into YAML.",
            error.Message);
    }
}
