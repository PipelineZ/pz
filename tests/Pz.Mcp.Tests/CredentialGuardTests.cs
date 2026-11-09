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
}
