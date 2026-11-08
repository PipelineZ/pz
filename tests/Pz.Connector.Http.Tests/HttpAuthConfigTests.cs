using System.Text.Json;
using Json.Schema;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Auth;

namespace Pz.Connector.Http.Tests;

public class HttpAuthConfigTests
{
    private static Dictionary<string, object?> AzureAd(string method) => new()
    {
        ["type"] = "azure_ad",
        ["method"] = method,
        ["scope"] = "https://graph.microsoft.com/.default",
        ["tenant_id"] = "t",
        ["client_id"] = "c",
        ["client_secret"] = "s3cret",
    };

    private static Dictionary<string, object?> OAuth2() => new()
    {
        ["type"] = "oauth2_client_credentials",
        ["token_url"] = "https://login.example.com/token",
        ["client_id"] = "c",
        ["client_secret"] = "s3cret",
    };

    private static ConnectorConfig Config(string baseUrl, Dictionary<string, object?>? auth, string? baseDir = null)
    {
        var values = new Dictionary<string, object?> { ["base_url"] = baseUrl };
        if (auth is not null)
        {
            values["auth"] = auth;
        }

        if (baseDir is not null)
        {
            values["base_dir"] = baseDir;
        }

        return new ConnectorConfig(values);
    }

    [Fact]
    public async Task Validate_accepts_each_new_auth_type()
    {
        var managed = new Dictionary<string, object?>
        {
            ["type"] = "azure_ad", ["method"] = "managed_identity", ["scope"] = "api://x/.default",
        };
        var chain = new Dictionary<string, object?>
        {
            ["type"] = "azure_ad", ["method"] = "credential_chain", ["scope"] = "api://x/.default",
        };
        foreach (var auth in new[] { AzureAd("service_principal"), managed, chain, OAuth2() })
        {
            var result = await new HttpConnector().ValidateAsync(
                Config("https://api.example.com", auth), CancellationToken.None);
            Assert.True(result.IsValid, string.Join("; ", result.Errors));
        }
    }

    [Fact]
    public async Task Validate_aggregates_auth_errors_with_connection_errors()
    {
        var bad = new Dictionary<string, object?> { ["type"] = "azure_ad", ["method"] = "managed_identity" };
        var noUrl = new Dictionary<string, object?>
        {
            ["type"] = "oauth2_client_credentials", ["client_id"] = "c", ["client_secret"] = "s",
        };
        // A bad base_url and the auth problem must surface in the same pass, one error each.
        foreach (var (auth, field) in new[] { (bad, "auth.scope"), (noUrl, "auth.token_url") })
        {
            var result = await new HttpConnector().ValidateAsync(
                Config("not a url", auth), CancellationToken.None);
            Assert.Equal(2, result.Errors.Count);
            Assert.Contains(result.Errors, e => e.Contains("base_url", StringComparison.Ordinal));
            Assert.Contains(result.Errors, e => e.Contains(field, StringComparison.Ordinal));
        }

        // Two independent auth problems in one block are both reported.
        var both = new Dictionary<string, object?> { ["type"] = "azure_ad", ["method"] = "nope" };
        var multi = await new HttpConnector().ValidateAsync(
            Config("https://api.example.com", both), CancellationToken.None);
        Assert.Equal(2, multi.Errors.Count);
    }

    [Fact]
    public void Parse_keeps_oauth_types_unbound()
    {
        var managed = new Dictionary<string, object?>
        {
            ["type"] = "azure_ad", ["method"] = "managed_identity", ["scope"] = "api://x/.default",
        };
        foreach (var auth in new[] { managed, OAuth2() })
        {
            var errors = new List<string>();
            var parsed = HttpConnectionConfig.Parse(Config("https://api.example.com", auth), errors);
            Assert.Empty(errors);
            Assert.NotNull(parsed);
            Assert.Null(parsed.Authenticator);
            Assert.NotNull(parsed.AuthFactory);
            Assert.True(parsed.HasCredentials);
        }
    }

    [Fact]
    public void Bind_produces_the_live_authenticator()
    {
        var errors = new List<string>();
        var parsed = HttpConnectionConfig.Parse(Config("https://api.example.com", OAuth2()), errors)!;
        using var bound = (OAuth2ClientCredentialsAuthenticator)parsed.Bind(TimeProvider.System).Authenticator!;
        Assert.NotNull(bound);
    }

    [Fact]
    public void Unknown_auth_type_lists_all_five()
    {
        var errors = new List<string>();
        HttpConnectionConfig.Parse(Config("https://api.example.com",
            new Dictionary<string, object?> { ["type"] = "kerberos" }), errors);
        var error = Assert.Single(errors);
        foreach (var name in new[] { "api_key", "bearer", "basic", "azure_ad", "oauth2_client_credentials" })
        {
            Assert.Contains(name, error, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Static_types_still_parse_to_an_authenticator()
    {
        var errors = new List<string>();
        var parsed = HttpConnectionConfig.Parse(Config("https://api.example.com",
            new Dictionary<string, object?> { ["type"] = "bearer", ["token"] = "t" }), errors)!;
        Assert.Empty(errors);
        Assert.NotNull(parsed.Authenticator);
        Assert.Null(parsed.AuthFactory);
        Assert.True(parsed.HasCredentials);
    }

    [Fact]
    public void Cleartext_warning_fires_for_oauth_types()
    {
        var errors = new List<string>();
        var parsed = HttpConnectionConfig.Parse(Config("http://api.example.com", OAuth2()), errors)!;
        Assert.Single(parsed.Warnings());
    }

    [Theory]
    [InlineData("https://graph.microsoft.com/.default", false)]
    [InlineData("api://my-api/.DEFAULT", false)]
    [InlineData("api://my-api/user_impersonation", true)]
    [InlineData("https://graph.microsoft.com", true)]
    public void Azure_scope_not_ending_in_dot_default_warns(string scope, bool warns)
    {
        var auth = AzureAd("service_principal");
        auth["scope"] = scope;
        var errors = new List<string>();
        var parsed = HttpConnectionConfig.Parse(Config("https://api.example.com", auth), errors)!;
        Assert.Empty(errors);
        if (warns)
        {
            var warning = Assert.Single(parsed.Warnings());
            Assert.Contains($"'{scope}'", warning, StringComparison.Ordinal);
            Assert.Contains("/.default", warning, StringComparison.Ordinal);
        }
        else
        {
            Assert.Empty(parsed.Warnings());
        }
    }

    [Fact]
    public void Certificate_inside_the_project_warns()
    {
        var auth = AzureAd("service_principal");
        auth.Remove("client_secret");
        auth["certificate_path"] = "certs/app.pem";
        var errors = new List<string>();
        var parsed = HttpConnectionConfig.Parse(Config("https://api.example.com", auth, "/proj"), errors)!;
        Assert.Empty(errors);
        var warning = Assert.Single(parsed.Warnings());
        Assert.Contains("certificate_path", warning, StringComparison.Ordinal);
        Assert.Contains("outside the project", warning, StringComparison.Ordinal);
    }

    [Fact]
    public void Certificate_outside_the_project_does_not_warn()
    {
        var auth = AzureAd("service_principal");
        auth.Remove("client_secret");
        auth["certificate_path"] = "/etc/pz/app.pem";
        var errors = new List<string>();
        var parsed = HttpConnectionConfig.Parse(Config("https://api.example.com", auth, "/proj"), errors)!;
        Assert.Empty(errors);
        Assert.Empty(parsed.Warnings());
    }

    [Fact]
    public void Schema_accepts_both_new_types_and_marks_secrets_writeOnly()
    {
        var text = new HttpConnector().ConnectionConfigSchema;
        var schema = JsonSchema.FromText(text);

        var azure = JsonSerializer.Deserialize<JsonElement>("""
            {"base_url":"https://api.example.com","auth":{"type":"azure_ad","method":"service_principal",
             "tenant_id":"t","client_id":"c","client_secret":"s","certificate_path":"a.pem",
             "certificate_password":"p","scope":"x/.default","authority_host":"https://login.example.com"}}
            """);
        Assert.True(schema.Evaluate(azure).IsValid);

        var oauth = JsonSerializer.Deserialize<JsonElement>("""
            {"base_url":"https://api.example.com","auth":{"type":"oauth2_client_credentials",
             "token_url":"https://login.example.com/token","client_id":"c","client_secret":"s",
             "scope":"x","client_auth":"basic","params":{"resource":"r","n":1,"b":true}}}
            """);
        Assert.True(schema.Evaluate(oauth).IsValid);

        using var doc = JsonDocument.Parse(text);
        var props = doc.RootElement.GetProperty("properties").GetProperty("auth").GetProperty("properties");
        Assert.True(props.GetProperty("client_secret").GetProperty("writeOnly").GetBoolean());
        Assert.True(props.GetProperty("certificate_password").GetProperty("writeOnly").GetBoolean());
    }

    [Fact]
    public void Schema_rejects_an_unknown_auth_key()
    {
        var schema = JsonSchema.FromText(new HttpConnector().ConnectionConfigSchema);
        var bad = JsonSerializer.Deserialize<JsonElement>("""
            {"base_url":"https://api.example.com","auth":{"type":"oauth2_client_credentials",
             "tokn_url":"https://login.example.com/token"}}
            """);
        Assert.False(schema.Evaluate(bad).IsValid);
    }
}
