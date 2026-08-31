using Pz.Connectors.Abstractions;

namespace Pz.Connector.Gcs.Tests;

/// <summary>Offline proof of the gcs auth matrix: `auth` selects the method, each method's required
/// fields are checked offline and aggregate (the AzureAuth shape), and the method decides which data
/// plane exists — hmac maps to the DuckDB secret (native tier), service_account/adc map to a
/// Google SDK client (universal write tier). Client construction never touches the network here:
/// only the offline failure shapes are proven.</summary>
public sealed class GcsAuthTests
{
    private static ConnectorConfig Config(params (string Key, object? Value)[] values) =>
        new(values.ToDictionary(v => v.Key, v => v.Value));

    [Fact]
    public void Missing_auth_names_the_field_and_the_methods()
    {
        var result = GcsAuth.Validate(Config());
        var error = Assert.Single(result.Errors);
        Assert.Contains("'auth'", error, StringComparison.Ordinal);
        Assert.Contains("hmac", error, StringComparison.Ordinal);
        Assert.Contains("service_account", error, StringComparison.Ordinal);
        Assert.Contains("adc", error, StringComparison.Ordinal);
    }

    [Fact]
    public void Unknown_auth_is_rejected_with_the_method_list()
    {
        var result = GcsAuth.Validate(Config(("auth", "oauth")));
        var error = Assert.Single(result.Errors);
        Assert.Contains("'oauth'", error, StringComparison.Ordinal);
    }

    [Fact]
    public void Hmac_requires_key_id_and_secret_aggregated()
    {
        var result = GcsAuth.Validate(Config(("auth", "hmac")));
        Assert.Equal(2, result.Errors.Count);
        Assert.Contains(result.Errors, e => e.Contains("'key_id'", StringComparison.Ordinal));
        Assert.Contains(result.Errors, e => e.Contains("'secret'", StringComparison.Ordinal));
    }

    [Fact]
    public void Valid_hmac_passes()
    {
        Assert.True(GcsAuth.Validate(Config(("auth", "hmac"), ("key_id", "k"), ("secret", "s"))).IsValid);
    }

    [Fact]
    public void Service_account_requires_exactly_one_of_key_file_or_key_json()
    {
        var neither = GcsAuth.Validate(Config(("auth", "service_account")));
        var neitherError = Assert.Single(neither.Errors);
        Assert.Contains("'key_file'", neitherError, StringComparison.Ordinal);
        Assert.Contains("'key_json'", neitherError, StringComparison.Ordinal);

        var both = GcsAuth.Validate(Config(("auth", "service_account"), ("key_file", "f"), ("key_json", "{}")));
        var bothError = Assert.Single(both.Errors);
        Assert.Contains("not both", bothError, StringComparison.Ordinal);
    }

    [Fact]
    public void Adc_needs_no_further_fields()
    {
        Assert.True(GcsAuth.Validate(Config(("auth", "adc"))).IsValid);
    }

    [Fact]
    public void IsHmac_reflects_the_selected_method()
    {
        Assert.True(GcsAuth.IsHmac(Config(("auth", "hmac"), ("key_id", "k"), ("secret", "s"))));
        Assert.False(GcsAuth.IsHmac(Config(("auth", "adc"))));
    }

    [Fact]
    public void Creating_an_sdk_client_for_hmac_is_a_named_permanent_refusal()
    {
        // The SDK authenticates with OAuth only; hmac exists solely for the native DuckDB tier.
        var ex = Assert.Throws<PzConnectorException>(() =>
            GcsAuth.CreateStorageClient(Config(("auth", "hmac"), ("key_id", "k"), ("secret", "s"))));
        Assert.False(ex.IsTransient);
        Assert.Contains("hmac", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Malformed_key_json_is_a_named_permanent_error()
    {
        var ex = Assert.Throws<PzConnectorException>(() =>
            GcsAuth.CreateStorageClient(Config(("auth", "service_account"), ("key_json", "not json"))));
        Assert.False(ex.IsTransient);
        Assert.Contains("'key_json'", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Missing_key_file_is_a_named_permanent_error()
    {
        var ex = Assert.Throws<PzConnectorException>(() => GcsAuth.CreateStorageClient(
            Config(("auth", "service_account"), ("key_file", "/nonexistent/pz-test-key.json"))));
        Assert.False(ex.IsTransient);
        Assert.Contains("'key_file'", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_well_formed_service_account_key_builds_a_client_offline()
    {
        // A syntactically valid (fake) service-account key: client construction is offline; only a
        // real call would mint a token. Proves the config->client mapping without any network.
        var client = GcsAuth.CreateStorageClient(Config(
            ("auth", "service_account"), ("key_json", FakeServiceAccountJson)));
        Assert.NotNull(client);
    }

    /// <summary>A structurally valid service-account key with a freshly generated throwaway RSA key —
    /// never a real credential.</summary>
    private static string FakeServiceAccountJson
    {
        get
        {
            using var rsa = System.Security.Cryptography.RSA.Create(2048);
            var pem = rsa.ExportPkcs8PrivateKeyPem().Replace("\n", "\\n");
            return $$"""
                {
                  "type": "service_account",
                  "project_id": "pz-test",
                  "private_key_id": "0000000000000000000000000000000000000000",
                  "private_key": "{{pem}}",
                  "client_email": "pz-test@pz-test.iam.gserviceaccount.com",
                  "client_id": "000000000000000000000",
                  "token_uri": "https://oauth2.googleapis.com/token"
                }
                """;
        }
    }
}
