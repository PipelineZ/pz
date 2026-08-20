using Pz.Connectors.Abstractions;

namespace Pz.Connector.AzureBlob.Tests;

public sealed class AzureAuthTests
{
    private static ConnectorConfig Cfg(params (string k, object? v)[] pairs) =>
        new(new Dictionary<string, object?>(pairs.Select(p => new KeyValuePair<string, object?>(p.k, p.v))));

    [Fact]
    public void Connection_string_secret_uses_connection_string_param()
    {
        var sql = AzureAuth.CreateSecretSql(
            Cfg(("auth", "connection_string"), ("connection_string", "DefaultEndpointsProtocol=https;AccountName=x")), "pz_azure_s");
        Assert.StartsWith("create or replace secret pz_azure_s (type azure", sql, StringComparison.Ordinal);
        Assert.Contains("connection_string 'DefaultEndpointsProtocol=https;AccountName=x'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Account_key_assembles_connection_string()
    {
        var sql = AzureAuth.CreateSecretSql(Cfg(("auth", "account_key"), ("account_name", "acct"), ("account_key", "KEY==")), "pz_azure_s");
        Assert.Contains("AccountName=acct", sql, StringComparison.Ordinal);
        Assert.Contains("AccountKey=KEY==", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Service_principal_secret_shape()
    {
        var sql = AzureAuth.CreateSecretSql(
            Cfg(("auth", "service_principal"), ("tenant_id", "t"), ("client_id", "c"), ("client_secret", "sec"), ("account_name", "acct")), "pz_azure_s");
        Assert.Contains("provider service_principal", sql, StringComparison.Ordinal);
        Assert.Contains("tenant_id 't'", sql, StringComparison.Ordinal);
        Assert.Contains("client_id 'c'", sql, StringComparison.Ordinal);
        Assert.Contains("client_secret 'sec'", sql, StringComparison.Ordinal);
        Assert.Contains("account_name 'acct'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Credential_chain_with_optional_chain()
    {
        var sql = AzureAuth.CreateSecretSql(Cfg(("auth", "credential_chain"), ("account_name", "acct"), ("chain", "cli;env")), "pz_azure_s");
        Assert.Contains("provider credential_chain", sql, StringComparison.Ordinal);
        Assert.Contains("chain 'cli;env'", sql, StringComparison.Ordinal);
        Assert.Contains("account_name 'acct'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Managed_identity_secret_shape()
    {
        var sql = AzureAuth.CreateSecretSql(Cfg(("auth", "managed_identity"), ("account_name", "acct")), "pz_azure_s");
        Assert.Contains("provider managed_identity", sql, StringComparison.Ordinal);
        Assert.Contains("account_name 'acct'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Escapes_single_quotes_in_secret_literals()
    {
        var sql = AzureAuth.CreateSecretSql(Cfg(("auth", "service_principal"), ("tenant_id", "t"), ("client_id", "c"), ("client_secret", "a'b"), ("account_name", "acct")), "pz_azure_s");
        Assert.Contains("client_secret 'a''b'", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Validate_service_principal_missing_fields_aggregates()
    {
        var result = AzureAuth.Validate(Cfg(("auth", "service_principal"), ("tenant_id", "t")));
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("client_id", StringComparison.Ordinal));
        Assert.Contains(result.Errors, e => e.Contains("client_secret", StringComparison.Ordinal));
        Assert.Contains(result.Errors, e => e.Contains("account_name", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_unknown_auth_is_error()
    {
        var result = AzureAuth.Validate(Cfg(("auth", "sas")));
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("auth", StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_missing_auth_discriminator_fails()
    {
        var result = AzureAuth.Validate(Cfg());
        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("auth", StringComparison.Ordinal));
    }

    // The account_key arm must build a DataLake client from a StorageSharedKeyCredential plus the dfs
    // service URI. Reusing the assembled blob connection string bakes in BlobEndpoint=<endpoint> and
    // points the DataLake client at the wrong (blob) host.
    [Fact]
    public void DataLake_account_key_uses_default_dfs_endpoint()
    {
        var client = AzureAuth.CreateDataLakeFileSystemClient(
            Cfg(("auth", "account_key"), ("account_name", "acct"), ("account_key", "S2V5")), "fs");
        Assert.Equal("acct.dfs.core.windows.net", client.Uri.Host);
    }

    [Fact]
    public void DataLake_account_key_honors_custom_endpoint_as_the_dfs_endpoint()
    {
        var client = AzureAuth.CreateDataLakeFileSystemClient(
            Cfg(("auth", "account_key"), ("account_name", "acct"), ("account_key", "S2V5"), ("endpoint", "http://127.0.0.1:10005/acct")),
            "fs");
        Assert.StartsWith("http://127.0.0.1:10005/acct/fs", client.Uri.ToString(), StringComparison.Ordinal);
    }

    // A bad config value is a named, non-transient PzConnectorException everywhere in this codebase --
    // never BlobServiceClient's raw FormatException leaking out of the connector.
    [Fact]
    public void CreateBlobContainerClient_with_malformed_connection_string_throws_named_exception()
    {
        const string malformed = "this-is-not-a-valid-connection-string-secret-XYZ123";
        var ex = Assert.Throws<PzConnectorException>(() => AzureAuth.CreateBlobContainerClient(
            Cfg(("auth", "connection_string"), ("connection_string", malformed)), "container"));

        Assert.False(ex.IsTransient);
        Assert.Contains("connection_string", ex.Message, StringComparison.Ordinal);
        Assert.Contains("malformed", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(malformed, ex.Message, StringComparison.Ordinal);
        Assert.IsType<FormatException>(ex.InnerException);
    }

    [Fact]
    public void CreateDataLakeFileSystemClient_with_malformed_connection_string_throws_named_exception()
    {
        const string malformed = "also-not-a-valid-connection-string-ABC789";
        var ex = Assert.Throws<PzConnectorException>(() => AzureAuth.CreateDataLakeFileSystemClient(
            Cfg(("auth", "connection_string"), ("connection_string", malformed)), "fs"));

        Assert.False(ex.IsTransient);
        Assert.Contains("connection_string", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(malformed, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateBlobContainerClient_with_malformed_endpoint_names_endpoint_field()
    {
        var ex = Assert.Throws<PzConnectorException>(() => AzureAuth.CreateBlobContainerClient(
            Cfg(("auth", "credential_chain"), ("account_name", "acct"), ("endpoint", "not a valid uri \t\n")), "container"));

        Assert.False(ex.IsTransient);
        Assert.Contains("endpoint", ex.Message, StringComparison.Ordinal);
    }

    // No `endpoint` was configured at all here -- the malformed value is `account_name`, which
    // ServiceUri interpolates into a service URI only when `endpoint` is absent. The error must
    // blame the field the user actually set, not the one they didn't.
    [Fact]
    public void CreateBlobContainerClient_with_malformed_account_name_and_no_endpoint_names_account_name_field()
    {
        var ex = Assert.Throws<PzConnectorException>(() => AzureAuth.CreateBlobContainerClient(
            Cfg(("auth", "credential_chain"), ("account_name", "not a valid account \t\n")), "container"));

        Assert.False(ex.IsTransient);
        Assert.Contains("account_name", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("'endpoint'", ex.Message, StringComparison.Ordinal);
    }
}
