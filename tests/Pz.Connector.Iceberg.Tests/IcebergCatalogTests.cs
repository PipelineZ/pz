using Pz.Connectors.Abstractions;

namespace Pz.Connector.Iceberg.Tests;

/// <summary>The offline, aggregate catalog key matrix: every stray or missing key is one error
/// naming the catalog it belongs to.</summary>
public sealed class IcebergCatalogTests
{
    private static ConnectorConfig Config(params (string Key, object? Value)[] values) =>
        new(values.ToDictionary(v => v.Key, v => v.Value));

    [Fact]
    public void Rest_is_the_default_and_requires_an_http_endpoint()
    {
        Assert.Equal("rest", IcebergCatalog.Of(Config()));
        Assert.Equal(["catalog 'rest' requires 'endpoint'"], IcebergCatalog.Validate(Config()));
        Assert.Equal(["'endpoint' must be an http:// or https:// URL"],
            IcebergCatalog.Validate(Config(("endpoint", "cat.example.com"))));
        Assert.Empty(IcebergCatalog.Validate(Config(("endpoint", "https://cat.example.com/api"), ("warehouse", "wh"))));
    }

    [Fact]
    public void Rest_auth_is_a_token_or_a_client_pair_never_both()
    {
        var both = IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("token", "t"), ("client_id", "i"), ("client_secret", "s")));
        Assert.Equal(["declare either 'token' or 'client_id'/'client_secret', not both"], both);

        var half = IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("client_id", "i")));
        Assert.Equal(["'client_id' and 'client_secret' must be declared together"], half);

        var tuning = IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("token", "t"), ("oauth2_scope", "x")));
        Assert.Equal(["'oauth2_scope' requires 'client_id' and 'client_secret'"], tuning);

        Assert.Empty(IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("client_id", "i"), ("client_secret", "s"),
            ("oauth2_server_uri", "http://a"), ("oauth2_scope", "x"))));
    }

    [Fact]
    public void Rest_forbids_a_files_root()
    {
        Assert.Equal(["'root' belongs to catalog 'files' and is not valid for catalog 'rest'"],
            IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("root", "/lake"))));
    }

    [Fact]
    public void Aws_catalogs_forbid_rest_keys_and_s3_tables_needs_its_bucket_arn()
    {
        var glue = IcebergCatalog.Validate(Config(("catalog", "glue"), ("endpoint", "http://c"), ("token", "t")));
        Assert.Equal(
            [
                "'endpoint' belongs to catalog 'rest' and is not valid for catalog 'glue'",
                "'token' belongs to catalog 'rest' and is not valid for catalog 'glue'",
            ],
            glue);
        Assert.Empty(IcebergCatalog.Validate(Config(("catalog", "glue"))));
        Assert.Empty(IcebergCatalog.Validate(Config(("catalog", "glue"), ("warehouse", "123456789012"), ("storage_region", "eu-west-1"))));

        Assert.Equal(["catalog 's3_tables' requires 'warehouse'"], IcebergCatalog.Validate(Config(("catalog", "s3_tables"))));
        Assert.Empty(IcebergCatalog.Validate(Config(("catalog", "s3_tables"), ("warehouse", "arn:aws:s3tables:x"))));
    }

    [Fact]
    public void A_url_shaped_warehouse_is_refused_for_every_catalog_that_attaches_one()
    {
        // DuckDB attaches a URL-shaped warehouse read-only, silently -- refused up front rather than
        // discovered later as a write that silently no-ops.
        Assert.Equal(
            ["'warehouse' looks like a URL; DuckDB attaches a URL-shaped warehouse read-only -- give the catalog's warehouse NAME instead"],
            IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("warehouse", "s3://bucket/wh/"))));
        Assert.Empty(IcebergCatalog.Validate(Config(("catalog", "glue"), ("warehouse", "123456789012"))));

        var glueUrl = IcebergCatalog.Validate(Config(("catalog", "glue"), ("warehouse", "https://glue.example.com/catalog")));
        Assert.Contains("looks like a URL", string.Join(' ', glueUrl));

        var s3TablesUrl = IcebergCatalog.Validate(Config(("catalog", "s3_tables"), ("warehouse", "s3://bucket/table-bucket")));
        Assert.Contains("looks like a URL", string.Join(' ', s3TablesUrl));
    }

    [Fact]
    public void Files_requires_a_root_and_forbids_catalog_keys()
    {
        Assert.Equal(["catalog 'files' requires 'root'"], IcebergCatalog.Validate(Config(("catalog", "files"))));
        var stray = IcebergCatalog.Validate(Config(("catalog", "files"), ("root", "/lake"), ("endpoint", "http://c"), ("nested_namespaces", true)));
        Assert.Equal(
            [
                "'endpoint' belongs to catalog 'rest' and is not valid for catalog 'files'",
                "'nested_namespaces' belongs to catalog 'rest' and is not valid for catalog 'files'",
            ],
            stray);
        Assert.Empty(IcebergCatalog.Validate(Config(("catalog", "files"), ("root", "s3://b/wh/"))));
    }

    [Fact]
    public void Unknown_catalog_is_one_error()
    {
        Assert.Equal(["unknown catalog 'hive' (expected rest, glue, s3_tables or files)"],
            IcebergCatalog.Validate(Config(("catalog", "hive"))));
    }

    [Fact]
    public void Storage_keys_are_declared_together_and_tuning_keys_need_them()
    {
        Assert.Equal(["'storage_key_id' and 'storage_secret_key' must be declared together"],
            IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("storage_key_id", "AK"))));
        Assert.Equal(["'storage_endpoint' requires 'storage_key_id' and 'storage_secret_key'"],
            IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("storage_endpoint", "minio:9000"))));

        // region is not a credential: it is meaningful on its own (the credential chain's region).
        Assert.Empty(IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("storage_region", "eu-west-1"))));
    }

    [Fact]
    public void Files_storage_keys_require_an_object_store_root()
    {
        Assert.Equal(["'storage_key_id' and 'storage_secret_key' require an object-store 'root' (a URL such as s3://bucket/prefix/)"],
            IcebergCatalog.Validate(Config(("catalog", "files"), ("root", "/lake"), ("storage_key_id", "AK"), ("storage_secret_key", "SK"))));
        Assert.Empty(IcebergCatalog.Validate(Config(("catalog", "files"), ("root", "s3://b/"), ("storage_key_id", "AK"), ("storage_secret_key", "SK"))));
    }

    [Fact]
    public void Storage_defaults_to_s3_and_is_inferred_from_an_azure_files_root()
    {
        Assert.Equal("s3", IcebergCatalog.StorageOf(Config(("endpoint", "http://c"))));
        Assert.Equal("s3", IcebergCatalog.StorageOf(Config(("catalog", "files"), ("root", "s3://b/"))));
        Assert.Equal("azure", IcebergCatalog.StorageOf(Config(("catalog", "files"), ("root", "az://c/wh/"))));
        Assert.Equal("azure", IcebergCatalog.StorageOf(Config(("catalog", "files"), ("root", "abfss://c@acct.dfs.core.windows.net/wh/"))));
        Assert.Equal("azure", IcebergCatalog.StorageOf(Config(("endpoint", "http://c"), ("storage", "azure"))));
        Assert.True(IcebergCatalog.IsAzureUrl("AZURE://c/x"));
        Assert.False(IcebergCatalog.IsAzureUrl("s3://c/x"));
        Assert.False(IcebergCatalog.IsAzureUrl("/lake"));
    }

    [Fact]
    public void Storage_must_be_s3_or_azure()
    {
        Assert.Equal(["'storage' must be 's3' or 'azure' (got 'gcs')"],
            IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("storage", "gcs"))));
    }

    [Fact]
    public void Azure_keys_are_refused_under_s3_storage_and_s3_keys_under_azure()
    {
        Assert.Equal(["'storage_auth' belongs to storage 'azure' and is not valid for storage 's3'",
                      "'storage_account_name' belongs to storage 'azure' and is not valid for storage 's3'"],
            IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("storage_auth", "credential_chain"), ("storage_account_name", "acct"))));

        Assert.Equal(["'storage_region' belongs to storage 's3' and is not valid for storage 'azure'",
                      "'storage_use_ssl' belongs to storage 's3' and is not valid for storage 'azure'"],
            IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("storage", "azure"), ("storage_region", "x"), ("storage_use_ssl", false))));
    }

    [Fact]
    public void Aws_catalogs_refuse_azure_storage()
    {
        Assert.Equal(["storage 'azure' is not valid for catalog 'glue' (an AWS catalog stores its tables on S3)"],
            IcebergCatalog.Validate(Config(("catalog", "glue"), ("storage", "azure"))));
        Assert.Equal(["storage 'azure' is not valid for catalog 's3_tables' (an AWS catalog stores its tables on S3)"],
            IcebergCatalog.Validate(Config(("catalog", "s3_tables"), ("warehouse", "arn:x"), ("storage", "azure"))));
    }

    [Fact]
    public void Azure_auth_methods_require_their_fields()
    {
        Assert.Equal(["'storage_auth' must be one of connection_string, account_key, service_principal, credential_chain (got 'sas')"],
            IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("storage", "azure"), ("storage_auth", "sas"))));

        Assert.Equal(["storage_auth 'connection_string' requires 'storage_connection_string'"],
            IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("storage", "azure"), ("storage_auth", "connection_string"))));
        Assert.Equal(["storage_auth 'account_key' requires 'storage_account_name'", "storage_auth 'account_key' requires 'storage_account_key'"],
            IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("storage", "azure"), ("storage_auth", "account_key"))));
        Assert.Equal(["storage_auth 'service_principal' requires 'storage_tenant_id'", "storage_auth 'service_principal' requires 'storage_client_id'",
                      "storage_auth 'service_principal' requires 'storage_client_secret'", "storage_auth 'service_principal' requires 'storage_account_name'"],
            IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("storage", "azure"), ("storage_auth", "service_principal"))));
        Assert.Equal(["storage_auth 'credential_chain' requires 'storage_account_name'"],
            IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("storage", "azure"), ("storage_auth", "credential_chain"))));

        Assert.Empty(IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("storage", "azure"), ("storage_auth", "credential_chain"),
            ("storage_account_name", "acct"), ("storage_chain", "cli;env"))));
        Assert.Empty(IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("storage", "azure"), ("storage_auth", "account_key"),
            ("storage_account_name", "acct"), ("storage_account_key", "k"), ("storage_endpoint", "http://127.0.0.1:10000/acct"))));
    }

    [Fact]
    public void Azure_fields_need_storage_auth_and_tuning_keys_belong_to_one_method()
    {
        Assert.Equal(["'storage_account_name' requires 'storage_auth'"],
            IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("storage", "azure"), ("storage_account_name", "acct"))));
        Assert.Equal(["'storage_chain' applies to storage_auth 'credential_chain' only"],
            IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("storage", "azure"), ("storage_auth", "connection_string"),
                ("storage_connection_string", "cs"), ("storage_chain", "cli"))));
        Assert.Equal(["'storage_endpoint' under storage 'azure' applies to storage_auth 'account_key' only"],
            IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("storage", "azure"), ("storage_auth", "connection_string"),
                ("storage_connection_string", "cs"), ("storage_endpoint", "http://x"))));
    }

    [Fact]
    public void Rest_without_storage_auth_under_azure_relies_on_vending_and_is_valid()
    {
        Assert.Empty(IcebergCatalog.Validate(Config(("endpoint", "http://c"), ("warehouse", "wh"), ("storage", "azure"))));
    }

    [Fact]
    public void Files_under_azure_needs_an_azure_root_and_an_auth_method()
    {
        Assert.Equal(["catalog 'files' with storage 'azure' requires 'storage_auth' (nothing vends credentials for a bare root)"],
            IcebergCatalog.Validate(Config(("catalog", "files"), ("root", "az://c/wh/"))));
        Assert.Equal(["'root' is not an Azure URL (az://, azure://, abfss://) but storage is 'azure'",
                      "catalog 'files' with storage 'azure' requires 'storage_auth' (nothing vends credentials for a bare root)"],
            IcebergCatalog.Validate(Config(("catalog", "files"), ("root", "s3://b/"), ("storage", "azure"))));
        Assert.Equal(["'root' is not an Azure URL (az://, azure://, abfss://) but storage is 'azure'"],
            IcebergCatalog.Validate(Config(("catalog", "files"), ("root", "/lake"), ("storage", "azure"),
                ("storage_auth", "connection_string"), ("storage_connection_string", "cs"))));
        Assert.Equal(["'root' is an Azure URL; declare storage 'azure' (or omit 'storage' to infer it)"],
            IcebergCatalog.Validate(Config(("catalog", "files"), ("root", "az://c/wh/"), ("storage", "s3"))));
        Assert.Empty(IcebergCatalog.Validate(Config(("catalog", "files"), ("root", "az://c/wh/"),
            ("storage_auth", "connection_string"), ("storage_connection_string", "cs"))));
    }

    [Fact]
    public void Storage_credentials_flag_covers_both_families()
    {
        Assert.True(IcebergCatalog.HasStorageCredentials(Config(("storage_key_id", "AK"))));
        Assert.True(IcebergCatalog.HasStorageCredentials(Config(("storage", "azure"), ("storage_auth", "credential_chain"))));
        Assert.False(IcebergCatalog.HasStorageCredentials(Config(("storage", "azure"))));
        Assert.False(IcebergCatalog.HasStorageCredentials(Config()));
    }

    [Fact]
    public void Connection_schema_accepts_every_azure_key()
    {
        var schema = Json.Schema.JsonSchema.FromText(new IcebergConnector().ConnectionConfigSchema);
        var doc = System.Text.Json.JsonDocument.Parse("""
            { "catalog": "rest", "endpoint": "http://c", "storage": "azure", "storage_auth": "service_principal",
              "storage_tenant_id": "t", "storage_client_id": "c", "storage_client_secret": "s", "storage_account_name": "a",
              "storage_connection_string": "x", "storage_account_key": "k", "storage_chain": "cli" }
            """);
        Assert.True(schema.Evaluate(doc.RootElement).IsValid);
        var bad = System.Text.Json.JsonDocument.Parse("""{ "storage": "gcs" }""");
        Assert.False(schema.Evaluate(bad.RootElement).IsValid);
    }
}
