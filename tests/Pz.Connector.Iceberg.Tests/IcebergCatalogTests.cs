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
            ["'warehouse' ('s3://bucket/wh/') looks like a URL; DuckDB attaches a URL-shaped warehouse read-only -- give the catalog's warehouse NAME instead"],
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
}
