using Pz.Connectors.Abstractions;

namespace Pz.Connector.DuckLake.Tests;

public sealed class DuckLakeCatalogTests
{
    private static ConnectorConfig Config(params (string Key, object? Value)[] values) =>
        new(values.ToDictionary(v => v.Key, v => v.Value));

    [Fact]
    public void Catalog_defaults_to_duckdb()
    {
        Assert.Equal("duckdb", DuckLakeCatalog.Of(Config(("path", "c.ducklake"))));
        Assert.Equal("postgres", DuckLakeCatalog.Of(Config(("catalog", "postgres"))));
    }

    [Fact]
    public void Duckdb_catalog_needs_only_path()
    {
        Assert.Empty(DuckLakeCatalog.Validate(Config(("path", "c.ducklake"))));
        Assert.Empty(DuckLakeCatalog.Validate(Config(("path", "c.ducklake"), ("data_path", "lake/"))));
    }

    [Fact]
    public void Sqlite_catalog_needs_path_and_data_path()
    {
        var errors = DuckLakeCatalog.Validate(Config(("catalog", "sqlite")));
        Assert.Contains(errors, e => e.Contains("'path'", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'data_path'", StringComparison.Ordinal));
        Assert.Equal(2, errors.Count);
    }

    [Fact]
    public void Postgres_catalog_needs_host_database_and_data_path()
    {
        var errors = DuckLakeCatalog.Validate(Config(("catalog", "postgres"), ("user", "pz")));
        Assert.Contains(errors, e => e.Contains("'host'", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'database'", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'data_path'", StringComparison.Ordinal));
        Assert.Equal(3, errors.Count);

        Assert.Empty(DuckLakeCatalog.Validate(Config(("catalog", "postgres"), ("host", "h"), ("database", "d"),
            ("data_path", "s3://b/"), ("port", 5433L), ("user", "u"), ("password", "p"))));
    }

    [Fact]
    public void Quack_catalog_needs_a_quack_uri_token_and_data_path()
    {
        var errors = DuckLakeCatalog.Validate(Config(("catalog", "quack"), ("uri", "http://x"), ("token", "t")));
        Assert.Contains(errors, e => e.Contains("'uri'", StringComparison.Ordinal) && e.Contains("quack:", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'data_path'", StringComparison.Ordinal));

        Assert.Empty(DuckLakeCatalog.Validate(Config(("catalog", "quack"), ("uri", "quack:lake.internal:9494"),
            ("token", "secret"), ("data_path", "s3://b/"))));
    }

    [Fact]
    public void Motherduck_catalog_needs_database_token_and_data_path()
    {
        var errors = DuckLakeCatalog.Validate(Config(("catalog", "motherduck")));
        Assert.Equal(3, errors.Count);
        Assert.Empty(DuckLakeCatalog.Validate(Config(("catalog", "motherduck"), ("database", "d"), ("token", "t"), ("data_path", "s3://b/"))));
    }

    [Fact]
    public void Keys_belonging_to_another_catalog_are_each_one_error()
    {
        var errors = DuckLakeCatalog.Validate(Config(("path", "c.ducklake"), ("host", "h"), ("token", "t")));
        Assert.Equal(2, errors.Count);
        Assert.Contains(errors, e => e.Contains("'host'", StringComparison.Ordinal) && e.Contains("postgres", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'token'", StringComparison.Ordinal));
    }

    [Fact]
    public void Storage_keys_come_as_a_pair_and_the_rest_require_the_pair()
    {
        var half = DuckLakeCatalog.Validate(Config(("path", "c.ducklake"), ("storage_key_id", "k")));
        Assert.Single(half, e => e.Contains("storage_secret_key", StringComparison.Ordinal));

        var orphan = DuckLakeCatalog.Validate(Config(("path", "c.ducklake"), ("storage_region", "eu-west-1")));
        Assert.Single(orphan, e => e.Contains("storage_region", StringComparison.Ordinal));

        Assert.Empty(DuckLakeCatalog.Validate(Config(("path", "c.ducklake"), ("data_path", "s3://b/"),
            ("storage_key_id", "k"), ("storage_secret_key", "s"), ("storage_region", "eu-west-1"),
            ("storage_endpoint", "minio:9000"), ("storage_url_style", "path"), ("storage_use_ssl", false))));
    }

    [Fact]
    public void Storage_credentials_require_an_object_store_data_path()
    {
        var missing = DuckLakeCatalog.Validate(Config(("path", "c.ducklake"), ("storage_key_id", "k"), ("storage_secret_key", "s")));
        var missingError = Assert.Single(missing);
        Assert.Contains("data_path", missingError, StringComparison.Ordinal);

        var local = DuckLakeCatalog.Validate(Config(("path", "c.ducklake"), ("data_path", "lake/data"),
            ("storage_key_id", "k"), ("storage_secret_key", "s")));
        var localError = Assert.Single(local);
        Assert.Contains("data_path", localError, StringComparison.Ordinal);

        Assert.Empty(DuckLakeCatalog.Validate(Config(("path", "c.ducklake"), ("data_path", "s3://b/"),
            ("storage_key_id", "k"), ("storage_secret_key", "s"))));
    }

    [Theory]
    [InlineData("quack:localhost", "localhost", 9494)]
    [InlineData("quack:lake.internal:18443", "lake.internal", 18443)]
    [InlineData("quack://lake.internal:18443", "lake.internal", 18443)]
    public void Quack_uris_parse_host_and_port(string uri, string host, int port)
    {
        Assert.True(DuckLakeCatalog.TryParseQuackUri(uri, out var h, out var p));
        Assert.Equal(host, h);
        Assert.Equal(port, p);
    }

    [Theory]
    [InlineData("http://x")]
    [InlineData("quack:")]
    [InlineData("quack:host:notaport")]
    public void Non_quack_uris_do_not_parse(string uri)
    {
        Assert.False(DuckLakeCatalog.TryParseQuackUri(uri, out _, out _));
    }
}
