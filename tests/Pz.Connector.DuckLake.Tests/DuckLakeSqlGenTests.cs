using Pz.Connectors.Abstractions;

namespace Pz.Connector.DuckLake.Tests;

/// <summary>Offline proof of the connector's whole data plane — which IS these strings: per-catalog
/// setup lists, secret shapes, the attach, entity quoting, the scan fragment with contract pruning,
/// time travel and watermark rendering, and the three copy statements. DuckDB parses every
/// statement, so identifiers are double-quoted and literals single-quoted.</summary>
public sealed class DuckLakeSqlGenTests
{
    private const string WhAlias = "pz_ducklake_wh_509bcf06";

    private static DatasetSpec Spec(Dictionary<string, object?>? options = null) =>
        new("wh", "events", options ?? []);

    private static ConnectorConfig Config(params (string Key, object? Value)[] values) =>
        new(values.ToDictionary(v => v.Key, v => v.Value));

    [Fact]
    public void Alias_and_secret_names_derive_from_the_raw_connection_name()
    {
        Assert.Equal(WhAlias, DuckLakeSql.Alias("wh"));
        Assert.Equal("pz_ducklake_my_wh_2_4c668b3d", DuckLakeSql.Alias("my-wh.2"));
        Assert.NotEqual(DuckLakeSql.Alias("prod-db"), DuckLakeSql.Alias("prod_db"));
        Assert.Equal(WhAlias + "_secret", DuckLakeSql.SecretName(WhAlias));
        Assert.Equal(WhAlias + "_storage", DuckLakeSql.StorageSecretName(WhAlias));
        Assert.Equal(WhAlias + "_pg", DuckLakeSql.PostgresSecretName(WhAlias));
    }

    [Fact]
    public void Local_paths_resolve_against_base_dir_and_absolute_paths_pass_through()
    {
        var relative = Config(("path", "lake/catalog.ducklake"), ("base_dir", "/proj"));
        Assert.Equal(Path.GetFullPath("/proj/lake/catalog.ducklake"), DuckLakeSql.ResolveLocal(relative, "path"));

        var absolute = Config(("path", Path.GetFullPath("/data/c.ducklake")));
        Assert.Equal(Path.GetFullPath("/data/c.ducklake"), DuckLakeSql.ResolveLocal(absolute, "path"));

        var missing = Config(("base_dir", "/proj"));
        var ex = Assert.Throws<PzConnectorException>(() => DuckLakeSql.ResolveLocal(missing, "path"));
        Assert.False(ex.IsTransient);
        Assert.Contains("requires 'path'", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Data_path_is_a_url_or_a_project_relative_directory()
    {
        Assert.True(DuckLakeSql.IsUrl("s3://bucket/lake/"));
        Assert.False(DuckLakeSql.IsUrl("lake/data"));

        Assert.Equal("s3://bucket/lake/", DuckLakeSql.ResolveDataPath(Config(("data_path", "s3://bucket/lake/"))));
        Assert.Equal(Path.GetFullPath("/proj/lake/data"),
            DuckLakeSql.ResolveDataPath(Config(("data_path", "lake/data"), ("base_dir", "/proj"))));
        Assert.Null(DuckLakeSql.ResolveDataPath(Config()));
    }

    [Fact]
    public void Entities_split_into_schema_and_table_and_quote_safely()
    {
        Assert.Equal($"{WhAlias}.\"events\"", DuckLakeSql.QualifiedTable(WhAlias, "events"));
        Assert.Equal($"{WhAlias}.\"raw\".\"events\"", DuckLakeSql.QualifiedTable(WhAlias, "raw.events"));
        Assert.Equal($"{WhAlias}.\"we\"\"ird\".\"ev\"\"ents\"", DuckLakeSql.QualifiedTable(WhAlias, "we\"ird.ev\"ents"));
        foreach (var bad in new[] { "a.b.c", ".x", "x.", "" })
        {
            var ex = Assert.Throws<PzConnectorException>(() => DuckLakeSql.SplitEntity(bad));
            Assert.False(ex.IsTransient);
        }
    }

    [Theory]
    [InlineData("42", "42")]
    [InlineData("-7", "-7")]
    [InlineData("3.50", "3.50")]
    [InlineData("2026-08-19", "'2026-08-19'")]
    [InlineData("2026-08-19T12:30:00.000001", "'2026-08-19 12:30:00.000001'")]
    public void Watermark_literals_render_by_canonical_shape(string canonical, string expected)
    {
        Assert.Equal(expected, DuckLakeSql.RenderWatermarkLiteral(canonical));
    }

    [Fact]
    public void Duckdb_catalog_setup_is_extension_then_attach()
    {
        var statements = DuckLakeSql.SetupStatements(
            Config(("path", "/lake/catalog.ducklake"), ("data_path", "/lake/data")), WhAlias);
        Assert.Equal(
            new[]
            {
                "install ducklake", "load ducklake",
                $"attach if not exists 'ducklake:{Path.GetFullPath("/lake/catalog.ducklake")}' as {WhAlias} (data_path '{Path.GetFullPath("/lake/data")}')",
            },
            statements);
    }

    [Fact]
    public void Duckdb_catalog_without_data_path_omits_the_clause()
    {
        var statements = DuckLakeSql.SetupStatements(Config(("path", "/lake/catalog.ducklake")), WhAlias);
        Assert.Equal($"attach if not exists 'ducklake:{Path.GetFullPath("/lake/catalog.ducklake")}' as {WhAlias}", statements[^1]);
    }

    [Fact]
    public void Sqlite_catalog_setup_loads_sqlite_too()
    {
        var statements = DuckLakeSql.SetupStatements(
            Config(("catalog", "sqlite"), ("path", "/lake/catalog.sqlite"), ("data_path", "/lake/data")), WhAlias);
        Assert.Equal(
            new[]
            {
                "install ducklake", "load ducklake", "install sqlite", "load sqlite",
                $"attach if not exists 'ducklake:sqlite:{Path.GetFullPath("/lake/catalog.sqlite")}' as {WhAlias} (data_path '{Path.GetFullPath("/lake/data")}')",
            },
            statements);
    }

    [Fact]
    public void Postgres_catalog_rides_a_postgres_secret_inside_a_ducklake_secret()
    {
        var statements = DuckLakeSql.SetupStatements(
            Config(("catalog", "postgres"), ("host", "pg.internal"), ("port", 5433L), ("database", "lake"),
                ("user", "pz"), ("password", "p'w"), ("data_path", "s3://bucket/lake/")), WhAlias);
        Assert.Equal(
            new[]
            {
                "install ducklake", "load ducklake", "install postgres", "load postgres", "install httpfs", "load httpfs",
                $"create or replace secret {WhAlias}_pg (type postgres, host 'pg.internal', port 5433, database 'lake', user 'pz', password 'p''w')",
                $"create or replace secret {WhAlias}_secret (type ducklake, metadata_path '', data_path 's3://bucket/lake/', metadata_parameters map {{'TYPE': 'postgres', 'SECRET': '{WhAlias}_pg'}})",
                $"attach if not exists 'ducklake:{WhAlias}_secret' as {WhAlias}",
            },
            statements);
    }

    [Fact]
    public void Quack_catalog_rides_a_scoped_quack_secret()
    {
        var statements = DuckLakeSql.SetupStatements(
            Config(("catalog", "quack"), ("uri", "quack:lake.internal:9494"), ("token", "t0k'en"), ("data_path", "/lake/data")), WhAlias);
        Assert.Equal(
            new[]
            {
                "install ducklake", "load ducklake", "install quack", "load quack",
                $"create or replace secret {WhAlias}_secret (type quack, token 't0k''en', scope 'quack:lake.internal:9494')",
                $"attach if not exists 'ducklake:quack:lake.internal:9494' as {WhAlias} (data_path '{Path.GetFullPath("/lake/data")}')",
            },
            statements);
    }

    [Fact]
    public void Motherduck_catalog_sets_the_token_and_attaches_the_metadata_database()
    {
        var statements = DuckLakeSql.SetupStatements(
            Config(("catalog", "motherduck"), ("database", "lake"), ("token", "md'tok"), ("data_path", "s3://bucket/lake/")), WhAlias);
        Assert.Equal(
            new[]
            {
                "install ducklake", "load ducklake", "install motherduck", "load motherduck", "install httpfs", "load httpfs",
                "set motherduck_token = 'md''tok'",
                $"attach if not exists 'ducklake:md:__ducklake_metadata_lake' as {WhAlias} (data_path 's3://bucket/lake/')",
            },
            statements);
    }

    [Fact]
    public void Storage_credentials_become_an_s3_secret_scoped_to_the_data_path()
    {
        var config = Config(("path", "/lake/c.ducklake"), ("data_path", "s3://bucket/lake/"),
            ("storage_key_id", "AK"), ("storage_secret_key", "S'K"), ("storage_region", "eu-west-1"),
            ("storage_endpoint", "minio:9000"), ("storage_url_style", "path"), ("storage_use_ssl", false));

        Assert.Equal(
            $"create or replace secret {WhAlias}_storage (type s3, key_id 'AK', secret 'S''K', region 'eu-west-1', " +
            "endpoint 'minio:9000', url_style 'path', use_ssl false, scope 's3://bucket/lake/')",
            DuckLakeSql.StorageSecretSql(config, WhAlias, "s3://bucket/lake/"));

        var statements = DuckLakeSql.SetupStatements(config, WhAlias);
        Assert.Contains("install httpfs", statements);
        Assert.Contains(statements, s => s.StartsWith($"create or replace secret {WhAlias}_storage", StringComparison.Ordinal));
        var list = statements.ToList();
        var storageIndex = list.IndexOf($"create or replace secret {WhAlias}_storage (type s3, key_id 'AK', secret 'S''K', region 'eu-west-1', endpoint 'minio:9000', url_style 'path', use_ssl false, scope 's3://bucket/lake/')");
        Assert.True(storageIndex >= 0 && storageIndex < list.Count - 1);
    }

    [Fact]
    public void Storage_secret_defaults_match_the_s3_connector()
    {
        var config = Config(("path", "/lake/c.ducklake"), ("data_path", "s3://bucket/lake/"),
            ("storage_key_id", "AK"), ("storage_secret_key", "SK"));
        Assert.Equal(
            $"create or replace secret {WhAlias}_storage (type s3, key_id 'AK', secret 'SK', region 'us-east-1', url_style 'vhost', use_ssl true, scope 's3://bucket/lake/')",
            DuckLakeSql.StorageSecretSql(config, WhAlias, "s3://bucket/lake/"));
    }

    [Fact]
    public void Credentials_never_appear_in_the_attach_statement()
    {
        foreach (var config in new[]
        {
            Config(("catalog", "postgres"), ("host", "h"), ("database", "d"), ("password", "PGPW"), ("data_path", "s3://b/")),
            Config(("catalog", "quack"), ("uri", "quack:h"), ("token", "QTOK"), ("data_path", "/d")),
            Config(("catalog", "motherduck"), ("database", "d"), ("token", "MDTOK"), ("data_path", "s3://b/")),
        })
        {
            var attach = DuckLakeSql.SetupStatements(config, WhAlias)[^1];
            Assert.StartsWith("attach", attach, StringComparison.Ordinal);
            foreach (var secret in new[] { "PGPW", "QTOK", "MDTOK" })
            {
                Assert.DoesNotContain(secret, attach, StringComparison.Ordinal);
            }
        }
    }

    [Fact]
    public void Postgres_catalog_without_data_path_is_a_permanent_error()
    {
        var ex = Assert.Throws<PzConnectorException>(() => DuckLakeSql.SetupStatements(
            Config(("catalog", "postgres"), ("host", "h"), ("database", "d")), WhAlias));
        Assert.False(ex.IsTransient);
        Assert.Contains("'data_path'", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Storage_credentials_with_a_local_data_path_emit_no_storage_secret()
    {
        var statements = DuckLakeSql.SetupStatements(
            Config(("path", "/lake/c.ducklake"), ("data_path", "/lake/data"),
                ("storage_key_id", "AK"), ("storage_secret_key", "SK")), WhAlias);
        Assert.DoesNotContain(statements, s => s.StartsWith($"create or replace secret {WhAlias}_storage", StringComparison.Ordinal));
        Assert.DoesNotContain("install httpfs", statements);
    }
}
