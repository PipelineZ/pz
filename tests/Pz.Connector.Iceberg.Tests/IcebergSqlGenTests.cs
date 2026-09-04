using Pz.Connectors.Abstractions;

namespace Pz.Connector.Iceberg.Tests;

/// <summary>Offline proof of the connector's whole data plane — which IS these strings: per-catalog
/// setup lists, secret shapes, the attach, entity quoting, the catalog and files scan fragments with
/// contract pruning, time travel and watermark rendering, and the three copy statements. DuckDB
/// parses every statement, so identifiers are double-quoted and literals single-quoted.</summary>
public sealed class IcebergSqlGenTests
{
    private const string WhAlias = "pz_iceberg_wh_509bcf06";

    private static DatasetSpec Spec(Dictionary<string, object?>? options = null, string entity = "raw.events") =>
        new("wh", entity, options ?? []);

    /// <summary>Last write wins, so a test can override one of <see cref="Rest"/>'s base keys.</summary>
    private static ConnectorConfig Config(params (string Key, object? Value)[] values)
    {
        var dictionary = new Dictionary<string, object?>();
        foreach (var (key, value) in values)
        {
            dictionary[key] = value;
        }

        return new(dictionary);
    }

    private static ConnectorConfig Rest(params (string Key, object? Value)[] extra) =>
        Config([("catalog", "rest"), ("endpoint", "https://cat.example.com/api"), ("warehouse", "wh"), .. extra]);

    [Fact]
    public void Alias_and_secret_names_derive_from_the_raw_connection_name()
    {
        Assert.Equal(WhAlias, IcebergSql.Alias("wh"));
        Assert.Equal("pz_iceberg_my_wh_2_4c668b3d", IcebergSql.Alias("my-wh.2"));
        Assert.NotEqual(IcebergSql.Alias("prod-db"), IcebergSql.Alias("prod_db"));
        Assert.Equal(WhAlias + "_secret", IcebergSql.SecretName(WhAlias));
        Assert.Equal(WhAlias + "_storage", IcebergSql.StorageSecretName(WhAlias));
    }

    [Fact]
    public void Root_resolves_against_base_dir_or_passes_a_url_through()
    {
        Assert.Equal(Path.GetFullPath("/proj/lake"), IcebergSql.ResolveRoot(Config(("root", "lake"), ("base_dir", "/proj"))));
        Assert.Equal(Path.GetFullPath("/data/lake"), IcebergSql.ResolveRoot(Config(("root", Path.GetFullPath("/data/lake")))));
        Assert.Equal("s3://bucket/warehouse", IcebergSql.ResolveRoot(Config(("root", "s3://bucket/warehouse/"))));

        var ex = Assert.Throws<PzConnectorException>(() => IcebergSql.ResolveRoot(Config(("base_dir", "/proj"))));
        Assert.False(ex.IsTransient);
        Assert.Contains("requires 'root'", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Catalog_entities_are_namespace_dot_table_and_quote_safely()
    {
        Assert.Equal($"{WhAlias}.\"raw\".\"events\"", IcebergSql.QualifiedTable(WhAlias, "raw.events"));
        Assert.Equal($"{WhAlias}.\"we\"\"ird\".\"ev\"\"ents\"", IcebergSql.QualifiedTable(WhAlias, "we\"ird.ev\"ents"));
        foreach (var bad in new[] { "events", "a.b.c", ".x", "x.", "", "main.events" })
        {
            var ex = Assert.Throws<PzConnectorException>(() => IcebergSql.QualifiedTable(WhAlias, bad));
            Assert.False(ex.IsTransient);
        }

        Assert.Contains("default schema", Assert.Throws<PzConnectorException>(
            () => IcebergSql.QualifiedTable(WhAlias, "main.events")).Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Files_entities_map_to_table_directories()
    {
        Assert.Equal("s3://b/wh/raw/events", IcebergSql.TablePath("s3://b/wh", "raw.events"));
        Assert.Equal("s3://b/wh/events", IcebergSql.TablePath("s3://b/wh", "events"));
        Assert.Equal(Path.Combine("/lake", "raw", "events"), IcebergSql.TablePath("/lake", "raw.events"));
        Assert.Equal(Path.Combine("/lake", "events"), IcebergSql.TablePath("/lake", "events"));
        Assert.False(Assert.Throws<PzConnectorException>(() => IcebergSql.TablePath("/lake", "a.b.c")).IsTransient);
    }

    [Fact]
    public void The_main_namespace_is_an_ordinary_directory_for_files_but_still_refused_on_a_catalog()
    {
        // A `files` read never touches DuckDB's binder -- it's a raw path under `root`, so `main` is
        // just another namespace directory there.
        Assert.Equal(Path.Combine("/lake", "main", "events"), IcebergSql.TablePath("/lake", "main.events"));

        // A catalog-attached table DOES go through the binder, which reserves `main` for its own
        // default schema -- that refusal stays.
        Assert.Contains("default schema", Assert.Throws<PzConnectorException>(
            () => IcebergSql.QualifiedTable(WhAlias, "main.events")).Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("42", "42")]
    [InlineData("-7", "-7")]
    [InlineData("3.50", "3.50")]
    [InlineData("2026-08-19", "'2026-08-19'")]
    [InlineData("2026-08-19T12:30:00.000001", "'2026-08-19 12:30:00.000001'")]
    public void Watermark_literals_render_by_canonical_shape(string canonical, string expected)
    {
        Assert.Equal(expected, IcebergSql.RenderWatermarkLiteral(canonical));
    }

    [Fact]
    public void Rest_catalog_with_a_bearer_token_rides_an_iceberg_secret()
    {
        var statements = IcebergSql.SetupStatements(Rest(("token", "t'ok")), WhAlias);
        Assert.Equal(
            new[]
            {
                "install iceberg", "load iceberg", "install httpfs", "load httpfs",
                $"create or replace secret {WhAlias}_secret (type iceberg, token 't''ok')",
                $"attach if not exists 'wh' as {WhAlias} (type iceberg, endpoint 'https://cat.example.com/api', secret {WhAlias}_secret)",
            },
            statements);
    }

    [Fact]
    public void Rest_catalog_with_an_oauth2_client_pair_rides_an_iceberg_secret()
    {
        var statements = IcebergSql.SetupStatements(
            Rest(("client_id", "id"), ("client_secret", "s'ec"), ("oauth2_server_uri", "https://auth.example.com/token"), ("oauth2_scope", "PRINCIPAL_ROLE:ALL")),
            WhAlias);
        Assert.Equal(
            $"create or replace secret {WhAlias}_secret (type iceberg, client_id 'id', client_secret 's''ec', " +
            "oauth2_server_uri 'https://auth.example.com/token', oauth2_scope 'PRINCIPAL_ROLE:ALL')",
            statements[4]);
        Assert.Equal(
            $"attach if not exists 'wh' as {WhAlias} (type iceberg, endpoint 'https://cat.example.com/api', secret {WhAlias}_secret)",
            statements[^1]);
    }

    [Fact]
    public void Rest_catalog_without_credentials_attaches_unauthenticated_with_an_empty_warehouse()
    {
        var statements = IcebergSql.SetupStatements(Config(("endpoint", "http://localhost:8181")), WhAlias);
        Assert.Equal(
            new[]
            {
                "install iceberg", "load iceberg", "install httpfs", "load httpfs",
                $"attach if not exists '' as {WhAlias} (type iceberg, endpoint 'http://localhost:8181', authorization_type 'none')",
            },
            statements);
    }

    [Fact]
    public void Rest_catalog_with_storage_keys_turns_vending_off_and_leaves_the_secret_unscoped()
    {
        var statements = IcebergSql.SetupStatements(
            Rest(("token", "tok"), ("storage_key_id", "AK"), ("storage_secret_key", "S'K"), ("storage_region", "eu-west-1"),
                ("storage_endpoint", "minio:9000"), ("storage_url_style", "path"), ("storage_use_ssl", false)),
            WhAlias);
        Assert.Equal(
            new[]
            {
                "install iceberg", "load iceberg", "install httpfs", "load httpfs",
                $"create or replace secret {WhAlias}_storage (type s3, key_id 'AK', secret 'S''K', region 'eu-west-1', endpoint 'minio:9000', url_style 'path', use_ssl false)",
                $"create or replace secret {WhAlias}_secret (type iceberg, token 'tok')",
                $"attach if not exists 'wh' as {WhAlias} (type iceberg, endpoint 'https://cat.example.com/api', secret {WhAlias}_secret, access_delegation_mode 'none')",
            },
            statements);
    }

    [Fact]
    public void Nested_namespaces_flag_rides_the_attach()
    {
        var statements = IcebergSql.SetupStatements(Rest(("nested_namespaces", true)), WhAlias);
        Assert.EndsWith("authorization_type 'none', support_nested_namespaces true)", statements[^1], StringComparison.Ordinal);
    }

    [Fact]
    public void Glue_catalog_signs_with_the_credential_chain_by_default()
    {
        var statements = IcebergSql.SetupStatements(Config(("catalog", "glue"), ("storage_region", "eu-central-1")), WhAlias);
        Assert.Equal(
            new[]
            {
                "install iceberg", "load iceberg", "install httpfs", "load httpfs", "install aws", "load aws",
                $"create or replace secret {WhAlias}_storage (type s3, provider credential_chain, region 'eu-central-1')",
                $"attach if not exists ':' as {WhAlias} (type iceberg, endpoint_type 'glue')",
            },
            statements);
    }

    [Fact]
    public void Glue_catalog_with_explicit_keys_and_an_account_warehouse()
    {
        var statements = IcebergSql.SetupStatements(
            Config(("catalog", "glue"), ("warehouse", "123456789012:cat"), ("storage_key_id", "AK"), ("storage_secret_key", "SK")), WhAlias);
        Assert.DoesNotContain("install aws", statements);
        Assert.Equal(
            $"create or replace secret {WhAlias}_storage (type s3, key_id 'AK', secret 'SK', region 'us-east-1', url_style 'vhost', use_ssl true)",
            statements[4]);
        Assert.Equal($"attach if not exists '123456789012:cat' as {WhAlias} (type iceberg, endpoint_type 'glue')", statements[^1]);
    }

    [Fact]
    public void S3_tables_catalog_attaches_the_bucket_arn()
    {
        var statements = IcebergSql.SetupStatements(
            Config(("catalog", "s3_tables"), ("warehouse", "arn:aws:s3tables:us-east-1:123456789012:bucket/b")), WhAlias);
        Assert.Contains("install aws", statements);
        Assert.Equal(
            $"attach if not exists 'arn:aws:s3tables:us-east-1:123456789012:bucket/b' as {WhAlias} (type iceberg, endpoint_type 's3_tables')",
            statements[^1]);
    }

    [Fact]
    public void S3_tables_catalog_with_explicit_keys_skips_aws_and_uses_an_unscoped_secret()
    {
        var statements = IcebergSql.SetupStatements(
            Config(("catalog", "s3_tables"), ("warehouse", "arn:aws:s3tables:us-east-1:123456789012:bucket/b"),
                ("storage_key_id", "AK"), ("storage_secret_key", "SK")),
            WhAlias);
        Assert.DoesNotContain("install aws", statements);
        Assert.Equal(
            $"create or replace secret {WhAlias}_storage (type s3, key_id 'AK', secret 'SK', region 'us-east-1', url_style 'vhost', use_ssl true)",
            statements[4]);
        Assert.Equal(
            $"attach if not exists 'arn:aws:s3tables:us-east-1:123456789012:bucket/b' as {WhAlias} (type iceberg, endpoint_type 's3_tables')",
            statements[^1]);
    }

    [Fact]
    public void Files_catalog_with_a_local_root_needs_only_the_iceberg_extension()
    {
        Assert.Equal(
            new[] { "install iceberg", "load iceberg" },
            IcebergSql.SetupStatements(Config(("catalog", "files"), ("root", "/lake")), WhAlias));
    }

    [Fact]
    public void Files_catalog_with_an_object_store_root_scopes_the_storage_secret_to_it()
    {
        var statements = IcebergSql.SetupStatements(
            Config(("catalog", "files"), ("root", "s3://bucket/wh/"), ("storage_key_id", "AK"), ("storage_secret_key", "SK")), WhAlias);
        Assert.Equal(
            new[]
            {
                "install iceberg", "load iceberg", "install httpfs", "load httpfs",
                $"create or replace secret {WhAlias}_storage (type s3, key_id 'AK', secret 'SK', region 'us-east-1', url_style 'vhost', use_ssl true, scope 's3://bucket/wh')",
            },
            statements);
    }

    [Fact]
    public void Credentials_never_appear_in_the_attach_statement()
    {
        foreach (var config in new[]
        {
            Rest(("token", "TOK"), ("storage_key_id", "AK"), ("storage_secret_key", "STSEC")),
            Rest(("client_id", "CID"), ("client_secret", "CSEC")),
            Config(("catalog", "s3_tables"), ("warehouse", "arn:x"), ("storage_key_id", "AK"), ("storage_secret_key", "STSEC")),
        })
        {
            var attach = IcebergSql.SetupStatements(config, WhAlias)[^1];
            Assert.StartsWith("attach", attach, StringComparison.Ordinal);
            foreach (var secret in new[] { "TOK", "STSEC", "CSEC" })
            {
                Assert.DoesNotContain(secret, attach, StringComparison.Ordinal);
            }
        }
    }

    [Fact]
    public void Plain_scan_is_the_bare_qualified_table()
    {
        Assert.Equal($"{WhAlias}.\"raw\".\"events\"", IcebergSql.ScanFragment(WhAlias, Spec()));
    }

    [Fact]
    public void Version_option_time_travels_the_scan_by_snapshot_id()
    {
        var spec = Spec(new Dictionary<string, object?> { ["version"] = 4830783628919130688UL });
        Assert.Equal($"{WhAlias}.\"raw\".\"events\" at (version => 4830783628919130688)", IcebergSql.ScanFragment(WhAlias, spec));
        Assert.Equal($"{WhAlias}.\"raw\".\"events\" at (version => 3)",
            IcebergSql.ScanFragment(WhAlias, Spec(new Dictionary<string, object?> { ["version"] = 3L })));
    }

    [Fact]
    public void Timestamp_option_time_travels_the_scan()
    {
        var spec = Spec(new Dictionary<string, object?> { ["timestamp"] = "2026-05-26 00:00:00" });
        Assert.Equal($"{WhAlias}.\"raw\".\"events\" at (timestamp => timestamp '2026-05-26 00:00:00')",
            IcebergSql.ScanFragment(WhAlias, spec));
    }

    [Fact]
    public void Version_and_timestamp_together_are_a_permanent_error()
    {
        var spec = Spec(new Dictionary<string, object?> { ["version"] = 3L, ["timestamp"] = "2026-05-26 00:00:00" });
        var ex = Assert.Throws<PzConnectorException>(() => IcebergSql.ScanFragment(WhAlias, spec));
        Assert.False(ex.IsTransient);
        Assert.Contains("version", ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("latest")]
    [InlineData("-1")]
    public void Non_snapshot_id_version_is_a_permanent_error(string version)
    {
        var spec = Spec(new Dictionary<string, object?> { ["version"] = version });
        Assert.False(Assert.Throws<PzConnectorException>(() => IcebergSql.ScanFragment(WhAlias, spec)).IsTransient);
    }

    [Fact]
    public void Metadata_version_is_refused_on_a_catalog_scan()
    {
        var spec = Spec(new Dictionary<string, object?> { ["metadata_version"] = "00003-abc" });
        var ex = Assert.Throws<PzConnectorException>(() => IcebergSql.ScanFragment(WhAlias, spec));
        Assert.False(ex.IsTransient);
        Assert.Contains("files", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Timestamp_option_given_as_a_datetime_renders_invariantly()
    {
        var dt = Spec(new Dictionary<string, object?> { ["timestamp"] = new DateTime(2026, 5, 26, 13, 4, 5, DateTimeKind.Utc) });
        Assert.Equal($"{WhAlias}.\"raw\".\"events\" at (timestamp => timestamp '2026-05-26 13:04:05.000000')",
            IcebergSql.ScanFragment(WhAlias, dt));

        var dto = Spec(new Dictionary<string, object?> { ["timestamp"] = new DateTimeOffset(2026, 5, 26, 15, 4, 5, TimeSpan.FromHours(2)) });
        Assert.Equal($"{WhAlias}.\"raw\".\"events\" at (timestamp => timestamp '2026-05-26 13:04:05.000000')",
            IcebergSql.ScanFragment(WhAlias, dto));
    }

    [Fact]
    public void Contract_watermark_and_time_travel_compose()
    {
        var spec = new DatasetSpec("wh", "raw.events", new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
            ["version"] = 3L,
        })
        {
            WatermarkCursor = "id",
            WatermarkValue = "100",
            WatermarkUpperBound = "200",
        };
        Assert.Equal(
            $"(select \"id\", \"name\" from {WhAlias}.\"raw\".\"events\" at (version => 3) where \"id\" > 100 and \"id\" <= 200)",
            IcebergSql.ScanFragment(WhAlias, spec));
    }

    [Fact]
    public void Inclusive_lower_bound_renders_gte()
    {
        var spec = Spec() with { WatermarkCursor = "id", WatermarkValue = "100", WatermarkLowerInclusive = true };
        Assert.Equal($"(select * from {WhAlias}.\"raw\".\"events\" where \"id\" >= 100)", IcebergSql.ScanFragment(WhAlias, spec));
    }

    [Fact]
    public void Scan_predicates_are_injection_safe()
    {
        var spec = new DatasetSpec("wh", "ra'w.ev'en\"ts", new Dictionary<string, object?> { ["timestamp"] = "x' or 1=1" })
        {
            WatermarkCursor = "up\"dated",
            WatermarkValue = "o'clock",
        };
        Assert.Equal(
            $"(select * from {WhAlias}.\"ra'w\".\"ev'en\"\"ts\" at (timestamp => timestamp 'x'' or 1=1') where \"up\"\"dated\" > 'o''clock')",
            IcebergSql.ScanFragment(WhAlias, spec));
    }

    [Fact]
    public void Files_scan_is_iceberg_scan_over_the_table_directory()
    {
        Assert.Equal("iceberg_scan('s3://b/wh/raw/events', allow_moved_paths = true)",
            IcebergSql.FilesScanFragment("s3://b/wh", Spec()));
    }

    [Fact]
    public void Files_scan_options_render_metadata_version_snapshot_and_timestamp()
    {
        Assert.Equal("iceberg_scan('s3://b/wh/raw/events', allow_moved_paths = true, version => '00003-ab''c')",
            IcebergSql.FilesScanFragment("s3://b/wh", Spec(new Dictionary<string, object?> { ["metadata_version"] = "00003-ab'c" })));
        Assert.Equal("iceberg_scan('s3://b/wh/raw/events', allow_moved_paths = true, snapshot_from_id => 42)",
            IcebergSql.FilesScanFragment("s3://b/wh", Spec(new Dictionary<string, object?> { ["version"] = 42L })));
        Assert.Equal("iceberg_scan('s3://b/wh/raw/events', allow_moved_paths = true, snapshot_from_timestamp => timestamp '2026-05-26 00:00:00')",
            IcebergSql.FilesScanFragment("s3://b/wh", Spec(new Dictionary<string, object?> { ["timestamp"] = "2026-05-26 00:00:00" })));
    }

    [Fact]
    public void Files_scan_composes_contract_and_watermark_and_escapes_the_path()
    {
        var spec = new DatasetSpec("wh", "raw.ev'ents", new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
        })
        {
            WatermarkCursor = "id",
            WatermarkValue = "5",
        };
        Assert.Equal(
            "(select \"id\" from iceberg_scan('s3://b/wh/raw/ev''ents', allow_moved_paths = true) where \"id\" > 5)",
            IcebergSql.FilesScanFragment("s3://b/wh", spec));
    }

    [Fact]
    public void Rest_catalog_under_azure_without_an_auth_method_loads_azure_and_leaves_vending_on()
    {
        var statements = IcebergSql.SetupStatements(Rest(("token", "tok"), ("storage", "azure")), WhAlias);
        Assert.Equal(
            new[]
            {
                "install iceberg", "load iceberg", "install httpfs", "load httpfs", "install azure", "load azure",
                $"create or replace secret {WhAlias}_secret (type iceberg, token 'tok')",
                $"attach if not exists 'wh' as {WhAlias} (type iceberg, endpoint 'https://cat.example.com/api', secret {WhAlias}_secret)",
            },
            statements);
    }

    [Fact]
    public void Rest_catalog_with_an_azure_connection_string_turns_vending_off_and_leaves_the_secret_unscoped()
    {
        var statements = IcebergSql.SetupStatements(
            Rest(("token", "tok"), ("storage", "azure"), ("storage_auth", "connection_string"), ("storage_connection_string", "AccountName=a;AccountKey=k'k")),
            WhAlias);
        Assert.Equal(
            new[]
            {
                "install iceberg", "load iceberg", "install httpfs", "load httpfs", "install azure", "load azure",
                $"create or replace secret {WhAlias}_storage (type azure, connection_string 'AccountName=a;AccountKey=k''k')",
                $"create or replace secret {WhAlias}_secret (type iceberg, token 'tok')",
                $"attach if not exists 'wh' as {WhAlias} (type iceberg, endpoint 'https://cat.example.com/api', secret {WhAlias}_secret, access_delegation_mode 'none')",
            },
            statements);
    }

    [Fact]
    public void Azure_secret_bodies_mirror_the_azureblob_connector()
    {
        Assert.Equal(
            $"create or replace secret {WhAlias}_storage (type azure, connection_string 'DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=k;EndpointSuffix=core.windows.net')",
            IcebergSql.AzureStorageSecretSql(Config(("storage_auth", "account_key"), ("storage_account_name", "acct"), ("storage_account_key", "k")), WhAlias, scope: null));
        Assert.Equal(
            $"create or replace secret {WhAlias}_storage (type azure, connection_string 'DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=k;BlobEndpoint=http://127.0.0.1:10000/acct')",
            IcebergSql.AzureStorageSecretSql(Config(("storage_auth", "account_key"), ("storage_account_name", "acct"), ("storage_account_key", "k"),
                ("storage_endpoint", "http://127.0.0.1:10000/acct")), WhAlias, scope: null));
        Assert.Equal(
            $"create or replace secret {WhAlias}_storage (type azure, provider service_principal, tenant_id 't', client_id 'c', client_secret 's''s', account_name 'acct')",
            IcebergSql.AzureStorageSecretSql(Config(("storage_auth", "service_principal"), ("storage_tenant_id", "t"), ("storage_client_id", "c"),
                ("storage_client_secret", "s's"), ("storage_account_name", "acct")), WhAlias, scope: null));
        Assert.Equal(
            $"create or replace secret {WhAlias}_storage (type azure, provider credential_chain, account_name 'acct')",
            IcebergSql.AzureStorageSecretSql(Config(("storage_auth", "credential_chain"), ("storage_account_name", "acct")), WhAlias, scope: null));
        Assert.Equal(
            $"create or replace secret {WhAlias}_storage (type azure, provider credential_chain, chain 'cli;env', account_name 'acct', scope 'az://c/wh/')",
            IcebergSql.AzureStorageSecretSql(Config(("storage_auth", "credential_chain"), ("storage_account_name", "acct"), ("storage_chain", "cli;env")), WhAlias, scope: "az://c/wh"));
    }

    [Fact]
    public void Files_catalog_with_an_azure_root_loads_azure_and_scopes_the_secret_with_a_trailing_slash()
    {
        var statements = IcebergSql.SetupStatements(
            Config(("catalog", "files"), ("root", "az://c/wh/"), ("storage_auth", "connection_string"), ("storage_connection_string", "cs")), WhAlias);
        Assert.Equal(
            new[]
            {
                "install iceberg", "load iceberg", "install httpfs", "load httpfs", "install azure", "load azure",
                $"create or replace secret {WhAlias}_storage (type azure, connection_string 'cs', scope 'az://c/wh/')",
            },
            statements);
    }

    [Fact]
    public void Azure_credentials_never_appear_in_the_attach_statement()
    {
        var attach = IcebergSql.SetupStatements(
            Rest(("token", "TOK"), ("storage", "azure"), ("storage_auth", "account_key"), ("storage_account_name", "acct"), ("storage_account_key", "AZKEY")),
            WhAlias)[^1];
        Assert.StartsWith("attach", attach, StringComparison.Ordinal);
        Assert.DoesNotContain("AZKEY", attach, StringComparison.Ordinal);
        Assert.DoesNotContain("TOK", attach, StringComparison.Ordinal);
    }

    [Fact]
    public void Append_copy_ensures_namespace_and_table_then_inserts()
    {
        Assert.True(IcebergSql.TryCopySql(WhAlias, "raw.events", "append", [], out var sql, out var mechanism));
        Assert.Equal(
            $"create schema if not exists {WhAlias}.\"raw\";\n" +
            $"create table if not exists {WhAlias}.\"raw\".\"events\" as select * from {{{{source}}}} limit 0;\n" +
            $"insert into {WhAlias}.\"raw\".\"events\" select * from {{{{source}}}};",
            sql);
        Assert.Equal("iceberg insert", mechanism);
    }

    [Fact]
    public void Replace_copy_is_a_transactional_delete_then_insert()
    {
        Assert.True(IcebergSql.TryCopySql(WhAlias, "raw.events", "replace", [], out var sql, out var mechanism));
        Assert.Equal(
            $"create schema if not exists {WhAlias}.\"raw\";\n" +
            $"create table if not exists {WhAlias}.\"raw\".\"events\" as select * from {{{{source}}}} limit 0;\n" +
            "begin transaction;\n" +
            $"delete from {WhAlias}.\"raw\".\"events\";\n" +
            $"insert into {WhAlias}.\"raw\".\"events\" select * from {{{{source}}}};\n" +
            "commit;",
            sql);
        Assert.Equal("iceberg overwrite", mechanism);
    }

    [Fact]
    public void Merge_copy_matches_on_every_key()
    {
        Assert.True(IcebergSql.TryCopySql(WhAlias, "raw.events", "merge", ["id", "region"], out var sql, out var mechanism));
        Assert.Equal(
            $"create schema if not exists {WhAlias}.\"raw\";\n" +
            $"create table if not exists {WhAlias}.\"raw\".\"events\" as select * from {{{{source}}}} limit 0;\n" +
            $"merge into {WhAlias}.\"raw\".\"events\" as t " +
            "using (select s.* from {{source}} as s qualify row_number() over (partition by s.\"id\", s.\"region\") = 1) as s " +
            "on t.\"id\" = s.\"id\" and t.\"region\" = s.\"region\" " +
            "when matched then update when not matched then insert;",
            sql);
        Assert.Equal("iceberg merge", mechanism);
    }

    [Fact]
    public void Keyless_merge_and_bare_entities_throw_and_unknown_modes_have_no_native_shape()
    {
        Assert.False(Assert.Throws<PzConnectorException>(
            () => IcebergSql.TryCopySql(WhAlias, "raw.events", "merge", [], out _, out _)).IsTransient);
        Assert.False(Assert.Throws<PzConnectorException>(
            () => IcebergSql.TryCopySql(WhAlias, "events", "append", [], out _, out _)).IsTransient);
        Assert.False(IcebergSql.TryCopySql(WhAlias, "raw.events", "upsert_all", [], out _, out _));
    }
}
