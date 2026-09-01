using Pz.Connectors.Abstractions;

namespace Pz.Connector.Gcs.Tests;

/// <summary>Offline proof of the gcs connector's shared text generation: the scoped CREATE SECRET
/// (DuckDB's <c>type gcs</c>, HMAC key pair), the deterministic collision-safe secret names, and the
/// ratified `root:` composition rules — the same shapes the s3 connector ratified, minus region
/// (gcs has none) and with endpoint/url_style/use_ssl emitted only when configured (DuckDB's own
/// storage.googleapis.com defaults are correct and stay theirs).</summary>
public sealed class GcsSqlTests
{
    private static ConnectorConfig Conn(Dictionary<string, object?>? extra = null)
    {
        var values = new Dictionary<string, object?>
        {
            ["auth"] = "hmac",
            ["key_id"] = "GOOGHMAC_TEST",
            ["secret"] = "HMAC_SECRET_VALUE",
        };
        foreach (var (k, v) in extra ?? [])
        {
            values[k] = v;
        }

        return new ConnectorConfig(values);
    }

    [Fact]
    public void Secret_sql_is_type_gcs_with_key_pair_only_by_default()
    {
        Assert.Equal(
            "create or replace secret s (type gcs, key_id 'GOOGHMAC_TEST', secret 'HMAC_SECRET_VALUE')",
            GcsSql.CreateSecretSql(Conn(), "s"));
    }

    [Fact]
    public void Secret_sql_appends_endpoint_url_style_and_use_ssl_only_when_configured()
    {
        var config = Conn(new Dictionary<string, object?>
        {
            ["endpoint"] = "localhost:9000",
            ["url_style"] = "path",
            ["use_ssl"] = false,
        });
        Assert.Equal(
            "create or replace secret s (type gcs, key_id 'GOOGHMAC_TEST', secret 'HMAC_SECRET_VALUE', " +
            "endpoint 'localhost:9000', url_style 'path', use_ssl false)",
            GcsSql.CreateSecretSql(config, "s"));
    }

    [Fact]
    public void Secret_literals_are_single_quote_escaped()
    {
        var config = Conn(new Dictionary<string, object?> { ["secret"] = "se'cret" });
        Assert.Contains("secret 'se''cret'", GcsSql.CreateSecretSql(config, "s"), StringComparison.Ordinal);
    }

    [Fact]
    public void Missing_key_id_is_a_named_permanent_error()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["auth"] = "hmac", ["secret"] = "x" });
        var ex = Assert.Throws<PzConnectorException>(() => GcsSql.CreateSecretSql(config, "s"));
        Assert.False(ex.IsTransient);
        Assert.Contains("'key_id'", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Setup_statements_are_install_load_and_the_secret()
    {
        var setup = GcsSql.SetupStatements(Conn(), "sec_name");
        Assert.Equal(3, setup.Count);
        Assert.Equal("install httpfs", setup[0]);
        Assert.Equal("load httpfs", setup[1]);
        Assert.StartsWith("create or replace secret sec_name (type gcs", setup[2], StringComparison.Ordinal);
    }

    [Fact]
    public void Secret_names_are_direction_prefixed_sanitized_and_hash_suffixed()
    {
        // Same determinism/collision rule as s3: two names that sanitize identically ("prod-db"/"prod_db")
        // must still yield distinct secrets, so the raw name's hash rides along.
        var src = GcsSql.SourceSecretName("prod-lake");
        var snk = GcsSql.SinkSecretName("prod-lake");
        Assert.StartsWith("pz_gcs_src_prod_lake_", src, StringComparison.Ordinal);
        Assert.StartsWith("pz_gcs_snk_prod_lake_", snk, StringComparison.Ordinal);
        Assert.Equal(src.Length, "pz_gcs_src_prod_lake_".Length + 8);
        Assert.NotEqual(
            GcsSql.SourceSecretName("prod-db")["pz_gcs_src_prod_db_".Length..],
            GcsSql.SourceSecretName("prod_db")["pz_gcs_src_prod_db_".Length..]);
    }

    [Fact]
    public void Split_root_yields_bucket_and_optional_prefix()
    {
        Assert.Equal(((string?)null, ""), GcsSql.SplitRoot(null));
        Assert.Equal(("my-bucket", ""), GcsSql.SplitRoot("my-bucket"));
        Assert.Equal(("my-bucket", "raw/in"), GcsSql.SplitRoot("/my-bucket/raw/in/"));
    }

    [Fact]
    public void Join_skips_empty_sides()
    {
        Assert.Equal("a/b", GcsSql.Join("a", "b"));
        Assert.Equal("b", GcsSql.Join("", "b"));
        Assert.Equal("a", GcsSql.Join("a", ""));
    }
}
