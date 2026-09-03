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
}
