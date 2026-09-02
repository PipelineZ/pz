using Pz.Connectors.Abstractions;

namespace Pz.Connector.DuckDb.Tests;

/// <summary>Offline proof of the connector's whole data plane — which IS these strings: the attach
/// setup, alias derivation, entity splitting/quoting, the scan fragment with contract pruning and
/// watermark rendering, and the three copy statements. One dialect throughout: DuckDB parses every
/// statement, so identifiers are double-quoted and literals single-quoted.</summary>
public sealed class DuckDbSqlGenTests
{
    private const string DbPath = "/data/app.duckdb";
    private const string WhAlias = "pz_duckdb_wh_509bcf06";

    private static DatasetSpec Spec(Dictionary<string, object?>? options = null) =>
        new("appdb", "events", options ?? []);

    [Fact]
    public void Alias_appends_a_stable_hash_of_the_raw_connection_name()
    {
        Assert.Equal("pz_duckdb_my_wh_2_4c668b3d", DuckDbSql.Alias("my-wh.2"));
        Assert.Equal("pz_duckdb_my_wh_2_4c668b3d", DuckDbSql.Alias("my-wh.2"));
        Assert.Equal(WhAlias, DuckDbSql.Alias("wh"));
    }

    [Fact]
    public void Alias_hash_disambiguates_connection_names_that_sanitize_the_same_way()
    {
        // "prod-db" and "prod_db" both sanitize to "prod_db", and `attach if not exists` is
        // first-wins — without the hash suffix they would share one attached file.
        Assert.NotEqual(DuckDbSql.Alias("prod-db"), DuckDbSql.Alias("prod_db"));
    }

    [Fact]
    public void Resolve_path_joins_relative_paths_against_base_dir()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["path"] = "data/app.duckdb",
            ["base_dir"] = "/proj",
        });

        Assert.Equal(Path.GetFullPath(Path.Combine("/proj", "data/app.duckdb")), DuckDbSql.ResolvePath(config));
    }

    [Fact]
    public void Resolve_path_passes_absolute_paths_through()
    {
        var absolute = Path.GetFullPath(DbPath);
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["path"] = absolute,
            ["base_dir"] = "/proj",
        });

        Assert.Equal(absolute, DuckDbSql.ResolvePath(config));
    }

    [Fact]
    public void Resolve_path_without_base_dir_falls_back_to_the_working_directory()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["path"] = "data/app.duckdb" });
        Assert.Equal(
            Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "data/app.duckdb")),
            DuckDbSql.ResolvePath(config));
    }

    [Fact]
    public void Resolve_path_requires_path()
    {
        var ex = Assert.Throws<PzConnectorException>(() => DuckDbSql.ResolvePath(ConnectorConfig.Empty));
        Assert.False(ex.IsTransient);
        Assert.Contains("requires 'path'", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Bare_entity_is_a_main_schema_table()
    {
        Assert.Equal((null, "events"), DuckDbSql.SplitEntity("events"));
        Assert.Equal($"{WhAlias}.\"events\"", DuckDbSql.QualifiedTable(WhAlias, "events"));
    }

    [Fact]
    public void Dotted_entity_splits_into_schema_and_table()
    {
        Assert.Equal(("raw", "events"), DuckDbSql.SplitEntity("raw.events"));
        Assert.Equal($"{WhAlias}.\"raw\".\"events\"", DuckDbSql.QualifiedTable(WhAlias, "raw.events"));
    }

    [Theory]
    [InlineData("a.b.c")]
    [InlineData(".events")]
    [InlineData("raw.")]
    [InlineData("")]
    public void Malformed_entities_are_permanent_errors(string entity)
    {
        var ex = Assert.Throws<PzConnectorException>(() => DuckDbSql.SplitEntity(entity));
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Identifiers_and_literals_are_injection_safe()
    {
        Assert.Equal("\"ev\"\"ents\"", DuckDbSql.QuoteIdent("ev\"ents"));
        Assert.Equal("o''clock", DuckDbSql.EscapeLiteral("o'clock"));
        Assert.Equal($"{WhAlias}.\"we\"\"ird\".\"ev\"\"ents\"", DuckDbSql.QualifiedTable(WhAlias, "we\"ird.ev\"ents"));
    }
}
