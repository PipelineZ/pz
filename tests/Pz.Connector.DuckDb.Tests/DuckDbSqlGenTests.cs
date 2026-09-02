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

    [Fact]
    public void Setup_is_one_read_write_attach()
    {
        Assert.Equal(
            [$"attach if not exists '{DbPath}' as {WhAlias}"],
            DuckDbSql.SetupStatements(DbPath, WhAlias));
    }

    [Fact]
    public void Setup_escapes_the_path_literal()
    {
        Assert.Equal(
            [$"attach if not exists '/da''ta/app.duckdb' as {WhAlias}"],
            DuckDbSql.SetupStatements("/da'ta/app.duckdb", WhAlias));
    }

    [Fact]
    public void Plain_scan_is_the_bare_qualified_table()
    {
        Assert.Equal($"{WhAlias}.\"events\"", DuckDbSql.ScanFragment(WhAlias, Spec()));
    }

    [Fact]
    public void Declared_contract_prunes_the_read()
    {
        var spec = Spec(new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        });

        Assert.Equal(
            $"(select \"id\", \"name\" from {WhAlias}.\"events\")",
            DuckDbSql.ScanFragment(WhAlias, spec));
    }

    [Fact]
    public void Plain_incremental_watermark_is_pushed_down()
    {
        var spec = Spec() with { WatermarkCursor = "id", WatermarkValue = "100" };
        Assert.Equal(
            $"(select * from {WhAlias}.\"events\" where \"id\" > 100)",
            DuckDbSql.ScanFragment(WhAlias, spec));
    }

    [Fact]
    public void Inclusive_lower_bound_renders_gte()
    {
        var spec = Spec() with { WatermarkCursor = "id", WatermarkValue = "100", WatermarkLowerInclusive = true };
        Assert.Equal(
            $"(select * from {WhAlias}.\"events\" where \"id\" >= 100)",
            DuckDbSql.ScanFragment(WhAlias, spec));
    }

    [Fact]
    public void Window_upper_bound_joins_the_predicate_chain()
    {
        var spec = Spec() with
        {
            WatermarkCursor = "updated_at",
            WatermarkValue = "2026-01-01T00:00:00.000000",
            WatermarkUpperBound = "2026-01-02T00:00:00.000000",
        };

        Assert.Equal(
            $"(select * from {WhAlias}.\"events\" " +
            "where \"updated_at\" > '2026-01-01 00:00:00.000000' " +
            "and \"updated_at\" <= '2026-01-02 00:00:00.000000')",
            DuckDbSql.ScanFragment(WhAlias, spec));
    }

    [Fact]
    public void Schema_qualified_entity_scans_the_qualified_table()
    {
        var spec = new DatasetSpec("appdb", "raw.events", new Dictionary<string, object?>());
        Assert.Equal($"{WhAlias}.\"raw\".\"events\"", DuckDbSql.ScanFragment(WhAlias, spec));
    }

    [Theory]
    [InlineData("42", "42")]
    [InlineData("-7", "-7")]
    [InlineData("3.50", "3.50")]
    [InlineData("2026-08-19", "'2026-08-19'")]
    [InlineData("2026-08-19T12:30:00.000001", "'2026-08-19 12:30:00.000001'")]
    public void Watermark_literals_render_by_canonical_shape(string canonical, string expected)
    {
        Assert.Equal(expected, DuckDbSql.RenderWatermarkLiteral(canonical));
    }

    [Fact]
    public void Scan_predicates_are_injection_safe()
    {
        var spec = new DatasetSpec("appdb", "ev'en\"ts", new Dictionary<string, object?>())
        {
            WatermarkCursor = "up\"dated",
            WatermarkValue = "o'clock",
        };
        Assert.Equal(
            $"(select * from {WhAlias}.\"ev'en\"\"ts\" where \"up\"\"dated\" > 'o''clock')",
            DuckDbSql.ScanFragment(WhAlias, spec));
    }
}
