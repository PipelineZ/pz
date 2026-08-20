using Pz.Connectors.Abstractions;

namespace Pz.Connector.Sqlite.Tests;

/// <summary>Offline proof of the connector's whole data plane — which IS these strings:
/// setup-statement shapes, the sqlite_scan fragment, contract pruning, watermark literal rendering,
/// quoting/injection, path resolution, and the sink copy statements. One dialect throughout: every
/// statement here is parsed by DuckDB, so identifiers are double-quoted and literals single-quoted —
/// there is no MySQL-style backtick split-brain to defend.</summary>
public sealed class SqliteSqlGenTests
{
    private static DatasetSpec Spec(Dictionary<string, object?>? options = null) =>
        new("appdb", "events", options ?? []);

    private const string DbPath = "/data/app.db";
    private const string WhSinkAlias = "pz_sqlite_snk_wh_509bcf06";

    [Fact]
    public void Source_setup_is_install_and_load_only()
    {
        // Reads need no attach and no alias — sqlite_scan is self-contained.
        var statements = SqliteSql.SetupStatements();
        Assert.Equal(["install sqlite", "load sqlite"], statements);
    }

    [Fact]
    public void Sink_setup_is_install_load_then_read_write_attach()
    {
        var statements = SqliteSql.SinkSetupStatements(DbPath, SqliteSql.SinkAlias("wh"));
        Assert.Equal(3, statements.Count);
        Assert.Equal("install sqlite", statements[0]);
        Assert.Equal("load sqlite", statements[1]);
        Assert.Equal($"attach if not exists '{DbPath}' as {WhSinkAlias} (type sqlite)", statements[2]);
    }

    [Fact]
    public void Sink_alias_appends_a_stable_hash_of_the_raw_connection_name()
    {
        Assert.Equal("pz_sqlite_snk_my_wh_2_4c668b3d", SqliteSql.SinkAlias("my-wh.2"));
        Assert.Equal("pz_sqlite_snk_my_wh_2_4c668b3d", SqliteSql.SinkAlias("my-wh.2"));
    }

    [Fact]
    public void Sink_alias_hash_disambiguates_connection_names_that_sanitize_the_same_way()
    {
        // "prod-db" and "prod_db" both sanitize to "prod_db", and `attach if not exists` is
        // first-wins — without the hash suffix they would share one attached file.
        var first = SqliteSql.SinkAlias("prod-db");
        var second = SqliteSql.SinkAlias("prod_db");
        Assert.NotEqual(first, second);
        Assert.StartsWith("pz_sqlite_snk_prod_db_", first, StringComparison.Ordinal);
        Assert.StartsWith("pz_sqlite_snk_prod_db_", second, StringComparison.Ordinal);
    }

    [Fact]
    public void Plain_scan_selects_star_from_sqlite_scan()
    {
        Assert.Equal(
            $"sqlite_scan('{DbPath}', 'events')",
            SqliteSql.ScanFragment(DbPath, Spec()));
    }

    [Fact]
    public void Declared_contract_prunes_the_read()
    {
        var spec = Spec(new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        });

        Assert.Equal(
            $"(select \"id\", \"name\" from sqlite_scan('{DbPath}', 'events'))",
            SqliteSql.ScanFragment(DbPath, spec));
    }

    [Fact]
    public void Plain_incremental_watermark_is_pushed_down()
    {
        // The database-source rule: the UNWINDOWED watermark rides the fragment, and
        // DuckDB's sqlite scanner pushes the filter into the file scan.
        var spec = new DatasetSpec("appdb", "events", new Dictionary<string, object?>()) { WatermarkCursor = "id", WatermarkValue = "100" };
        Assert.Equal(
            $"(select * from sqlite_scan('{DbPath}', 'events') where \"id\" > 100)",
            SqliteSql.ScanFragment(DbPath, spec));
    }

    [Fact]
    public void Inclusive_lower_bound_renders_gte()
    {
        var spec = new DatasetSpec("appdb", "events", new Dictionary<string, object?>())
        {
            WatermarkCursor = "id",
            WatermarkValue = "100",
            WatermarkLowerInclusive = true,
        };

        Assert.Equal(
            $"(select * from sqlite_scan('{DbPath}', 'events') where \"id\" >= 100)",
            SqliteSql.ScanFragment(DbPath, spec));
    }

    [Fact]
    public void Window_upper_bound_joins_the_predicate_chain()
    {
        var spec = new DatasetSpec("appdb", "events", new Dictionary<string, object?>())
        {
            WatermarkCursor = "updated_at",
            WatermarkValue = "2026-01-01T00:00:00.000000",
            WatermarkUpperBound = "2026-01-02T00:00:00.000000",
        };

        Assert.Equal(
            $"(select * from sqlite_scan('{DbPath}', 'events') " +
            "where \"updated_at\" > '2026-01-01 00:00:00.000000' " +
            "and \"updated_at\" <= '2026-01-02 00:00:00.000000')",
            SqliteSql.ScanFragment(DbPath, spec));
    }

    [Theory]
    [InlineData("42", "42")]
    [InlineData("-7", "-7")]
    [InlineData("3.50", "3.50")]
    [InlineData("2026-08-19", "'2026-08-19'")]
    [InlineData("2026-08-19T12:30:00.000001", "'2026-08-19 12:30:00.000001'")]
    public void Watermark_literals_render_by_canonical_shape(string canonical, string expected)
    {
        // The T→space conversion matters more here than for MySQL: a text-stored sqlite cursor
        // compares LEXICALLY, and sqlite's own timestamp convention is the space form.
        Assert.Equal(expected, SqliteSql.RenderWatermarkLiteral(canonical));
    }

    [Fact]
    public void Identifiers_literals_and_paths_are_injection_safe()
    {
        var spec = new DatasetSpec("appdb", "ev'en\"ts", new Dictionary<string, object?>()) { WatermarkCursor = "up\"dated", WatermarkValue = "o'clock" };
        Assert.Equal(
            "(select * from sqlite_scan('/da''ta/app.db', 'ev''en\"ts') " +
            "where \"up\"\"dated\" > 'o''clock')",
            SqliteSql.ScanFragment("/da'ta/app.db", spec));
    }

    [Fact]
    public void Resolve_path_joins_relative_paths_against_base_dir()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["path"] = "data/app.db",
            ["base_dir"] = "/proj",
        });

        Assert.Equal(Path.GetFullPath(Path.Combine("/proj", "data/app.db")), SqliteSql.ResolvePath(config));
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

        Assert.Equal(absolute, SqliteSql.ResolvePath(config));
    }

    [Fact]
    public void Resolve_path_without_base_dir_falls_back_to_the_working_directory()
    {
        // Hosts that never inject base_dir (the TestKit, a bare engine embed) still resolve.
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["path"] = "data/app.db" });
        Assert.Equal(Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "data/app.db")), SqliteSql.ResolvePath(config));
    }

    [Fact]
    public void Resolve_path_requires_path()
    {
        var ex = Assert.Throws<PzConnectorException>(() => SqliteSql.ResolvePath(new ConnectorConfig(new Dictionary<string, object?>())));
        Assert.False(ex.IsTransient);
        Assert.Contains("path", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Append_copy_is_a_create_if_missing_plus_insert_batch()
    {
        var spec = new OutputSpec("wh", "events_out", "append", "fail_on_change", new Dictionary<string, object?>());
        var sink = new SqliteSink(new ConnectorConfig(new Dictionary<string, object?> { ["path"] = DbPath }));
        Assert.True(sink.TryGetNativeCopy(spec, out var copy));

        Assert.Equal(
            $"create table if not exists {WhSinkAlias}.\"events_out\" as select * from {{{{source}}}} limit 0;\n" +
            $"insert into {WhSinkAlias}.\"events_out\" select * from {{{{source}}}};", copy!.CopySql);
        Assert.Equal("sqlite insert", copy.Mechanism);
        Assert.Empty(copy.Finalizations);
    }

    [Fact]
    public void Replace_copy_is_a_single_create_or_replace()
    {
        var spec = new OutputSpec("wh", "events_out", "replace", "fail_on_change", new Dictionary<string, object?>());
        var sink = new SqliteSink(new ConnectorConfig(new Dictionary<string, object?> { ["path"] = DbPath }));
        Assert.True(sink.TryGetNativeCopy(spec, out var copy));

        Assert.Equal(
            $"create or replace table {WhSinkAlias}.\"events_out\" as select * from {{{{source}}}}", copy!.CopySql);
        Assert.Equal("sqlite create-or-replace", copy.Mechanism);
    }

    [Fact]
    public void Merge_mode_has_no_native_copy()
    {
        var spec = new OutputSpec("wh", "events_out", "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = ["id"] };
        var sink = new SqliteSink(new ConnectorConfig(new Dictionary<string, object?> { ["path"] = DbPath }));
        Assert.False(sink.TryGetNativeCopy(spec, out _));
    }

    [Fact]
    public void Sink_copy_attach_embeds_the_resolved_absolute_path()
    {
        var spec = new OutputSpec("wh", "events_out", "append", "fail_on_change", new Dictionary<string, object?>());
        var sink = new SqliteSink(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["path"] = "data/app.db",
            ["base_dir"] = "/proj",
        }));

        Assert.True(sink.TryGetNativeCopy(spec, out var copy));
        var expected = SqliteSql.EscapeLiteral(Path.GetFullPath(Path.Combine("/proj", "data/app.db")));
        Assert.Equal($"attach if not exists '{expected}' as {WhSinkAlias} (type sqlite)", copy!.SetupStatements[2]);
    }
}
