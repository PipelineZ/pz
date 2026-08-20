using Pz.Connectors.Abstractions;

namespace Pz.Connector.MySql.Tests;

/// <summary>Offline proof of the connector's whole data plane — which IS these strings:
/// setup-statement shapes, the mysql_query fragment, contract pruning, query: handling, watermark
/// literal rendering, quoting/injection, and the sink copy statements.</summary>
public sealed class MySqlSqlGenTests
{
    private static ConnectorConfig Config(string? user = "pz", string? password = "pw") =>
        new(new Dictionary<string, object?>
        {
            ["host"] = "db.example.com",
            ["database"] = "analytics",
            ["user"] = user,
            ["password"] = password,
        });

    private static DatasetSpec Spec(Dictionary<string, object?>? options = null) =>
        new("wh", "orders", options ?? []);

    private const string WhSourceAlias = "pz_mysql_src_wh_509bcf06";
    private const string WhSinkAlias = "pz_mysql_snk_wh_509bcf06";

    [Fact]
    public void Setup_statements_install_load_create_secret_then_read_only_attach()
    {
        var statements = MySqlSql.SetupStatements(Config(), MySqlSql.SourceAlias("wh"), readOnly: true);

        Assert.Equal(4, statements.Count);
        Assert.Equal("install mysql", statements[0]);
        Assert.Equal("load mysql", statements[1]);
        Assert.Equal(
            $"create or replace secret {WhSourceAlias}_secret (type mysql, host 'db.example.com', " +
            "port 3306, database 'analytics', user 'pz', password 'pw')", statements[2]);
        Assert.Equal(
            $"attach if not exists '' as {WhSourceAlias} (type mysql, secret {WhSourceAlias}_secret, read_only)",
            statements[3]);
    }

    [Fact]
    public void Sink_attach_is_read_write_with_its_own_alias()
    {
        var statements = MySqlSql.SetupStatements(Config(), MySqlSql.SinkAlias("wh"), readOnly: false);
        Assert.Equal(
            $"attach if not exists '' as {WhSinkAlias} (type mysql, secret {WhSinkAlias}_secret)",
            statements[3]);
    }

    [Fact]
    public void Create_secret_honors_port_and_ssl_mode_and_omits_absent_optionals()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["host"] = "h",
            ["database"] = "d",
            ["port"] = 3307,
            ["ssl_mode"] = "required",
        });

        Assert.Equal(
            "create or replace secret my_secret (type mysql, host 'h', port 3307, database 'd', ssl_mode 'required')",
            MySqlSql.CreateSecretSql(config, "my_secret"));
    }

    [Fact]
    public void Create_secret_escapes_a_password_with_a_space_a_quote_and_an_equals_sign()
    {
        // On the secret route a password is an ordinary ''-escaped SQL string literal, not a
        // key=value DSN token, so these characters must render correctly rather than be refused.
        var config = new ConnectorConfig(new Dictionary<string, object?>
        {
            ["host"] = "h",
            ["database"] = "d",
            ["password"] = "p a=s'sw'ord",
        });

        Assert.Equal(
            "create or replace secret my_secret (type mysql, host 'h', port 3306, database 'd', " +
            "password 'p a=s''sw''ord')",
            MySqlSql.CreateSecretSql(config, "my_secret"));
    }

    [Fact]
    public void Alias_appends_a_stable_hash_of_the_raw_connection_name()
    {
        Assert.Equal("pz_mysql_src_my_wh_2_4c668b3d", MySqlSql.SourceAlias("my-wh.2"));
        // Deterministic: a fresh process or run reproduces the exact alias.
        Assert.Equal("pz_mysql_src_my_wh_2_4c668b3d", MySqlSql.SourceAlias("my-wh.2"));
    }

    [Fact]
    public void Alias_hash_disambiguates_connection_names_that_sanitize_the_same_way()
    {
        // "prod-db" and "prod_db" both sanitize to "prod_db" -- without the hash suffix they would
        // collide onto one alias, and `attach if not exists` is first-wins (silent wrong-server I/O).
        var first = MySqlSql.SourceAlias("prod-db");
        var second = MySqlSql.SourceAlias("prod_db");
        Assert.NotEqual(first, second);
        Assert.StartsWith("pz_mysql_src_prod_db_", first, StringComparison.Ordinal);
        Assert.StartsWith("pz_mysql_src_prod_db_", second, StringComparison.Ordinal);
    }

    [Fact]
    public void Plain_scan_selects_star_from_the_entity()
    {
        Assert.True(new MySqlSource(Config()).TryGetNativeScan(Spec(), out var scan));
        Assert.Equal($"mysql_query('{WhSourceAlias}', 'SELECT * FROM `orders`')", scan!.SqlFragment);
        Assert.Equal("mysql_query", scan.Mechanism);
    }

    [Fact]
    public void Declared_contract_prunes_the_read()
    {
        var spec = Spec(new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        });

        Assert.Equal("SELECT `id`, `name` FROM `orders`", MySqlSql.InnerSelect(spec));
    }

    [Fact]
    public void Query_option_goes_to_mysql_verbatim_when_nothing_to_add()
    {
        var spec = Spec(new Dictionary<string, object?> { ["query"] = "SELECT a, b FROM t1 JOIN t2 USING (k);" });
        Assert.Equal("SELECT a, b FROM t1 JOIN t2 USING (k)", MySqlSql.InnerSelect(spec));
    }

    [Fact]
    public void Query_option_wraps_as_subselect_when_watermarked()
    {
        var spec = new DatasetSpec("wh", "orders",
            new Dictionary<string, object?> { ["query"] = "SELECT a, b FROM t1" })
        {
            WatermarkCursor = "b",
            WatermarkValue = "42",
        };

        Assert.Equal("SELECT * FROM (SELECT a, b FROM t1) pzq WHERE `b` > 42", MySqlSql.InnerSelect(spec));
    }

    [Fact]
    public void Plain_incremental_watermark_is_pushed_down()
    {
        // The deliberate divergence from the file connectors: a database source pushes the
        // UNWINDOWED watermark into MySQL — extraction savings are the point of incremental EL here.
        var spec = new DatasetSpec("wh", "orders", new Dictionary<string, object?>()) { WatermarkCursor = "id", WatermarkValue = "100" };
        Assert.Equal("SELECT * FROM `orders` WHERE `id` > 100", MySqlSql.InnerSelect(spec));
    }

    [Fact]
    public void Inclusive_lower_bound_renders_gte()
    {
        var spec = new DatasetSpec("wh", "orders", new Dictionary<string, object?>())
        {
            WatermarkCursor = "id",
            WatermarkValue = "100",
            WatermarkLowerInclusive = true,
        };

        Assert.Equal("SELECT * FROM `orders` WHERE `id` >= 100", MySqlSql.InnerSelect(spec));
    }

    [Fact]
    public void Window_upper_bound_joins_the_predicate_chain()
    {
        var spec = new DatasetSpec("wh", "orders", new Dictionary<string, object?>())
        {
            WatermarkCursor = "updated_at",
            WatermarkValue = "2026-01-01T00:00:00.000000",
            WatermarkUpperBound = "2026-01-02T00:00:00.000000",
        };

        Assert.Equal(
            "SELECT * FROM `orders` WHERE `updated_at` > '2026-01-01 00:00:00.000000' " +
            "AND `updated_at` <= '2026-01-02 00:00:00.000000'", MySqlSql.InnerSelect(spec));
    }

    [Theory]
    [InlineData("42", "42")]
    [InlineData("-7", "-7")]
    [InlineData("3.50", "3.50")]
    [InlineData("2026-08-14", "'2026-08-14'")]
    [InlineData("2026-08-14T12:30:00.000001", "'2026-08-14 12:30:00.000001'")]
    public void Watermark_literals_render_by_canonical_shape(string canonical, string expected)
    {
        Assert.Equal(expected, MySqlSql.RenderWatermarkLiteral(canonical));
    }

    [Fact]
    public void Identifiers_and_literals_are_injection_safe()
    {
        var spec = new DatasetSpec("wh", "or`ders", new Dictionary<string, object?>()) { WatermarkCursor = "up`dated", WatermarkValue = "o'clock" };
        Assert.Equal("SELECT * FROM `or``ders` WHERE `up``dated` > 'o''clock'", MySqlSql.InnerSelect(spec));

        // The whole inner SELECT single-quote-escapes again into the mysql_query literal.
        Assert.True(new MySqlSource(Config()).TryGetNativeScan(spec, out var scan));
        Assert.Contains("WHERE `up``dated` > ''o''''clock''", scan!.SqlFragment, StringComparison.Ordinal);
    }

    [Fact]
    public void Append_copy_is_a_create_if_missing_plus_insert_batch()
    {
        var spec = new OutputSpec("wh", "orders_out", "append", "fail_on_change", new Dictionary<string, object?>());
        Assert.True(new MySqlSink(Config()).TryGetNativeCopy(spec, out var copy));

        Assert.Equal(
            $"create table if not exists {WhSinkAlias}.\"orders_out\" as select * from {{{{source}}}} limit 0;\n" +
            $"insert into {WhSinkAlias}.\"orders_out\" select * from {{{{source}}}};", copy!.CopySql);
        Assert.Equal("mysql insert", copy.Mechanism);
        Assert.Empty(copy.Finalizations);
    }

    [Fact]
    public void Replace_copy_is_a_single_create_or_replace()
    {
        var spec = new OutputSpec("wh", "orders_out", "replace", "fail_on_change", new Dictionary<string, object?>());
        Assert.True(new MySqlSink(Config()).TryGetNativeCopy(spec, out var copy));

        Assert.Equal(
            $"create or replace table {WhSinkAlias}.\"orders_out\" as select * from {{{{source}}}}", copy!.CopySql);
        Assert.Equal("mysql create-or-replace", copy.Mechanism);
    }

    [Fact]
    public void Merge_mode_has_no_native_copy()
    {
        var spec = new OutputSpec("wh", "orders_out", "merge", "fail_on_change", new Dictionary<string, object?>()) { Keys = ["id"] };
        Assert.False(new MySqlSink(Config()).TryGetNativeCopy(spec, out _));
    }
}
