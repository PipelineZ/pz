using Pz.Connectors.Abstractions;

namespace Pz.Connector.Quack.Tests;

/// <summary>Offline proof of the connector's whole data plane — which IS these strings: the setup
/// list (extension, scoped secret, attach), alias derivation, entity quoting, the scan fragment
/// with contract pruning and watermark rendering, and the three copy statements.</summary>
public sealed class QuackSqlGenTests
{
    private const string WhAlias = "pz_quack_wh_509bcf06";
    private const string Uri = "quack:lake.internal:9494";

    private static DatasetSpec Spec(Dictionary<string, object?>? options = null) => new("wh", "events", options ?? []);

    private static ConnectorConfig Config(string token = "t0k'en") =>
        new(new Dictionary<string, object?> { ["uri"] = Uri, ["token"] = token });

    [Theory]
    [InlineData("quack:localhost", "localhost", 9494)]
    [InlineData("quack:lake.internal:18443", "lake.internal", 18443)]
    [InlineData("quack://lake.internal:18443", "lake.internal", 18443)]
    [InlineData("quack://lake.internal:9494/", "lake.internal", 9494)]
    [InlineData("quack://lake.internal/", "lake.internal", 9494)]
    [InlineData("quack:10.0.0.7:18443", "10.0.0.7", 18443)]
    [InlineData("quack:[::1]", "[::1]", 9494)]
    [InlineData("quack:[::1]:18443", "[::1]", 18443)]
    [InlineData("quack://[fe80::1%25eth0]:9494/", "[fe80::1%25eth0]", 9494)]
    public void Uris_parse_host_and_port(string uri, string host, int port)
    {
        Assert.True(QuackUri.TryParse(uri, out var h, out var p));
        Assert.Equal(host, h);
        Assert.Equal(port, p);
    }

    [Theory]
    [InlineData("http://x")]
    [InlineData("quack:")]
    [InlineData("quack:host:notaport")]
    [InlineData("quack://lake.internal/db")]
    [InlineData("quack:::1")]
    [InlineData("quack:[::1")]
    [InlineData("quack:[]")]
    [InlineData("quack:[::1]x")]
    [InlineData("quack:[::1]:")]
    [InlineData("quack:host:1:2")]
    public void Non_quack_uris_do_not_parse(string uri) => Assert.False(QuackUri.TryParse(uri, out _, out _));

    [Fact]
    public void Alias_and_secret_derive_from_the_raw_connection_name()
    {
        Assert.Equal(WhAlias, QuackSql.Alias("wh"));
        Assert.Equal("pz_quack_my_wh_2_4c668b3d", QuackSql.Alias("my-wh.2"));
        Assert.NotEqual(QuackSql.Alias("prod-db"), QuackSql.Alias("prod_db"));
        Assert.Equal(WhAlias + "_secret", QuackSql.SecretName(WhAlias));
    }

    [Fact]
    public void Setup_is_extension_scoped_secret_then_attach()
    {
        Assert.Equal(
            [
                "install quack", "load quack",
                $"create or replace secret {WhAlias}_secret (type quack, token 't0k''en', scope '{Uri}')",
                $"attach if not exists '{Uri}' as {WhAlias}",
            ],
            QuackSql.SetupStatements(Config(), WhAlias));
    }

    /// <summary>Every accepted uri spelling must canonicalise to the identical <c>quack:host:port</c>
    /// form in both the secret's scope and the attach string — the scoped secret only matches an
    /// attach naming the server identically.</summary>
    [Fact]
    public void Every_accepted_uri_spelling_yields_the_same_secret_and_attach()
    {
        var expected = QuackSql.SetupStatements(Config(), WhAlias);
        foreach (var uri in new[] { "quack:lake.internal", "quack:lake.internal:9494", "quack://lake.internal:9494" })
        {
            var config = new ConnectorConfig(new Dictionary<string, object?> { ["uri"] = uri, ["token"] = "t0k'en" });
            Assert.Equal(expected, QuackSql.SetupStatements(config, WhAlias));
        }

        // A bracketed IPv6 literal canonicalises with its brackets, which is the form the server prints.
        var v6 = new ConnectorConfig(new Dictionary<string, object?> { ["uri"] = "quack://[::1]/", ["token"] = "t0k'en" });
        Assert.Equal(
            [
                "install quack", "load quack",
                $"create or replace secret {WhAlias}_secret (type quack, token 't0k''en', scope 'quack:[::1]:9494')",
                $"attach if not exists 'quack:[::1]:9494' as {WhAlias}",
            ],
            QuackSql.SetupStatements(v6, WhAlias));
    }

    [Fact]
    public void Setup_rejects_an_unparsable_uri_permanently()
    {
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["uri"] = "http://x", ["token"] = "t0k'en" });
        Assert.False(Assert.Throws<PzConnectorException>(() => QuackSql.SetupStatements(config, WhAlias)).IsTransient);
    }

    [Fact]
    public void The_token_never_rides_the_attach()
    {
        var statements = QuackSql.SetupStatements(Config("QTOKEN"), WhAlias);
        Assert.DoesNotContain("QTOKEN", statements[^1], StringComparison.Ordinal);
        Assert.Single(statements, s => s.Contains("QTOKEN", StringComparison.Ordinal));
    }

    [Fact]
    public void Setup_requires_uri_and_token()
    {
        Assert.False(Assert.Throws<PzConnectorException>(
            () => QuackSql.SetupStatements(new ConnectorConfig(new Dictionary<string, object?> { ["uri"] = Uri }), WhAlias)).IsTransient);
        Assert.False(Assert.Throws<PzConnectorException>(
            () => QuackSql.SetupStatements(new ConnectorConfig(new Dictionary<string, object?> { ["token"] = "abcd" }), WhAlias)).IsTransient);
    }

    [Fact]
    public void Entities_split_into_schema_and_table_and_quote_safely()
    {
        Assert.Equal($"{WhAlias}.\"events\"", QuackSql.QualifiedTable(WhAlias, "events"));
        Assert.Equal($"{WhAlias}.\"raw\".\"events\"", QuackSql.QualifiedTable(WhAlias, "raw.events"));
        Assert.Equal($"{WhAlias}.\"we\"\"ird\".\"ev\"\"ents\"", QuackSql.QualifiedTable(WhAlias, "we\"ird.ev\"ents"));
        foreach (var bad in new[] { "a.b.c", ".x", "x.", "" })
        {
            Assert.False(Assert.Throws<PzConnectorException>(() => QuackSql.SplitEntity(bad)).IsTransient);
        }
    }

    [Fact]
    public void Scan_fragment_prunes_and_pushes_the_watermark_window()
    {
        Assert.Equal($"{WhAlias}.\"events\"", QuackSql.ScanFragment(WhAlias, Spec()));

        var spec = new DatasetSpec("wh", "events", new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        })
        {
            WatermarkCursor = "id",
            WatermarkValue = "100",
            WatermarkUpperBound = "200",
        };
        Assert.Equal(
            $"(select \"id\", \"name\" from {WhAlias}.\"events\" where \"id\" > 100 and \"id\" <= 200)",
            QuackSql.ScanFragment(WhAlias, spec));

        var inclusive = Spec() with { WatermarkCursor = "id", WatermarkValue = "100", WatermarkLowerInclusive = true };
        Assert.Equal($"(select * from {WhAlias}.\"events\" where \"id\" >= 100)", QuackSql.ScanFragment(WhAlias, inclusive));
    }

    [Theory]
    [InlineData("42", "42")]
    [InlineData("-7", "-7")]
    [InlineData("3.50", "3.50")]
    [InlineData("2026-08-19", "'2026-08-19'")]
    [InlineData("2026-08-19T12:30:00.000001", "'2026-08-19 12:30:00.000001'")]
    public void Watermark_literals_render_by_canonical_shape(string canonical, string expected) =>
        Assert.Equal(expected, QuackSql.RenderWatermarkLiteral(canonical));

    [Fact]
    public void Scan_predicates_are_injection_safe()
    {
        var spec = new DatasetSpec("wh", "ev'en\"ts", new Dictionary<string, object?>()) { WatermarkCursor = "up\"dated", WatermarkValue = "o'clock" };
        Assert.Equal($"(select * from {WhAlias}.\"ev'en\"\"ts\" where \"up\"\"dated\" > 'o''clock')", QuackSql.ScanFragment(WhAlias, spec));
    }

    [Fact]
    public void Copy_statements_cover_append_replace_and_merge()
    {
        var table = $"{WhAlias}.\"events\"";
        Assert.True(QuackSql.TryCopySql(table, "append", [], out var append, out var m1));
        Assert.Equal($"create table if not exists {table} as select * from {{{{source}}}} limit 0;\ninsert into {table} select * from {{{{source}}}};", append);
        Assert.Equal("quack insert", m1);

        Assert.True(QuackSql.TryCopySql(table, "replace", [], out var replace, out var m2));
        Assert.Equal($"create or replace table {table} as select * from {{{{source}}}}", replace);
        Assert.Equal("quack create-or-replace", m2);

        Assert.True(QuackSql.TryCopySql(table, "merge", ["id", "region"], out var merge, out var m3));
        const string scratch = "pz_quack_merge_ec11af20";
        Assert.Equal(
            $"create table if not exists {table} as select * from {{{{source}}}} limit 0;\n" +
            $"create or replace temp table {scratch} as select s.* from {{{{source}}}} as s " +
            "qualify row_number() over (partition by s.\"id\", s.\"region\") = 1 union all by name " +
            $"select t.* from {table} as t where not exists (select 1 from {{{{source}}}} as s " +
            "where t.\"id\" = s.\"id\" and t.\"region\" = s.\"region\");\n" +
            $"create or replace table {table} as select * from {scratch};\n" +
            $"drop table {scratch};",
            merge);
        Assert.Equal("quack merge-by-replace", m3);

        Assert.False(Assert.Throws<PzConnectorException>(() => QuackSql.TryCopySql(table, "merge", [], out _, out _)).IsTransient);
        Assert.False(QuackSql.TryCopySql(table, "upsert_all", [], out _, out _));
    }
}
