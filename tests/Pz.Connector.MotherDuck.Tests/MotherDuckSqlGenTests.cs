using Pz.Connectors.Abstractions;

namespace Pz.Connector.MotherDuck.Tests;

/// <summary>Offline proof of the connector's whole data plane — which IS these strings: the setup
/// list (extension, session token, alias-less attach), database-qualified entity quoting, the scan
/// fragment with contract pruning and watermark rendering, and the three copy statements.</summary>
public sealed class MotherDuckSqlGenTests
{
    private const string Db = "\"lake\"";

    private static DatasetSpec Spec(Dictionary<string, object?>? options = null) => new("wh", "events", options ?? []);

    private static ConnectorConfig Config(string database = "lake", string token = "md'tok") =>
        new(new Dictionary<string, object?> { ["database"] = database, ["token"] = token });

    [Fact]
    public void Database_is_the_quoted_attach_name()
    {
        Assert.Equal(Db, MotherDuckSql.Database(Config()));
        Assert.Equal("\"my\"\"db\"", MotherDuckSql.Database(Config("my\"db")));
    }

    [Fact]
    public void Setup_is_extension_session_token_then_alias_less_attach()
    {
        Assert.Equal(
            [
                "install motherduck", "load motherduck",
                "set motherduck_token = 'md''tok'",
                "attach if not exists 'md:lake'",
            ],
            MotherDuckSql.SetupStatements(Config()));
    }

    [Fact]
    public void The_token_never_rides_the_attach()
    {
        var statements = MotherDuckSql.SetupStatements(Config(token: "MDTOKEN"));
        Assert.DoesNotContain("MDTOKEN", statements[^1], StringComparison.Ordinal);
        Assert.Single(statements, s => s.Contains("MDTOKEN", StringComparison.Ordinal));
    }

    [Fact]
    public void Setup_requires_database_and_token()
    {
        Assert.False(Assert.Throws<PzConnectorException>(
            () => MotherDuckSql.SetupStatements(new ConnectorConfig(new Dictionary<string, object?> { ["database"] = "lake" }))).IsTransient);
        Assert.False(Assert.Throws<PzConnectorException>(
            () => MotherDuckSql.SetupStatements(new ConnectorConfig(new Dictionary<string, object?> { ["token"] = "t" }))).IsTransient);
    }

    [Fact]
    public void Entities_qualify_under_the_database()
    {
        Assert.Equal($"{Db}.\"events\"", MotherDuckSql.QualifiedTable(Db, "events"));
        Assert.Equal($"{Db}.\"raw\".\"events\"", MotherDuckSql.QualifiedTable(Db, "raw.events"));
        foreach (var bad in new[] { "a.b.c", ".x", "x.", "" })
        {
            Assert.False(Assert.Throws<PzConnectorException>(() => MotherDuckSql.SplitEntity(bad)).IsTransient);
        }
    }

    [Fact]
    public void Scan_fragment_prunes_and_pushes_the_watermark_window()
    {
        Assert.Equal($"{Db}.\"events\"", MotherDuckSql.ScanFragment(Db, Spec()));

        var spec = new DatasetSpec("wh", "events", new Dictionary<string, object?>
        {
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        })
        {
            WatermarkCursor = "id",
            WatermarkValue = "100",
            WatermarkUpperBound = "200",
        };
        Assert.Equal($"(select \"id\", \"name\" from {Db}.\"events\" where \"id\" > 100 and \"id\" <= 200)", MotherDuckSql.ScanFragment(Db, spec));

        var inclusive = Spec() with { WatermarkCursor = "id", WatermarkValue = "100", WatermarkLowerInclusive = true };
        Assert.Equal($"(select * from {Db}.\"events\" where \"id\" >= 100)", MotherDuckSql.ScanFragment(Db, inclusive));
    }

    [Theory]
    [InlineData("42", "42")]
    [InlineData("-7", "-7")]
    [InlineData("3.50", "3.50")]
    [InlineData("2026-08-19", "'2026-08-19'")]
    [InlineData("2026-08-19T12:30:00.000001", "'2026-08-19 12:30:00.000001'")]
    public void Watermark_literals_render_by_canonical_shape(string canonical, string expected) =>
        Assert.Equal(expected, MotherDuckSql.RenderWatermarkLiteral(canonical));

    [Fact]
    public void Copy_statements_cover_append_replace_and_merge()
    {
        var table = $"{Db}.\"events\"";
        Assert.True(MotherDuckSql.TryCopySql(table, "append", [], out var append, out var m1));
        Assert.Equal($"create table if not exists {table} as select * from {{{{source}}}} limit 0;\ninsert into {table} select * from {{{{source}}}};", append);
        Assert.Equal("motherduck insert", m1);

        Assert.True(MotherDuckSql.TryCopySql(table, "replace", [], out var replace, out var m2));
        Assert.Equal($"create or replace table {table} as select * from {{{{source}}}}", replace);
        Assert.Equal("motherduck create-or-replace", m2);

        Assert.True(MotherDuckSql.TryCopySql(table, "merge", ["id"], out var merge, out var m3));
        Assert.Equal(
            $"create table if not exists {table} as select * from {{{{source}}}} limit 0;\n" +
            $"merge into {table} as t using (select s.* from {{{{source}}}} as s qualify row_number() over (partition by s.\"id\") = 1) as s " +
            "on t.\"id\" = s.\"id\" when matched then update when not matched then insert;",
            merge);
        Assert.Equal("motherduck merge", m3);

        Assert.False(Assert.Throws<PzConnectorException>(() => MotherDuckSql.TryCopySql(table, "merge", [], out _, out _)).IsTransient);
        Assert.False(MotherDuckSql.TryCopySql(table, "upsert_all", [], out _, out _));
    }
}
