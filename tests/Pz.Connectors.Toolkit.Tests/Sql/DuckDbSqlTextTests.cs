using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;
using Pz.Connectors.Toolkit.Sql;

namespace Pz.Connectors.Toolkit.Tests.Sql;

public sealed class DuckDbSqlTextTests
{
    [Fact]
    public void Identifiers_and_literals_are_injection_safe()
    {
        Assert.Equal("\"ev\"\"ents\"", DuckDbSqlText.QuoteIdentifier("ev\"ents"));
        Assert.Equal("o''clock", DuckDbSqlText.EscapeLiteral("o'clock"));
    }

    // The T→space conversion lets the literal coerce against DATE/TIMESTAMP columns and still compare
    // correctly against a text-stored cursor (SQLite's own timestamp convention is the space form).
    [Theory]
    [InlineData("42", "42")]
    [InlineData("-7", "-7")]
    [InlineData("3.50", "3.50")]
    [InlineData("2026-08-19", "'2026-08-19'")]
    [InlineData("2026-08-19T12:30:00.000001", "'2026-08-19 12:30:00.000001'")]
    [InlineData("1.2.3", "'1.2.3'")]
    [InlineData("o'clock", "'o''clock'")]
    public void Watermark_literals_render_by_canonical_shape(string canonical, string expected) =>
        Assert.Equal(expected, DuckDbSqlText.WatermarkLiteral(canonical));

    [Fact]
    public void Connection_aliases_keep_names_that_sanitize_alike_apart()
    {
        var dashed = DuckDbSqlText.ConnectionAlias("pz_duckdb", "prod-db");
        var underscored = DuckDbSqlText.ConnectionAlias("pz_duckdb", "prod_db");

        Assert.StartsWith("pz_duckdb_prod_db_", dashed);
        Assert.StartsWith("pz_duckdb_prod_db_", underscored);
        Assert.NotEqual(dashed, underscored);
        Assert.Equal(dashed, DuckDbSqlText.ConnectionAlias("pz_duckdb", "prod-db"));
    }

    [Fact]
    public void Stable_suffix_is_eight_lowercase_hex_chars_of_sha256()
    {
        // SHA-256("abc") = ba7816bf...
        Assert.Equal("ba7816bf", DuckDbSqlText.StableSuffix("abc"));
    }

    [Fact]
    public void Contract_columns_are_null_when_none_are_declared()
    {
        var none = new DatasetSpec("c", "e", new Dictionary<string, object?>());
        var empty = new DatasetSpec("c", "e", new Dictionary<string, object?> { ["columns"] = new Dictionary<string, string>() });
        var declared = new DatasetSpec("c", "e", new Dictionary<string, object?> { ["columns"] = new Dictionary<string, string> { ["id"] = "int" } });

        Assert.Null(ColumnTypeCatalog.ContractColumns(none));
        Assert.Null(ColumnTypeCatalog.ContractColumns(empty));
        Assert.Equal("int", ColumnTypeCatalog.ContractColumns(declared)!["id"]);
    }
}
