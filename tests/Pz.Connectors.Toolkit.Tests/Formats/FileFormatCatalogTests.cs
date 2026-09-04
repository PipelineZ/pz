using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connectors.Toolkit.Tests.Formats;

/// <summary>The catalog's output IS the data plane of five connectors: these strings are goldens.</summary>
public sealed class FileFormatCatalogTests
{
    private static Dictionary<string, object?> Opts(string? format = null, Dictionary<string, string>? columns = null)
    {
        var o = new Dictionary<string, object?>();
        if (format is not null) o["format"] = format;
        if (columns is not null) o["columns"] = columns;
        return o;
    }

    private static string Duck(string typeName, string column) => typeName switch
    {
        "bigint" => "BIGINT",
        "varchar" => "VARCHAR",
        "int" => "INTEGER",
        _ => throw new PzConnectorException($"column '{column}': unknown type '{typeName}'", isTransient: false),
    };

    private static FormatReadRequest Req(string url = "'s3://b/k.csv'", int files = 1, Dictionary<string, string>? declared = null) =>
        new(url, files, declared, Duck);

    [Fact]
    public void Resolve_uses_default_when_format_absent()
    {
        Assert.Equal("csv", FileFormatCatalog.Resolve(Opts(), "csv", "localfiles", "dataset 'x'").Name);
        Assert.Equal("parquet", FileFormatCatalog.Resolve(Opts(), "parquet", "s3", "dataset 'x'").Name);
    }

    [Fact]
    public void Resolve_without_default_and_without_format_is_PZ0361()
    {
        var ex = Assert.Throws<PzConnectorException>(() => FileFormatCatalog.Resolve(Opts(), null, "s3", "output 'o'"));
        Assert.False(ex.IsTransient);
        Assert.StartsWith("PZ0361: output 'o': s3 requires 'format'", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Resolve_unknown_format_is_PZ0361_naming_the_supported_set()
    {
        var ex = Assert.Throws<PzConnectorException>(() => FileFormatCatalog.Resolve(Opts("orc"), "csv", "gcs", "dataset 'd'"));
        Assert.Equal("PZ0361: dataset 'd': gcs does not support format 'orc' (supported: csv, json, parquet)", ex.Message);
    }

    [Fact]
    public void Resolve_is_case_insensitive_and_returns_the_canonical_name()
    {
        Assert.Equal("parquet", FileFormatCatalog.Resolve(Opts("Parquet"), "csv", "localfiles", "dataset 'x'").Name);
    }

    [Fact]
    public void Csv_fragments_match_the_two_state_contract_model()
    {
        var csv = FileFormatCatalog.Resolve(Opts("csv"), null, "s3", "dataset 'd'");
        Assert.Equal("read_csv('s3://b/k.csv', header = true, auto_detect = true)",
            FileFormatCatalog.ReadFragment(csv, Opts("csv"), Req(), "dataset 'd'"));
        var declared = new Dictionary<string, string> { ["id"] = "bigint", ["it's"] = "varchar" };
        Assert.Equal("read_csv('s3://b/k.csv', header = true, auto_detect = false, columns = {'id': 'BIGINT', 'it''s': 'VARCHAR'})",
            FileFormatCatalog.ReadFragment(csv, Opts("csv"), Req(declared: declared), "dataset 'd'"));
        Assert.True(FileFormatCatalog.SchemaInferred(csv, null));
        Assert.False(FileFormatCatalog.SchemaInferred(csv, declared));
        Assert.Equal("sniff_csv('s3://b/k.csv')", FileFormatCatalog.SniffFragment(csv, Opts("csv"), "'s3://b/k.csv'"));
        Assert.Equal("read_csv", FileFormatCatalog.ReadMechanism(csv));
    }

    [Fact]
    public void Json_fragments_are_newline_delimited_in_both_states()
    {
        var json = FileFormatCatalog.Resolve(Opts("json"), null, "s3", "dataset 'd'");
        Assert.Equal("read_json('s3://b/k.csv', auto_detect = true, format = 'newline_delimited')",
            FileFormatCatalog.ReadFragment(json, Opts("json"), Req(), "dataset 'd'"));
        var declared = new Dictionary<string, string> { ["id"] = "bigint" };
        Assert.Equal("read_json('s3://b/k.csv', columns = {'id': 'BIGINT'}, format = 'newline_delimited')",
            FileFormatCatalog.ReadFragment(json, Opts("json"), Req(declared: declared), "dataset 'd'"));
        Assert.Null(FileFormatCatalog.SniffFragment(json, Opts("json"), "'s3://b/k.csv'"));
        Assert.Equal("read_json", FileFormatCatalog.ReadMechanism(json));
    }

    [Fact]
    public void Parquet_fragment_ignores_the_contract_and_is_never_inferred()
    {
        var parquet = FileFormatCatalog.Resolve(Opts("parquet"), null, "s3", "dataset 'd'");
        var declared = new Dictionary<string, string> { ["id"] = "bigint" };
        Assert.Equal("read_parquet(['a', 'b'])",
            FileFormatCatalog.ReadFragment(parquet, Opts("parquet"), Req("['a', 'b']", 2, declared), "dataset 'd'"));
        Assert.False(FileFormatCatalog.SchemaInferred(parquet, null));
        Assert.Equal("read_parquet", FileFormatCatalog.ReadMechanism(parquet));
    }

    [Fact]
    public void Copy_clauses_are_the_three_existing_shapes()
    {
        Assert.Equal("format parquet", FileFormatCatalog.CopyClause(FileFormatCatalog.Resolve(Opts("parquet"), null, "s3", "output 'o'"), Opts("parquet"), "output 'o'"));
        Assert.Equal("format csv, header", FileFormatCatalog.CopyClause(FileFormatCatalog.Resolve(Opts("csv"), null, "s3", "output 'o'"), Opts("csv"), "output 'o'"));
        Assert.Equal("format json", FileFormatCatalog.CopyClause(FileFormatCatalog.Resolve(Opts("json"), null, "s3", "output 'o'"), Opts("json"), "output 'o'"));
    }

    [Fact]
    public void Extensions_and_setup_statements_are_empty_for_the_builtin_formats()
    {
        foreach (var name in new[] { "csv", "json", "parquet" })
        {
            var f = FileFormatCatalog.Resolve(Opts(name), null, "s3", "dataset 'd'");
            Assert.Equal(name, f.Extension);
            Assert.Empty(FileFormatCatalog.SetupStatements(f));
            Assert.True(f.NativeRead);
            Assert.True(f.NativeWrite);
        }
    }

    [Fact]
    public void EnsureWritable_and_universal_tier_accept_the_builtin_formats()
    {
        var csv = FileFormatCatalog.Resolve(Opts("csv"), null, "s3", "output 'o'");
        FileFormatCatalog.EnsureWritable(csv, "s3", "output 'o'");
        FileFormatCatalog.EnsureUniversalTierSupported(csv, Opts("csv"), "sftp", "output 'o'");
    }

    [Fact]
    public void Schema_properties_carry_the_format_enum()
    {
        Assert.Contains("\"format\": { \"enum\": [\"csv\", \"json\", \"parquet\"] }", FileFormatCatalog.SchemaProperties, StringComparison.Ordinal);
        Assert.Equal(["csv", "json", "parquet"], FileFormatCatalog.Names);
    }
}
