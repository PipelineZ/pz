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
        Assert.Equal("PZ0361: output 'o': s3 requires 'format' (supported: csv, json, parquet, tsv)", ex.Message);
    }

    [Fact]
    public void Resolve_unknown_format_is_PZ0361_naming_the_supported_set()
    {
        var ex = Assert.Throws<PzConnectorException>(() => FileFormatCatalog.Resolve(Opts("orc"), "csv", "gcs", "dataset 'd'"));
        Assert.Equal("PZ0361: dataset 'd': gcs does not support format 'orc' (supported: csv, json, parquet, tsv)", ex.Message);
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
    public void EnsureUniversalTierSupported_refuses_a_format_without_the_universal_tier()
    {
        var native = new FileFormat("x", "x", NativeRead: true, NativeWrite: true, UniversalTier: false, [], new HashSet<string>(StringComparer.Ordinal));
        var ex = Assert.Throws<PzConnectorException>(() =>
            FileFormatCatalog.EnsureUniversalTierSupported(native, Opts(), "sftp", "dataset 'd'"));
        Assert.False(ex.IsTransient);
        Assert.StartsWith("PZ0361: dataset 'd': format 'x' is native-only", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Schema_properties_carry_the_format_enum()
    {
        Assert.Contains("\"format\": { \"enum\": [\"csv\", \"json\", \"parquet\", \"tsv\"] }", FileFormatCatalog.SchemaProperties, StringComparison.Ordinal);
        Assert.Equal(["csv", "json", "parquet", "tsv"], FileFormatCatalog.Names);
    }

    private static Dictionary<string, object?> With(string format, string key, object? value)
    {
        var o = Opts(format);
        o[key] = value;
        return o;
    }

    [Fact]
    public void Tsv_is_csv_with_a_tab_delimiter_and_its_own_extension()
    {
        var o = Opts("tsv");
        var tsv = FileFormatCatalog.Resolve(o, null, "s3", "dataset 'd'");
        Assert.Equal("tsv", tsv.Extension);
        Assert.Equal('\t', FileFormatCatalog.Delimiter(tsv, o, "dataset 'd'"));
        Assert.Equal("read_csv('s3://b/k.csv', header = true, auto_detect = true, delim = '\\t')",
            FileFormatCatalog.ReadFragment(tsv, o, Req(), "dataset 'd'"));
        var declared = new Dictionary<string, string> { ["id"] = "bigint" };
        Assert.Equal("read_csv('s3://b/k.csv', header = true, auto_detect = false, columns = {'id': 'BIGINT'}, delim = '\\t')",
            FileFormatCatalog.ReadFragment(tsv, o, Req(declared: declared), "dataset 'd'"));
        Assert.Equal("format csv, header, delimiter '\\t'", FileFormatCatalog.CopyClause(tsv, o, "output 'o'"));
        Assert.Equal("sniff_csv('s3://b/k.csv', delim = '\\t')", FileFormatCatalog.SniffFragment(tsv, o, "'s3://b/k.csv'"));
        Assert.Equal("read_csv", FileFormatCatalog.ReadMechanism(tsv));
        Assert.True(FileFormatCatalog.SchemaInferred(tsv, null));
    }

    [Fact]
    public void Csv_delimiter_option_changes_fragment_and_copy_clause_only_when_not_a_comma()
    {
        var pipe = With("csv", "delimiter", "|");
        var csv = FileFormatCatalog.Resolve(pipe, null, "s3", "dataset 'd'");
        Assert.Equal('|', FileFormatCatalog.Delimiter(csv, pipe, "dataset 'd'"));
        Assert.Equal("read_csv('s3://b/k.csv', header = true, auto_detect = true, delim = '|')",
            FileFormatCatalog.ReadFragment(csv, pipe, Req(), "dataset 'd'"));
        Assert.Equal("format csv, header, delimiter '|'", FileFormatCatalog.CopyClause(csv, pipe, "output 'o'"));

        var comma = With("csv", "delimiter", ",");
        Assert.Equal("read_csv('s3://b/k.csv', header = true, auto_detect = true)",
            FileFormatCatalog.ReadFragment(FileFormatCatalog.Resolve(comma, null, "s3", "dataset 'd'"), comma, Req(), "dataset 'd'"));

        var quote = With("csv", "delimiter", "'");
        Assert.Equal("format csv, header, delimiter ''''",
            FileFormatCatalog.CopyClause(FileFormatCatalog.Resolve(quote, null, "s3", "output 'o'"), quote, "output 'o'"));
    }

    [Theory]
    [InlineData(";;")]
    [InlineData("")]
    [InlineData("é")]
    [InlineData("\"")]
    [InlineData("\n")]
    public void Csv_delimiter_must_be_one_ascii_character(string bad)
    {
        var ex = Assert.Throws<PzConnectorException>(() => FileFormatCatalog.Resolve(With("csv", "delimiter", bad), null, "s3", "dataset 'd'"));
        Assert.StartsWith(
            "PZ0362: dataset 'd': 'delimiter' must be one ASCII character other than a quote, newline or carriage return",
            ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Delimiter_on_tsv_and_layout_on_csv_are_PZ0362()
    {
        var ex1 = Assert.Throws<PzConnectorException>(() => FileFormatCatalog.Resolve(With("tsv", "delimiter", ","), null, "s3", "dataset 'd'"));
        Assert.Equal("PZ0362: dataset 'd': 'delimiter' is not an option of format 'tsv' -- remove it or change the format", ex1.Message);
        var ex2 = Assert.Throws<PzConnectorException>(() => FileFormatCatalog.Resolve(With("csv", "layout", "array"), null, "s3", "dataset 'd'"));
        Assert.Equal("PZ0362: dataset 'd': 'layout' is not an option of format 'csv' -- remove it or change the format", ex2.Message);
    }

    [Fact]
    public void Json_layout_array_changes_read_and_copy_shapes()
    {
        var o = With("json", "layout", "array");
        var json = FileFormatCatalog.Resolve(o, null, "s3", "dataset 'd'");
        Assert.Equal("array", FileFormatCatalog.JsonLayout(json, o));
        Assert.Equal("read_json('s3://b/k.csv', auto_detect = true, format = 'array')",
            FileFormatCatalog.ReadFragment(json, o, Req(), "dataset 'd'"));
        var declared = new Dictionary<string, string> { ["id"] = "bigint" };
        Assert.Equal("read_json('s3://b/k.csv', columns = {'id': 'BIGINT'}, format = 'array')",
            FileFormatCatalog.ReadFragment(json, o, Req(declared: declared), "dataset 'd'"));
        Assert.Equal("format json, array true", FileFormatCatalog.CopyClause(json, o, "output 'o'"));
        Assert.Equal("ndjson", FileFormatCatalog.JsonLayout(json, Opts("json")));
    }

    [Fact]
    public void Json_layout_must_be_ndjson_or_array()
    {
        var ex = Assert.Throws<PzConnectorException>(() => FileFormatCatalog.Resolve(With("json", "layout", "lines"), null, "s3", "dataset 'd'"));
        Assert.Equal("PZ0362: dataset 'd': 'layout' must be 'ndjson' or 'array' (got 'lines')", ex.Message);
    }

    [Fact]
    public void Json_array_is_native_only_on_the_universal_tier()
    {
        var o = With("json", "layout", "array");
        var json = FileFormatCatalog.Resolve(o, null, "sftp", "output 'o'");
        var ex = Assert.Throws<PzConnectorException>(() => FileFormatCatalog.EnsureUniversalTierSupported(json, o, "sftp", "output 'o'"));
        Assert.StartsWith("PZ0361: output 'o': json 'layout: array' is native-only", ex.Message, StringComparison.Ordinal);
        FileFormatCatalog.EnsureUniversalTierSupported(FileFormatCatalog.Resolve(Opts("tsv"), null, "sftp", "output 'o'"), Opts("tsv"), "sftp", "output 'o'");
    }

    [Fact]
    public void Schema_properties_carry_tsv_delimiter_and_layout()
    {
        Assert.Equal(["csv", "json", "parquet", "tsv"], FileFormatCatalog.Names);
        Assert.Contains("\"delimiter\": { \"type\": \"string\", \"minLength\": 1, \"maxLength\": 1 }", FileFormatCatalog.SchemaProperties, StringComparison.Ordinal);
        Assert.Contains("\"layout\": { \"enum\": [\"ndjson\", \"array\"] }", FileFormatCatalog.SchemaProperties, StringComparison.Ordinal);
    }
}
