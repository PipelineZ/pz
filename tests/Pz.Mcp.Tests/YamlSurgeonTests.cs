using Pz.Core.Loading;
using Pz.Core.Validation;
using Pz.Mcp.Editing;

namespace Pz.Mcp.Tests;

/// <summary>Golden-style tests for the surgical, comment-preserving YAML editor: every
/// scenario asserts the FULL file text before/after, so any byte-level drift outside the edited block
/// fails the test immediately.</summary>
public sealed class YamlSurgeonTests : IDisposable
{
    private readonly string _tmp = Path.Combine(Path.GetTempPath(), "pz-yamlsurgeon-" + Guid.NewGuid().ToString("N"));

    public YamlSurgeonTests() => Directory.CreateDirectory(_tmp);

    public void Dispose()
    {
        try { Directory.Delete(_tmp, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static string PgBlock(int indentLevels) =>
        CanonicalYaml.MappingEntry("pg", new Dictionary<string, object?>
        {
            ["connector"] = "postgres",
            ["connection"] = new Dictionary<string, object?> { ["host"] = "${PGHOST}" },
        }, indentLevels);

    // ---- Scenario 1: insert preserves comments outside the new block -------------------------------

    [Fact]
    public void Insert_preserves_comments_outside_the_new_block()
    {
        var file = Path.Combine(_tmp, "connections.yml");
        File.WriteAllText(file,
            "# my warehouse project\nconnections:\n  raw:\n    connector: localfiles\n" +
            "    connection:\n      root: data   # relative to project\n");

        YamlSurgeon.InsertMappingEntry(file, ["connections"], "pg", PgBlock(1));

        Assert.Equal(
            "# my warehouse project\nconnections:\n  raw:\n    connector: localfiles\n" +
            "    connection:\n      root: data   # relative to project\n" +
            "  pg:\n    connector: postgres\n    connection:\n      host: ${PGHOST}\n",
            File.ReadAllText(file));
    }

    [Fact]
    public void Insert_preserves_a_comment_between_existing_entries()
    {
        var file = Path.Combine(_tmp, "connections.yml");
        File.WriteAllText(file,
            "# my warehouse project\nconnections:\n  raw:\n    connector: localfiles\n\n" +
            "  # a second connection\n  out:\n    connector: localfiles\n    connection:\n      root: out\n");

        YamlSurgeon.InsertMappingEntry(file, ["connections"], "pg", PgBlock(1));

        Assert.Equal(
            "# my warehouse project\nconnections:\n  raw:\n    connector: localfiles\n\n" +
            "  # a second connection\n  out:\n    connector: localfiles\n    connection:\n      root: out\n" +
            "  pg:\n    connector: postgres\n    connection:\n      host: ${PGHOST}\n",
            File.ReadAllText(file));
    }

    // ---- Scenario 2: insert into an empty/missing mapping and into a missing file ------------------

    [Fact]
    public void Insert_into_an_empty_connections_mapping()
    {
        var file = Path.Combine(_tmp, "connections.yml");
        File.WriteAllText(file, "connections:\n");

        YamlSurgeon.InsertMappingEntry(file, ["connections"], "pg", PgBlock(1));

        Assert.Equal(
            "connections:\n  pg:\n    connector: postgres\n    connection:\n      host: ${PGHOST}\n",
            File.ReadAllText(file));
    }

    [Fact]
    public void Insert_into_a_missing_connections_key()
    {
        var file = Path.Combine(_tmp, "connections.yml");
        File.WriteAllText(file, "# nothing declared yet\n");

        YamlSurgeon.InsertMappingEntry(file, ["connections"], "pg", PgBlock(1));

        Assert.Equal(
            "# nothing declared yet\nconnections:\n  pg:\n    connector: postgres\n    connection:\n      host: ${PGHOST}\n",
            File.ReadAllText(file));
    }

    [Fact]
    public void Insert_into_a_missing_file_creates_it()
    {
        var file = Path.Combine(_tmp, "connections.yml");
        Assert.False(File.Exists(file));

        YamlSurgeon.InsertMappingEntry(file, ["connections"], "pg", PgBlock(1));

        Assert.Equal(
            "connections:\n  pg:\n    connector: postgres\n    connection:\n      host: ${PGHOST}\n",
            File.ReadAllText(file));
    }

    [Fact]
    public void Insert_creates_a_missing_nested_mapping_two_levels_deep()
    {
        // Load-bearing for a later task: ["connections","raw","entities"] where "entities" does not
        // exist yet under an existing "raw" connection.
        var file = Path.Combine(_tmp, "connections.yml");
        File.WriteAllText(file, "connections:\n  raw:\n    connector: localfiles\n    root: data\n");

        var block = CanonicalYaml.MappingEntry("orders", new Dictionary<string, object?>
        {
            ["read"] = new Dictionary<string, object?> { ["path"] = "orders.csv" },
        }, indentLevels: 3);

        YamlSurgeon.InsertMappingEntry(file, ["connections", "raw", "entities"], "orders", block);

        Assert.Equal(
            "connections:\n  raw:\n    connector: localfiles\n    root: data\n" +
            "    entities:\n      orders:\n        read:\n          path: orders.csv\n",
            File.ReadAllText(file));
    }

    // ---- Scenario 3: replace returns true when the replaced block held a comment -------------------

    [Fact]
    public void Replace_reports_a_dropped_comment_and_leaves_siblings_untouched()
    {
        var file = Path.Combine(_tmp, "connections.yml");
        File.WriteAllText(file,
            "connections:\n  raw:\n    connector: localfiles   # was localfiles\n    root: data\n" +
            "  out:\n    connector: localfiles\n    root: out\n");

        var dropped = YamlSurgeon.ReplaceMappingEntry(file, ["connections"], "raw",
            CanonicalYaml.MappingEntry("raw", new Dictionary<string, object?>
            {
                ["connector"] = "s3",
                ["root"] = "s3://bucket/prefix",
            }, indentLevels: 1));

        Assert.True(dropped);
        Assert.Equal(
            "connections:\n  raw:\n    connector: s3\n    root: s3://bucket/prefix\n" +
            "  out:\n    connector: localfiles\n    root: out\n",
            File.ReadAllText(file));
    }

    [Fact]
    public void Replace_without_a_comment_in_the_replaced_block_returns_false()
    {
        var file = Path.Combine(_tmp, "connections.yml");
        File.WriteAllText(file, "connections:\n  raw:\n    connector: localfiles\n    root: data\n");

        var dropped = YamlSurgeon.ReplaceMappingEntry(file, ["connections"], "raw",
            CanonicalYaml.MappingEntry("raw", new Dictionary<string, object?>
            {
                ["connector"] = "s3",
                ["root"] = "s3://bucket/prefix",
            }, indentLevels: 1));

        Assert.False(dropped);
        Assert.Equal(
            "connections:\n  raw:\n    connector: s3\n    root: s3://bucket/prefix\n",
            File.ReadAllText(file));
    }

    // ---- Scenario 4: remove a middle entry preserves surrounding blank lines/comments --------------

    [Fact]
    public void Remove_a_middle_entry_preserves_surrounding_blank_lines_and_comments()
    {
        var file = Path.Combine(_tmp, "connections.yml");
        File.WriteAllText(file,
            "connections:\n  a:\n    connector: localfiles\n\n" +
            "  # comment about b\n  b:\n    connector: localfiles\n\n" +
            "  c:\n    connector: localfiles\n");

        YamlSurgeon.RemoveMappingEntry(file, ["connections"], "b");

        Assert.Equal(
            "connections:\n  a:\n    connector: localfiles\n\n" +
            "  # comment about b\n\n" +
            "  c:\n    connector: localfiles\n",
            File.ReadAllText(file));
    }

    [Fact]
    public void Remove_the_last_entry_preserves_everything_before_it()
    {
        var file = Path.Combine(_tmp, "connections.yml");
        File.WriteAllText(file,
            "connections:\n  a:\n    connector: localfiles\n  b:\n    connector: localfiles\n    root: data\n");

        YamlSurgeon.RemoveMappingEntry(file, ["connections"], "b");

        Assert.Equal("connections:\n  a:\n    connector: localfiles\n", File.ReadAllText(file));
    }

    // ---- Scenario 5: duplicate insert / missing replace / missing remove --> PZ0602 ----------------

    [Fact]
    public void Insert_of_a_duplicate_key_throws_PZ0602()
    {
        var file = Path.Combine(_tmp, "connections.yml");
        File.WriteAllText(file, "connections:\n  raw:\n    connector: localfiles\n");

        var ex = Assert.Throws<PzConfigException>(() =>
            YamlSurgeon.InsertMappingEntry(file, ["connections"], "raw", PgBlock(1)));
        Assert.Equal(PzErrorCode.McpMutationTarget, ex.Error.Code);
    }

    [Fact]
    public void Replace_of_a_missing_key_throws_PZ0602()
    {
        var file = Path.Combine(_tmp, "connections.yml");
        File.WriteAllText(file, "connections:\n  raw:\n    connector: localfiles\n");

        var ex = Assert.Throws<PzConfigException>(() =>
            YamlSurgeon.ReplaceMappingEntry(file, ["connections"], "nope", PgBlock(1)));
        Assert.Equal(PzErrorCode.McpMutationTarget, ex.Error.Code);
    }

    [Fact]
    public void Remove_of_a_missing_key_throws_PZ0602()
    {
        var file = Path.Combine(_tmp, "connections.yml");
        File.WriteAllText(file, "connections:\n  raw:\n    connector: localfiles\n");

        var ex = Assert.Throws<PzConfigException>(() =>
            YamlSurgeon.RemoveMappingEntry(file, ["connections"], "nope"));
        Assert.Equal(PzErrorCode.McpMutationTarget, ex.Error.Code);
    }

    [Fact]
    public void Remove_from_a_missing_file_throws_PZ0602()
    {
        var file = Path.Combine(_tmp, "connections.yml");

        var ex = Assert.Throws<PzConfigException>(() =>
            YamlSurgeon.RemoveMappingEntry(file, ["connections"], "raw"));
        Assert.Equal(PzErrorCode.McpMutationTarget, ex.Error.Code);
    }

    // ---- Scenario 6: CanonicalYaml.MappingEntry exact rendering -------------------------------------

    [Fact]
    public void CanonicalYaml_renders_a_nested_mapping_exactly()
    {
        var rendered = CanonicalYaml.MappingEntry("pg", new Dictionary<string, object?>
        {
            ["connector"] = "postgres",
            ["connection"] = new Dictionary<string, object?> { ["host"] = "${PGHOST}" },
        }, indentLevels: 1);

        Assert.Equal("  pg:\n    connector: postgres\n    connection:\n      host: ${PGHOST}\n", rendered);
    }

    [Fact]
    public void CanonicalYaml_quotes_scalars_that_would_otherwise_be_mangled()
    {
        var rendered = CanonicalYaml.MappingEntry("e", new Dictionary<string, object?>
        {
            ["a"] = "123",
            ["b"] = "true",
            ["c"] = "  leading space",
            ["d"] = "plain",
        }, indentLevels: 0);

        Assert.Equal(
            "e:\n  a: \"123\"\n  b: \"true\"\n  c: \"  leading space\"\n  d: plain\n",
            rendered);
    }

    // ---- Fix round 1: embedded newline/tab in a quoted scalar must be escaped, not left literal -----

    [Fact]
    public void CanonicalYaml_escapes_embedded_newline_and_tab_in_quoted_scalars()
    {
        // A LONE '\r' (no '\n' after it) must trigger quoting AND be escaped -- otherwise a raw CR is
        // spliced into connections.yml.
        var rendered = CanonicalYaml.MappingEntry("e", new Dictionary<string, object?>
        {
            ["nl"] = "line one\nline two",
            ["tab"] = "col1\tcol2",
            ["cr"] = "before\rafter",
        }, indentLevels: 0);

        Assert.Equal(
            "e:\n  nl: \"line one\\nline two\"\n  tab: \"col1\\tcol2\"\n  cr: \"before\\rafter\"\n",
            rendered);
        Assert.DoesNotContain('\r', rendered.Replace("\\r", string.Empty, StringComparison.Ordinal));
    }

    [Fact]
    public void CanonicalYaml_quoted_newline_and_tab_round_trip_through_YamlMapper()
    {
        var withNewline = "line one\nline two";
        var withTab = "col1\tcol2";
        var withCarriageReturn = "before\rafter";
        var rendered = CanonicalYaml.MappingEntry("e", new Dictionary<string, object?>
        {
            ["nl"] = withNewline,
            ["tab"] = withTab,
            ["cr"] = withCarriageReturn,
        }, indentLevels: 0);

        var file = Path.Combine(_tmp, "roundtrip.yml");
        File.WriteAllText(file, rendered);

        var loaded = YamlMapper.LoadFile(file, "roundtrip.yml");
        var nested = Assert.IsType<Dictionary<string, object?>>(loaded["e"]);
        Assert.Equal(withNewline, nested["nl"]);
        Assert.Equal(withTab, nested["tab"]);
        Assert.Equal(withCarriageReturn, nested["cr"]);
    }

    // ---- Scenario 7: byte-splice purity — the splice never reflows anything outside its range ------

    [Fact]
    public void Splice_is_byte_exact_outside_the_edited_range()
    {
        var file = Path.Combine(_tmp, "connections.yml");
        var before =
            "# my warehouse project\nconnections:\n  raw:\n    connector: localfiles\n" +
            "    connection:\n      root: data   # relative to project\n";
        File.WriteAllText(file, before);

        var inserted = PgBlock(1);
        var spliceStart = before.Length; // appended strictly after the last existing line
        YamlSurgeon.InsertMappingEntry(file, ["connections"], "pg", inserted);

        var after = File.ReadAllText(file);
        Assert.Equal(before[..spliceStart] + inserted + before[spliceStart..], after);
    }
}
