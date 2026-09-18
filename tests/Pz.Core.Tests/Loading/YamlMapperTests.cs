using System.Text;
using Pz.Core.Loading;
using Pz.Core.Validation;

namespace Pz.Core.Tests.Loading;

public class YamlMapperTests
{
    private static object? LoadString(string yaml, out PzError? error)
    {
        var path = Path.Combine(Path.GetTempPath(), $"pz-yamlmapper-{Guid.NewGuid():N}.yml");
        File.WriteAllText(path, yaml);
        try
        {
            var loaded = YamlMapper.LoadFile(path, "project.yml");
            error = null;
            return loaded;
        }
        catch (PzConfigException ex)
        {
            error = ex.Error;
            return null;
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Cyclic_mapping_alias_is_a_config_error_not_a_stack_overflow()
    {
        LoadString("a: &a\n  self: *a\n", out var error);
        Assert.NotNull(error);
        Assert.Equal(PzErrorCode.YamlShape, error.Code);
        Assert.Equal("project.yml", error.File);
        Assert.Contains("alias", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Cyclic_sequence_alias_is_a_config_error_not_a_stack_overflow()
    {
        LoadString("a: &a\n  - *a\n", out var error);
        Assert.NotNull(error);
        Assert.Equal(PzErrorCode.YamlShape, error.Code);
    }

    [Fact]
    public void Scanner_state_exception_is_reported_as_malformed_yaml()
    {
        // YamlDotNet's Scanner throws a raw InvalidOperationException (not YamlException) on a
        // multiline plain scalar inside an unclosed flow sequence; it must still surface as PZ0101.
        LoadString("name: [unclosed\nversion 0.1.0\n", out var error);
        Assert.NotNull(error);
        Assert.Equal(PzErrorCode.YamlShape, error.Code);
        Assert.Equal("project.yml", error.File);
        Assert.Contains("Malformed YAML", error.Message);
    }

    [Fact]
    public void Alias_expansion_beyond_the_node_budget_is_a_config_error()
    {
        // 8 chained anchors, each referencing the previous 10 times: ~10^8 nodes if fully
        // expanded, far past any real project and past the mapper's budget.
        var yaml = new StringBuilder("vars:\n  a0: &a0 [x, x, x, x, x, x, x, x, x, x]\n");
        for (var i = 1; i < 8; i++)
        {
            var p = $"*a{i - 1}";
            yaml.Append($"  a{i}: &a{i} [{p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}, {p}]\n");
        }

        LoadString(yaml.ToString(), out var error);
        Assert.NotNull(error);
        Assert.Equal(PzErrorCode.YamlShape, error.Code);
        Assert.Contains("alias", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Shared_aliases_without_a_cycle_still_load()
    {
        var loaded = LoadString("defaults: &d\n  x: 1\na: *d\nb: *d\n", out var error);
        Assert.Null(error);
        var map = Assert.IsType<Dictionary<string, object?>>(loaded);
        var a = Assert.IsType<Dictionary<string, object?>>(map["a"]);
        var b = Assert.IsType<Dictionary<string, object?>>(map["b"]);
        Assert.Equal(1L, a["x"]);
        Assert.Equal(1L, b["x"]);
    }

    // -- root document shape ------------------------------------------------------------------------
    // A list/scalar root or a second document used to fall back to Convert's own "not a
    // Dictionary<string,object?>" default, which LoadFile then swallowed into an empty {} -- silently
    // discarding whatever the author actually wrote, indistinguishable from a genuinely empty file.

    [Fact]
    public void A_list_root_is_a_config_error_not_an_empty_map()
    {
        LoadString("- a\n- b\n", out var error);
        Assert.NotNull(error);
        Assert.Equal(PzErrorCode.YamlShape, error.Code);
        Assert.Contains("mapping", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void A_scalar_root_is_a_config_error_not_an_empty_map()
    {
        LoadString("just text\n", out var error);
        Assert.NotNull(error);
        Assert.Equal(PzErrorCode.YamlShape, error.Code);
    }

    [Fact]
    public void A_second_yaml_document_is_a_config_error()
    {
        LoadString("a: 1\n---\nb: 2\n", out var error);
        Assert.NotNull(error);
        Assert.Equal(PzErrorCode.YamlShape, error.Code);
        Assert.Contains("document", error.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void An_empty_file_is_still_an_empty_map_not_an_error()
    {
        var map = Assert.IsType<Dictionary<string, object?>>(LoadString("", out var error));
        Assert.Null(error);
        Assert.Empty(map);
    }

    // -- scalar typing ----------------------------------------------------------------------------
    // Quoting is how YAML says "this is a string". Re-typing a quoted scalar turns a password
    // "0123456" into 123456 and a connector version "1.10" into 1.1 — a different package.

    [Theory]
    [InlineData("v: \"0123456\"", "0123456")]
    [InlineData("v: '0123456'", "0123456")]
    [InlineData("v: \"1.10\"", "1.10")]
    [InlineData("v: \"1e5\"", "1e5")]
    [InlineData("v: \"true\"", "true")]
    [InlineData("v: 'false'", "false")]
    [InlineData("v: \"42\"", "42")]
    [InlineData("v: |\n  42\n", "42\n")]
    [InlineData("v: >\n  true\n", "true\n")]
    public void A_quoted_or_block_scalar_stays_a_string(string yaml, string expected)
    {
        var map = Assert.IsType<Dictionary<string, object?>>(LoadString(yaml, out var error));
        Assert.Null(error);
        Assert.Equal(expected, Assert.IsType<string>(map["v"]));
    }

    [Fact]
    public void A_plain_scalar_is_still_typed()
    {
        var map = Assert.IsType<Dictionary<string, object?>>(
            LoadString("i: 42\nd: 1.5\nt: true\nf: false\ns: hello\nz: 0123456\n", out var error));
        Assert.Null(error);
        Assert.Equal(42L, map["i"]);
        Assert.Equal(1.5, map["d"]);
        Assert.Equal(true, map["t"]);
        Assert.Equal(false, map["f"]);
        Assert.Equal("hello", map["s"]);
        Assert.Equal(123456L, map["z"]);
    }

    [Fact]
    public void Quoted_scalars_stay_strings_inside_sequences_and_nested_mappings()
    {
        var map = Assert.IsType<Dictionary<string, object?>>(
            LoadString("headers:\n  X-Tenant: \"007\"\nvalues: [\"1\", 2, 'true']\n", out var error));
        Assert.Null(error);
        Assert.Equal("007", Assert.IsType<Dictionary<string, object?>>(map["headers"])["X-Tenant"]);
        Assert.Equal(new object?[] { "1", 2L, "true" }, Assert.IsType<List<object?>>(map["values"]));
    }

    /// <summary>A quoted mapping KEY was never re-typed (keys are always text); pinned so the scalar
    /// rule cannot grow to cover it by accident.</summary>
    [Fact]
    public void A_quoted_key_is_plain_text()
    {
        var map = Assert.IsType<Dictionary<string, object?>>(LoadString("\"200\": ok\n", out _));
        Assert.Equal("ok", map["200"]);
    }

    // -- scalar interpolation hook -----------------------------------------------------------------
    // The overload a loader passes an interpolator to: the substituted text is typed by the SAME
    // plain/quoted rule as any other scalar, which is what lets a whole-value "${VAR}" reference come
    // out as an int/bool when the substitution looks like one, while a quoted reference never does.

    private static object? LoadStringInterpolated(string yaml, YamlScalarInterpolator interpolate)
    {
        var path = Path.Combine(Path.GetTempPath(), $"pz-yamlmapper-{Guid.NewGuid():N}.yml");
        File.WriteAllText(path, yaml);
        try
        {
            return YamlMapper.LoadFile(path, "project.yml", interpolate);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void A_substituted_plain_scalar_is_typed_by_its_new_text()
    {
        var map = Assert.IsType<Dictionary<string, object?>>(
            LoadStringInterpolated("port: PLACEHOLDER\n", (text, _, _) => text == "PLACEHOLDER" ? "5432" : text));
        Assert.Equal(5432L, map["port"]);
    }

    [Fact]
    public void A_substituted_quoted_scalar_stays_a_string_even_though_the_result_looks_numeric()
    {
        var map = Assert.IsType<Dictionary<string, object?>>(
            LoadStringInterpolated("port: \"PLACEHOLDER\"\n", (text, _, _) => text == "PLACEHOLDER" ? "5432" : text));
        Assert.Equal("5432", map["port"]);
    }

    [Fact]
    public void The_interpolator_sees_the_key_path_to_the_scalar()
    {
        var seen = new List<string>();
        LoadStringInterpolated("a:\n  b: x\n  c: [y]\n", (text, _, path) =>
        {
            seen.Add(string.Join('.', path));
            return text;
        });

        Assert.Contains("a.b", seen);
        Assert.Contains("a.c", seen); // a sequence element does not add its own path segment
    }

    [Fact]
    public void No_interpolator_means_the_ordinary_two_argument_LoadFile_behaviour()
    {
        var map = Assert.IsType<Dictionary<string, object?>>(
            LoadStringInterpolated("i: 42\n", null!));
        Assert.Equal(42L, map["i"]);
    }
}
