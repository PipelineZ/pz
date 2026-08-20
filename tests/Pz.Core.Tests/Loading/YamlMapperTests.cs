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
}
