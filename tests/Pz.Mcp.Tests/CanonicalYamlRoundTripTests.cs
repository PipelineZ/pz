using Pz.Core.Loading;
using Pz.Mcp.Editing;

namespace Pz.Mcp.Tests;

/// <summary>What the authoring tools write, the loader must read back as the same value of the same
/// type. <see cref="CanonicalYaml"/> quotes every string that would otherwise read as a number, a
/// boolean or null — which only helps if the loader then leaves quoted scalars alone.</summary>
public sealed class CanonicalYamlRoundTripTests
{
    private static object? RoundTrip(object? value)
    {
        var path = Path.Combine(Path.GetTempPath(), $"pz-canonical-roundtrip-{Guid.NewGuid():N}.yml");
        File.WriteAllText(path, CanonicalYaml.MappingEntry("v", value, indentLevels: 0));
        try
        {
            return YamlMapper.LoadFile(path, "connections.yml")["v"];
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData("0123456")]
    [InlineData("1.10")]
    [InlineData("1e5")]
    [InlineData("42")]
    [InlineData("true")]
    [InlineData("False")]
    [InlineData("null")]
    [InlineData("~")]
    [InlineData("yes")]
    [InlineData("")]
    [InlineData(" padded ")]
    [InlineData("plain text")]
    [InlineData("a: b")]
    [InlineData("line one\nline two")]
    [InlineData("${DB_PASSWORD}")]
    public void A_string_comes_back_as_the_same_string(string value)
    {
        Assert.Equal(value, Assert.IsType<string>(RoundTrip(value)));
    }

    [Fact]
    public void Typed_values_come_back_with_their_type()
    {
        Assert.Equal(42L, RoundTrip(42L));
        Assert.Equal(1.5, RoundTrip(1.5));
        Assert.Equal(true, RoundTrip(true));
        Assert.Equal(false, RoundTrip(false));
    }

    [Fact]
    public void Number_like_strings_survive_inside_nested_maps_and_lists()
    {
        var value = new Dictionary<string, object?>
        {
            ["headers"] = new Dictionary<string, object?> { ["X-Tenant"] = "007" },
            ["codes"] = new List<object?> { "1", 2L, "true", true },
        };

        var loaded = Assert.IsType<Dictionary<string, object?>>(RoundTrip(value));

        Assert.Equal("007", Assert.IsType<Dictionary<string, object?>>(loaded["headers"])["X-Tenant"]);
        Assert.Equal(new object?[] { "1", 2L, "true", true }, Assert.IsType<List<object?>>(loaded["codes"]));
    }
}
