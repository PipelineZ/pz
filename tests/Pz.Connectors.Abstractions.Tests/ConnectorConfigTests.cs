using System.Globalization;
using Pz.Connectors.Abstractions;

namespace Pz.Connectors.Abstractions.Tests;

/// <summary>YAML values reach a connector already typed. Asking for one as text must give back how the
/// author would have written it, on any machine: `.`-decimal numbers whatever the host's culture, and
/// YAML's lowercase booleans rather than .NET's <c>True</c>.</summary>
public sealed class ConnectorConfigTests
{
    private static ConnectorConfig Config(object? value) => new(new Dictionary<string, object?> { ["k"] = value });

    [Fact]
    public void GetString_renders_numbers_the_same_under_every_culture()
    {
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("de-DE");

            Assert.Equal("1.5", Config(1.5).GetString("k"));
            Assert.Equal("1234567", Config(1234567L).GetString("k"));
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [Theory]
    [InlineData(true, "true")]
    [InlineData(false, "false")]
    public void GetString_renders_a_boolean_the_way_yaml_spells_it(bool value, string expected)
    {
        Assert.Equal(expected, Config(value).GetString("k"));
    }

    [Fact]
    public void GetString_leaves_text_alone_and_missing_is_null()
    {
        Assert.Equal("0123456", Config("0123456").GetString("k"));
        Assert.Null(Config(null).GetString("k"));
        Assert.Null(ConnectorConfig.Empty.GetString("k"));
    }

    /// <summary>A quoted number is a string by the time it gets here; the typed accessors still read it,
    /// so `port: "5432"` keeps working.</summary>
    [Fact]
    public void Typed_accessors_read_a_quoted_value()
    {
        Assert.Equal(5432L, Config("5432").GetInt("k"));
        Assert.True(Config("true").GetBool("k"));
    }
}
