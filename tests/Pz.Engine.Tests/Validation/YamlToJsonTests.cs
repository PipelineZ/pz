using System.Text.Json;
using System.Text.Json.Nodes;
using Pz.Engine.Validation;

namespace Pz.Engine.Tests.Validation;

/// <summary>Pure conversion coverage of <see cref="YamlToJson.Convert"/> -- the loader-shaped
/// (scalars + <c>Dictionary&lt;string,object?&gt;</c>/<c>List&lt;object?&gt;</c>) value tree to
/// <see cref="JsonNode"/> transform used ahead of JSON Schema evaluation. Because the loader already
/// parsed quoted-vs-plain YAML scalars into typed CLR values before this runs, "quoted vs plain" here
/// means a string scalar that merely looks like another type (e.g. <c>"123"</c>, <c>"true"</c>) must stay
/// a JSON string.</summary>
public sealed class YamlToJsonTests
{
    [Fact]
    public void Null_converts_to_null() =>
        Assert.Null(YamlToJson.Convert(null));

    [Theory]
    [InlineData(42L)]
    [InlineData(0L)]
    [InlineData(-7L)]
    public void Long_converts_to_json_number(long value)
    {
        var node = YamlToJson.Convert(value);

        Assert.Equal(JsonValueKind.Number, node!.GetValueKind());
        Assert.Equal(value, node.GetValue<long>());
    }

    [Theory]
    [InlineData(1.5)]
    [InlineData(0.0)]
    [InlineData(-3.25)]
    public void Double_converts_to_json_number(double value)
    {
        var node = YamlToJson.Convert(value);

        Assert.Equal(JsonValueKind.Number, node!.GetValueKind());
        Assert.Equal(value, node.GetValue<double>());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Bool_converts_to_json_boolean(bool value)
    {
        var node = YamlToJson.Convert(value);

        Assert.Equal(value ? JsonValueKind.True : JsonValueKind.False, node!.GetValueKind());
        Assert.Equal(value, node.GetValue<bool>());
    }

    [Theory]
    [InlineData("plain")]
    [InlineData("")]
    [InlineData("has spaces and \"quotes\"")]
    [InlineData("unicode: héllo wörld 日本語")]
    [InlineData("123")]
    [InlineData("true")]
    public void String_converts_to_json_string_even_when_it_looks_like_another_type(string value)
    {
        var node = YamlToJson.Convert(value);

        Assert.Equal(JsonValueKind.String, node!.GetValueKind());
        Assert.Equal(value, node.GetValue<string>());
    }

    [Fact]
    public void Empty_dictionary_converts_to_empty_object()
    {
        var node = YamlToJson.Convert(new Dictionary<string, object?>());

        var obj = Assert.IsType<JsonObject>(node);
        Assert.Empty(obj);
    }

    [Fact]
    public void Dictionary_converts_to_object_preserving_every_key_and_type()
    {
        var dict = new Dictionary<string, object?>
        {
            ["name"] = "acme",
            ["count"] = 3L,
            ["ratio"] = 1.5,
            ["active"] = true,
            ["missing"] = null,
        };

        var node = YamlToJson.Convert(dict);
        var obj = Assert.IsType<JsonObject>(node);

        Assert.Equal(5, obj.Count);
        Assert.Equal("acme", obj["name"]!.GetValue<string>());
        Assert.Equal(3L, obj["count"]!.GetValue<long>());
        Assert.Equal(1.5, obj["ratio"]!.GetValue<double>());
        Assert.True(obj["active"]!.GetValue<bool>());
        Assert.True(obj.ContainsKey("missing"));
        Assert.Null(obj["missing"]);
    }

    [Fact]
    public void Empty_list_converts_to_empty_array()
    {
        var node = YamlToJson.Convert(new List<object?>());

        var array = Assert.IsType<JsonArray>(node);
        Assert.Empty(array);
    }

    [Fact]
    public void List_converts_to_array_preserving_order_and_type()
    {
        var list = new List<object?> { 1L, "two", 3.0, false, null };

        var node = YamlToJson.Convert(list);
        var array = Assert.IsType<JsonArray>(node);

        Assert.Equal(5, array.Count);
        Assert.Equal(1L, array[0]!.GetValue<long>());
        Assert.Equal("two", array[1]!.GetValue<string>());
        Assert.Equal(3.0, array[2]!.GetValue<double>());
        Assert.False(array[3]!.GetValue<bool>());
        Assert.Null(array[4]);
    }

    [Fact]
    public void Nested_dictionary_and_list_convert_recursively()
    {
        var value = new Dictionary<string, object?>
        {
            ["tags"] = new List<object?> { "a", "b" },
            ["nested"] = new Dictionary<string, object?> { ["inner"] = 7L },
        };

        var node = YamlToJson.Convert(value);
        var obj = Assert.IsType<JsonObject>(node);

        var tags = Assert.IsType<JsonArray>(obj["tags"]);
        Assert.Equal(new List<object?> { "a", "b" }, tags.Select(t => t!.GetValue<string>()).Cast<object?>().ToList());

        var nested = Assert.IsType<JsonObject>(obj["nested"]);
        Assert.Equal(7L, nested["inner"]!.GetValue<long>());
    }

    [Fact]
    public void List_of_dictionaries_converts_recursively()
    {
        var value = new List<object?>
        {
            new Dictionary<string, object?> { ["id"] = 1L },
            new Dictionary<string, object?> { ["id"] = 2L },
        };

        var node = YamlToJson.Convert(value);
        var array = Assert.IsType<JsonArray>(node);

        Assert.Equal(2, array.Count);
        Assert.Equal(1L, ((JsonObject)array[0]!)["id"]!.GetValue<long>());
        Assert.Equal(2L, ((JsonObject)array[1]!)["id"]!.GetValue<long>());
    }

    // int is accepted alongside long: Scriban hands call-site kwargs over as int where the YAML loader
    // produces long, and a source() option written at its call site reaches this converter.
    [Fact]
    public void Int_converts_to_json_number()
    {
        var node = YamlToJson.Convert(42);

        Assert.Equal(JsonValueKind.Number, node!.GetValueKind());
        Assert.Equal(42, node.GetValue<int>());
    }

    [Theory]
    [InlineData(3.14f)]
    [InlineData('x')]
    public void Unsupported_clr_type_throws_ArgumentException_naming_the_type(object value)
    {
        var ex = Assert.Throws<ArgumentException>(() => YamlToJson.Convert(value));

        Assert.Contains(value.GetType().Name, ex.Message, StringComparison.Ordinal);
        Assert.Equal("yamlValue", ex.ParamName);
    }
}
