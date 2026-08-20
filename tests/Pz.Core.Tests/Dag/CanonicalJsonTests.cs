using System.Text.Json;
using Pz.Core.Dag;

namespace Pz.Core.Tests.Dag;

/// <summary>Determinism-critical: <see cref="CanonicalJson.Serialize"/> feeds <see cref="NodeId.Compute"/>,
/// so its output must be byte-stable for equal input -- key order sorted ordinal, no whitespace, unicode
/// escaped consistently. Structural assertions (key order, escaping) are checked by re-parsing the output
/// rather than pinning the exact escaped bytes, except where the JSON spec itself guarantees the escape
/// (quote/backslash).</summary>
public sealed class CanonicalJsonTests
{
    [Fact]
    public void Null_serializes_to_json_null() =>
        Assert.Equal("null", CanonicalJson.Serialize(null));

    [Theory]
    [InlineData(true, "true")]
    [InlineData(false, "false")]
    public void Bool_serializes_to_json_literal(bool value, string expected) =>
        Assert.Equal(expected, CanonicalJson.Serialize(value));

    [Fact]
    public void Long_serializes_to_plain_number() =>
        Assert.Equal("-7", CanonicalJson.Serialize(-7L));

    [Fact]
    public void Int_serializes_to_plain_number() =>
        Assert.Equal("42", CanonicalJson.Serialize(42));

    [Fact]
    public void Double_serializes_round_trippable() =>
        Assert.Equal("1.5", CanonicalJson.Serialize(1.5));

    [Fact]
    public void String_serializes_quoted() =>
        Assert.Equal("\"hello\"", CanonicalJson.Serialize("hello"));

    [Fact]
    public void String_with_quote_and_backslash_round_trips_through_parse()
    {
        const string original = "a\"b\\c";

        var serialized = CanonicalJson.Serialize(original);

        using var doc = JsonDocument.Parse(serialized);
        Assert.Equal(original, doc.RootElement.GetString());
    }

    [Fact]
    public void Unicode_string_round_trips_through_parse()
    {
        const string original = "héllo wörld 日本語";

        var serialized = CanonicalJson.Serialize(original);

        using var doc = JsonDocument.Parse(serialized);
        Assert.Equal(original, doc.RootElement.GetString());
    }

    [Fact]
    public void ReadOnlyDictionary_of_object_has_keys_sorted_ordinal_regardless_of_insertion_order()
    {
        IReadOnlyDictionary<string, object?> dict = new Dictionary<string, object?>
        {
            ["zebra"] = 1L,
            ["apple"] = 2L,
            ["Mango"] = 3L,
        };

        var serialized = CanonicalJson.Serialize(dict);

        using var doc = JsonDocument.Parse(serialized);
        var keys = doc.RootElement.EnumerateObject().Select(p => p.Name).ToArray();
        // Ordinal: uppercase ASCII sorts before lowercase ASCII.
        Assert.Equal(new[] { "Mango", "apple", "zebra" }, keys);
    }

    [Fact]
    public void ReadOnlyDictionary_of_string_serializes_values_as_strings_with_sorted_keys() =>
        Assert.Equal("""{"a":"one","b":"two"}""", CanonicalJson.Serialize(
            new Dictionary<string, string> { ["b"] = "two", ["a"] = "one" }));

    [Fact]
    public void Enumerable_of_object_preserves_insertion_order_not_sorted() =>
        Assert.Equal("[3,1,2]", CanonicalJson.Serialize(new List<object?> { 3L, 1L, 2L }));

    [Fact]
    public void Empty_dictionary_serializes_to_empty_object() =>
        Assert.Equal("{}", CanonicalJson.Serialize(new Dictionary<string, object?>()));

    [Fact]
    public void Empty_list_serializes_to_empty_array() =>
        Assert.Equal("[]", CanonicalJson.Serialize(new List<object?>()));

    [Fact]
    public void Nested_structure_serializes_deterministically_with_sorted_keys_at_every_level()
    {
        var value = new Dictionary<string, object?>
        {
            ["tags"] = new List<object?> { "b", "a" },
            ["nested"] = new Dictionary<string, object?> { ["z"] = 1L, ["a"] = 2L },
            ["missing"] = null,
        };

        Assert.Equal("""{"missing":null,"nested":{"a":2,"z":1},"tags":["b","a"]}""",
            CanonicalJson.Serialize(value));
    }

    [Fact]
    public void Serialize_is_deterministic_across_repeated_calls()
    {
        var value = new Dictionary<string, object?> { ["b"] = 1L, ["a"] = 2L };

        Assert.Equal(CanonicalJson.Serialize(value), CanonicalJson.Serialize(value));
    }

    [Fact]
    public void Unsupported_type_throws_NotSupportedException_naming_the_type()
    {
        var ex = Assert.Throws<NotSupportedException>(() => CanonicalJson.Serialize(Guid.Empty));

        Assert.Contains("Guid", ex.Message, StringComparison.Ordinal);
    }
}
