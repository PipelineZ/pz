using System.Numerics;
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

    // -- Exotic kwarg types Scriban can hand a source()/sink() call (#125) -----------------------
    // A huge integer literal (too big for long) evaluates to BigInteger, and an `m`-suffixed literal
    // evaluates to decimal -- both are real values a real source()/sink() kwarg can carry, and both
    // used to throw NotSupportedException uncaught. Both have an obvious lossless JSON form, so both
    // are supported outright rather than refused.

    [Fact]
    public void BigInteger_serializes_as_a_raw_json_number_preserving_every_digit()
    {
        var huge = BigInteger.Parse("99999999999999999999999999999999999999");

        var serialized = CanonicalJson.Serialize(huge);

        Assert.Equal("99999999999999999999999999999999999999", serialized);
        // Round-trips through a real JSON parser too -- JSON numbers are arbitrary precision, so this
        // is valid JSON, not merely valid-looking text.
        using var doc = JsonDocument.Parse(serialized);
        Assert.Equal(huge, BigInteger.Parse(doc.RootElement.GetRawText()));
    }

    [Fact]
    public void Negative_BigInteger_round_trips()
    {
        var huge = BigInteger.Parse("-99999999999999999999999999999999999999");
        Assert.Equal("-99999999999999999999999999999999999999", CanonicalJson.Serialize(huge));
    }

    [Fact]
    public void Decimal_serializes_round_trippable() =>
        Assert.Equal("1.5", CanonicalJson.Serialize(1.5m));

    [Fact]
    public void DateTime_serializes_as_an_iso8601_string()
    {
        var serialized = CanonicalJson.Serialize(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc));

        using var doc = JsonDocument.Parse(serialized);
        Assert.Equal("2026-01-01T00:00:00Z", doc.RootElement.GetString());
    }

    [Fact]
    public void BigInteger_nested_in_a_list_still_serializes()
    {
        var value = new List<object?> { 1L, BigInteger.Parse("99999999999999999999999999999999999999") };
        Assert.Equal("[1,99999999999999999999999999999999999999]", CanonicalJson.Serialize(value));
    }
}
