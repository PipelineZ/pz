using Pz.Core.Dag;

namespace Pz.Core.Tests.Dag;

/// <summary>Determinism/stability coverage of <see cref="NodeId"/>: same content always yields the same
/// id (across processes and .NET versions, since <see cref="NodeId.Compute"/> is a plain SHA-256 truncated
/// to 8 bytes -- not <c>string.GetHashCode</c>, which is randomized per-process), different content yields
/// a different id, and the record-struct's value equality/formatting behave as every caller
/// (<c>pz retry</c>, watermark provenance, artifact writers) relies on.</summary>
public sealed class NodeIdTests
{
    [Fact]
    public void Compute_is_deterministic_for_the_same_input() =>
        Assert.Equal(NodeId.Compute("same content").Value, NodeId.Compute("same content").Value);

    [Fact]
    public void Compute_differs_for_different_input() =>
        Assert.NotEqual(NodeId.Compute("a").Value, NodeId.Compute("b").Value);

    [Fact]
    public void Compute_returns_16_lowercase_hex_characters()
    {
        var id = NodeId.Compute("pipeline stg select * from x");

        Assert.Equal(16, id.Value.Length);
        Assert.Matches("^[0-9a-f]{16}$", id.Value);
    }

    [Fact]
    public void Compute_of_empty_string_matches_the_well_known_sha256_prefix() =>
        // sha256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 -- first 8 bytes:
        Assert.Equal("e3b0c44298fc1c14", NodeId.Compute("").Value);

    [Fact]
    public void ToString_returns_the_underlying_value()
    {
        var id = NodeId.Compute("abc");

        Assert.Equal(id.Value, id.ToString());
    }

    [Fact]
    public void NodeId_with_equal_value_compares_equal() =>
        Assert.Equal(NodeId.Compute("x"), NodeId.Compute("x"));

    [Fact]
    public void NodeId_with_different_value_compares_unequal() =>
        Assert.NotEqual(NodeId.Compute("x"), NodeId.Compute("y"));

    [Fact]
    public void Unicode_content_is_supported_and_deterministic() =>
        Assert.Equal(NodeId.Compute("héllo日本語").Value, NodeId.Compute("héllo日本語").Value);

    [Fact]
    public void Whitespace_only_difference_changes_the_id() =>
        Assert.NotEqual(NodeId.Compute("select 1").Value, NodeId.Compute("select  1").Value);
}
