using System.Text.Json.Nodes;
using Pz.Connectors.Toolkit.Json;

namespace Pz.Connectors.Toolkit.Tests.Json;

public class JsonPointerTests
{
    private static readonly JsonNode Doc = JsonNode.Parse(
        """{ "data": { "items": [ { "id": 1 }, { "id": 2 } ], "next~/": "tok" }, "empty": null }""")!;

    [Theory]
    [InlineData("", true)]                    // root
    [InlineData("/data", true)]
    [InlineData("/data/items/1/id", true)]
    [InlineData("/data/items/7", false)]      // index out of range
    [InlineData("/nope", false)]              // missing key
    [InlineData("/empty", true)]              // explicit null resolves (result is null node)
    public void Resolves_paths(string pointer, bool expected)
    {
        Assert.Equal(expected, JsonPointer.TryResolve(Doc, pointer, out _));
    }

    [Fact]
    public void Unescapes_tilde_sequences()
    {
        Assert.True(JsonPointer.TryResolve(Doc, "/data/next~0~1", out var node));
        Assert.Equal("tok", node!.GetValue<string>());
    }

    [Fact]
    public void Root_pointer_returns_document()
    {
        Assert.True(JsonPointer.TryResolve(Doc, "", out var node));
        Assert.Same(Doc, node);
    }

    [Fact]
    public void Malformed_pointer_throws()
    {
        Assert.Throws<ArgumentException>(() => JsonPointer.TryResolve(Doc, "data", out _));
    }
}
