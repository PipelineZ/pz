using Google.Protobuf.WellKnownTypes;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

public sealed class MessageMappingTests
{
    [Fact]
    public void An_all_string_nested_map_answers_to_both_dictionary_shapes_in_declared_order()
    {
        var columns = new Struct();
        columns.Fields["id"] = Value.ForString("bigint");
        columns.Fields["customer"] = Value.ForString("varchar");
        var config = new Struct();
        config.Fields["columns"] = Value.ForStruct(columns);

        var map = StructMapping.ToDictionary(config);
        var typed = Assert.IsAssignableFrom<IReadOnlyDictionary<string, string>>(map["columns"]);
        Assert.Equal(["id", "customer"], typed.Keys.ToArray());
        Assert.Equal("varchar", typed["customer"]);
        var loose = Assert.IsAssignableFrom<IReadOnlyDictionary<string, object?>>(map["columns"]);
        Assert.Equal("bigint", loose["id"]);
    }

    [Fact]
    public void A_mixed_nested_map_is_a_plain_object_map()
    {
        var inner = new Struct();
        inner.Fields["limit"] = Value.ForNumber(10);
        inner.Fields["name"] = Value.ForString("x");
        var config = new Struct();
        config.Fields["opts"] = Value.ForStruct(inner);

        var map = StructMapping.ToDictionary(config);
        Assert.IsNotAssignableFrom<IReadOnlyDictionary<string, string>>(map["opts"]);
        var loose = Assert.IsAssignableFrom<IReadOnlyDictionary<string, object?>>(map["opts"]);
        Assert.Equal(10d, loose["limit"]);
    }
}
