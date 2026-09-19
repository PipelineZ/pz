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
        // Integral: normalized to long, not the raw double the wire's number kind always carries --
        // see Integral_number_values_normalize_to_long for the full contract.
        Assert.Equal(10L, loose["limit"]);
    }

    /// <summary>protobuf's <c>Struct</c> has only <c>number</c> (a double), so an integer connector
    /// option (`max_connections: 5`) arrives here as <c>5.0</c>. A connector reading it via
    /// <c>ConnectorConfig.GetInt</c> needs it to already be a whole-number type, and <c>long &gt; 2^53</c>
    /// loses precision round-tripped through a double anyway, so the safe range to convert is exactly
    /// the one a double still represents exactly.</summary>
    [Theory]
    [InlineData(5.0, 5L)]
    [InlineData(-3.0, -3L)]
    [InlineData(0.0, 0L)]
    [InlineData(9_007_199_254_740_992d, 9_007_199_254_740_992L)] // 2^53, inclusive boundary
    public void Integral_number_values_normalize_to_long(double wire, long expected)
    {
        var config = new Struct();
        config.Fields["k"] = Value.ForNumber(wire);

        var map = StructMapping.ToDictionary(config);

        var actual = Assert.IsType<long>(map["k"]);
        Assert.Equal(expected, actual);
    }

    /// <summary>A fractional value, or one wider than a double can represent exactly, stays a double --
    /// normalizing it to long would silently change its value (a non-integral) or its precision (out of
    /// range), which is worse than leaving it as the double it actually is.</summary>
    [Theory]
    [InlineData(2.5)]
    [InlineData(9_007_199_254_740_994d)] // 2^53 + 2: the next double after the boundary is exact
    public void Non_integral_or_out_of_range_number_values_stay_double(double wire)
    {
        var config = new Struct();
        config.Fields["k"] = Value.ForNumber(wire);

        var map = StructMapping.ToDictionary(config);

        Assert.Equal(wire, map["k"]);
    }
}
