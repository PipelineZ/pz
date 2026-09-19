namespace Pz.Connectors.Sdk.Tests;

/// <summary>The numeric-option probe is answered by the SDK, from what <see cref="StructMapping"/>
/// made of the wire value -- so these go through the same mapping a real Validate call does.</summary>
public sealed class ConformanceProbeTests
{
    private static IReadOnlyDictionary<string, object?> OverTheWire(string key, double number)
    {
        var wire = new Google.Protobuf.WellKnownTypes.Struct();
        wire.Fields[key] = Google.Protobuf.WellKnownTypes.Value.ForNumber(number);
        return StructMapping.ToDictionary(wire);
    }

    [Fact]
    public void A_whole_number_that_crossed_as_a_double_is_answered_clean()
    {
        Assert.True(ConformanceProbe.TryAnswerNumericOption(
            OverTheWire(ConformanceProbe.NumericOptionKey, 424242), out var errors));
        Assert.Empty(errors);
    }

    [Fact]
    public void A_fractional_probe_value_is_refused_in_the_agreed_words()
    {
        Assert.True(ConformanceProbe.TryAnswerNumericOption(
            OverTheWire(ConformanceProbe.NumericOptionKey, 0.5), out var errors));
        Assert.Equal(["pz_conformance_numeric_probe: not integral"], errors);
    }

    [Fact]
    public void A_real_config_is_left_to_the_connector()
    {
        Assert.False(ConformanceProbe.TryAnswerNumericOption(OverTheWire("port", 5432), out _));

        var withOtherKeys = new Dictionary<string, object?>
        {
            [ConformanceProbe.NumericOptionKey] = 1L,
            ["host"] = "db",
        };
        Assert.False(ConformanceProbe.TryAnswerNumericOption(withOtherKeys, out _));
    }
}
