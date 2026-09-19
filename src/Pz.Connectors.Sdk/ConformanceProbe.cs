namespace Pz.Connectors.Sdk;

/// <summary>Questions <c>pz connector test</c> asks of the SDK rather than of the connector built on
/// it. Whether a whole number survives the wire as an integer is decided by this assembly's own
/// <see cref="StructMapping"/>, so this assembly answers for it -- every connector built on the SDK
/// proves the contract without its author writing a line, and no connector ever sees the probe key.</summary>
internal static class ConformanceProbe
{
    /// <summary>Spelled exactly as the host's conformance suite spells it; the two assemblies do not
    /// reference each other.</summary>
    internal const string NumericOptionKey = "pz_conformance_numeric_probe";

    /// <summary>The host tells an SDK that answers the probe from a connector that merely tolerates an
    /// unknown key by this exact reply to a fractional probe value.</summary>
    internal const string NumericOptionRefusal = NumericOptionKey + ": not integral";

    /// <summary>True when <paramref name="config"/> is the numeric-option probe -- that key and nothing
    /// else, which no real connection config is. <paramref name="errors"/> is empty when the value
    /// reached this process as an integer and <see cref="NumericOptionRefusal"/> when it did not.</summary>
    internal static bool TryAnswerNumericOption(
        IReadOnlyDictionary<string, object?> config, out IReadOnlyList<string> errors)
    {
        errors = [];
        if (config.Count != 1 || !config.TryGetValue(NumericOptionKey, out var value))
        {
            return false;
        }

        if (value is not long)
        {
            errors = [NumericOptionRefusal];
        }

        return true;
    }
}
