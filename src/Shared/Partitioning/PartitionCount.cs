using System.Globalization;
using Pz.Connectors.Abstractions;

namespace Pz.Shared.Partitioning;

/// <summary>Parses a source dataset's <c>partitions:</c> option: absent means one partition, and
/// anything but a whole number in <c>[1, max]</c> is a named, non-transient config error.
///
/// <para>The connector's config schema already declares the option an integer, so <c>pz validate</c>
/// rejects a bad value first; this parse still runs for a caller that skips validation, and it must
/// refuse rather than coerce (<see cref="Convert.ToInt32(object)"/> would round <c>3.7</c> to 4 and turn
/// <c>true</c> into 1).</para>
///
/// <para>Compiled into the postgres and sqlserver connectors from this one source file: connectors
/// never reference each other.</para></summary>
internal static class PartitionCount
{
    internal const int Min = 1;

    internal static int Parse(DatasetSpec spec, int max)
    {
        if (!spec.Options.TryGetValue("partitions", out var raw) || raw is null)
        {
            return Min;
        }

        if (!TryParseInt32(raw, out var n))
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': 'partitions' must be an integer, got '{raw}'",
                isTransient: false, code: PzConnectorErrorCodes.Config);
        }

        if (n < Min || n > max)
        {
            throw new PzConnectorException(
                $"dataset '{spec.Dataset}': 'partitions' must be between {Min} and {max}, got {n}",
                isTransient: false, code: PzConnectorErrorCodes.Config);
        }

        return n;
    }

    // Integral CLR values pass through; fractional numbers and booleans are refused outright; anything
    // else (a YAML scalar arrives as a string) must parse as a plain integer.
    private static bool TryParseInt32(object raw, out int value)
    {
        switch (raw)
        {
            case int i:
                value = i;
                return true;
            case long l when l is >= int.MinValue and <= int.MaxValue:
                value = (int)l;
                return true;
            case long or double or float or decimal or bool:
                value = 0;
                return false;
            default:
                return int.TryParse(
                    Convert.ToString(raw, CultureInfo.InvariantCulture), NumberStyles.Integer,
                    CultureInfo.InvariantCulture, out value);
        }
    }
}
