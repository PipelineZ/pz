using System.Globalization;
using Pz.Connectors.Abstractions;
using Pz.Shared.Csv;

namespace Pz.Connector.Http;

/// <summary>Converts one text cell (a CSV/TSV field, an XML value, a raw-mode text cursor) to the CLR
/// value <c>ArrowBatchBuilder.AppendRow</c> expects for a contract type. The parses are exactly
/// <see cref="CsvColumnWriter"/>'s — the same <see cref="NumberStyles"/>, <see cref="DateTimeStyles"/>
/// and invariant culture per type — so a value lands identically whichever path reads it. An empty
/// value is NULL for every type, varchar included.</summary>
internal static class TextValueParser
{
    public static object? Parse(string text, string typeName, string context)
    {
        if (text.Length == 0)
        {
            return null;
        }

        var invariant = CultureInfo.InvariantCulture;
        return typeName switch
        {
            "int" => int.TryParse(text, NumberStyles.Integer, invariant, out var v) ? v : throw Invalid(text, typeName, context),
            "bigint" => long.TryParse(text, NumberStyles.Integer, invariant, out var v) ? v : throw Invalid(text, typeName, context),
            "double" => double.TryParse(text, NumberStyles.Float | NumberStyles.AllowThousands, invariant, out var v)
                ? v
                : throw Invalid(text, typeName, context),
            "decimal" => decimal.TryParse(text, NumberStyles.Number, invariant, out var v) ? v : throw Invalid(text, typeName, context),
            "varchar" => text,
            "boolean" => bool.TryParse(text, out var v) ? v : throw Invalid(text, typeName, context),
            "date" => DateOnly.TryParse(text, invariant, DateTimeStyles.None, out var v) ? v : throw Invalid(text, typeName, context),
            "timestamp" => DateTimeOffset.TryParse(text, invariant,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var v)
                ? v
                : throw Invalid(text, typeName, context),
            _ => throw new PzConnectorException(
                $"{context}: unknown columns: contract type '{typeName}'", isTransient: false, code: PzConnectorErrorCodes.Config),
        };
    }

    private static PzConnectorException Invalid(string text, string typeName, string context) =>
        new($"{context}: value '{text}' is not a valid {typeName}", isTransient: false);
}
