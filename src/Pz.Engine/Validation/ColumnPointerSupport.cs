using System.Text.Json;
using Pz.Core.Validation;

namespace Pz.Engine.Validation;

/// <summary>A column's long-form <c>pointer</c> reaches the connector as the <c>column_pointers</c> read
/// option. A connector opts in by listing that key in its dataset schema; one that does not would read the
/// top-level key named like the column (or NULL) instead, a silent wrong read. So both tier 3 and the
/// SourceLoad executor refuse it, and fail closed: an empty or unparsable schema lists nothing.</summary>
public static class ColumnPointerSupport
{
    public const string Option = "column_pointers";

    /// <summary>Whether <paramref name="schemaText"/>'s top-level <c>properties</c> lists the option.</summary>
    public static bool IsListedIn(string? schemaText)
    {
        if (string.IsNullOrEmpty(schemaText))
        {
            return false;
        }

        try
        {
            using var schemaDoc = JsonDocument.Parse(schemaText);
            return schemaDoc.RootElement.ValueKind == JsonValueKind.Object
                && schemaDoc.RootElement.TryGetProperty("properties", out var properties)
                && properties.ValueKind == JsonValueKind.Object
                && properties.TryGetProperty(Option, out _);
        }
        catch (JsonException)
        {
            return false;
        }
    }

    /// <summary>The refusal for a dataset whose options carry pointers its connector does not list, or null
    /// when there is nothing to refuse.</summary>
    public static PzError? Refusal(IReadOnlyDictionary<string, object?> options, string schemaText,
        string connector, string connection, string dataset, string filePath)
    {
        if (!options.TryGetValue(Option, out var raw) || IsListedIn(schemaText))
        {
            return null;
        }

        var columns = raw is IReadOnlyDictionary<string, object?> map
            ? string.Join(", ", map.Keys.Order(StringComparer.Ordinal).Select(c => $"'{c}'"))
            : "(unknown)";
        return new PzError(PzErrorCode.ConnectorConfigInvalid,
            $"connection '{connection}': dataset '{dataset}': column(s) {columns} declare a 'pointer', which " +
            $"connector '{connector}' does not support", filePath, null,
            "remove 'pointer' from those columns -- only connectors that read JSON records (such as http) " +
            "read a column from a nested pointer");
    }
}
