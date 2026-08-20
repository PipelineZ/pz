using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connectors.Toolkit.Formats;

/// <summary>Projects JSON records through a declared `columns:` contract into Arrow-ready rows —
/// the NdjsonCodec drift posture: extra keys ignored, missing/null → Arrow null, type mismatch →
/// permanent error naming the column and context but never the (untrusted) value.</summary>
public static class ContractProjector
{
    public static Schema BuildSchema(IReadOnlyDictionary<string, string> columns)
    {
        ArgumentNullException.ThrowIfNull(columns);
        var fields = columns.Select(c => new Field(c.Key, ArrowType(c.Value, c.Key), nullable: true)).ToList();
        return new Schema(fields, null);
    }

    private static IArrowType ArrowType(string typeName, string columnName) => typeName switch
    {
        "int" => Int32Type.Default,
        "bigint" => Int64Type.Default,
        "double" => DoubleType.Default,
        "decimal" => new Decimal128Type(38, 9),
        "varchar" => StringType.Default,
        "boolean" => BooleanType.Default,
        "date" => Date32Type.Default,
        "timestamp" => new TimestampType(TimeUnit.Microsecond, "+00:00"),
        _ => throw new PzConnectorException($"column '{columnName}': unknown columns: contract type '{typeName}' — supported types: int, bigint, double, decimal, varchar, boolean, date, timestamp", isTransient: false),
    };

    public static object?[] ProjectRow(JsonNode? record, IReadOnlyDictionary<string, string> columns, string context)
    {
        ArgumentNullException.ThrowIfNull(columns);
        var row = new object?[columns.Count];
        if (record is not JsonObject obj)
        {
            return row;
        }

        var i = 0;
        foreach (var (name, typeName) in columns)
        {
            row[i++] = Extract(obj, name, typeName, context);
        }

        return row;
    }

    private static object? Extract(JsonObject obj, string name, string typeName, string context)
    {
        if (!obj.TryGetPropertyValue(name, out var node) || node is null)
        {
            return null;
        }

        return ConvertScalar(node, typeName, name, context);
    }

    /// <summary>Converts one JSON scalar to the CLR value <c>ArrowBatchBuilder.AppendRow</c> expects
    /// for the contract type. Public so callers with their own extraction (e.g. a JSON-pointer cursor)
    /// share one conversion truth. Null node → null.</summary>
    public static object? ConvertScalar(JsonNode? node, string typeName, string columnName, string context)
    {
        if (node is null)
        {
            return null;
        }

        try
        {
            return typeName switch
            {
                "int" => node.GetValue<int>(),
                "bigint" => node.GetValue<long>(),
                "double" => node.GetValue<double>(),
                "decimal" => node.GetValue<decimal>(),
                "varchar" => node.GetValueKind() == JsonValueKind.String
                    ? node.GetValue<string>()
                    : node.ToJsonString(),
                "boolean" => node.GetValue<bool>(),
                "date" => DateOnly.ParseExact(node.GetValue<string>(), "yyyy-MM-dd", CultureInfo.InvariantCulture),
                "timestamp" => ParseTimestamp(node.GetValue<string>(), columnName, context),
                _ => throw new PzConnectorException(
                    $"{context}: column '{columnName}': unknown columns: contract type '{typeName}'",
                    isTransient: false),
            };
        }
        catch (Exception ex) when (ex is InvalidOperationException or FormatException or OverflowException)
        {
            throw new PzConnectorException(
                $"{context}: column '{columnName}': value is not a valid {typeName}", isTransient: false,
                innerException: ex);
        }
    }

    private static DateTimeOffset ParseTimestamp(string text, string name, string context)
    {
        if (!DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var value))
        {
            throw new PzConnectorException(
                $"{context}: column '{name}': value is not a valid timestamp", isTransient: false);
        }

        return value;
    }
}
