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
        => ProjectRow(record, columns, context, ContractReadOptions.Default);

    /// <summary>As <see cref="ProjectRow(JsonNode?, IReadOnlyDictionary{string, string}, string)"/>, under
    /// <paramref name="options"/>: a column with a pointer reads the pointed-at node (a missing step is
    /// NULL, like a missing key), and timestamps follow its epoch rule.</summary>
    public static object?[] ProjectRow(JsonNode? record, IReadOnlyDictionary<string, string> columns, string context,
        ContractReadOptions options)
    {
        ArgumentNullException.ThrowIfNull(columns);
        ArgumentNullException.ThrowIfNull(options);
        if (record is not JsonObject obj)
        {
            // A record that isn't a JSON object means 'items' resolved one level off (e.g. an
            // array of scalars/arrays rather than an array of row objects): projecting it would
            // silently land a row of all NULLs -- a green node that read nothing real, and whose
            // all-NULL cursor column means its watermark never advances.
            throw new PzConnectorException($"{context}: record is not an object (check 'items')",
                isTransient: false);
        }

        var row = new object?[columns.Count];
        var i = 0;
        foreach (var (name, typeName) in columns)
        {
            row[i++] = Extract(obj, name, typeName, context, options);
        }

        return row;
    }

    private static object? Extract(JsonObject obj, string name, string typeName, string context,
        ContractReadOptions options)
    {
        JsonNode? node;
        if (options.Pointers is { } pointers && pointers.TryGetValue(name, out var pointer))
        {
            Json.JsonPointer.TryResolve(obj, pointer, out node);
        }
        else
        {
            obj.TryGetPropertyValue(name, out node);
        }

        return node is null ? null : ConvertScalar(node, typeName, name, context, options);
    }

    /// <summary>Converts one JSON scalar to the CLR value <c>ArrowBatchBuilder.AppendRow</c> expects
    /// for the contract type. Public so callers with their own extraction (e.g. a JSON-pointer cursor)
    /// share one conversion truth. Null node → null.</summary>
    public static object? ConvertScalar(JsonNode? node, string typeName, string columnName, string context)
        => ConvertScalar(node, typeName, columnName, context, ContractReadOptions.Default);

    /// <summary>As <see cref="ConvertScalar(JsonNode?, string, string, string)"/>, under the epoch rule of
    /// <paramref name="options"/> (its pointers do not apply: the caller already holds the node).</summary>
    public static object? ConvertScalar(JsonNode? node, string typeName, string columnName, string context,
        ContractReadOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (node is null)
        {
            return null;
        }

        if (typeName == "timestamp" && node.GetValueKind() == JsonValueKind.Number)
        {
            return EpochTimestamp(node, columnName, context, options);
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

    private const long TicksPerMillisecond = TimeSpan.TicksPerMillisecond;

    private static DateTimeOffset EpochTimestamp(JsonNode node, string name, string context,
        ContractReadOptions options)
    {
        if (options.EpochUnit is not { } unit)
        {
            var next = options.EpochUnitOption is { } optionName
                ? $"; if it is epoch time, set '{optionName}: s' (or '{optionName}: ms') on the dataset"
                : "; this connector does not read epoch numbers as timestamps";
            throw new PzConnectorException(
                $"{context}: column '{name}': value is a JSON number, not a timestamp string{next}",
                isTransient: false);
        }

        // Exact in decimal; the range checks run before the tick product can overflow.
        var seconds = unit == EpochUnit.Seconds;
        var ticksPerUnit = seconds ? TimeSpan.TicksPerSecond : TicksPerMillisecond;
        var minUnits = (DateTimeOffset.MinValue.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) / (decimal)ticksPerUnit;
        var maxUnits = (DateTimeOffset.MaxValue.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) / (decimal)ticksPerUnit;
        var where = $"{context}: column '{name}': epoch {(seconds ? "seconds" : "milliseconds")} value";
        var option = options.EpochUnitOption;
        decimal value;
        try
        {
            value = node.GetValue<decimal>();
        }
        catch (Exception ex) when (ex is FormatException or OverflowException or InvalidOperationException)
        {
            throw new PzConnectorException($"{where} is out of range for a timestamp", isTransient: false,
                innerException: ex);
        }

        // The unit-confusion guards come first. A seconds feed read as milliseconds lands in early 1970,
        // and a milliseconds feed read as seconds lands centuries ahead: both would be silently wrong
        // timestamps, and a wrong cursor would move the watermark wrongly for good.
        if (!seconds && value < MinPlausibleMilliseconds)
        {
            throw new PzConnectorException(
                $"{where} is out of range (before 1973-03-03, below 1e11 ms); it looks like seconds" +
                (option is null ? "" : $" -- set '{option}: s'"), isTransient: false);
        }

        if (seconds && value > MaxPlausibleSeconds)
        {
            throw new PzConnectorException(
                $"{where} is out of range (after 3000-01-01); it looks like milliseconds" +
                (option is null ? "" : $" -- set '{option}: ms'"), isTransient: false);
        }

        if (value < minUnits || value > maxUnits)
        {
            throw new PzConnectorException(seconds
                ? $"{where} is out of range for a timestamp"
                : $"{where} is out of range for a timestamp (microsecond and nanosecond epochs are not supported: " +
                    "land the column as bigint and convert it in SQL)", isTransient: false);
        }

        return DateTimeOffset.UnixEpoch.AddTicks((long)decimal.Truncate(value * ticksPerUnit));
    }

    /// <summary>1e11 ms is 1973-03-03T09:46:40Z: a smaller milliseconds value is almost always seconds.</summary>
    public const decimal MinPlausibleMilliseconds = 100_000_000_000m;

    /// <summary>3000-01-01T00:00:00Z in seconds: a larger seconds value is almost always milliseconds.</summary>
    public const decimal MaxPlausibleSeconds = 32_503_680_000m;

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
