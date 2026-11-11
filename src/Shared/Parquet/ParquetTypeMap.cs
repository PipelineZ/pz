using Parquet.Schema;
using Pz.Connectors.Abstractions;

namespace Pz.Shared.Parquet;

/// <summary>Maps a parquet footer's logical/physical type to the v0 contract type-name matrix (int,
/// bigint, double, decimal, varchar, boolean, date, timestamp). Parquet is self-describing, so this is
/// where a parquet schema's column types come from. <see cref="DateTimeDataField"/> and
/// <see cref="DecimalDataField"/> are matched before the plain CLR-type fallback because Parquet.Net
/// gives DATE and TIMESTAMP the same base CLR type, <see cref="DateTime"/>: only
/// <see cref="DateTimeDataField.DateTimeFormat"/> tells them apart.
///
/// <para>Compiled into the localfiles and azureblob connectors from this one source file: connectors
/// never reference each other.</para></summary>
internal static class ParquetTypeMap
{
    /// <summary>The v0 type name for <paramref name="field"/>; an unsupported type is a permanent
    /// failure naming the column and <paramref name="sourceName"/>.</summary>
    internal static string ToV0TypeName(DataField field, string sourceName) => field switch
    {
        DecimalDataField => "decimal",
        DateTimeDataField { DateTimeFormat: DateTimeFormat.Date } => "date",
        DateTimeDataField => "timestamp", // DateAndTime, DateAndTimeMicros, Timestamp, Impala variants
        _ => MapClrType(field, sourceName),
    };

    private static string MapClrType(DataField field, string sourceName)
    {
        var clrType = Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType;

        if (clrType == typeof(int)) return "int";
        if (clrType == typeof(long)) return "bigint";
        if (clrType == typeof(double)) return "double";
        if (clrType == typeof(string)) return "varchar";
        if (clrType == typeof(bool)) return "boolean";
        if (clrType == typeof(DateTime)) return "timestamp"; // unreachable in practice: DateTimeDataField matches first

        throw new PzConnectorException(
            $"column '{field.Name}': {sourceName} does not support parquet type '{clrType.Name}'",
            isTransient: false);
    }
}
