using Parquet.Schema;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.AzureBlob;

/// <summary>Maps a parquet footer's logical/physical type to the fixed v0 type-name matrix
/// <see cref="AzureTypeNameMap"/> already uses (int, bigint, double, decimal, varchar, boolean, date,
/// timestamp). Replicated (not shared) from <c>Pz.Connector.LocalFiles.ParquetTypeMap</c> -- connector
/// projects deliberately do not reference each other or Pz.Core, so this is the source-of-truth parquet→v0
/// mapping copied verbatim-in-spirit; keep in lockstep with LocalFiles' ParquetTypeMap if the matrix grows.
/// <see cref="Parquet.Schema.DateTimeDataField"/>/<see cref="DecimalDataField"/> are checked before the
/// plain CLR-type fallback because Parquet.Net represents DATE and TIMESTAMP alike as a base CLR type of
/// <see cref="DateTime"/> -- only the subclass's <see cref="DateTimeDataField.DateTimeFormat"/>
/// distinguishes them.</summary>
internal static class AzureParquetTypeMap
{
    internal static string ToV0TypeName(DataField field) => field switch
    {
        DecimalDataField => "decimal",
        DateTimeDataField { DateTimeFormat: DateTimeFormat.Date } => "date",
        DateTimeDataField => "timestamp", // DateAndTime, DateAndTimeMicros, Timestamp, Impala variants
        _ => MapClrType(field),
    };

    private static string MapClrType(DataField field)
    {
        var clrType = Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType;

        if (clrType == typeof(int)) return "int";
        if (clrType == typeof(long)) return "bigint";
        if (clrType == typeof(double)) return "double";
        if (clrType == typeof(string)) return "varchar";
        if (clrType == typeof(bool)) return "boolean";
        if (clrType == typeof(DateTime)) return "timestamp"; // fallback; DateTimeDataField case above wins in practice

        throw new PzConnectorException(
            $"column '{field.Name}': azure parquet source does not support parquet type '{clrType.Name}'",
            isTransient: false);
    }
}
