using Parquet.Schema;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.LocalFiles;

/// <summary>Maps a parquet footer's logical/physical type (parquet is self-describing, so this is the
/// ONLY place LocalFiles parquet ever derives a type from) to the same fixed v0 type-name
/// matrix <see cref="TypeNameMap"/> already uses for CSV: int, bigint, double, decimal, varchar,
/// boolean, date, timestamp. <see cref="Parquet.Schema.DateTimeDataField"/>/<see cref="DecimalDataField"/>
/// are checked before the plain CLR-type fallback because Parquet.Net represents DATE and
/// TIMESTAMP alike as a base CLR type of <see cref="DateTime"/> -- only the subclass's
/// <see cref="DateTimeDataField.DateTimeFormat"/> distinguishes them (mirrors
/// <see cref="LocalFilesSink"/>'s write-side <c>DateTimeDataField</c> construction, in reverse).</summary>
internal static class ParquetTypeMap
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
            $"column '{field.Name}': localfiles parquet source does not support parquet type '{clrType.Name}'",
            isTransient: false);
    }
}
