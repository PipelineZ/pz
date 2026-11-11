using Apache.Arrow;
using Apache.Arrow.Types;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using Pz.Connectors.Abstractions;

namespace Pz.Shared.Parquet;

/// <summary>The universal-tier parquet write path: maps an Arrow schema of the v0 type matrix to
/// Parquet.Net fields and writes each Arrow batch as one row group.
///
/// <para>Compiled into the localfiles, azureblob, gcs and sftp connectors from this one source file:
/// connectors never reference each other.</para></summary>
internal static class ParquetBatchWriter
{
    /// <summary>Maps every column of <paramref name="schema"/> to its Parquet.Net field. Decimal columns
    /// are refused with <paramref name="decimalRefusal"/> (prefixed with the column name), because this
    /// path cannot write them; every other type outside the v0 matrix is refused naming
    /// <paramref name="sinkName"/>. Call it before opening any destination, so a refused schema leaves
    /// nothing to clean up.</summary>
    internal static DataField[] BuildDataFields(Schema schema, string sinkName, string decimalRefusal) =>
        [.. schema.FieldsList.Select(field => BuildDataField(field, sinkName, decimalRefusal))];

    /// <summary>Opens a parquet writer for <paramref name="fields"/> over <paramref name="dest"/>, which the
    /// caller keeps owning.</summary>
    internal static Task<ParquetWriter> CreateWriterAsync(Stream dest, DataField[] fields, CancellationToken ct) =>
        ParquetWriter.CreateAsync(new ParquetSchema(fields), dest, cancellationToken: ct);

    /// <summary>Writes <paramref name="batch"/> as one row group. Every value is copied out of the
    /// batch's Arrow arrays into managed arrays before Parquet.Net sees it, and the row group's bytes are
    /// written when its writer is disposed (only the file footer waits for the writer's own dispose), so
    /// nothing from <paramref name="batch"/>'s buffers is referenced once this returns and the engine may
    /// recycle them.</summary>
    internal static async Task WriteRowGroupAsync(
        ParquetWriter writer, DataField[] fields, RecordBatch batch, string sinkName, CancellationToken ct)
    {
        using var rowGroup = writer.CreateRowGroup();
        for (var i = 0; i < fields.Length; i++)
        {
            await WriteColumnAsync(rowGroup, fields[i], batch.Column(i), sinkName, ct).ConfigureAwait(false);
        }
    }

    private static DataField BuildDataField(Apache.Arrow.Field field, string sinkName, string decimalRefusal) => field.DataType switch
    {
        Int32Type => new DataField(field.Name, typeof(int), isNullable: true),
        Int64Type => new DataField(field.Name, typeof(long), isNullable: true),
        DoubleType => new DataField(field.Name, typeof(double), isNullable: true),
        BooleanType => new DataField(field.Name, typeof(bool), isNullable: true),
        StringType => new DataField(field.Name, typeof(string), isNullable: true),
        Date32Type => new DateTimeDataField(field.Name, DateTimeFormat.Date, isNullable: true),
        // DateAndTimeMicros, not DateAndTime: Parquet.Net's DateTimeDataField hardcodes millisecond units
        // for DateAndTime whatever `unit:` says (it honours a unit only for the *Micros/*Nanos formats), so
        // DateAndTime would silently truncate every timestamp to millisecond precision.
        TimestampType => new DateTimeDataField(field.Name, DateTimeFormat.DateAndTimeMicros,
            isAdjustedToUTC: true, isNullable: true),
        Decimal128Type => throw new PzConnectorException($"column '{field.Name}': {decimalRefusal}", isTransient: false),
        _ => throw new NotSupportedException(
            $"{sinkName} does not support Arrow type '{field.DataType}' for column '{field.Name}'"),
    };

    private static Task WriteColumnAsync(
        ParquetRowGroupWriter rowGroup, DataField field, IArrowArray array, string sinkName, CancellationToken ct) =>
        array switch
        {
            Int32Array a => rowGroup.WriteAsync(field, BuildNullable(a.Length, i => a.IsNull(i) ? (int?)null : a.GetValue(i)), cancellationToken: ct),
            Int64Array a => rowGroup.WriteAsync(field, BuildNullable(a.Length, i => a.IsNull(i) ? (long?)null : a.GetValue(i)), cancellationToken: ct),
            DoubleArray a => rowGroup.WriteAsync(field, BuildNullable(a.Length, i => a.IsNull(i) ? (double?)null : a.GetValue(i)), cancellationToken: ct),
            BooleanArray a => rowGroup.WriteAsync(field, BuildNullable(a.Length, i => a.IsNull(i) ? (bool?)null : a.GetValue(i)), cancellationToken: ct),
            Date32Array a => rowGroup.WriteAsync(field, BuildNullable(a.Length, i => a.IsNull(i) ? (DateTime?)null : a.GetDateTime(i)!.Value.Date), cancellationToken: ct),
            TimestampArray a => rowGroup.WriteAsync(field, BuildNullable(a.Length, i => a.IsNull(i) ? (DateTime?)null : a.GetTimestamp(i)!.Value.UtcDateTime), cancellationToken: ct),
            StringArray a => rowGroup.WriteAsync(field, BuildStrings(a)),
            _ => throw new NotSupportedException(
                $"{sinkName} does not support array type '{array.GetType()}' for column '{field.Name}'"),
        };

    private static ReadOnlyMemory<T?> BuildNullable<T>(int length, Func<int, T?> selector) where T : struct
    {
        var values = new T?[length];
        for (var i = 0; i < length; i++)
        {
            values[i] = selector(i);
        }

        return values;
    }

    private static List<string?> BuildStrings(StringArray array)
    {
        var values = new List<string?>(array.Length);
        for (var i = 0; i < array.Length; i++)
        {
            values.Add(array.IsNull(i) ? null : array.GetString(i));
        }

        return values;
    }
}
