using Apache.Arrow;
using Apache.Arrow.Types;
using Parquet;
using Parquet.Schema;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Gcs;

/// <summary>Parquet schema mapping + per-row-group write for the gcs universal sink. Replicated
/// (not shared) from <c>Pz.Connector.AzureBlob.AzureBlobFormat</c>'s parquet half per the
/// no-cross-connector-reference rule; csv/json ride the toolkit's shared codecs directly.</summary>
internal static class GcsFormat
{
    internal static DataField[] BuildDataFields(Schema schema) =>
        schema.FieldsList.Select(BuildDataField).ToArray();

    private static DataField BuildDataField(Apache.Arrow.Field field) => field.DataType switch
    {
        Int32Type => new DataField(field.Name, typeof(int), isNullable: true),
        Int64Type => new DataField(field.Name, typeof(long), isNullable: true),
        DoubleType => new DataField(field.Name, typeof(double), isNullable: true),
        BooleanType => new DataField(field.Name, typeof(bool), isNullable: true),
        StringType => new DataField(field.Name, typeof(string), isNullable: true),
        Date32Type => new DateTimeDataField(field.Name, DateTimeFormat.Date, isNullable: true),
        TimestampType => new DateTimeDataField(field.Name, DateTimeFormat.DateAndTime,
            isAdjustedToUTC: true, unit: DateTimeTimeUnit.Micros, isNullable: true),
        Decimal128Type => throw new PzConnectorException(
            $"column '{field.Name}': gcs universal parquet write does not support decimal128 -- " +
            "use 'hmac' auth so the native COPY path carries this output",
            isTransient: false),
        _ => throw new NotSupportedException(
            $"gcs universal parquet sink does not support Arrow type '{field.DataType}' for column '{field.Name}'"),
    };

    internal static Task<ParquetWriter> CreateParquetWriterAsync(Stream dest, DataField[] fields, CancellationToken ct) =>
        ParquetWriter.CreateAsync(new ParquetSchema(fields), dest, cancellationToken: ct);

    /// <summary>Writes one <see cref="RecordBatch"/> as one parquet row group. All values are copied out of
    /// the batch's Arrow arrays into freshly-allocated managed arrays/lists before being handed to
    /// Parquet.Net, and the row group is flushed to <paramref name="writer"/>'s underlying stream by the
    /// time this returns (row-group bytes are written eagerly on <c>Dispose</c> of the row group writer;
    /// only the file footer is deferred to the writer's own final <c>DisposeAsync</c>) -- so by the time
    /// this method returns, nothing from <paramref name="batch"/>'s buffers is referenced anymore
    /// (the engine may recycle its pooled buffers the instant this returns).</summary>
    internal static async Task WriteRowGroupAsync(ParquetWriter writer, DataField[] fields, RecordBatch batch, CancellationToken ct)
    {
        using var rowGroup = writer.CreateRowGroup();
        for (var i = 0; i < fields.Length; i++)
        {
            await WriteColumnAsync(rowGroup, fields[i], batch.Column(i), ct).ConfigureAwait(false);
        }
    }

    private static Task WriteColumnAsync(ParquetRowGroupWriter rowGroup, DataField field, IArrowArray array, CancellationToken ct) =>
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
                $"gcs universal parquet sink does not support array type '{array.GetType()}' for column '{field.Name}'"),
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
