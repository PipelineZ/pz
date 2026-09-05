using Apache.Arrow;
using Apache.Arrow.Types;
using Parquet;
using Parquet.Schema;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connector.AzureBlob;

/// <summary>Pure, offline-testable batch→bytes serializers for the azure universal write path: parquet
/// (Parquet.Net, one row group per batch) and RFC-4180 csv. The parquet body is a deliberate
/// cross-connector replication of LocalFiles' <c>ParquetSinkWriteSession</c> (see
/// <c>connectors/Pz.Connector.LocalFiles/LocalFilesSink.cs</c>) -- the ratified pattern in this codebase
/// is to replicate small serialization bodies across connectors rather than have one connector reference
/// another. csv instead lives in the toolkit's shared <see cref="CsvWriteCodec"/> (as NDJSON does in
/// <c>NdjsonWriteCodec</c>), which both connectors reference -- that is a shared library, not a
/// connector-to-connector reference, and it is what keeps the two sinks' csv bytes identical by
/// construction.
///
/// Two call shapes:
///  - <see cref="WriteParquetAsync"/>/<see cref="WriteCsvAsync"/> write a complete, self-contained file to
///    <paramref name="dest"/> (a whole <c>IReadOnlyList&lt;RecordBatch&gt;</c>) in one call -- this is what
///    the offline unit tests exercise over a <see cref="MemoryStream"/>.
///  - The internal per-row-group primitives (<see cref="BuildDataFields"/>,
///    <see cref="CreateParquetWriterAsync"/>, <see cref="WriteRowGroupAsync"/>) are reused by
///    <see cref="AzureWriteSession"/> to stream one batch at a time into an open blob stream across the
///    session's lifetime -- <see cref="WriteParquetAsync"/> itself is implemented in terms of them (open
///    once, loop, close), so there is exactly one code path for "batch bytes -> stream" regardless of
///    caller. The csv session holds a <see cref="CsvWriteCodec"/> directly for the same reason.
///
/// decimal128: like LocalFiles' Parquet.Net writer, decimal128 parquet output is a permanent failure naming
/// the column here too -- Parquet.Net 6.0.3's low-level row-group API this replicates has no first-class
/// decimal128 write support. The native COPY path is the decimal-capable route for azure parquet sinks;
/// csv has no such restriction (decimal128 already prints fine as text).</summary>
internal static class AzureBlobFormat
{
    /// <summary>Writes a complete parquet file (one row group per batch) to <paramref name="dest"/>. Does
    /// not dispose <paramref name="dest"/> -- ownership stays with the caller.</summary>
    public static async Task WriteParquetAsync(Stream dest, Schema schema, IReadOnlyList<RecordBatch> batches, CancellationToken ct)
    {
        var fields = BuildDataFields(schema);
        var writer = await CreateParquetWriterAsync(dest, fields, ct).ConfigureAwait(false);
        await using (writer.ConfigureAwait(false))
        {
            foreach (var batch in batches)
            {
                await WriteRowGroupAsync(writer, fields, batch, ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Writes a complete csv file (header + one line per row, RFC-4180) to <paramref name="dest"/>.
    /// Does not dispose <paramref name="dest"/> -- ownership stays with the caller. The encoding is the
    /// toolkit's shared <see cref="CsvWriteCodec"/>, so this connector and LocalFiles emit the same bytes
    /// from one implementation. <paramref name="delimiter"/> is tab for tsv, comma otherwise -- the tsv
    /// suffix carries no other formatting difference over csv.</summary>
    public static async Task WriteCsvAsync(
        Stream dest, Schema schema, IReadOnlyList<RecordBatch> batches, CancellationToken ct, char delimiter = ',')
    {
        var writer = new CsvWriteCodec(dest, schema, "azure universal csv sink", leaveOpen: true, delimiter: delimiter);
        await using (writer.ConfigureAwait(false))
        {
            foreach (var batch in batches)
            {
                await writer.WriteBatchAsync(batch, ct).ConfigureAwait(false);
            }

            await writer.FlushAsync(ct).ConfigureAwait(false);
        }
    }

    // --- parquet: schema mapping + per-row-group write (replicated from LocalFiles ParquetSinkWriteSession) ---

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
            $"column '{field.Name}': azure universal parquet write does not support decimal128 -- " +
            "use the native COPY path (azure sink 'format: parquet' already prefers native COPY when available)",
            isTransient: false),
        _ => throw new NotSupportedException(
            $"azure universal parquet sink does not support Arrow type '{field.DataType}' for column '{field.Name}'"),
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
                $"azure universal parquet sink does not support array type '{array.GetType()}' for column '{field.Name}'"),
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
