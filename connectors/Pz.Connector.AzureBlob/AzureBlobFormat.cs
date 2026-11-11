using Apache.Arrow;
using Apache.Arrow.Types;
using Parquet;
using Parquet.Schema;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;
using Pz.Shared.Parquet;

namespace Pz.Connector.AzureBlob;

/// <summary>Whole-file batch-to-bytes serializers for the azure universal write path, used by the
/// offline tests over a <see cref="MemoryStream"/>: parquet through the shared
/// <see cref="ParquetBatchWriter"/> (one row group per batch) and csv/tsv through the toolkit's
/// <see cref="CsvWriteCodec"/>. The write sessions stream one batch at a time through the same two
/// writers, so a file's bytes do not depend on which entry point produced it. Decimal columns are refused
/// on the parquet path, which cannot write them; the native COPY path can, and csv prints them as
/// text.</summary>
internal static class AzureBlobFormat
{
    internal const string ParquetSinkName = "azure universal parquet sink";

    internal const string ParquetDecimalRefusal =
        "azure universal parquet write does not support decimal128 -- " +
        "use the native COPY path (azure sink 'format: parquet' already prefers native COPY when available)";

    /// <summary>Writes a complete parquet file (one row group per batch) to <paramref name="dest"/>. Does
    /// not dispose <paramref name="dest"/> -- ownership stays with the caller.</summary>
    public static async Task WriteParquetAsync(Stream dest, Schema schema, IReadOnlyList<RecordBatch> batches, CancellationToken ct)
    {
        var fields = ParquetBatchWriter.BuildDataFields(schema, ParquetSinkName, ParquetDecimalRefusal);
        var writer = await ParquetBatchWriter.CreateWriterAsync(dest, fields, ct).ConfigureAwait(false);
        await using (writer.ConfigureAwait(false))
        {
            foreach (var batch in batches)
            {
                await ParquetBatchWriter.WriteRowGroupAsync(writer, fields, batch, ParquetSinkName, ct).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Writes a complete csv file (header + one line per row, RFC-4180) to <paramref name="dest"/>.
    /// Does not dispose <paramref name="dest"/> -- ownership stays with the caller. The encoding is the
    /// toolkit's shared <see cref="CsvWriteCodec"/>, so this connector and LocalFiles emit the same bytes
    /// from one implementation. <paramref name="delimiter"/> is tab for tsv, comma otherwise -- the tsv
    /// suffix carries no other formatting difference over csv.</summary>
    public static async Task WriteCsvAsync(
        Stream dest, Schema schema, IReadOnlyList<RecordBatch> batches, char delimiter, CancellationToken ct)
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
}
