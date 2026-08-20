using System.Runtime.CompilerServices;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Abstractions.Memory;
using Sylvan.Data.Csv;

namespace Pz.Connector.LocalFiles;

/// <summary>Pivots a Sylvan CSV reader into the universal Arrow batch stream, one declared contract
/// column at a time, without boxing.
///
/// This is the read-side twin of <see cref="DataReaderSource"/>, applied to the same shape of problem:
/// materializing a <c>string</c> per cell (<c>GetString</c>), parsing it into an <c>object?[]</c> row (one
/// box per non-string value) and handing that to <see cref="ArrowBatchBuilder.AppendRow"/>, whose
/// per-column appenders unbox it again, is — against a local file, where nothing is waiting on a network —
/// the whole cost of a universal-tier read: a 5M-row load spent ~5.0 s in the reader against ~1.3 s for
/// the same file through DuckDB's native <c>read_csv</c>.
///
/// Instead each column gets one <see cref="CsvColumnWriter"/> that parses straight from Sylvan's
/// <see cref="CsvDataReader.GetFieldSpan"/> — the reader's own char buffer, never copied — and lays the
/// value down in Arrow's memory layout itself, so a cell costs a bounds check and a store rather than a
/// pair of builder appends. Final buffers come from <see cref="PooledNativeAllocator.Shared"/>.
///
/// Behaviour is deliberately identical to the boxed path, cell for cell: the same invariant-culture
/// parses, the same "empty means NULL for every type including varchar" policy, the same batch
/// boundaries, and the same <see cref="PzConnectorException"/> message naming file, line, column, value
/// and type.</summary>
internal static class CsvArrowReader
{
    /// <summary>Streams <paramref name="csv"/>'s rows as Arrow batches, reading exactly the columns of
    /// <paramref name="schema"/> from the ordinals in <paramref name="ordinals"/> (resolved by the
    /// caller against the file's real header, so a contract column may sit anywhere in the file).
    ///
    /// <paramref name="rowNumberOffset"/> is added to the reader's own 1-based data-row number when a
    /// parse fails, so a split partition — whose reader starts counting again from 1 partway down the
    /// file — still names the row the author would count to in the whole file.
    /// It is zero for a whole-file read, which is what keeps that case's message identical.</summary>
    internal static async IAsyncEnumerable<RecordBatch> ReadAsync(
        CsvDataReader csv,
        Schema schema,
        IReadOnlyList<string> typeNames,
        int[] ordinals,
        string path,
        BatchOptions options,
        long rowNumberOffset,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var writers = BuildWriters(schema, typeNames, path);
        var maxRowsPerBatch = BatchOptions.Default.MaxRowsPerBatch;
        var pendingRows = 0;

        // Byte accounting in eighths of a byte: every width in play is either a whole number of bytes or
        // a single validity bit, so integers reproduce the boxed path's `double` estimate exactly (the
        // ceiling below is the same comparison) without a floating-point add per cell.
        var eighths = 0L;

        while (await csv.ReadAsync(ct).ConfigureAwait(false))
        {
            var line = csv.RowNumber + rowNumberOffset;
            for (var i = 0; i < writers.Length; i++)
            {
                eighths += writers[i].Append(csv.GetFieldSpan(ordinals[i]), line) + 1;
            }

            pendingRows++;
            if ((eighths + 7) / 8 >= options.TargetBatchBytes || pendingRows >= maxRowsPerBatch)
            {
                yield return Build(schema, writers, pendingRows);
                pendingRows = 0;
                eighths = 0;
            }
        }

        if (pendingRows > 0)
        {
            yield return Build(schema, writers, pendingRows);
        }
    }

    private static RecordBatch Build(Schema schema, CsvColumnWriter[] writers, int rows)
    {
        var arrays = new IArrowArray[writers.Length];
        for (var i = 0; i < writers.Length; i++)
        {
            arrays[i] = writers[i].BuildAndReset();
        }

        return new RecordBatch(schema, arrays, rows);
    }

    private static CsvColumnWriter[] BuildWriters(Schema schema, IReadOnlyList<string> typeNames, string path)
    {
        var allocator = PooledNativeAllocator.Shared;
        var writers = new CsvColumnWriter[schema.FieldsList.Count];
        for (var i = 0; i < writers.Length; i++)
        {
            writers[i] = CsvColumnWriter.Create(schema.FieldsList[i], typeNames[i], path, allocator);
        }

        return writers;
    }
}
