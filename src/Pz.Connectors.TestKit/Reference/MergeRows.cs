using System.Globalization;
using Apache.Arrow;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connectors.TestKit.Reference;

/// <summary>The keyed-upsert core behind
/// <see cref="InMemorySinkWriteSession.CommitAsync"/>'s merge mode -- folds a set of "existing" (already
/// committed) batches and a set of "incoming" (this session's) batches into one row-per-key map
/// (last-writer-wins: incoming rows are absorbed AFTER existing ones, so an incoming row with a key
/// already present overwrites it), then rebuilds the result as fresh <see cref="RecordBatch"/>es via
/// <see cref="ArrowBatchBuilder"/>. Covers exactly the fixed v0 type matrix:
/// int32/int64/double/decimal128/utf8/bool/date32/timestamp -- the same matrix every other v0 sink/source
/// in this repo supports.</summary>
internal static class MergeRows
{
    public static IReadOnlyList<RecordBatch> Build(
        Schema schema, IReadOnlyList<string> keys, IReadOnlyList<RecordBatch> existing, IReadOnlyList<RecordBatch> incoming)
    {
        var keyIndexes = keys.Select(k => schema.GetFieldIndex(k)).ToArray();
        var merged = new Dictionary<string, object?[]>(StringComparer.Ordinal);

        void Absorb(IReadOnlyList<RecordBatch> batches)
        {
            foreach (var batch in batches)
            {
                for (var row = 0; row < batch.Length; row++)
                {
                    var values = ExtractRow(batch, row);
                    var key = string.Join('\u0001', keyIndexes.Select(i => CanonicalKeyPart(values[i])));
                    merged[key] = values;
                }
            }
        }

        Absorb(existing);
        Absorb(incoming);

        var builder = new ArrowBatchBuilder(schema);
        var result = new List<RecordBatch>();
        foreach (var values in merged.Values)
        {
            builder.AppendRow(values);
            if (builder.TryTakeBatch(out var full))
            {
                result.Add(full!);
            }
        }

        var flushed = builder.Flush();
        if (flushed is not null)
        {
            result.Add(flushed);
        }

        return result;
    }

    /// <summary>Extracts one row as a boxed <c>object?[]</c> (one entry per schema field, in field
    /// order) shaped EXACTLY as <see cref="ArrowBatchBuilder.AppendRow"/> expects, since the extracted
    /// row is fed straight back into a fresh builder to reconstruct the merged batch (e.g. a Date32
    /// column must come back as a boxed <see cref="DateOnly"/>, not the <see cref="DateTime"/>
    /// <c>Date32Array.GetDateTime</c> itself returns).</summary>
    private static object?[] ExtractRow(RecordBatch batch, int row)
    {
        var values = new object?[batch.ColumnCount];
        for (var col = 0; col < batch.ColumnCount; col++)
        {
            values[col] = GetAppendableScalar(batch.Column(col), row);
        }

        return values;
    }

    private static object? GetAppendableScalar(IArrowArray array, int index)
    {
        if (array.IsNull(index))
        {
            return null;
        }

        return array switch
        {
            Int32Array a => a.GetValue(index),
            Int64Array a => a.GetValue(index),
            DoubleArray a => a.GetValue(index),
            Decimal128Array a => a.GetValue(index),
            BooleanArray a => a.GetValue(index),
            Date32Array a => DateOnly.FromDateTime(a.GetDateTime(index)!.Value),
            TimestampArray a => a.GetTimestamp(index),
            StringArray a => a.GetString(index),
            _ => throw new NotSupportedException($"unsupported array type {array.GetType()} in InMemorySink merge"),
        };
    }

    private static string CanonicalKeyPart(object? value) => value switch
    {
        null => "\0",
        int i => i.ToString(CultureInfo.InvariantCulture),
        long l => l.ToString(CultureInfo.InvariantCulture),
        double d => d.ToString("R", CultureInfo.InvariantCulture),
        decimal m => m.ToString(CultureInfo.InvariantCulture),
        bool b => b.ToString(CultureInfo.InvariantCulture),
        DateOnly d => d.ToString("O", CultureInfo.InvariantCulture),
        DateTimeOffset dto => dto.ToString("O", CultureInfo.InvariantCulture),
        string s => s,
        _ => value.ToString() ?? "\0",
    };
}
