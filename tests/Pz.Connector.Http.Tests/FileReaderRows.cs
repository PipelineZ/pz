using Apache.Arrow;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

/// <summary>Runs a file format reader over bytes and returns every row as CLR values, asserting each
/// batch carries <see cref="FileOutput.Schema"/>.</summary>
internal static class FileReaderRows
{
    public static readonly DateTimeOffset FetchedAt = new(2026, 11, 8, 9, 30, 0, TimeSpan.Zero);

    public const string Label = "s.d";

    public static async Task<List<object?[]>> ReadAsync(IFileFormatReader reader, byte[] bytes,
        HttpDatasetConfig config, string name, BatchOptions? options = null)
    {
        var rows = new List<object?[]>();
        var expected = FileOutput.Schema(config);
        using var source = new FileSource(name, new MemoryStream(bytes));
        await foreach (var batch in reader.ReadAsync(
            source, config, FetchedAt, options ?? new BatchOptions(), Label, CancellationToken.None))
        {
            using (batch)
            {
                Assert.Equal(expected.FieldsList.Select(f => f.Name), batch.Schema.FieldsList.Select(f => f.Name));
                Assert.Equal(expected.FieldsList.Select(f => f.DataType.TypeId),
                    batch.Schema.FieldsList.Select(f => f.DataType.TypeId));
                for (var r = 0; r < batch.Length; r++)
                {
                    var row = new object?[batch.ColumnCount];
                    for (var c = 0; c < batch.ColumnCount; c++)
                    {
                        row[c] = Value(batch.Column(c), r);
                    }

                    rows.Add(row);
                }
            }
        }

        return rows;
    }

    private static object? Value(IArrowArray array, int row) => array switch
    {
        Int32Array a => a.GetValue(row),
        Int64Array a => a.GetValue(row),
        DoubleArray a => a.GetValue(row),
        Decimal128Array a => a.GetValue(row),
        BooleanArray a => a.GetValue(row),
        StringArray a => a.GetString(row),
        Date32Array a => a.GetDateOnly(row),
        TimestampArray a => a.GetTimestamp(row),
        _ => throw new InvalidOperationException(array.GetType().Name),
    };
}
