using System.Text.Json;
using System.Text.Json.Nodes;
using Apache.Arrow;
using Pz.Connectors.Abstractions.Memory;
using Pz.Connectors.Toolkit.Formats;
using Pz.Connectors.Toolkit.Json;

namespace Pz.Connector.Http;

/// <summary>The output shape of a file-mode dataset, shared by every file format reader.
/// Contract mode: the contract columns in declared order, then `file_column` (varchar) when set.
/// Raw mode: `payload`, `pz_file`, `pz_row`, `pz_fetched_at`, then the cursor column when declared —
/// the cursor joins so the engine's post-land MAX(cursor) watermark capture works unchanged.</summary>
internal static class FileOutput
{
    public static IReadOnlyDictionary<string, string> Columns(HttpDatasetConfig config)
    {
        if (config.IsContractMode)
        {
            var contract = new Dictionary<string, string>(config.Columns!);
            if (config.File?.FileColumn is { } fileColumn)
            {
                contract[fileColumn] = "varchar";
            }

            return contract;
        }

        var raw = new Dictionary<string, string>
        {
            ["payload"] = "varchar",
            ["pz_file"] = "varchar",
            ["pz_row"] = "bigint",
            ["pz_fetched_at"] = "timestamp",
        };
        if (config.Cursor is { } cursor)
        {
            raw[cursor] = config.CursorType!;
        }

        return raw;
    }

    public static Schema Schema(HttpDatasetConfig config) => ContractProjector.BuildSchema(Columns(config));

    /// <summary>One raw-mode row. The cursor is resolved from <paramref name="payload"/> by
    /// `cursor_pointer`: a JSON string is text and follows the text value rules
    /// (<see cref="TextValueParser"/>); any other JSON value converts as a JSON page's cursor does.</summary>
    public static object?[] RawRow(JsonObject payload, string fileName, long row, DateTimeOffset fetchedAt,
        HttpDatasetConfig config, string context)
    {
        var values = new object?[config.Cursor is null ? 4 : 5];
        values[0] = payload.ToJsonString();
        values[1] = fileName;
        values[2] = row;
        values[3] = fetchedAt;
        if (config.Cursor is { } cursor)
        {
            JsonPointer.TryResolve(payload, config.CursorPointer, out var node);
            values[4] = node is JsonValue value && value.GetValueKind() == JsonValueKind.String
                ? TextValueParser.Parse(value.GetValue<string>(), config.CursorType!, $"{context}: column '{cursor}'")
                : ContractProjector.ConvertScalar(node, config.CursorType!, cursor, context);
        }

        return values;
    }

    /// <summary>Returns <paramref name="batch"/> with a constant <paramref name="value"/> column appended,
    /// under <paramref name="schema"/> (the full output schema, whose last field is that column).
    /// Ownership: the returned batch takes over <paramref name="batch"/>'s arrays and disposes them with
    /// its own, so the caller disposes only the returned batch and never <paramref name="batch"/>
    /// (disposing both would return the same pooled buffers twice). <paramref name="batch"/> must not
    /// own a separate memory owner of its own — true of every batch built from an array list.</summary>
    public static RecordBatch AppendFileColumn(RecordBatch batch, Schema schema, string value)
    {
        var builder = new StringArray.Builder();
        for (var i = 0; i < batch.Length; i++)
        {
            builder.Append(value);
        }

        var arrays = new IArrowArray[batch.ColumnCount + 1];
        for (var i = 0; i < batch.ColumnCount; i++)
        {
            arrays[i] = batch.Column(i);
        }

        arrays[^1] = builder.Build(PooledNativeAllocator.Shared);
        return new RecordBatch(schema, arrays, batch.Length);
    }
}
