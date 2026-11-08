using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Toolkit.Formats;
using Pz.Connectors.Toolkit.Json;

namespace Pz.Connector.Http;

/// <summary>Reads a JSON file, as one record per line (`layout: lines`, blank lines skipped) or as one
/// document whose `items` pointer resolves to the array of records (`layout: document`). A document is
/// parsed whole, so at most <paramref name="maxDocumentBytes"/> (`max_response_mb`) are read; a larger
/// file must use `layout: lines`. Contract mode projects through <see cref="ContractProjector"/>; raw
/// mode lands each record as its payload, a non-object record wrapped as `{"value": record}`.</summary>
internal sealed class HttpJsonFileReader(long maxDocumentBytes) : IFileFormatReader
{
    public async IAsyncEnumerable<RecordBatch> ReadAsync(FileSource file, HttpDatasetConfig config,
        DateTimeOffset fetchedAt, BatchOptions options, string label, [EnumeratorCancellation] CancellationToken ct)
    {
        // A corrupt deflate stream throws InvalidDataException from whichever read reaches it.
        await using var batches = ReadCoreAsync(file, config, fetchedAt, options, label, ct).GetAsyncEnumerator(ct);
        while (true)
        {
            bool more;
            try
            {
                more = await batches.MoveNextAsync().ConfigureAwait(false);
            }
            catch (InvalidDataException ex)
            {
                throw new PzConnectorException($"{label}: file '{file.Name}': {ex.Message}", isTransient: false,
                    innerException: ex);
            }

            if (!more)
            {
                yield break;
            }

            yield return batches.Current;
        }
    }

    private async IAsyncEnumerable<RecordBatch> ReadCoreAsync(FileSource file, HttpDatasetConfig config,
        DateTimeOffset fetchedAt, BatchOptions options, string label, [EnumeratorCancellation] CancellationToken ct)
    {
        var records = config.File!.JsonLayout == HttpJsonLayout.Document
            ? ReadDocumentAsync(file, config, label, ct)
            : ReadLinesAsync(file, config, label, ct);
        var builder = new ArrowBatchBuilder(FileOutput.Schema(config), options.TargetBatchBytes,
            maxRowsPerBatch: options.MaxRowsPerBatch);
        var hasFileColumn = config.File.FileColumn is not null;

        long row = 0;
        await foreach (var (record, context) in records.ConfigureAwait(false))
        {
            row++;
            if (config.IsContractMode)
            {
                var values = ContractProjector.ProjectRow(record, config.Columns!, context);
                if (hasFileColumn)
                {
                    System.Array.Resize(ref values, values.Length + 1);
                    values[^1] = file.Name;
                }

                builder.AppendRow(values);
            }
            else
            {
                // A node has one parent: a wrapped array element is copied, not moved.
                var payload = record as JsonObject ?? new JsonObject { ["value"] = record?.DeepClone() };
                builder.AppendRow(FileOutput.RawRow(payload, file.Name, row, fetchedAt, config, context));
            }

            if (builder.TryTakeBatch(out var batch))
            {
                yield return batch!;
            }
        }

        if (builder.Flush() is { } tail)
        {
            yield return tail;
        }
    }

    private static async IAsyncEnumerable<(JsonNode? Record, string Context)> ReadLinesAsync(FileSource file,
        HttpDatasetConfig config, string label, [EnumeratorCancellation] CancellationToken ct)
    {
        using var reader = new StreamReader(file.Stream, config.File!.Encoding ?? Encoding.UTF8,
            detectEncodingFromByteOrderMarks: true, leaveOpen: true);
        long line = 0;
        while (await reader.ReadLineAsync(ct).ConfigureAwait(false) is { } text)
        {
            line++;
            if (string.IsNullOrWhiteSpace(text))
            {
                continue;
            }

            var context = $"{label}: file '{file.Name}' line {line}";
            JsonNode? node;
            try
            {
                node = JsonNode.Parse(text);
            }
            catch (JsonException ex)
            {
                throw new PzConnectorException($"{context} is not valid JSON", isTransient: false, innerException: ex);
            }

            yield return (node, context);
        }
    }

    private async IAsyncEnumerable<(JsonNode? Record, string Context)> ReadDocumentAsync(FileSource file,
        HttpDatasetConfig config, string label, [EnumeratorCancellation] CancellationToken ct)
    {
        var context = $"{label}: file '{file.Name}'";
        using var buffer = await BufferCappedAsync(file.Stream, context, ct).ConfigureAwait(false);

        JsonNode? root;
        try
        {
            if (config.File!.Encoding is { } encoding and not UTF8Encoding)
            {
                using var text = new StreamReader(buffer, encoding, detectEncodingFromByteOrderMarks: true);
                root = JsonNode.Parse(await text.ReadToEndAsync(ct).ConfigureAwait(false));
            }
            else
            {
                root = JsonNode.Parse(buffer);
            }
        }
        catch (JsonException ex)
        {
            throw new PzConnectorException($"{context} is not a valid JSON document", isTransient: false,
                innerException: ex);
        }

        var pointer = config.ItemsPointer;
        if (!JsonPointer.TryResolve(root, pointer, out var items) || items is not JsonArray array)
        {
            throw new PzConnectorException($"{context}: items pointer '{pointer}' does not resolve to an array",
                isTransient: false);
        }

        for (var i = 0; i < array.Count; i++)
        {
            yield return (array[i], $"{context} record {i + 1}");
        }
    }

    /// <summary>Copies at most the cap into memory, counting bytes as they arrive: an archive member's
    /// stream has no reliable length.</summary>
    private async Task<MemoryStream> BufferCappedAsync(Stream source, string context, CancellationToken ct)
    {
        var buffer = new MemoryStream();
        var chunk = new byte[81920];
        int read;
        while ((read = await source.ReadAsync(chunk, ct).ConfigureAwait(false)) > 0)
        {
            if (buffer.Length + read > maxDocumentBytes)
            {
                await buffer.DisposeAsync().ConfigureAwait(false);
                throw new PzConnectorException(
                    $"{context} exceeds 'max_response_mb' ({FormatCap(maxDocumentBytes)}) for a json document; " +
                    "use layout: lines (one record per line) for large files", isTransient: false);
            }

            buffer.Write(chunk, 0, read);
        }

        buffer.Position = 0;
        return buffer;
    }

    private static string FormatCap(long bytes) =>
        bytes % (1024 * 1024) == 0 ? $"{bytes / (1024 * 1024)} MiB" : $"{bytes} bytes";
}
