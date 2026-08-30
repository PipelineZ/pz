using System.Runtime.CompilerServices;
using System.Text.Json.Nodes;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connector.Sftp;

/// <summary>NDJSON → Arrow via the toolkit contract projector (the http connector's read shape):
/// one JsonNode per LF-framed line, projected through the declared columns: contract into
/// ArrowBatchBuilder rows. A json read REQUIRES a contract — there is no managed NDJSON schema
/// inference — and the caller enforces that before ever constructing this reader.</summary>
internal static class SftpJsonReader
{
    public static async IAsyncEnumerable<RecordBatch> ReadAsync(
        Stream stream, IReadOnlyDictionary<string, string> columns, string context,
        BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        var schema = ContractProjector.BuildSchema(columns);
        var builder = new ArrowBatchBuilder(schema, options.TargetBatchBytes,
            maxRowsPerBatch: options.MaxRowsPerBatch);

        using var reader = new StreamReader(stream);
        long line = 0;
        while (await reader.ReadLineAsync(ct).ConfigureAwait(false) is { } text)
        {
            line++;
            if (text.Length == 0)
            {
                continue;
            }

            JsonNode? node;
            try
            {
                node = JsonNode.Parse(text);
            }
            catch (System.Text.Json.JsonException ex)
            {
                throw new PzConnectorException(
                    $"{context}: line {line} is not valid JSON", isTransient: false, innerException: ex);
            }

            builder.AppendRow(ContractProjector.ProjectRow(node, columns, $"{context}: line {line}"));
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
}
