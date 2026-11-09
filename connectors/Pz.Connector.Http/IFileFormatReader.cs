using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http;

/// <summary>Parses one downloaded file into Arrow batches.</summary>
internal interface IFileFormatReader
{
    /// <summary>Streams <paramref name="file"/> as batches, each with
    /// <see cref="FileOutput.Schema"/>(<paramref name="config"/>) and owned by the caller. A failure is a
    /// <see cref="PzConnectorException"/> naming <paramref name="label"/> and the file.</summary>
    IAsyncEnumerable<RecordBatch> ReadAsync(FileSource file, HttpDatasetConfig config, DateTimeOffset fetchedAt,
        BatchOptions options, string label, CancellationToken ct);
}
