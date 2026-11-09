using System.Runtime.CompilerServices;
using System.Xml;
using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http;

/// <summary>Turns a file's undecodable bytes into a permanent error naming the file. A corrupt deflate
/// stream throws <see cref="InvalidDataException"/> from whichever read reaches it, and malformed XML
/// (a DTD included) throws <see cref="XmlException"/>; neither is retryable.</summary>
internal static class FileReaderErrors
{
    public static async IAsyncEnumerable<RecordBatch> WrapAsync(IAsyncEnumerable<RecordBatch> inner, string label,
        string name, [EnumeratorCancellation] CancellationToken ct)
    {
        await using var batches = inner.GetAsyncEnumerator(ct);
        while (true)
        {
            bool more;
            try
            {
                more = await batches.MoveNextAsync().ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is InvalidDataException or XmlException)
            {
                throw new PzConnectorException($"{label}: file '{name}': {ex.Message}", isTransient: false,
                    innerException: ex);
            }

            if (!more)
            {
                yield break;
            }

            yield return batches.Current;
        }
    }
}
