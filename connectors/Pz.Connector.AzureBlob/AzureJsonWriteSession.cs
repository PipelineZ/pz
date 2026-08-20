using Apache.Arrow;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connector.AzureBlob;

/// <summary>Json (NDJSON) write session: the temp blob's write stream opened once over the session's
/// lifetime, <see cref="NdjsonWriteCodec.WriteAsync"/> appending each received batch's newline-delimited JSON
/// lines directly to it. Unlike <see cref="AzureCsvWriteSession"/>, NDJSON has no header/footer framing --
/// every batch's lines are self-contained and simply append, so there is no header-written-once state to
/// track (mirrors <c>AzureCsvWriteSession</c> minus the header).</summary>
internal sealed class AzureJsonWriteSession : AzureWriteSession
{
    private Stream? _blobStream;
    private bool _closed;

    private AzureJsonWriteSession(BlobContainerClient container, string tempBlobName, string finalBlobName,
        Stream blobStream, IOperationGate? gate)
        : base(container, tempBlobName, finalBlobName, gate)
    {
        _blobStream = blobStream;
    }

    internal static async Task<AzureJsonWriteSession> CreateAsync(
        BlobContainerClient container, string tempBlobName, string finalBlobName, Schema arrowSchema,
        CancellationToken ct, IOperationGate? gate)
    {
        var tempBlob = container.GetBlockBlobClient(tempBlobName);
        Stream? stream = null;
        try
        {
            // Idempotent: overwrite:true open is repeat-safe before any data is written.
            stream = await GatedAsync(gate, "azure.open_write", async innerCt =>
            {
                try
                {
                    return await tempBlob.OpenWriteAsync(overwrite: true, cancellationToken: innerCt).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is RequestFailedException or IOException)
                {
                    throw new PzConnectorException(
                        $"azure blob '{tempBlobName}': open failed: {ex.Message}", AzureTransient.IsTransient(ex),
                        innerException: ex);
                }
            }, ct).ConfigureAwait(false);
            return new AzureJsonWriteSession(container, tempBlobName, finalBlobName, stream, gate);
        }
        catch
        {
            await CleanupFailedCreateAsync(stream, tempBlob, gate).ConfigureAwait(false);
            throw;
        }
    }

    private static async Task CleanupFailedCreateAsync(Stream? stream, BlockBlobClient tempBlob, IOperationGate? gate)
    {
        if (stream is not null)
        {
            await stream.DisposeAsync().ConfigureAwait(false);
        }
        await TryDeleteAsync(tempBlob, gate).ConfigureAwait(false);
    }

    // Deliberately not gated: streaming writes over one open stream are not discrete ops.
    protected override async ValueTask WriteBatchCoreAsync(RecordBatch batch, CancellationToken ct)
    {
        try
        {
            await NdjsonWriteCodec.WriteAsync(batch, _blobStream!, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is RequestFailedException or IOException)
        {
            throw new PzConnectorException(
                $"azure blob '{TempBlob.Name}': write failed: {ex.Message}", AzureTransient.IsTransient(ex), innerException: ex);
        }
    }

    protected override async ValueTask CloseWriterAsync()
    {
        if (_closed)
        {
            return;
        }

        _closed = true;

        if (_blobStream is not null)
        {
            try
            {
                // Disposing the blob write stream flushes and commits the temp blob's block list.
                await _blobStream.FlushAsync().ConfigureAwait(false);
                await _blobStream.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is RequestFailedException or IOException)
            {
                throw new PzConnectorException(
                    $"azure blob '{TempBlob.Name}': close failed: {ex.Message}", AzureTransient.IsTransient(ex), innerException: ex);
            }
            _blobStream = null;
        }
    }

    private static async Task TryDeleteAsync(BlockBlobClient blob, IOperationGate? gate)
    {
        try
        {
            // Idempotent: DeleteIfExists is idempotent by definition.
            await GatedAsync(gate, "azure.delete_temp",
                innerCt => blob.DeleteIfExistsAsync(cancellationToken: innerCt), CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch
        {
            // Suppressed by design: never mask the construction failure with cleanup fallout.
        }
    }
}
