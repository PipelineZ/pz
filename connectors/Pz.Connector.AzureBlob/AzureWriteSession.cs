using Apache.Arrow;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Parquet;
using Parquet.Schema;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connector.AzureBlob;

internal enum AzureSessionState { Open, Committed, Aborted }

/// <summary>Shared temp-blob-write + server-side-copy-promote commit protocol for the azure universal write
/// session, mirroring <c>LocalFileWriteSessionBase</c> (<c>connectors/Pz.Connector.LocalFiles/LocalFilesSink.cs</c>)
/// state-machine-for-state-machine: Open→Committed / Open→Aborted, commit-attempted permanently disables an
/// implicit abort on Dispose (Commit's true outcome is unknown once invoked -- see <see cref="ISinkWriteSession.AbortAsync"/>'s
/// doc), dispose-without-commit aborts, a session already Committed/Aborted rejects further writes/commits/aborts
/// with <see cref="InvalidOperationException"/>.
///
/// Commit: close the format writer (flushes remaining buffered bytes + finalizes/commits the temp blob's
/// block list), server-side copy temp -&gt; final (<see cref="BlobBaseClient.StartCopyFromUriAsync(Uri, Azure.Storage.Blobs.Models.BlobCopyFromUriOptions?, CancellationToken)"/>,
/// same-account so no SAS is needed), delete the temp blob. Abort: close the format writer, delete the temp
/// blob (the final blob is never touched, so it never exists after an aborted session).</summary>
internal abstract class AzureWriteSession(
    BlobContainerClient container, string tempBlobName, string finalBlobName, IOperationGate? gate)
    : ISinkWriteSession
{
    private AzureSessionState _state = AzureSessionState.Open;
    private bool _commitAttempted;
    private long _rowsWritten;
    private long _batchesWritten;

    protected BlockBlobClient TempBlob { get; } = container.GetBlockBlobClient(tempBlobName);

    /// <summary>Routes one discrete write-session op through the engine-supplied gate when present; with
    /// no gate, calls straight through. Classification into PzConnectorException happens INSIDE
    /// <paramref name="op"/>, so the gate always sees fully-classified transient/permanent exceptions
    /// (same shape as HTTP's FetchPageAsync wrapper).</summary>
    private protected static Task<T> GatedAsync<T>(IOperationGate? gate, string opLabel,
        Func<CancellationToken, Task<T>> op, CancellationToken ct)
        => gate is null ? op(ct) : gate.ExecuteAsync(opLabel, idempotent: true, op, ct);

    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        EnsureOpen("write to");
        await WriteBatchCoreAsync(batch, ct).ConfigureAwait(false);
        _rowsWritten += batch.Length;
        _batchesWritten++;
    }

    public async ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        EnsureOpen("commit");
        _commitAttempted = true;

        await CloseWriterAsync().ConfigureAwait(false);

        var finalBlob = container.GetBlockBlobClient(finalBlobName);
        // Idempotent: same-source overwrite copy is repeat-safe.
        await GatedAsync(gate, "azure.commit_copy", async innerCt =>
        {
            try
            {
                var copyOperation = await finalBlob.StartCopyFromUriAsync(TempBlob.Uri, cancellationToken: innerCt)
                    .ConfigureAwait(false);
                await copyOperation.WaitForCompletionAsync(innerCt).ConfigureAwait(false);
                return true;
            }
            catch (Exception ex) when (ex is RequestFailedException or IOException)
            {
                throw new PzConnectorException(
                    $"azure blob '{finalBlobName}': commit copy-promote failed: {ex.Message}",
                    AzureTransient.IsTransient(ex), innerException: ex);
            }
        }, ct).ConfigureAwait(false);

        // The copy-promote above already succeeded -- the final blob now holds the committed data. A
        // transient (or any) failure deleting the now-orphaned temp blob must never turn an already-landed
        // commit into a failed one, so route through the same best-effort helper Abort/Dispose use rather
        // than letting a raw delete failure propagate and mask a commit that in fact succeeded.
        await TryDeleteTempBlobAsync(ct).ConfigureAwait(false);

        _state = AzureSessionState.Committed;
        return new WriteResult(_rowsWritten, _batchesWritten);
    }

    public async ValueTask AbortAsync(CancellationToken ct)
    {
        EnsureOpen("abort");

        await CloseWriterAsync().ConfigureAwait(false);
        await TryDeleteTempBlobAsync(ct).ConfigureAwait(false);

        _state = AzureSessionState.Aborted;
    }

    public async ValueTask DisposeAsync()
    {
        if (_state != AzureSessionState.Open)
        {
            return;
        }

        if (_commitAttempted)
        {
            // Commit was attempted (and threw) -- per Commit-xor-Abort this must NOT count as an implicit
            // abort. Just release local resources; the temp/final blobs' fate is unknown (the copy-promote
            // may or may not have completed) so they are deliberately left alone.
            await CloseWriterAsync().ConfigureAwait(false);
            return;
        }

        await CloseWriterAsync().ConfigureAwait(false);
        await TryDeleteTempBlobAsync(CancellationToken.None).ConfigureAwait(false);
        _state = AzureSessionState.Aborted;
    }

    protected abstract ValueTask WriteBatchCoreAsync(RecordBatch batch, CancellationToken ct);

    /// <summary>Flushes and releases the underlying format writer/blob stream. Idempotent -- may be called
    /// more than once (Commit/Abort followed by Dispose).</summary>
    protected abstract ValueTask CloseWriterAsync();

    private async Task TryDeleteTempBlobAsync(CancellationToken ct)
    {
        try
        {
            // Idempotent: DeleteIfExists is idempotent by definition.
            // Gate exhaustion in this best-effort path stays suppressed by the catch below exactly
            // like any other cleanup failure -- it never surfaces to the caller.
            await GatedAsync(gate, "azure.delete_temp",
                innerCt => TempBlob.DeleteIfExistsAsync(cancellationToken: innerCt), ct).ConfigureAwait(false);
        }
        catch
        {
            // Suppressed by design: never mask an earlier failure with cleanup fallout (mirrors
            // LocalFilesSink.TryDeleteDir).
        }
    }

    private void EnsureOpen(string action)
    {
        if (_state != AzureSessionState.Open)
        {
            throw new InvalidOperationException($"cannot {action} a session already {_state.ToString().ToLowerInvariant()}");
        }
    }
}

/// <summary>Parquet write session: one Parquet.Net writer opened once over the temp blob's write stream
/// (<see cref="BlockBlobClient.OpenWriteAsync(bool, Azure.Storage.Blobs.Models.BlockBlobOpenWriteOptions?, CancellationToken)"/>,
/// which is a forward-only, non-seekable stream -- Parquet.Net's row-group writer only needs to write
/// forward, confirmed against a non-seekable stream wrapper), one row group written per received batch.</summary>
internal sealed class AzureParquetWriteSession : AzureWriteSession
{
    private readonly DataField[] _fields;
    private Stream? _blobStream;
    private ParquetWriter? _writer;
    private bool _closed;

    private AzureParquetWriteSession(BlobContainerClient container, string tempBlobName, string finalBlobName,
        DataField[] fields, Stream blobStream, ParquetWriter writer, IOperationGate? gate)
        : base(container, tempBlobName, finalBlobName, gate)
    {
        _fields = fields;
        _blobStream = blobStream;
        _writer = writer;
    }

    internal static async Task<AzureParquetWriteSession> CreateAsync(
        BlobContainerClient container, string tempBlobName, string finalBlobName, Schema arrowSchema,
        CancellationToken ct, IOperationGate? gate)
    {
        var fields = AzureBlobFormat.BuildDataFields(arrowSchema);
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
            var writer = await AzureBlobFormat.CreateParquetWriterAsync(stream, fields, ct).ConfigureAwait(false);
            return new AzureParquetWriteSession(container, tempBlobName, finalBlobName, fields, stream, writer, gate);
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
            await AzureBlobFormat.WriteRowGroupAsync(_writer!, _fields, batch, ct).ConfigureAwait(false);
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

        try
        {
            if (_writer is not null)
            {
                await _writer.DisposeAsync().ConfigureAwait(false);
                _writer = null;
            }

            if (_blobStream is not null)
            {
                await _blobStream.DisposeAsync().ConfigureAwait(false);
                _blobStream = null;
            }
        }
        catch (Exception ex) when (ex is RequestFailedException or IOException)
        {
            throw new PzConnectorException(
                $"azure blob '{TempBlob.Name}': close failed: {ex.Message}", AzureTransient.IsTransient(ex), innerException: ex);
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

/// <summary>Csv write session: a <see cref="CsvWriteCodec"/> opened once over the temp blob's write
/// stream, header written at session creation, rows appended per received batch. The encoding itself is
/// the toolkit's shared codec — the same one the LocalFiles sink uses, so csv output stays byte-identical
/// across connectors.</summary>
internal sealed class AzureCsvWriteSession : AzureWriteSession
{
    private CsvWriteCodec? _writer;
    private bool _closed;

    private AzureCsvWriteSession(BlobContainerClient container, string tempBlobName, string finalBlobName,
        CsvWriteCodec writer, IOperationGate? gate)
        : base(container, tempBlobName, finalBlobName, gate)
    {
        _writer = writer;
    }

    internal static async Task<AzureCsvWriteSession> CreateAsync(
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
            var writer = new CsvWriteCodec(stream, arrowSchema, "azure universal csv sink");
            return new AzureCsvWriteSession(container, tempBlobName, finalBlobName, writer, gate);
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
            await _writer!.WriteBatchAsync(batch, ct).ConfigureAwait(false);
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

        if (_writer is not null)
        {
            try
            {
                // Disposing the codec flushes and disposes the underlying blob write stream too
                // (default leaveOpen: false) -- that Dispose is what commits the temp blob's block list.
                await _writer.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is RequestFailedException or IOException)
            {
                throw new PzConnectorException(
                    $"azure blob '{TempBlob.Name}': close failed: {ex.Message}", AzureTransient.IsTransient(ex), innerException: ex);
            }
            _writer = null;
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
