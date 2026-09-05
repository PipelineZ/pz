using Apache.Arrow;
using Google.Cloud.Storage.V1;
using Parquet;
using Parquet.Schema;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connector.Gcs;

internal enum GcsSessionState { Open, Committed, Aborted }

/// <summary>Spool-then-atomic-upload commit protocol for the gcs universal write session. A gcs
/// object becomes visible only when its upload completes, so the upload IS the commit — there is no
/// temp object, no copy-promote, and no remote cleanup on abort (the final object is simply never
/// created). Batches are encoded into a local spool file
/// (<see cref="FileOptions.DeleteOnClose"/>, so the OS reclaims it on every exit path);
/// <see cref="CommitAsync"/> closes the format writer, rewinds the spool, and performs one gated
/// idempotent upload (same-content overwrite to the same name is repeat-safe).
///
/// State machine mirrors the azure sessions: Open→Committed / Open→Aborted, commit-attempted
/// permanently disables an implicit abort on Dispose (the upload's true outcome is unknown once
/// invoked — only local resources are released), dispose-without-commit aborts, a session already
/// Committed/Aborted rejects further writes/commits/aborts with
/// <see cref="InvalidOperationException"/>.</summary>
internal abstract class GcsWriteSession(
    StorageClient client, string bucket, string objectName, FileStream spool, IOperationGate? gate)
    : ISinkWriteSession
{
    private GcsSessionState _state = GcsSessionState.Open;
    private bool _commitAttempted;
    private long _rowsWritten;
    private long _batchesWritten;

    protected FileStream Spool { get; } = spool;

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
        await Spool.FlushAsync(ct).ConfigureAwait(false);

        // Idempotent: the upload is atomic server-side and a repeat writes the same bytes to the
        // same name. The op rewinds the spool itself so a gate-level retry re-reads from zero.
        await GatedAsync(gate, "gcs.upload", async innerCt =>
        {
            try
            {
                Spool.Position = 0;
                return await client.UploadObjectAsync(bucket, objectName, contentType: null, Spool,
                    cancellationToken: innerCt).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is Google.GoogleApiException or HttpRequestException or IOException
                or TimeoutException or System.Net.Sockets.SocketException
                or Google.Apis.Auth.OAuth2.Responses.TokenResponseException)
            {
                // TokenResponseException covers the OAuth token fetch the SDK performs before the
                // upload proper -- a dead/unreachable token endpoint must surface as the same
                // classified, named failure as the upload itself.
                throw new PzConnectorException(
                    $"gcs object '{objectName}': upload failed: {ex.Message}",
                    GcsTransient.IsTransient(ex), innerException: ex);
            }
        }, ct).ConfigureAwait(false);

        await Spool.DisposeAsync().ConfigureAwait(false);
        _state = GcsSessionState.Committed;
        return new WriteResult(_rowsWritten, _batchesWritten);
    }

    public async ValueTask AbortAsync(CancellationToken ct)
    {
        EnsureOpen("abort");

        await CloseWriterAsync().ConfigureAwait(false);
        await Spool.DisposeAsync().ConfigureAwait(false);

        _state = GcsSessionState.Aborted;
    }

    public async ValueTask DisposeAsync()
    {
        if (_state != GcsSessionState.Open)
        {
            return;
        }

        // Commit-attempted-then-threw must NOT count as an implicit abort (the upload's outcome is
        // unknown) -- but the spool is purely local either way, so releasing it is always safe.
        await CloseWriterAsync().ConfigureAwait(false);
        await Spool.DisposeAsync().ConfigureAwait(false);
        if (!_commitAttempted)
        {
            _state = GcsSessionState.Aborted;
        }
    }

    protected abstract ValueTask WriteBatchCoreAsync(RecordBatch batch, CancellationToken ct);

    /// <summary>Flushes and releases the format writer over the spool (never the spool itself).
    /// Idempotent -- may be called more than once (Commit/Abort followed by Dispose).</summary>
    protected abstract ValueTask CloseWriterAsync();

    /// <summary>Routes one discrete write-session op through the engine-supplied gate when present;
    /// with no gate, calls straight through. Classification into PzConnectorException happens INSIDE
    /// <paramref name="op"/>, so the gate always sees fully-classified transient/permanent
    /// exceptions (the azure/http shape).</summary>
    private protected static Task<T> GatedAsync<T>(IOperationGate? gate, string opLabel,
        Func<CancellationToken, Task<T>> op, CancellationToken ct)
        => gate is null ? op(ct) : gate.ExecuteAsync(opLabel, idempotent: true, op, ct);

    /// <summary>Opens the session's local spool file: delete-on-close so every exit path (commit,
    /// abort, dispose, even process death) reclaims it without an explicit cleanup step.</summary>
    internal static FileStream OpenSpool() => new(
        Path.Combine(Path.GetTempPath(), $"pz-gcs-{Guid.NewGuid():N}.spool"),
        FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, bufferSize: 81920,
        FileOptions.DeleteOnClose | FileOptions.Asynchronous);

    private void EnsureOpen(string action)
    {
        if (_state != GcsSessionState.Open)
        {
            throw new InvalidOperationException($"cannot {action} a session already {_state.ToString().ToLowerInvariant()}");
        }
    }
}

/// <summary>Parquet spool session: one Parquet.Net writer over the spool stream, one row group
/// written per received batch.</summary>
internal sealed class GcsParquetWriteSession : GcsWriteSession
{
    private readonly DataField[] _fields;
    private ParquetWriter? _writer;
    private bool _closed;

    private GcsParquetWriteSession(StorageClient client, string bucket, string objectName,
        FileStream spool, DataField[] fields, ParquetWriter writer, IOperationGate? gate)
        : base(client, bucket, objectName, spool, gate)
    {
        _fields = fields;
        _writer = writer;
    }

    internal static async Task<GcsParquetWriteSession> CreateAsync(
        StorageClient client, string bucket, string objectName, Schema arrowSchema,
        CancellationToken ct, IOperationGate? gate)
    {
        var fields = GcsFormat.BuildDataFields(arrowSchema);
        var spool = OpenSpool();
        try
        {
            var writer = await GcsFormat.CreateParquetWriterAsync(spool, fields, ct).ConfigureAwait(false);
            return new GcsParquetWriteSession(client, bucket, objectName, spool, fields, writer, gate);
        }
        catch
        {
            await spool.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    protected override ValueTask WriteBatchCoreAsync(RecordBatch batch, CancellationToken ct) =>
        new(GcsFormat.WriteRowGroupAsync(_writer!, _fields, batch, ct));

    protected override async ValueTask CloseWriterAsync()
    {
        if (_closed)
        {
            return;
        }

        _closed = true;
        if (_writer is not null)
        {
            // Writes the file footer to the spool; the spool itself stays open for the upload.
            await _writer.DisposeAsync().ConfigureAwait(false);
            _writer = null;
        }
    }
}

/// <summary>Csv spool session: the toolkit's shared <see cref="CsvWriteCodec"/> (the same one the
/// LocalFiles/azure sinks use, so csv output stays byte-identical across connectors) over the spool,
/// header written at session creation, rows appended per received batch.</summary>
internal sealed class GcsCsvWriteSession : GcsWriteSession
{
    private CsvWriteCodec? _writer;
    private bool _closed;

    private GcsCsvWriteSession(StorageClient client, string bucket, string objectName,
        FileStream spool, CsvWriteCodec writer, IOperationGate? gate)
        : base(client, bucket, objectName, spool, gate)
    {
        _writer = writer;
    }

    internal static GcsCsvWriteSession Create(
        StorageClient client, string bucket, string objectName, Schema arrowSchema, char delimiter, IOperationGate? gate)
    {
        var spool = OpenSpool();
        try
        {
            var writer = new CsvWriteCodec(spool, arrowSchema, "gcs universal csv sink", leaveOpen: true, delimiter: delimiter);
            return new GcsCsvWriteSession(client, bucket, objectName, spool, writer, gate);
        }
        catch
        {
            spool.Dispose();
            throw;
        }
    }

    protected override ValueTask WriteBatchCoreAsync(RecordBatch batch, CancellationToken ct) =>
        _writer!.WriteBatchAsync(batch, ct);

    protected override async ValueTask CloseWriterAsync()
    {
        if (_closed)
        {
            return;
        }

        _closed = true;
        if (_writer is not null)
        {
            // leaveOpen: the codec flushes its own buffering; the spool stays open for the upload.
            await _writer.DisposeAsync().ConfigureAwait(false);
            _writer = null;
        }
    }
}

/// <summary>Json (NDJSON) spool session: the toolkit's shared stateless
/// <see cref="NdjsonWriteCodec"/> appends each received batch's newline-delimited JSON straight to
/// the spool; there is no writer object to close.</summary>
internal sealed class GcsJsonWriteSession : GcsWriteSession
{
    private GcsJsonWriteSession(StorageClient client, string bucket, string objectName,
        FileStream spool, IOperationGate? gate)
        : base(client, bucket, objectName, spool, gate)
    {
    }

    internal static GcsJsonWriteSession Create(
        StorageClient client, string bucket, string objectName, IOperationGate? gate) =>
        new(client, bucket, objectName, OpenSpool(), gate);

    protected override ValueTask WriteBatchCoreAsync(RecordBatch batch, CancellationToken ct) =>
        new(NdjsonWriteCodec.WriteAsync(batch, Spool, ct));

    protected override ValueTask CloseWriterAsync() => default;
}
