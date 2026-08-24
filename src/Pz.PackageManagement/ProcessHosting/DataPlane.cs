using System.Net.Sockets;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Memory;
using Pz.Connectors.Abstractions.Memory;
using Pz.Connectors.Protocol;
using Pz.PackageManagement.Hosting;

namespace Pz.PackageManagement.ProcessHosting;

/// <summary>The data-plane half of PCP: raw Arrow IPC over the <c>&lt;control socket&gt;.data</c> unix
/// socket, ticketed and unary in direction. No protobuf, no per-batch acks -- backpressure is the
/// socket itself, exactly as an in-process connector's <see cref="IAsyncEnumerable{RecordBatch}"/>
/// applies backpressure through the consumer simply not calling <c>MoveNextAsync</c> again.
///
/// <para>The host always dials; a ticket (minted by the control-plane <c>OpenReadStream</c>/
/// <c>BeginWrite</c> RPC) is the 16-byte preamble that tells the connector which already-planned unit
/// of work this connection serves. Nothing here interprets configuration -- that was all decided when
/// the ticket was minted.</para></summary>
public static class DataPlane
{
    /// <summary>Connects to <paramref name="dataSocketPath"/>, writes the 16-byte ticket preamble, then
    /// reads a standard Arrow IPC stream off it. Batch buffers land in
    /// <see cref="PooledNativeAllocator.Shared"/> (passed to <see cref="ArrowStreamReader"/> as its
    /// allocator) so the no-LOH story matches in-proc; the caller owns and disposes each yielded batch.
    ///
    /// <para>A read is complete only when the stream ends with the Arrow IPC end-of-stream marker
    /// (continuation 0xFFFFFFFF + zero metadata length) -- every other way the stream can end surfaces
    /// as <see cref="ConnectorHostException"/> PZ0357 instead of a silent partial read:
    /// <list type="bullet">
    /// <item>no schema ever arrived (unknown/already-burned/malformed ticket);</item>
    /// <item>a mid-message truncation faults <see cref="ArrowStreamReader"/> directly (the NORMATIVE
    /// truncation convention: an IPC continuation marker promising a body that never arrives) --
    /// propagated rather than swallowed;</item>
    /// <item>the connection ends cleanly at a MESSAGE boundary but without the end-of-stream marker
    /// itself (e.g. the connector process was killed between two well-formed messages) -- byte-identical
    /// to a legitimate close as far as <see cref="ArrowStreamReader.ReadNextRecordBatchAsync"/> is
    /// concerned (it returns null with a non-null <see cref="ArrowStreamReader.Schema"/> either way), so
    /// this is told apart by remembering the last 8 bytes actually delivered to the reader and requiring
    /// them to be the marker.</item>
    /// </list></para></summary>
    public static async IAsyncEnumerable<RecordBatch> ReadStreamAsync(
        string dataSocketPath,
        ReadOnlyMemory<byte> ticket,
        [EnumeratorCancellation] CancellationToken ct)
    {
        ValidateTicketLength(ticket);
        var (socket, reader, tail) = await OpenReadConnectionAsync(
            dataSocketPath, ticket, PooledNativeAllocator.Shared, ct).ConfigureAwait(false);
        try
        {
            while (true)
            {
                var batch = await ReadNextBatchAsync(reader, tail, dataSocketPath, ct).ConfigureAwait(false);
                if (batch is null)
                {
                    yield break;
                }

                yield return batch;
            }
        }
        finally
        {
            reader.Dispose();
            socket.Dispose();
        }
    }

    /// <summary>Connects to <paramref name="dataSocketPath"/>, writes the ticket, writes the Arrow IPC
    /// start-of-stream (schema) message, and returns a writer whose <see cref="IDataPlaneWriter.WriteBatchAsync"/>
    /// serializes one batch at a time and whose <see cref="IDataPlaneWriter.CompleteAsync"/> writes
    /// end-of-stream and half-closes the socket -- the precondition <c>CommitWrite</c> waits on.</summary>
    public static async Task<IDataPlaneWriter> OpenWriteStreamAsync(
        string dataSocketPath, ReadOnlyMemory<byte> ticket, Schema schema, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(schema);
        ValidateTicketLength(ticket);
        var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        try
        {
            await ConnectAndSendTicketAsync(socket, dataSocketPath, ticket, ct).ConfigureAwait(false);
            var stream = new NetworkStream(socket, ownsSocket: false);
            var writer = new ArrowStreamWriter(stream, schema, leaveOpen: true);
            await writer.WriteStartAsync(ct).ConfigureAwait(false);
            return new DataPlaneWriter(socket, stream, writer);
        }
        catch (Exception ex)
        {
            // The socket must never leak regardless of WHY this failed -- caller cancellation included,
            // since a cancelled ct still leaves a live fd behind if it isn't disposed here.
            socket.Dispose();
            if (ex is OperationCanceledException)
            {
                throw;
            }

            throw DataPlaneFailed(dataSocketPath, "opening the write stream failed", ex);
        }
    }

    private static async Task<(Socket Socket, ArrowStreamReader Reader, TailTrackingStream Tail)> OpenReadConnectionAsync(
        string dataSocketPath, ReadOnlyMemory<byte> ticket, MemoryAllocator allocator, CancellationToken ct)
    {
        var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        try
        {
            await ConnectAndSendTicketAsync(socket, dataSocketPath, ticket, ct).ConfigureAwait(false);
            var stream = new NetworkStream(socket, ownsSocket: false);
            var tail = new TailTrackingStream(stream);
            var reader = new ArrowStreamReader(tail, allocator, leaveOpen: false);
            return (socket, reader, tail);
        }
        catch (Exception ex)
        {
            // Same leak hazard as OpenWriteStreamAsync above: dispose unconditionally, then decide
            // whether this was caller cancellation (rethrow as-is) or a real connect/handshake failure
            // (wrap as PZ0357).
            socket.Dispose();
            if (ex is OperationCanceledException)
            {
                throw;
            }

            throw DataPlaneFailed(dataSocketPath, "opening the read stream failed", ex);
        }
    }

    /// <summary>Fails fast on the caller's own bug rather than sending a malformed preamble that would
    /// desync the wire (the connector reads exactly <see cref="ProtocolConstants.TicketLength"/> bytes
    /// and treats whatever comes next as the ticket value, whether or not that boundary lines up with
    /// what was intended). Called before either public entry point allocates a socket, so this surfaces
    /// as a plain <see cref="ArgumentException"/> -- never wrapped as PZ0357, which is reserved for a
    /// connect/protocol failure against a real peer.</summary>
    private static void ValidateTicketLength(ReadOnlyMemory<byte> ticket)
    {
        if (ticket.Length != ProtocolConstants.TicketLength)
        {
            throw new ArgumentException(
                $"ticket must be exactly {ProtocolConstants.TicketLength} bytes, got {ticket.Length}",
                nameof(ticket));
        }
    }

    private static async Task ConnectAndSendTicketAsync(
        Socket socket, string dataSocketPath, ReadOnlyMemory<byte> ticket, CancellationToken ct)
    {
        await socket.ConnectAsync(new UnixDomainSocketEndPoint(dataSocketPath), ct).ConfigureAwait(false);
        // Stream.WriteAsync is defined to write the whole buffer (unlike Read, which may return short),
        // so this single call is the entire 16-byte preamble -- no length loop needed.
        await using var preambleStream = new NetworkStream(socket, ownsSocket: false);
        await preambleStream.WriteAsync(ticket, ct).ConfigureAwait(false);
        await preambleStream.FlushAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Reads one batch, translating a torn stream into PZ0357 rather than letting the raw
    /// Arrow/IO exception escape, and -- since <see cref="ArrowStreamReader.ReadNextRecordBatchAsync"/>
    /// returns null cleanly for every way a stream can END at a message boundary, whether or not that
    /// boundary is where the writer actually meant to stop -- checking both <see cref="ArrowStreamReader.Schema"/>
    /// (was a schema ever seen at all) and <paramref name="tail"/> (did the stream truly end with the
    /// Arrow end-of-stream marker, or just stop) before treating a null read as a legitimate end of
    /// stream.</summary>
    private static async Task<RecordBatch?> ReadNextBatchAsync(
        ArrowStreamReader reader, TailTrackingStream tail, string dataSocketPath, CancellationToken ct)
    {
        RecordBatch? batch;
        try
        {
            batch = await reader.ReadNextRecordBatchAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            throw DataPlaneFailed(dataSocketPath, "connector failed mid-stream", ex);
        }

        if (batch is null)
        {
            if (reader.Schema is null)
            {
                throw DataPlaneFailed(
                    dataSocketPath,
                    "the connector closed the data socket before sending a schema " +
                    "(unknown, already-used, or invalid ticket)",
                    innerException: null);
            }

            if (!tail.EndsWithArrowEndOfStreamMarker())
            {
                throw DataPlaneFailed(
                    dataSocketPath,
                    "stream ended without an Arrow end-of-stream marker -- the connector likely crashed mid-stream",
                    innerException: null);
            }
        }

        return batch;
    }

    private static ConnectorHostException DataPlaneFailed(string dataSocketPath, string cause, Exception? innerException)
    {
        var detail = innerException is null ? cause : $"{cause}: {innerException.GetType().Name}: {innerException.Message}";
        return new ConnectorHostException(
            "PZ0357",
            $"connector data-plane socket '{dataSocketPath}' failed: {detail}",
            "check connector logs and confirm the connector and host ABI versions are compatible");
    }

    /// <summary>Read-only wrapper that remembers the last 8 bytes it has delivered to its reader,
    /// without buffering or otherwise altering the stream. Its sole purpose is telling apart the two
    /// shapes <see cref="ArrowStreamReader.ReadNextRecordBatchAsync"/> cannot distinguish on its own: a
    /// stream that ends at a message boundary because the writer sent the Arrow IPC end-of-stream marker
    /// (continuation 0xFFFFFFFF + zero metadata length, exactly 8 bytes) versus one that ends there
    /// because the peer stopped sending mid-protocol (crashed, was killed) between two well-formed
    /// messages -- both make <c>ReadNextRecordBatchAsync</c> return null with a non-null
    /// <see cref="ArrowStreamReader.Schema"/>. See <see cref="ReadNextBatchAsync"/>, the only caller that
    /// inspects <see cref="EndsWithArrowEndOfStreamMarker"/>.</summary>
    private sealed class TailTrackingStream(Stream inner) : Stream
    {
        private static ReadOnlySpan<byte> EndOfStreamMarker => [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00];

        private readonly byte[] _tail = new byte[8];
        private int _filled;

        public bool EndsWithArrowEndOfStreamMarker() =>
            _filled == 8 && _tail.AsSpan().SequenceEqual(EndOfStreamMarker);

        public override bool CanRead => inner.CanRead;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush() => inner.Flush();

        public override int Read(byte[] buffer, int offset, int count)
        {
            var n = inner.Read(buffer, offset, count);
            Record(buffer.AsSpan(offset, n));
            return n;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var n = await inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            Record(buffer.Span[..n]);
            return n;
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        /// <summary>Folds <paramref name="data"/> into the trailing-8-bytes window in delivery order.
        /// A chunk of 8 or more bytes replaces the window outright (its own last 8 bytes); a smaller
        /// chunk shifts the window left and appends -- either way this is O(min(chunk length, 8)), never
        /// O(chunk length), so a multi-megabyte batch body costs the same 8-byte shuffle as a small
        /// one.</summary>
        private void Record(ReadOnlySpan<byte> data)
        {
            if (data.IsEmpty)
            {
                return;
            }

            if (data.Length >= 8)
            {
                data[^8..].CopyTo(_tail);
                _filled = 8;
                return;
            }

            var shift = data.Length;
            var keep = 8 - shift;
            System.Array.Copy(_tail, shift, _tail, 0, keep);
            data.CopyTo(_tail.AsSpan(keep));
            _filled = Math.Min(8, _filled + shift);
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                inner.Dispose();
            }

            base.Dispose(disposing);
        }
    }

    /// <summary>The write-side data-plane connection: one Arrow IPC stream writer over one ticketed
    /// socket. No per-batch acks -- the socket's own send buffer is the backpressure, so
    /// <see cref="WriteBatchAsync"/> completing means the batch was handed to the OS, not that the
    /// connector has consumed it.</summary>
    private sealed class DataPlaneWriter(Socket socket, NetworkStream stream, ArrowStreamWriter writer) : IDataPlaneWriter
    {
        private int _disposed;

        public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
        {
            ArgumentNullException.ThrowIfNull(batch);
            // Never disposes batch: the engine owns it until this call returns, exactly as the in-proc
            // write path requires (ArrowStreamWriter.WriteRecordBatchAsync only reads from it).
            await writer.WriteRecordBatchAsync(batch, ct).ConfigureAwait(false);
        }

        public async ValueTask CompleteAsync(CancellationToken ct)
        {
            await writer.WriteEndAsync(ct).ConfigureAwait(false);
            await stream.FlushAsync(ct).ConfigureAwait(false);
            // Half-close only: the receive side stays open, which is the precondition CommitWrite's
            // drain-then-commit ordering depends on -- a full close here would race the connector's own
            // read-to-end-of-stream detection instead of signaling it cleanly.
            socket.Shutdown(SocketShutdown.Send);
        }

        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return ValueTask.CompletedTask;
            }

            writer.Dispose();
            stream.Dispose();
            socket.Dispose();
            return ValueTask.CompletedTask;
        }
    }
}

/// <summary>One open write-side data-plane stream: batches go out one at a time with no per-batch ack,
/// and <see cref="CompleteAsync"/> is what tells the connector (and, through it, the control-plane
/// <c>CommitWrite</c> RPC) that no more are coming.</summary>
public interface IDataPlaneWriter : IAsyncDisposable
{
    /// <summary>Serializes one batch to the IPC stream. Does NOT take ownership -- the caller still
    /// owns and disposes <paramref name="batch"/> after this returns, exactly as the in-process
    /// <c>ISinkWriteSession.WriteBatchAsync</c> contract requires.</summary>
    ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct);

    /// <summary>Writes the Arrow IPC end-of-stream marker and half-closes the socket
    /// (<see cref="SocketShutdown.Send"/>) -- the precondition the control-plane <c>CommitWrite</c> RPC
    /// waits on before it may run.</summary>
    ValueTask CompleteAsync(CancellationToken ct);
}
