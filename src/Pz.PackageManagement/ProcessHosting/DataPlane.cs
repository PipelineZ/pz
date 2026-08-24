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
    /// <para>Two failure shapes never look like a clean end of stream (never silently under-yield):
    /// <list type="bullet">
    /// <item>the peer closes before ever sending a schema message (unknown/already-burned/malformed
    /// ticket) -- <see cref="ArrowStreamReader.Schema"/> stays null even though
    /// <see cref="ArrowStreamReader.ReadNextRecordBatchAsync(CancellationToken)"/> itself returns null
    /// cleanly for a zero-byte stream, so that case is checked explicitly;</item>
    /// <item>the peer signals a torn stream via the NORMATIVE truncation convention (an IPC continuation
    /// marker promising a body that never arrives) -- Apache.Arrow's reader already faults mid-parse on
    /// that shape, and this method propagates the fault rather than swallowing it.</item>
    /// </list>
    /// Both surface as <see cref="ConnectorHostException"/> PZ0357.</para></summary>
    public static async IAsyncEnumerable<RecordBatch> ReadStreamAsync(
        string dataSocketPath,
        ReadOnlyMemory<byte> ticket,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var (socket, reader) = await OpenReadConnectionAsync(
            dataSocketPath, ticket, PooledNativeAllocator.Shared, ct).ConfigureAwait(false);
        try
        {
            while (true)
            {
                var batch = await ReadNextBatchAsync(reader, dataSocketPath, ct).ConfigureAwait(false);
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
        var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        try
        {
            await ConnectAndSendTicketAsync(socket, dataSocketPath, ticket, ct).ConfigureAwait(false);
            var stream = new NetworkStream(socket, ownsSocket: false);
            var writer = new ArrowStreamWriter(stream, schema, leaveOpen: true);
            await writer.WriteStartAsync(ct).ConfigureAwait(false);
            return new DataPlaneWriter(socket, stream, writer);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            socket.Dispose();
            throw DataPlaneFailed(dataSocketPath, "opening the write stream failed", ex);
        }
    }

    private static async Task<(Socket Socket, ArrowStreamReader Reader)> OpenReadConnectionAsync(
        string dataSocketPath, ReadOnlyMemory<byte> ticket, MemoryAllocator allocator, CancellationToken ct)
    {
        var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        try
        {
            await ConnectAndSendTicketAsync(socket, dataSocketPath, ticket, ct).ConfigureAwait(false);
            var stream = new NetworkStream(socket, ownsSocket: false);
            var reader = new ArrowStreamReader(stream, allocator, leaveOpen: false);
            return (socket, reader);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            socket.Dispose();
            throw DataPlaneFailed(dataSocketPath, "opening the read stream failed", ex);
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
    /// returns null cleanly for BOTH a well-formed empty stream and a stream that never got a schema at
    /// all -- checking <see cref="ArrowStreamReader.Schema"/> to tell those two apart before treating a
    /// null read as a legitimate end of stream.</summary>
    private static async Task<RecordBatch?> ReadNextBatchAsync(
        ArrowStreamReader reader, string dataSocketPath, CancellationToken ct)
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

        if (batch is null && reader.Schema is null)
        {
            throw DataPlaneFailed(
                dataSocketPath,
                "the connector closed the data socket before sending a schema " +
                "(unknown, already-used, or invalid ticket)",
                innerException: null);
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
