using System.Net.Sockets;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Pz.Connectors.Protocol;

namespace PcpFakeConnector;

/// <summary>The connector side of the PCP data plane: a bare stream listener on
/// <c>&lt;control socket&gt;.data</c> that speaks Arrow IPC and nothing else.
///
/// <para>Data and control never share a stream. Nothing on this socket is protobuf-framed and nothing
/// here reads configuration — a connection carries a 16-byte ticket preamble, and everything the
/// connector needs to serve it was decided on the control plane when that ticket was minted. The host
/// always dials; the connector never has to locate anything.</para></summary>
internal sealed class DataPlaneListener : IAsyncDisposable
{
    private readonly Socket _listener;
    private readonly TicketRegistry _tickets;
    private readonly CancellationTokenSource _stopping = new();
    private readonly Task _acceptLoop;

    private DataPlaneListener(Socket listener, TicketRegistry tickets)
    {
        _listener = listener;
        _tickets = tickets;
        _acceptLoop = Task.Run(AcceptLoopAsync);
    }

    public static DataPlaneListener Start(string socketPath, TicketRegistry tickets)
    {
        if (File.Exists(socketPath))
        {
            File.Delete(socketPath);
        }

        var listener = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        try
        {
            listener.Bind(new UnixDomainSocketEndPoint(socketPath));
            listener.Listen(backlog: 32);
            SocketPermissions.RestrictToOwner(socketPath);
        }
        catch
        {
            listener.Dispose();
            throw;
        }

        return new DataPlaneListener(listener, tickets);
    }

    private async Task AcceptLoopAsync()
    {
        while (!_stopping.IsCancellationRequested)
        {
            Socket connection;
            try
            {
                connection = await _listener.AcceptAsync(_stopping.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException)
            {
                return;
            }

            _ = Task.Run(() => ServeAsync(connection));
        }
    }

    private async Task ServeAsync(Socket connection)
    {
        try
        {
            try
            {
                await using var stream = new NetworkStream(connection, ownsSocket: false);
                var ticket = new byte[ProtocolConstants.TicketLength];
                if (!await TryReadExactlyAsync(stream, ticket, _stopping.Token).ConfigureAwait(false) ||
                    !_tickets.TryBurn(ticket, out var entry))
                {
                    // Unknown or already-burned ticket: close without writing. A protocol violation is
                    // never answered with data, and never with a diagnosis either -- the host learns
                    // only that the peer hung up, before a single Arrow message, which no successful
                    // read ever looks like.
                    return;
                }

                switch (entry)
                {
                    case ReadTicket read:
                        try
                        {
                            await ServeReadAsync(connection, stream, read).ConfigureAwait(false);
                        }
                        catch
                        {
                            await SignalTruncatedAsync(stream).ConfigureAwait(false);
                            throw;
                        }

                        break;
                    case WriteTicket write:
                        await ServeWriteAsync(stream, write).ConfigureAwait(false);
                        break;
                }
            }
            catch (Exception ex) when (ex is OperationCanceledException or IOException or SocketException)
            {
                // The host hung up or the fixture is shutting down. A write stream's loss already
                // faulted its Drained gate; a read stream's loss reaches the host as the truncated
                // message written above.
            }
            catch (Exception ex)
            {
                // stderr is the diagnostics-of-last-resort channel, and it is never protocol: the
                // truncation tells the host the stream failed, this says why.
                await Console.Error.WriteLineAsync(
                    $"PcpFakeConnector: data-plane connection failed: {ex}").ConfigureAwait(false);
            }
        }
        finally
        {
            connection.Dispose();
        }
    }

    /// <summary>An Arrow IPC message header promising a body that never arrives — the deliberate
    /// truncation a failed read must end with.
    ///
    /// <para>THIS IS THE ONLY TRUNCATION SIGNAL THIS TRANSPORT HAS, and getting it wrong loses rows
    /// silently. An IPC reader treats a graceful close at a message boundary as end-of-stream, so
    /// simply closing after a partition throws is indistinguishable from a successful short read.
    /// Nor can the connection be reset instead: a unix socket has no RST — SO_LINGER 0 is accepted
    /// and does nothing, close() delivers a plain EOF either way (measured; the host read a clean
    /// end-of-stream) — and a named pipe has no reset either. Writing a header whose body cannot
    /// follow forces the reader to fault at the exact point the data stopped, on every v1 transport.
    /// The bytes are the IPC continuation token followed by a metadata length that will never be
    /// satisfied.</para></summary>
    private static readonly byte[] TruncationMarker = [0xFF, 0xFF, 0xFF, 0xFF, 0x10, 0x00, 0x00, 0x00];

    private static async Task SignalTruncatedAsync(Stream stream)
    {
        try
        {
            await stream.WriteAsync(TruncationMarker).ConfigureAwait(false);
            await stream.FlushAsync().ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
        {
            // The host is already gone, so it has already failed this read by other means.
        }
    }

    private async Task ServeReadAsync(Socket connection, Stream stream, ReadTicket read)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(_stopping.Token, read.OpToken);
        var ct = linked.Token;

        using var writer = new ArrowStreamWriter(stream, read.Schema, leaveOpen: true);
        await writer.WriteStartAsync(ct).ConfigureAwait(false);
        await foreach (var batch in read.Partition.ReadAsync(read.Options, ct).ConfigureAwait(false))
        {
            // The fixture stands where the engine stands in-proc, so it owns every batch the
            // partition yields and disposes it the moment it is on the wire.
            using (batch)
            {
                await writer.WriteRecordBatchAsync(batch, ct).ConfigureAwait(false);
            }
        }

        await writer.WriteEndAsync(ct).ConfigureAwait(false);
        await stream.FlushAsync(ct).ConfigureAwait(false);
        connection.Shutdown(SocketShutdown.Send);
    }

    private async Task ServeWriteAsync(Stream stream, WriteTicket write)
    {
        var state = write.Session;
        var pump = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!state.TryBeginPump(pump.Task))
        {
            // The control plane already closed this session (aborted, or committed). Writing into it
            // now would be a use-after-abort, so the connection just dies.
            state.Drained.TrySetException(new InvalidOperationException(
                $"write session '{state.SessionId}' was closed before its data stream opened"));
            return;
        }

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            _stopping.Token, state.Cancellation.Token);
        var ct = linked.Token;
        try
        {
            using var reader = new ArrowStreamReader(stream, leaveOpen: true);
            while (await reader.ReadNextRecordBatchAsync(ct).ConfigureAwait(false) is { } batch)
            {
                // Batches read off the wire are ours; ISinkWriteSession must not retain them past the
                // call, exactly as in-proc.
                using (batch)
                {
                    await state.Session.WriteBatchAsync(batch, ct).ConfigureAwait(false);
                }
            }

            // End-of-stream seen: only now may CommitWrite proceed.
            state.Drained.TrySetResult();
        }
        catch (Exception ex)
        {
            // A torn, cancelled or rejected write stream must fail the commit rather than silently
            // commit a prefix, so the control plane's awaiter observes the same exception.
            state.Drained.TrySetException(ex);
            throw;
        }
        finally
        {
            // Unconditional: the control plane waits on this before it commits, aborts or disposes the
            // session, so it must complete however the pump ended.
            pump.TrySetResult();
        }
    }

    private static async Task<bool> TryReadExactlyAsync(Stream stream, byte[] buffer, CancellationToken ct)
    {
        var read = 0;
        while (read < buffer.Length)
        {
            var n = await stream.ReadAsync(buffer.AsMemory(read), ct).ConfigureAwait(false);
            if (n == 0)
            {
                return false;
            }

            read += n;
        }

        return true;
    }

    public async ValueTask DisposeAsync()
    {
        await _stopping.CancelAsync().ConfigureAwait(false);
        _listener.Dispose();
        try
        {
            await _acceptLoop.ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException)
        {
            // Expected: the accept loop was torn down by the cancel/dispose above.
        }

        _stopping.Dispose();
    }
}
