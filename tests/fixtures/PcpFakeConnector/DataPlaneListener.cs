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
        using (connection)
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
                    // only that the peer hung up.
                    return;
                }

                switch (entry)
                {
                    case ReadTicket read:
                        await ServeReadAsync(connection, stream, read).ConfigureAwait(false);
                        break;
                    case WriteTicket write:
                        await ServeWriteAsync(stream, write).ConfigureAwait(false);
                        break;
                }
            }
            catch (Exception ex) when (ex is OperationCanceledException or IOException or SocketException)
            {
                // The host hung up or the fixture is shutting down. A read stream's loss is the host's
                // to observe; a write stream's loss already faulted its Drained gate below.
            }
            catch (Exception ex)
            {
                // The data plane carries no error frames, so a failure here would otherwise reach the
                // host as an unexplained truncation. stderr is the diagnostics-of-last-resort channel
                // for exactly this, and it is never protocol.
                await Console.Error.WriteLineAsync(
                    $"PcpFakeConnector: data-plane connection failed: {ex}").ConfigureAwait(false);
            }
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
        try
        {
            using var reader = new ArrowStreamReader(stream, leaveOpen: true);
            while (await reader.ReadNextRecordBatchAsync(_stopping.Token).ConfigureAwait(false) is { } batch)
            {
                // Batches read off the wire are ours; ISinkWriteSession must not retain them past the
                // call, exactly as in-proc.
                using (batch)
                {
                    await state.Session.WriteBatchAsync(batch, _stopping.Token).ConfigureAwait(false);
                }
            }

            // End-of-stream seen: only now may CommitWrite proceed.
            state.Drained.TrySetResult();
        }
        catch (Exception ex)
        {
            // A torn or rejected write stream must fail the commit rather than silently commit a
            // prefix, so the control plane's awaiter observes the same exception.
            state.Drained.TrySetException(ex);
            throw;
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
