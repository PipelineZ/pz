using System.Net.Sockets;
using Pz.Connectors.Abstractions;

namespace Pz.Shared.Probes;

/// <summary>Driverless reachability check for <c>pz validate --connect</c>: opens and closes one TCP
/// connection. It proves only that something accepts connections at the address; credentials are
/// verified by the first run.
///
/// <para>Compiled into the ducklake, iceberg and quack connectors from this one source file:
/// connectors never reference each other.</para></summary>
internal static class TcpProbe
{
    private static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Checks that <paramref name="host"/>:<paramref name="port"/> accepts a TCP connection
    /// within five seconds. <paramref name="what"/> names the endpoint in the result message. A
    /// bracketed IPv6 literal is accepted: the brackets are stripped for the socket and kept in the
    /// message.</summary>
    internal static async ValueTask<ConnectionCheck> CheckAsync(string host, int port, string what, CancellationToken ct)
    {
        try
        {
            using var client = new TcpClient();
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(ConnectTimeout);
            await client.ConnectAsync(host.Trim('[', ']'), port, timeout.Token).ConfigureAwait(false);
            return new ConnectionCheck(true, $"{what} reachable at {host}:{port} (tcp); credentials are verified at run time");
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return new ConnectionCheck(false, $"transient: {what} at {host}:{port} did not accept a connection within {ConnectTimeout.TotalSeconds:0}s");
        }
        catch (SocketException ex)
        {
            return new ConnectionCheck(false, $"transient: {what} at {host}:{port} is unreachable ({ex.SocketErrorCode})");
        }
    }
}
