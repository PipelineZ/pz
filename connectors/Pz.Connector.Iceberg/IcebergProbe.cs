using System.Net.Sockets;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Iceberg;

/// <summary>Zero-driver connectivity checks for <c>pz validate --connect</c>: a REST catalog by
/// TCP reachability only — credentials cannot be verified without a driver and are exercised by
/// the first run's attach, whose failure is a redacted PZ0311 — and a local <c>files</c> root by
/// its directory existing.</summary>
internal static class IcebergProbe
{
    private static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(5);

    internal static ValueTask<ConnectionCheck> CheckDirectory(string path)
    {
        if (!Directory.Exists(path))
        {
            return new(new ConnectionCheck(false,
                "permanent: the 'root' directory does not exist -- reads cannot create it; fix the connection's 'root'"));
        }

        return new(new ConnectionCheck(true, "root directory exists; tables are verified at run time"));
    }

    internal static async ValueTask<ConnectionCheck> TcpAsync(string host, int port, string what, CancellationToken ct)
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
