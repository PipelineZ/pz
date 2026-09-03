using System.Net.Sockets;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.DuckLake;

/// <summary>Zero-driver connectivity checks for <c>pz validate --connect</c>: a file catalog is
/// verified by its header magic; a server catalog by TCP reachability only — credentials cannot be
/// verified without a driver and are exercised by the first run's attach, whose failure is a
/// redacted PZ0311.</summary>
internal static class DuckLakeProbe
{
    private static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(5);

    internal static async ValueTask<ConnectionCheck> CheckFileAsync(
        string path, ReadOnlyMemory<byte> magic, int magicOffset, string kind, CancellationToken ct)
    {
        if (!File.Exists(path))
        {
            var directory = Path.GetDirectoryName(path);
            if (directory is { Length: > 0 } && !Directory.Exists(directory))
            {
                return new ConnectionCheck(false,
                    $"permanent: directory '{directory}' does not exist -- create it, or fix the connection's 'path'");
            }

            return new ConnectionCheck(true,
                $"'{path}' does not exist yet -- it will be created on first write; reads will fail until it exists");
        }

        var header = new byte[magicOffset + magic.Length];
        await using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
            var read = await stream.ReadAtLeastAsync(header, header.Length, throwOnEndOfStream: false, ct).ConfigureAwait(false);
            if (read < header.Length || !header.AsSpan(magicOffset).SequenceEqual(magic.Span))
            {
                return new ConnectionCheck(false, $"permanent: '{path}' is not a {kind} file (header magic mismatch)");
            }
        }

        return new ConnectionCheck(true, $"{kind} catalog file verified (header magic)");
    }

    internal static async ValueTask<ConnectionCheck> TcpAsync(string host, int port, string what, CancellationToken ct)
    {
        try
        {
            using var client = new TcpClient();
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(ConnectTimeout);
            await client.ConnectAsync(host, port, timeout.Token).ConfigureAwait(false);
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
