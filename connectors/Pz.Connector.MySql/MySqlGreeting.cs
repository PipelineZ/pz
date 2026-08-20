using System.Net.Sockets;
using System.Text;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.MySql;

/// <summary>The zero-driver connectivity probe: a MySQL server sends its handshake packet
/// unprompted on connect, so reachability + "this is a MySQL server" + the server version are all
/// verifiable without a driver and without credentials. What this deliberately cannot verify is the
/// credentials themselves — the check message says so, and a bad password surfaces at run time
/// through the native scan/copy's redacted PZ0311.</summary>
internal static class MySqlGreeting
{
    private const byte HandshakeV10 = 0x0a;
    private const byte ErrorPacket = 0xff;

    /// <summary>Sane cap on a first-packet payload: real handshake/error packets are a few dozen bytes;
    /// this only bounds a garbled/hostile length field, never a legitimate probe.</summary>
    private const int MaxPayloadLength = 4096;

    internal static async ValueTask<ConnectionCheck> ProbeAsync(string host, int port, CancellationToken ct)
    {
        try
        {
            using var client = new TcpClient();
            await client.ConnectAsync(host, port, ct).ConfigureAwait(false);
            await using var stream = client.GetStream();

            // The MySQL packet header (3-byte little-endian payload length + 1-byte sequence number)
            // names EXACTLY how many payload bytes follow, so reading it first and then that many
            // payload bytes (each via a loop, not a single ReadAsync) captures the whole packet
            // regardless of how TCP happens to split it across reads. A fixed-size prefix read would
            // truncate a split handshake's version string or an error packet's message.
            var header = new byte[4];
            if (!await ReadExactAsync(stream, header, ct).ConfigureAwait(false))
            {
                return NotAMySqlHandshake(host, port);
            }

            var payloadLength = header[0] | (header[1] << 8) | (header[2] << 16);
            var captured = Math.Min(payloadLength, MaxPayloadLength);
            var packet = new byte[4 + captured];
            header.CopyTo(packet, 0);
            if (captured > 0 && !await ReadExactAsync(stream, packet.AsMemory(4, captured), ct).ConfigureAwait(false))
            {
                return NotAMySqlHandshake(host, port);
            }

            if (!TryParse(packet, out var serverVersion, out var serverError))
            {
                return NotAMySqlHandshake(host, port);
            }

            if (serverError is not null)
            {
                // A pre-auth error packet (e.g. "Host … is not allowed to connect"): it IS a MySQL
                // server, but every run against it would fail the same way — report that now.
                return new ConnectionCheck(false, $"permanent: mysql server refused the connection: {serverError}");
            }

            return new ConnectionCheck(true,
                $"mysql server reachable (server version {serverVersion}); credentials are verified at run time (native scan/copy)");
        }
        catch (SocketException ex)
        {
            var transient = ex.SocketErrorCode is SocketError.TimedOut or SocketError.TryAgain
                or SocketError.NetworkUnreachable or SocketError.HostUnreachable;
            return new ConnectionCheck(false, $"{(transient ? "transient" : "permanent")}: {ex.Message}");
        }
        catch (IOException ex)
        {
            return new ConnectionCheck(false, $"transient: {ex.Message}");
        }
    }

    /// <summary>Parses the first server packet: 3-byte little-endian payload length, 1-byte sequence,
    /// then either a v10 handshake (0x0a + NUL-terminated server version) or an error packet
    /// (0xff + 2-byte errno + message). Internal for offline tests against canned bytes.</summary>
    internal static bool TryParse(ReadOnlySpan<byte> packet, out string? serverVersion, out string? serverError)
    {
        serverVersion = null;
        serverError = null;
        if (packet.Length < 5)
        {
            return false;
        }

        var payload = packet[4..];
        if (payload[0] == HandshakeV10)
        {
            var version = payload[1..];
            var nul = version.IndexOf((byte)0);
            serverVersion = Ascii(nul >= 0 ? version[..nul] : version);
            return true;
        }

        if (payload[0] == ErrorPacket && payload.Length > 3)
        {
            var message = payload[3..]; // skip the 2-byte error code
            if (message.Length > 0 && message[0] == (byte)'#' && message.Length > 6)
            {
                message = message[6..]; // skip a SQL-state marker if one is present
            }

            serverError = Ascii(message);
            return true;
        }

        return false;
    }

    private static string Ascii(ReadOnlySpan<byte> bytes) =>
        Encoding.ASCII.GetString(bytes).Trim(); // non-ASCII bytes decode as '?', which is fine for a probe message

    private static ConnectionCheck NotAMySqlHandshake(string host, int port) =>
        new(false, $"permanent: {host}:{port} is reachable but did not present a MySQL handshake");

    /// <summary>Loops <see cref="NetworkStream.ReadAsync(Memory{byte},CancellationToken)"/> until
    /// <paramref name="buffer"/> is completely filled or the stream ends (0-byte read) — a single
    /// <c>ReadAsync</c> call is free to return fewer bytes than requested when a TCP segment splits
    /// mid-packet. Returns false only on a genuine short stream (never a MySQL server).</summary>
    private static async ValueTask<bool> ReadExactAsync(NetworkStream stream, Memory<byte> buffer, CancellationToken ct)
    {
        var total = 0;
        while (total < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer[total..], ct).ConfigureAwait(false);
            if (read == 0)
            {
                return false;
            }

            total += read;
        }

        return true;
    }
}
