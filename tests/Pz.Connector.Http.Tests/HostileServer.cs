using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Pz.Connector.Http.Tests;

/// <summary>What a hostile endpoint does to one connection. <see cref="StubHttpServer"/> speaks
/// well-formed HTTP through <c>HttpListener</c> and therefore cannot misbehave at the transport
/// layer; this fixture writes the response bytes itself, so it can lie about Content-Length, cut a
/// body in half, send a RST instead of a FIN, or simply never answer.</summary>
public sealed record HostileReply(byte[] Bytes, bool Reset = false, bool Hang = false)
{
    public static HostileReply Raw(string text, bool reset = false) =>
        new(Encoding.UTF8.GetBytes(text.ReplaceLineEndings("\r\n")), reset);

    /// <summary>A well-formed response — the baseline the hostile cases deviate from.</summary>
    public static HostileReply Json(string body, int status = 200, params (string Name, string Value)[] headers)
    {
        var bytes = Encoding.UTF8.GetBytes(body);
        var head = new StringBuilder()
            .Append($"HTTP/1.1 {status} X\r\n")
            .Append("Content-Type: application/json\r\n")
            .Append($"Content-Length: {bytes.Length}\r\n")
            .Append("Connection: close\r\n");
        foreach (var (name, value) in headers)
        {
            head.Append($"{name}: {value}\r\n");
        }

        head.Append("\r\n");
        return new HostileReply([.. Encoding.UTF8.GetBytes(head.ToString()), .. bytes]);
    }

    /// <summary>Declares <paramref name="declaredLength"/> bytes then sends only the given prefix and
    /// closes — the shape a crashed/OOM-killed API server produces mid-flight.</summary>
    public static HostileReply Truncated(string prefix, int declaredLength, bool reset = false)
    {
        var head = $"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n" +
            $"Content-Length: {declaredLength}\r\nConnection: close\r\n\r\n";
        return new HostileReply([.. Encoding.UTF8.GetBytes(head), .. Encoding.UTF8.GetBytes(prefix)], reset);
    }

    /// <summary>Accepts the connection, reads the request, then never writes a byte and holds the
    /// socket open until the fixture is disposed — a black-holed request, not a refused one.</summary>
    public static readonly HostileReply Stall = new([], Hang: true);

    /// <summary>Accepts the connection and immediately sends RST — a mid-handshake connection reset.</summary>
    public static readonly HostileReply ResetImmediately = new([], Reset: true);
}

/// <summary>Raw-socket HTTP fixture whose whole purpose is to be a bad citizen. The script is
/// invoked once per request with the zero-based request index and the raw request text, so a
/// scenario can escalate (page 1 fine, page 2 hostile) the way a real API degrades.</summary>
public sealed class HostileServer : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly Func<int, string, HostileReply> _script;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly List<string> _requests = [];
    private readonly Task _loop;

    public Uri BaseUrl { get; }

    public IReadOnlyList<string> Requests
    {
        get { lock (_requests) { return _requests.ToArray(); } }
    }

    public int RequestCount
    {
        get { lock (_requests) { return _requests.Count; } }
    }

    public HostileServer(Func<int, string, HostileReply> script)
    {
        _script = script;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        BaseUrl = new Uri($"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/");
        _loop = Task.Run(AcceptAsync);
    }

    private async Task AcceptAsync()
    {
        while (!_shutdown.IsCancellationRequested)
        {
            Socket socket;
            try
            {
                socket = await _listener.AcceptSocketAsync(_shutdown.Token).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException
                or SocketException)
            {
                return; // normal teardown
            }

            _ = Task.Run(() => ServeAsync(socket));
        }
    }

    private async Task ServeAsync(Socket socket)
    {
        try
        {
            var request = await ReadRequestAsync(socket).ConfigureAwait(false);
            int index;
            lock (_requests)
            {
                index = _requests.Count;
                _requests.Add(request);
            }

            var reply = _script(index, request);
            if (reply.Hang)
            {
                // Hold the connection open with no response until the fixture shuts down. The client
                // must escape on its own (its own timeout / the caller's token) -- that is the point.
                try
                {
                    await Task.Delay(Timeout.Infinite, _shutdown.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // shutting down
                }
            }

            if (reply.Bytes.Length > 0)
            {
                await socket.SendAsync(reply.Bytes, SocketFlags.None).ConfigureAwait(false);
            }

            if (reply.Reset)
            {
                // Zero linger turns Close() into a RST rather than a graceful FIN.
                socket.LingerState = new LingerOption(true, 0);
            }
        }
        catch
        {
            // A hostile scenario routinely races the client's own abort; the assertions are on the
            // client side, never on this loop.
        }
        finally
        {
            try { socket.Dispose(); } catch { /* best effort */ }
        }
    }

    private static async Task<string> ReadRequestAsync(Socket socket)
    {
        var buffer = new byte[8192];
        var text = new StringBuilder();
        while (!text.ToString().Contains("\r\n\r\n", StringComparison.Ordinal))
        {
            var read = await socket.ReceiveAsync(buffer, SocketFlags.None).ConfigureAwait(false);
            if (read == 0)
            {
                break;
            }

            text.Append(Encoding.UTF8.GetString(buffer, 0, read));
        }

        return text.ToString();
    }

    public async ValueTask DisposeAsync()
    {
        await _shutdown.CancelAsync().ConfigureAwait(false);
        _listener.Stop();
        try { await _loop.ConfigureAwait(false); } catch { /* teardown best-effort */ }
        _shutdown.Dispose();
    }
}
