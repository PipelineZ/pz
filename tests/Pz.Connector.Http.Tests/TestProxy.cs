using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Pz.Connector.Http.Tests;

/// <summary>A minimal forward proxy on 127.0.0.1: <c>CONNECT host:port</c> opens a tunnel, an absolute-URI
/// request is forwarded. Every target host is dialled on 127.0.0.1 (same port), so a test can name an
/// unresolvable host such as <c>api.test</c> and only reach it through the proxy. With
/// <paramref name="user"/> set, a request without the matching basic <c>Proxy-Authorization</c> is answered
/// 407 with a challenge. Logs each target (<c>host:port</c>). Dispose it at the end of each test.</summary>
internal sealed class TestProxy : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _accepting;
    private readonly string? _expectedAuth;

    public TestProxy(string? user = null, string? password = null)
    {
        _expectedAuth = user is null ? null : "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes($"{user}:{password}"));
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        _accepting = AcceptAsync();
    }

    public Uri Url => new($"http://127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}/");

    /// <summary>Every target the proxy was asked for, authorised or not.</summary>
    public ConcurrentQueue<string> Targets { get; } = new();

    /// <summary>How many requests were answered 407.</summary>
    public int Challenges => _challenges;
    private int _challenges;

    private async Task AcceptAsync()
    {
        var connections = new List<Task>();
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                var socket = await _listener.AcceptSocketAsync(_stop.Token);
                connections.Add(ServeAsync(socket));
            }
        }
        catch (Exception) when (_stop.IsCancellationRequested)
        {
        }

        await Task.WhenAll(connections);
    }

    private async Task ServeAsync(Socket socket)
    {
        using var _ = socket;
        await using var client = new NetworkStream(socket, ownsSocket: false);
        try
        {
            // Keep-alive: a 407 leaves the connection open for the client's authorised retry.
            while (await TestHttp.ReadHeadAsync(client, _stop.Token) is { } head)
            {
                var parts = head.Split("\r\n")[0].Split(' ');
                var (method, target) = (parts[0], parts[1]);
                var authority = method == "CONNECT" ? target : new Uri(target).Authority;
                Targets.Enqueue(authority);
                if (_expectedAuth is not null && TestHttp.Header(head, "Proxy-Authorization") != _expectedAuth)
                {
                    await TestHttp.ReadBodyAsync(client, head, _stop.Token);
                    Interlocked.Increment(ref _challenges);
                    await client.WriteAsync(Encoding.ASCII.GetBytes(
                        "HTTP/1.1 407 Proxy Authentication Required\r\nProxy-Authenticate: Basic realm=\"pz\"\r\n" +
                        "Content-Length: 0\r\n\r\n"), _stop.Token);
                    continue;
                }

                var port = int.Parse(authority[(authority.LastIndexOf(':') + 1)..], System.Globalization.CultureInfo.InvariantCulture);
                using var upstream = new TcpClient();
                await upstream.ConnectAsync(IPAddress.Loopback, port, _stop.Token);
                await using var server = upstream.GetStream();
                if (method == "CONNECT")
                {
                    await client.WriteAsync("HTTP/1.1 200 Connection established\r\n\r\n"u8.ToArray(), _stop.Token);
                    await PipeAsync(client, server);
                    return;
                }

                // Forward an absolute-URI request as origin-form, without the proxy's own headers.
                var body = await TestHttp.ReadBodyAsync(client, head, _stop.Token);
                var uri = new Uri(target);
                var lines = head.Split("\r\n").Skip(1)
                    .Where(l => l.Length > 0 && !l.StartsWith("Proxy-", StringComparison.OrdinalIgnoreCase)
                        && !l.StartsWith("Connection:", StringComparison.OrdinalIgnoreCase));
                var forwarded = $"{method} {uri.PathAndQuery} HTTP/1.1\r\n{string.Join("\r\n", lines)}\r\nConnection: close\r\n\r\n";
                await server.WriteAsync(Encoding.ASCII.GetBytes(forwarded), _stop.Token);
                await server.WriteAsync(body, _stop.Token);
                await server.CopyToAsync(client, _stop.Token);
                return;
            }
        }
        catch (Exception ex) when (ex is IOException or SocketException or OperationCanceledException or ObjectDisposedException)
        {
        }
    }

    private async Task PipeAsync(NetworkStream a, NetworkStream b)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
        var first = await Task.WhenAny(a.CopyToAsync(b, linked.Token), b.CopyToAsync(a, linked.Token));
        await linked.CancelAsync();
        await first;
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        _listener.Stop();
        await _accepting.WaitAsync(TimeSpan.FromSeconds(30));
        _stop.Dispose();
    }
}
