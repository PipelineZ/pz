using System.Collections.Concurrent;
using System.Net;
using System.Text;

namespace Pz.Connectors.TestKit;

public sealed record StubRequest(string Method, Uri Url, IReadOnlyDictionary<string, string> Headers,
    string Body = "");

public sealed record StubResponse(int Status, string Body, IReadOnlyDictionary<string, string>? Headers = null);

/// <summary>In-proc scripted HTTP fixture for connector tests: exact-path routes, full request
/// capture, no docker, no network. The listener loop swallows teardown races by design — a test
/// that needs an unserved request asserts on <see cref="Requests"/>, never on listener state.</summary>
public sealed class StubHttpServer : IAsyncDisposable
{
    private readonly HttpListener _listener = new();
    private readonly ConcurrentDictionary<string, Func<StubRequest, StubResponse>> _routes = new();
    private readonly List<(string Prefix, Func<StubRequest, StubResponse> Handler)> _prefixRoutes = [];
    private readonly List<StubRequest> _requests = [];
    private readonly CancellationTokenSource _stopping = new();
    private readonly Task _loop;
    private Exception? _handlerError;

    public Uri BaseUrl { get; }

    public IReadOnlyList<StubRequest> Requests
    {
        get { lock (_requests) { return _requests.ToArray(); } }
    }

    public Exception? HandlerError => _handlerError;

    public StubHttpServer()
    {
        var port = FreePort();
        BaseUrl = new Uri($"http://127.0.0.1:{port}/");
        _listener.Prefixes.Add(BaseUrl.ToString());
        _listener.Start();
        _loop = Task.Run(ServeAsync);
    }

    public void Map(string path, Func<StubRequest, StubResponse> handler) => _routes[path] = handler;

    /// <summary>Prefix route for templated paths (e.g. keyed merge PUTs to /items/{key}). Exact
    /// <see cref="Map"/> routes always win; prefixes match in registration order.</summary>
    public void MapPrefix(string prefix, Func<StubRequest, StubResponse> handler)
    {
        lock (_prefixRoutes) { _prefixRoutes.Add((prefix, handler)); }
    }

    private async Task ServeAsync()
    {
        var ct = _stopping.Token;
        while (!ct.IsCancellationRequested && _listener.IsListening)
        {
            HttpListenerContext context;
            try
            {
                // Stop() completes only a waiter that has ALREADY registered. A waiter that registers
                // just after Stop() -- the listening check above passed, then teardown ran -- is
                // completed by nothing, ever; the loop would park on it forever and DisposeAsync would
                // await the loop forever. The cancellation token is the wake-up Stop() cannot give, so
                // the loop parks on the token as well as on the listener.
                context = await _listener.GetContextAsync().WaitAsync(ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is HttpListenerException or ObjectDisposedException
                                       or InvalidOperationException or OperationCanceledException)
            {
                return; // listener stopped: normal teardown
            }

            try
            {
                var headers = context.Request.Headers.AllKeys
                    .Where(k => k is not null)
                    .ToDictionary(k => k!, k => context.Request.Headers[k]!, StringComparer.OrdinalIgnoreCase);
                string body;
                using (var reader = new StreamReader(context.Request.InputStream, Encoding.UTF8))
                {
                    body = await reader.ReadToEndAsync(ct).ConfigureAwait(false);
                }

                var request = new StubRequest(context.Request.HttpMethod, context.Request.Url!, headers, body);
                lock (_requests) { _requests.Add(request); }

                Func<StubRequest, StubResponse>? resolved = null;
                if (!_routes.TryGetValue(context.Request.Url!.AbsolutePath, out resolved))
                {
                    lock (_prefixRoutes)
                    {
                        resolved = _prefixRoutes
                            .FirstOrDefault(r => context.Request.Url!.AbsolutePath.StartsWith(r.Prefix, StringComparison.Ordinal))
                            .Handler;
                    }
                }

                var response = resolved is not null
                    ? resolved(request)
                    : new StubResponse(404, """{"error":"no stub"}""");

                context.Response.StatusCode = response.Status;
                context.Response.ContentType = "application/json";

                // One request per connection. HttpListener's keep-alive handling drops idle connections
                // on its own schedule; a client that pooled one and reused it a millisecond too late
                // sees an immediate HttpRequestException -- a load-dependent flake once a suite drives
                // several requests through one server. Signalling `Connection: close` makes the client
                // open a fresh connection each time -- irrelevant for test throughput, and it removes
                // the race outright.
                context.Response.KeepAlive = false;
                foreach (var (name, value) in response.Headers ?? new Dictionary<string, string>())
                {
                    context.Response.Headers[name] = value;
                }

                var bytes = Encoding.UTF8.GetBytes(response.Body);
                await context.Response.OutputStream.WriteAsync(bytes, ct).ConfigureAwait(false);
                context.Response.Close();
            }
            catch (Exception ex)
            {
                // Record the first handler exception so tests can assert on it.
                if (_handlerError is null)
                {
                    _handlerError = ex;
                }

                // Attempt to write a 500 response with the exception details.
                try
                {
                    context.Response.StatusCode = 500;
                    context.Response.ContentType = "text/plain";
                    var bytes = Encoding.UTF8.GetBytes(ex.ToString());
                    await context.Response.OutputStream.WriteAsync(bytes).ConfigureAwait(false);
                }
                catch
                {
                    // Secondary write failure: best-effort teardown.
                }

                try
                {
                    context.Response.Close();
                }
                catch
                {
                    // Ignore close failures; loop continues.
                }
            }
        }
    }

    private static int FreePort()
    {
        var probe = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
    }

    public async ValueTask DisposeAsync()
    {
        // Cancel before Stop(): a loop parked in (or about to park in) GetContextAsync wakes on the
        // token whether or not the listener ever completes its waiter.
        await _stopping.CancelAsync().ConfigureAwait(false);
        _listener.Stop();
        try
        {
            await _loop.ConfigureAwait(false);
        }
        catch
        {
            // teardown best-effort: the loop catches both listener races and handler exceptions
        }

        _listener.Close();
        _stopping.Dispose();
    }
}
