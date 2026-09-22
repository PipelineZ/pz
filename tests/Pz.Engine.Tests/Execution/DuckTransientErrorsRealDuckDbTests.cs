using System.Net;
using System.Net.Sockets;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.TestSupport;

namespace Pz.Engine.Tests.Execution;

/// <summary>Validates <see cref="DuckTransientErrors"/>'s fixture strings against REAL DuckDB httpfs
/// error text — not just the hand-authored strings <c>DuckTransientErrorsTests</c> pins. Provokes an
/// actual connection failure (nothing listening on a closed local port) and an actual HTTP 503/404
/// (a tiny in-test <see cref="HttpListener"/>), so the classifier is proven against what DuckDB's
/// httpfs extension genuinely emits on this machine/version, not an assumption about its wording.
/// Needs network once, to install httpfs (cached under ~/.duckdb after); skipped offline.</summary>
[Trait("Category", "DuckDbExtension")]
public sealed class DuckTransientErrorsRealDuckDbTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-duck-transient-real", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;

    public async Task InitializeAsync()
    {
        DockerFacts.SkipIfOffline();
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "x.duckdb"));
        await _duck.ExecuteAsync("install httpfs");
        await _duck.ExecuteAsync("load httpfs");
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    /// <summary>A closed port: nothing is listening, so the OS refuses the connection at the socket
    /// level before any HTTP exchange happens -- the shape <c>DuckTransientErrorsTests</c> pins as
    /// "connection refused"/"connection reset".</summary>
    [SkippableFact]
    public async Task Closed_port_produces_a_real_duckdb_error_classified_transient()
    {
        var port = ClaimAndReleasePort();

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => _duck.ExecuteAsync(
            $"create table t1 as select * from read_parquet('http://127.0.0.1:{port}/f.parquet')"));

        Assert.True(DuckTransientErrors.IsTransient(ex.Message),
            $"expected a real closed-port DuckDB error to classify transient; actual message: {ex.Message}");
    }

    /// <summary>A real HTTP 503 response -- the shape a genuine S3/object-store throttling response
    /// (SlowDown) surfaces as through DuckDB's httpfs, which reports the HTTP status it received, not
    /// the S3 XML error body.</summary>
    [SkippableFact]
    public async Task Real_http_503_response_is_classified_transient()
    {
        using var server = new StatusCodeServer(HttpStatusCode.ServiceUnavailable);

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => _duck.ExecuteAsync(
            $"create table t2 as select * from read_parquet('{server.ParquetUrl}')"));

        Assert.True(DuckTransientErrors.IsTransient(ex.Message),
            $"expected a real HTTP 503 DuckDB error to classify transient; actual message: {ex.Message}");
    }

    /// <summary>A real HTTP 404 response -- permanent (not found), the cross-check that this classifier
    /// does not simply treat every httpfs failure as transient.</summary>
    [SkippableFact]
    public async Task Real_http_404_response_is_classified_permanent()
    {
        using var server = new StatusCodeServer(HttpStatusCode.NotFound);

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => _duck.ExecuteAsync(
            $"create table t3 as select * from read_parquet('{server.ParquetUrl}')"));

        Assert.False(DuckTransientErrors.IsTransient(ex.Message),
            $"expected a real HTTP 404 DuckDB error to classify permanent; actual message: {ex.Message}");
    }

    /// <summary>An ephemeral TCP port that is free at the moment of the call, and guaranteed closed by
    /// the time it returns (the listener that reserved it is stopped before the port number is handed
    /// back) -- a small, accepted race if another process grabs the exact same port in between.</summary>
    private static int ClaimAndReleasePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>A tiny local HTTP server that answers every request with one fixed status code and no
    /// body -- enough for DuckDB's httpfs to receive a real, non-2xx HTTP response and report it.</summary>
    private sealed class StatusCodeServer : IDisposable
    {
        private readonly HttpListener _listener;
        private readonly CancellationTokenSource _cts = new();
        private readonly Task _loop;

        public StatusCodeServer(HttpStatusCode status)
        {
            // HttpListener needs an already-free port up front (its own Prefixes API takes no "port 0"
            // shorthand) -- ask the OS for one via a probe TcpListener, released before HttpListener binds.
            int port;
            using (var probe = new TcpListener(IPAddress.Loopback, 0))
            {
                probe.Start();
                port = ((IPEndPoint)probe.LocalEndpoint).Port;
            }

            ParquetUrl = $"http://127.0.0.1:{port}/f.parquet";
            _listener = new HttpListener();
            _listener.Prefixes.Add($"http://127.0.0.1:{port}/");
            _listener.Start();
            _loop = Task.Run(async () =>
            {
                while (!_cts.IsCancellationRequested)
                {
                    HttpListenerContext context;
                    try
                    {
                        context = await _listener.GetContextAsync().ConfigureAwait(false);
                    }
                    catch (Exception) when (_cts.IsCancellationRequested || _listener.IsListening is false)
                    {
                        return;
                    }

                    context.Response.StatusCode = (int)status;
                    context.Response.Close();
                }
            });
        }

        public string ParquetUrl { get; }

        public void Dispose()
        {
            _cts.Cancel();
            _listener.Stop();
            _listener.Close();
            try { _loop.Wait(TimeSpan.FromSeconds(2)); } catch { /* best-effort */ }
            _cts.Dispose();
        }
    }
}
