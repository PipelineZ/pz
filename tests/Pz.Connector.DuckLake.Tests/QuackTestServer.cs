using System.Net;
using System.Net.Sockets;
using Pz.DuckDb;

namespace Pz.Connector.DuckLake.Tests;

/// <summary>A DuckDB server for the tests, in-process: a second <see cref="DuckSession"/> on its own
/// file runs <c>quack_serve</c> (which returns immediately and serves on background threads) on a
/// free loopback port with a fixed token; <c>quack_stop</c> on dispose. Tokens must be at least
/// four characters.</summary>
internal sealed class QuackTestServer(DuckSession server, string uri, string token) : IAsyncDisposable
{
    public string Uri => uri;

    public string Token => token;

    // FreePort's listener is closed before quack_serve binds, so another process can take the port in
    // between; quack_serve then fails synchronously ("Failed to bind ..."), and a fresh port is tried.
    private const int BindAttempts = 5;

    public static async Task<QuackTestServer> StartAsync(string dir, string host = "localhost")
    {
        var server = DuckSession.Open(Path.Combine(dir, "quack-server.duckdb"));
        await server.ExecuteAsync("install quack");
        await server.ExecuteAsync("load quack");
        for (var attempt = 1; ; attempt++)
        {
            var uri = $"quack:{host}:{FreePort()}";
            try
            {
                await server.ExecuteAsync($"call quack_serve('{uri}', token = 'pz-test-token')");
                return new QuackTestServer(server, uri, "pz-test-token");
            }
            catch (Exception ex) when (attempt < BindAttempts && ex.Message.Contains("Failed to bind", StringComparison.Ordinal))
            {
                // port taken between FreePort and the bind: pick another
            }
        }
    }

    private static int FreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await server.ExecuteAsync($"call quack_stop('{uri}')");
        }
        catch
        {
            // best-effort: the server dies with the session anyway
        }

        await server.DisposeAsync();
    }
}
