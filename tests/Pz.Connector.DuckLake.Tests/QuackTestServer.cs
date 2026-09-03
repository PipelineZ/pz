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

    public static async Task<QuackTestServer> StartAsync(string dir)
    {
        var port = FreePort();
        var uri = $"quack:localhost:{port}";
        var server = DuckSession.Open(Path.Combine(dir, "quack-server.duckdb"));
        await server.ExecuteAsync("install quack");
        await server.ExecuteAsync("load quack");
        await server.ExecuteAsync($"call quack_serve('{uri}', token = 'pz-test-token')");
        return new QuackTestServer(server, uri, "pz-test-token");
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
