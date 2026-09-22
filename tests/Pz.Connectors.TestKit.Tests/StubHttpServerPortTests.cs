using System.Net;
using System.Net.Sockets;

namespace Pz.Connectors.TestKit.Tests;

/// <summary>A free port is found by probing and then bound, and a parallel test or another process can
/// take it in between; the server must move on to another port rather than fail its caller.</summary>
public sealed class StubHttpServerPortTests
{
    [Fact]
    public async Task A_port_taken_between_probe_and_bind_is_skipped()
    {
        var taken = new TcpListener(IPAddress.Loopback, 0);
        taken.Start();
        try
        {
            var takenPort = ((IPEndPoint)taken.LocalEndpoint).Port;
            var calls = 0;
            int NextPort() => calls++ == 0 ? takenPort : FreshPort();

            await using var server = new StubHttpServer(NextPort);

            Assert.NotEqual(takenPort, server.BaseUrl.Port);
            server.Map("/ping", _ => new StubResponse(200, "pong"));
            using var client = new HttpClient();
            Assert.Equal("pong", await client.GetStringAsync(new Uri(server.BaseUrl, "ping")));
        }
        finally
        {
            taken.Stop();
        }
    }

    private static int FreshPort()
    {
        var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
    }
}
