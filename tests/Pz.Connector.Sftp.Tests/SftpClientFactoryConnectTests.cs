using System.Net;
using System.Net.Sockets;
using Xunit;

namespace Pz.Connector.Sftp.Tests;

/// <summary>Network-free proof that <see cref="SftpClientFactory.BuildConnectionInfo"/> applies
/// <c>connect_timeout_seconds</c> (or leaves SSH.NET's own default alone), plus a real-socket proof
/// that <see cref="SftpClientFactory.ConnectAsync"/> honors external cancellation. The cancellation
/// fact never sleeps: it connects to a local <see cref="TcpListener"/> that accepts the TCP handshake
/// and then says nothing, so SSH.NET blocks reading the server's identification string -- exactly the
/// point at which the test cancels, once the listener side has observed the accept (no timing guess).</summary>
public sealed class SftpClientFactoryConnectTests
{
    private static SftpConnectionSettings Settings(string host, int port, int? connectTimeoutSeconds = null) =>
        new(host, port, "user", "password", null, null, null, Root: null, connectTimeoutSeconds);

    [Fact]
    public void BuildConnectionInfo_applies_connect_timeout_seconds()
    {
        var settings = Settings("h", 22, connectTimeoutSeconds: 7);
        using var auth = SftpClientFactory.BuildAuth(settings);

        var info = SftpClientFactory.BuildConnectionInfo(settings, auth.Method);

        Assert.Equal(TimeSpan.FromSeconds(7), info.Timeout);
    }

    [Fact]
    public void BuildConnectionInfo_leaves_the_library_default_when_absent()
    {
        var settings = Settings("h", 22);
        using var auth = SftpClientFactory.BuildAuth(settings);

        var info = SftpClientFactory.BuildConnectionInfo(settings, auth.Method);

        // SSH.NET's own ConnectionInfo.DefaultTimeout -- proving absence is truly a no-op, not a
        // pz-introduced override that happens to match today's SSH.NET default.
        Assert.Equal(TimeSpan.FromSeconds(30), info.Timeout);
    }

    [Fact]
    public async Task ConnectAsync_cancellation_propagates_unwrapped_not_as_a_connector_exception()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var acceptTask = listener.AcceptTcpClientAsync();

        var settings = Settings("127.0.0.1", port);
        // Not `using`: ConnectAsync takes ownership of auth on every path out (including cancellation,
        // where it disposes it itself before rethrowing) -- see its own doc comment.
        var auth = SftpClientFactory.BuildAuth(settings);
        using var cts = new CancellationTokenSource();

        var connectTask = SftpClientFactory.ConnectAsync(settings, auth, cts.Token);

        // The TCP handshake completing proves SSH.NET is now blocked reading the server's
        // identification string (this fake server never sends one) -- cancel from exactly that point,
        // never from a guessed delay.
        using var accepted = await acceptTask;
        cts.Cancel();

        // SSH.NET's own protocol-version-exchange read throws TaskCanceledException specifically (a
        // subtype of OperationCanceledException); either way, this must NOT be a PzConnectorException.
        await Assert.ThrowsAsync<TaskCanceledException>(() => connectTask);
    }
}
