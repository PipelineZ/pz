using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol;

namespace Pz.Connectors.Sdk;

/// <summary>Serves one connector over PCP on the socket the host passed: Kestrel HTTP/2 for the
/// control plane, a bare listener for the Arrow IPC data plane, and the two orphan timers that make
/// a connector exit rather than linger when its host is gone.</summary>
internal static class PcpServer
{
    /// <summary>How long the process survives with no control connection before deciding it has been
    /// orphaned. A host that means to keep the connector alive keeps its control connection open and
    /// ends the process with the Shutdown RPC; anything else -- a crashed host, a killed run -- leaves a
    /// connector with no one to serve.</summary>
    private static readonly TimeSpan OrphanExitGrace = TimeSpan.FromSeconds(5);

    /// <summary>How long to wait for the FIRST control connection. Twice the handshake timeout: a host
    /// still within its own handshake budget has not failed yet.</summary>
    private static readonly TimeSpan FirstConnectionDeadline = ProtocolConstants.HandshakeTimeout * 2;

    /// <summary>Kept under <see cref="ProtocolConstants.ShutdownGrace"/>: the generic host's 30 s
    /// default is three times the grace the host allows between the Shutdown RPC and a kill.</summary>
    private static readonly TimeSpan HostShutdownTimeout = TimeSpan.FromSeconds(5);

    public static async Task<int> ServeAsync(
        string socketPath, IConnector connector, HostChannelPeer peer, ConnectorTelemetry telemetry, PcpServerHooks hooks)
    {
        var dataSocketPath = socketPath + ProtocolConstants.DataSocketSuffix;
        DeleteIfExists(socketPath);
        DeleteIfExists(dataSocketPath);

        var exit = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var watch = new ControlConnectionWatch(
            FirstConnectionDeadline,
            OrphanExitGrace,
            onNeverConnected: () =>
            {
                Console.Error.WriteLine(
                    $"pz connector: no control connection within {FirstConnectionDeadline.TotalSeconds:0}s " +
                    "of the socket being served; the host never dialed. Exiting rather than orphaning.");
                exit.TrySetResult(3);
            },
            onOrphaned: () => exit.TrySetResult(0));
        var tickets = new TicketRegistry();

        await using var dataPlane = DataPlaneListener.Start(dataSocketPath, tickets);

        // No args to the builder: nothing on argv is configuration. The content root is pinned to the
        // binary's own directory so an appsettings.json in whatever working directory the host spawned
        // from cannot steer the server.
        var builder = WebApplication.CreateSlimBuilder(
            new WebApplicationOptions { ContentRootPath = AppContext.BaseDirectory });
        builder.Logging.ClearProviders();
        builder.Logging.AddConsole(console => console.LogToStandardErrorThreshold = LogLevel.Trace);
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.WebHost.ConfigureKestrel(kestrel => kestrel.ListenUnixSocket(socketPath, listen =>
        {
            listen.Protocols = HttpProtocols.Http2;
            listen.Use(next => async connection =>
            {
                watch.Opened();
                try
                {
                    await next(connection).ConfigureAwait(false);
                }
                finally
                {
                    watch.Closed();
                }
            });
        }));
        builder.Services.AddGrpc();
        builder.Services.Configure<HostOptions>(host => host.ShutdownTimeout = HostShutdownTimeout);
        builder.Services.AddSingleton(connector);
        builder.Services.AddSingleton(tickets);
        builder.Services.AddSingleton(peer);
        builder.Services.AddSingleton(hooks);
        builder.Services.AddSingleton(telemetry);
        builder.Services.AddSingleton<PcpConnectorService>();

        var app = builder.Build();
        app.MapGrpcService<PcpConnectorService>();

        await app.StartAsync().ConfigureAwait(false);
        // Kestrel creates the socket file on bind, so this is the earliest point it can be locked down.
        SocketPermissions.RestrictToOwner(socketPath);
        // The first-connection clock starts now, not at process start: the host cannot dial before this.
        watch.Start();

        // SIGINT/SIGTERM and the Shutdown RPC both land here, via IHostApplicationLifetime.
        await using var stopping = app.Lifetime.ApplicationStopping.Register(() => exit.TrySetResult(0))
            .ConfigureAwait(false);
        var exitCode = await exit.Task.ConfigureAwait(false);
        await app.StopAsync().ConfigureAwait(false);
        // After the server has stopped, so nothing can start a span this flush would miss; bounded by
        // ConnectorTelemetry.FlushBound so a dead collector cannot push this exit past the host's grace.
        telemetry.FlushAndDispose();
        return exitCode;
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}

/// <summary>Both sockets are owner-only. A unix socket's file permissions are the whole access control
/// on this transport, and the socket carries credentials in one direction and data in the other.</summary>
internal static class SocketPermissions
{
    public static void RestrictToOwner(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            return;
        }

        // The bind that created the file already applied the process umask, so there is a window in
        // which the file may be group/world readable. The host creates the containing directory 0700,
        // which is what actually closes it; this narrows the file itself as well.
        File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
    }
}

/// <summary>Orphan prevention, on both sides of the first connection: fires
/// <paramref name="onNeverConnected"/> if the host never dials within
/// <paramref name="startupDeadline"/>, and <paramref name="onOrphaned"/> once the last control
/// connection has been gone for <paramref name="idleGrace"/>. A host that dies before connecting and
/// one that dies after leave the same orphan, and only the two timers together cover both.</summary>
internal sealed class ControlConnectionWatch(
    TimeSpan startupDeadline,
    TimeSpan idleGrace,
    Action onNeverConnected,
    Action onOrphaned)
{
    private readonly Lock _gate = new();
    private int _open;
    private bool _everConnected;
    private CancellationTokenSource? _countdown;

    /// <summary>Starts the first-connection clock. Called once the socket is actually listening, so the
    /// deadline measures the host's silence and not the connector's own startup.</summary>
    public void Start()
    {
        CancellationTokenSource countdown;
        lock (_gate)
        {
            if (_everConnected)
            {
                return;
            }

            countdown = new CancellationTokenSource();
            _countdown = countdown;
        }

        _ = CountdownAsync(countdown, startupDeadline, onNeverConnected);
    }

    public void Opened()
    {
        lock (_gate)
        {
            _open++;
            _everConnected = true;
            ClearCountdown();
        }
    }

    public void Closed()
    {
        CancellationTokenSource countdown;
        lock (_gate)
        {
            if (--_open > 0)
            {
                return;
            }

            countdown = new CancellationTokenSource();
            _countdown = countdown;
        }

        _ = CountdownAsync(countdown, idleGrace, onOrphaned);
    }

    private void ClearCountdown()
    {
        _countdown?.Cancel();
        _countdown?.Dispose();
        _countdown = null;
    }

    private static async Task CountdownAsync(CancellationTokenSource countdown, TimeSpan delay, Action onElapsed)
    {
        try
        {
            await Task.Delay(delay, countdown.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        onElapsed();
    }
}
