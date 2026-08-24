using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Protocol;

namespace PcpFakeConnector;

/// <summary>A real out-of-process PCP peer, written in C# on purpose: the protocol is meant to be
/// language-neutral, and a fixture that shares the host's language proves the wire format is
/// implementable by someone who is not the host.
///
/// <para>It serves the <c>PzConnector</c> service over gRPC on the unix socket it is handed and raw
/// Arrow IPC on <c>&lt;socket&gt;.data</c>, delegating every call to a real
/// <c>LocalFilesConnector</c>. Configuration and credentials arrive only through the <c>Configure</c>
/// RPC. The argv switches below choose which failure to stage — they are test switches, which is why
/// argv is the right surface for them and the wrong surface for config.</para></summary>
internal static class Program
{
    /// <summary>How long the fixture survives with no control connection before deciding it has been
    /// orphaned. A host that means to keep the connector alive keeps its control connection open (or
    /// its HTTP/2 pool from dropping it) and ends the process with the <c>Shutdown</c> RPC; anything
    /// else — a crashed host, a killed test run — leaves the connector with no one to serve, and it
    /// exits rather than lingering.</summary>
    private static readonly TimeSpan OrphanExitGrace = TimeSpan.FromSeconds(5);

    public static async Task<int> Main(string[] args)
    {
        if (OperatingSystem.IsWindows())
        {
            await Console.Error.WriteLineAsync(
                "PcpFakeConnector serves the protocol over unix domain sockets only; the named-pipe " +
                "transport is not implemented in this fixture.").ConfigureAwait(false);
            return 3;
        }

        FixtureOptions options;
        try
        {
            options = FixtureOptions.Parse(args);
        }
        catch (ArgumentException ex)
        {
            await Console.Error.WriteLineAsync($"PcpFakeConnector: {ex.Message}").ConfigureAwait(false);
            return 2;
        }

        if (options.DieImmediately)
        {
            await Console.Error.WriteLineAsync(
                "PcpFakeConnector: --die-immediately, exiting before the socket is served").ConfigureAwait(false);
            return 1;
        }

        var dataSocketPath = options.SocketPath + ProtocolConstants.DataSocketSuffix;
        DeleteIfExists(options.SocketPath);
        DeleteIfExists(dataSocketPath);

        var exit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var watch = new ControlConnectionWatch(OrphanExitGrace, () => exit.TrySetResult());
        var tickets = new TicketRegistry();

        await using var dataPlane = DataPlaneListener.Start(dataSocketPath, tickets);

        // No args to the builder: the failure switches are bare `--flag`s, and the command-line
        // configuration provider would swallow the argument that follows one of them as its value.
        // The content root is pinned to the binary's own directory so an appsettings.json in whatever
        // working directory the host happened to spawn from cannot steer the fixture.
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions { ContentRootPath = AppContext.BaseDirectory });
        builder.Logging.ClearProviders();
        builder.Logging.AddConsole(console => console.LogToStandardErrorThreshold = LogLevel.Trace);
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.WebHost.ConfigureKestrel(kestrel => kestrel.ListenUnixSocket(options.SocketPath, listen =>
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
        builder.Services.AddSingleton(options);
        builder.Services.AddSingleton(tickets);
        // Singleton: the configured connector, its open plans and its write sessions are per-process
        // state that every later RPC resolves against.
        builder.Services.AddSingleton<PcpService>();

        var app = builder.Build();
        app.MapGrpcService<PcpService>();

        await app.StartAsync().ConfigureAwait(false);
        // Kestrel creates the socket file on bind, so this is the earliest point it can be locked down.
        SocketPermissions.RestrictToOwner(options.SocketPath);

        // SIGINT/SIGTERM and the Shutdown RPC both land here, via IHostApplicationLifetime.
        await using var stopping = app.Lifetime.ApplicationStopping.Register(() => exit.TrySetResult())
            .ConfigureAwait(false);
        await exit.Task.ConfigureAwait(false);
        await app.StopAsync().ConfigureAwait(false);
        return 0;
    }

    private static void DeleteIfExists(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}

/// <summary>Which failure this fixture stages, and where to serve. Nothing here is configuration:
/// connection options and credentials reach the connector through the <c>Configure</c> RPC and no
/// other way.</summary>
internal sealed record FixtureOptions(
    string SocketPath,
    bool HangHandshake,
    bool DieImmediately,
    bool WrongProtocolMajor,
    bool MisreportCapabilities,
    bool FailCheckTransient)
{
    public static FixtureOptions Parse(string[] args)
    {
        string? socketPath = null;
        bool hangHandshake = false, dieImmediately = false, wrongProtocolMajor = false;
        bool misreportCapabilities = false, failCheckTransient = false;

        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--pz-socket":
                    if (++i >= args.Length)
                    {
                        throw new ArgumentException("--pz-socket needs a socket path");
                    }

                    socketPath = args[i];
                    break;
                case "--hang-handshake":
                    hangHandshake = true;
                    break;
                case "--die-immediately":
                    dieImmediately = true;
                    break;
                case "--wrong-protocol-major":
                    wrongProtocolMajor = true;
                    break;
                case "--misreport-capabilities":
                    misreportCapabilities = true;
                    break;
                case "--fail-check-transient":
                    failCheckTransient = true;
                    break;
                default:
                    throw new ArgumentException($"unrecognized argument '{args[i]}'");
            }
        }

        return new FixtureOptions(
            socketPath ?? throw new ArgumentException("--pz-socket <path> is required"),
            hangHandshake,
            dieImmediately,
            wrongProtocolMajor,
            misreportCapabilities,
            failCheckTransient);
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

/// <summary>Orphan prevention: counts live control-plane connections and fires once the last one has
/// been gone for the grace period. Nothing fires before the first connection arrives, so a fixture the
/// host has not dialed yet waits indefinitely.</summary>
internal sealed class ControlConnectionWatch(TimeSpan grace, Action onOrphaned)
{
    private readonly Lock _gate = new();
    private int _open;
    private bool _everConnected;
    private CancellationTokenSource? _countdown;

    public void Opened()
    {
        lock (_gate)
        {
            _open++;
            _everConnected = true;
            _countdown?.Cancel();
            _countdown?.Dispose();
            _countdown = null;
        }
    }

    public void Closed()
    {
        CancellationTokenSource countdown;
        lock (_gate)
        {
            if (--_open > 0 || !_everConnected)
            {
                return;
            }

            countdown = new CancellationTokenSource();
            _countdown = countdown;
        }

        _ = CountdownAsync(countdown);
    }

    private async Task CountdownAsync(CancellationTokenSource countdown)
    {
        try
        {
            await Task.Delay(grace, countdown.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        onOrphaned();
    }
}
