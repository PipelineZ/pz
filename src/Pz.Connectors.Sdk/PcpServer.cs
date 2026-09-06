namespace Pz.Connectors.Sdk;

internal static class PcpServer
{
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
    /// deadline measures the host's silence and not the fixture's own startup.</summary>
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
