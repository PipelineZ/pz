using Pz.Connectors.Abstractions;
using Pz.PackageManagement.ProcessHosting;

namespace Pz.PackageManagement.Hosting;

/// <summary>The connector host one project needs, plus the socket-root cleanup that outlives it:
/// external connectors are hosted out of process only, so this wraps a single
/// <see cref="ProcessConnectorHost"/> (null when every declared connector is builtin).</summary>
public sealed class ConnectorHosts : IAsyncDisposable
{
    private readonly string? _ownedSocketRoot;

    /// <summary><paramref name="ownedSocketRoot"/> is a directory this instance created for
    /// <paramref name="outOfProcess"/> to put per-instance socket directories under and must therefore
    /// remove — null when the socket root is run-scoped (collected with the run directory) or when
    /// there is no host at all. <see cref="ProcessConnectorHost"/> itself never creates or deletes the
    /// root.</summary>
    public ConnectorHosts(ProcessConnectorHost? outOfProcess, string? ownedSocketRoot = null)
    {
        OutOfProcess = outOfProcess;
        _ownedSocketRoot = ownedSocketRoot;
    }

    public ProcessConnectorHost? OutOfProcess { get; }

    /// <summary>Every hosted connector's identity, ordered by name — the same ordering, and the same
    /// one-entry-per-registered-name shape, the host's own <c>Installed</c> uses.</summary>
    public IReadOnlyList<ConnectorInfo> Installed => OutOfProcess?.Installed ?? [];

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (OutOfProcess is { } processHost)
            {
                await processHost.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            // After the host, never before: deleting the root out from under a child that is still
            // listening on a socket inside it would turn an orderly shutdown into a broken pipe.
            if (_ownedSocketRoot is not null && Directory.Exists(_ownedSocketRoot))
            {
                try
                {
                    Directory.Delete(_ownedSocketRoot, recursive: true);
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    // Best-effort: a leftover empty temp directory is not worth failing a run over.
                }
            }
        }
    }
}
