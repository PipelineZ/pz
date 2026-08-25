using Pz.Connectors.Abstractions;
using Pz.PackageManagement.ProcessHosting;

namespace Pz.PackageManagement.Hosting;

/// <summary>The connector hosts one project needs, held together: the ALC-based
/// <see cref="ConnectorHost"/> for <c>runtime: "dotnet"</c> packages and
/// <see cref="ProcessConnectorHost"/> for <c>runtime: "process"</c> ones.
///
/// <para>A package belongs to exactly one of them and neither can load the other's: a process package
/// ships no entry assembly for <see cref="ConnectorHost"/> to ALC-load, and
/// <see cref="ProcessConnectorHost"/> refuses anything that is not <c>runtime: "process"</c> with
/// PZ0354. <see cref="ManifestReader.TryRead"/>'s <c>runtime</c> is what decides, and the caller must
/// partition on it BEFORE constructing either host.</para>
///
/// <para>Disposal order is fixed: the out-of-process host first, because that is what reaps the child
/// processes, and a child left running past an in-process unload has nothing to serve.</para></summary>
public sealed class ConnectorHosts : IAsyncDisposable
{
    private readonly string? _ownedSocketRoot;

    /// <summary><paramref name="ownedSocketRoot"/> is a directory this instance created for
    /// <paramref name="outOfProcess"/> to put per-instance socket directories under and must therefore
    /// remove — null when the socket root is run-scoped (collected with the run directory) or when
    /// there is no out-of-process host at all. <see cref="ProcessConnectorHost"/> itself never creates
    /// or deletes the root.</summary>
    public ConnectorHosts(
        ConnectorHost? inProcess, ProcessConnectorHost? outOfProcess, string? ownedSocketRoot = null)
    {
        InProcess = inProcess;
        OutOfProcess = outOfProcess;
        _ownedSocketRoot = ownedSocketRoot;
    }

    public ConnectorHost? InProcess { get; }

    public ProcessConnectorHost? OutOfProcess { get; }

    /// <summary>Every hosted connector's identity across both hosts, ordered by name — the same
    /// ordering, and the same one-entry-per-registered-name shape, each host's own <c>Installed</c>
    /// uses. Names are unique across the pair: the registry layer refuses a name registered by both
    /// (PZ0305) before this composite is ever built.</summary>
    public IReadOnlyList<ConnectorInfo> Installed =>
        (InProcess?.Installed ?? [])
            .Concat(OutOfProcess?.Installed ?? [])
            .OrderBy(info => info.Name, StringComparer.Ordinal)
            .ToArray();

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (OutOfProcess is { } processHost)
            {
                await processHost.DisposeAsync().ConfigureAwait(false);
            }

            if (InProcess is { } host)
            {
                await host.DisposeAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            // After the process host, never before: deleting the root out from under a child that is
            // still listening on a socket inside it would turn an orderly shutdown into a broken pipe.
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
