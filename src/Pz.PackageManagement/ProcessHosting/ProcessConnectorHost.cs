using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol;
using Pz.PackageManagement.Hosting;

namespace Pz.PackageManagement.ProcessHosting;

/// <summary>The out-of-process connector host: a restored package whose manifest declares
/// <c>runtime: "process"</c> is never loaded into this process at all — it is spawned as a child,
/// handshaken, and driven over PCP.
///
/// <para><b>Loading spawns nothing.</b> <see cref="LoadFromDirectory"/> reads each package's manifest,
/// resolves the entrypoint for this RID, and registers a shim; the first call that actually needs a
/// live connector (<c>OpenAsync</c>, <c>ValidateAsync</c>, <c>CheckConnectionAsync</c>) is what spawns
/// one. That is what keeps <c>pz compile</c> — which reads identity and capabilities and nothing else
/// — from paying for a process per declared connection.</para>
///
/// <para><b>One process per <c>OpenAsync</c> call.</b> The spec's rule is one process per named
/// connection instance; the engine opens each connection instance exactly once per run, so
/// process-per-open and process-per-instance are the same thing under the only caller there is. This
/// is the simpler rule to implement and the one implemented here — a host that ever opens the same
/// instance twice would get two processes, and would need this revisited.</para>
///
/// <para>Every process this host spawns is owned by this host: the shims it hands out never kill one,
/// and <see cref="DisposeAsync"/> is where all of them go through the shutdown ladder.</para></summary>
public sealed class ProcessConnectorHost : IAsyncDisposable
{
    private readonly Dictionary<string, LazyProcessConnector> _connectorsByName;

    private ProcessConnectorHost(Dictionary<string, LazyProcessConnector> connectorsByName) =>
        _connectorsByName = connectorsByName;

    /// <summary>Layout consumed: <c>&lt;packagesRoot&gt;/&lt;PackageId&gt;/&lt;Version&gt;/pz.connector.json</c>
    /// declaring <c>runtime: "process"</c> plus an <c>entrypoints</c> map (RID → package-relative binary).
    ///
    /// <para>Failures, all raised before anything is spawned: a missing package directory or manifest is
    /// PZ0304; a manifest that does not declare <c>runtime: "process"</c>, or that ships no binary
    /// reachable from this host's RID, is PZ0354; a protocol-major range excluding this host is PZ0306;
    /// two packages registering one connector name is PZ0305.</para>
    ///
    /// <para><paramref name="socketRootDir"/> is the run-scoped directory each spawned process gets its
    /// own owner-only socket directory under. <paramref name="logSink"/> receives every connector
    /// <c>LogEvent</c> (level, message, fields) off the reverse channel — wired to <c>Pz.Diagnostics</c>
    /// by whoever constructs this host; null drops them. <paramref name="warn"/> reports declarations
    /// this host accepts but will not act on.</para></summary>
    public static ProcessConnectorHost LoadFromDirectory(
        string packagesRoot, IReadOnlyList<ConnectorPackageRef> required, string socketRootDir,
        Action<string>? warn = null,
        Action<int, string, IReadOnlyDictionary<string, string>>? logSink = null) =>
        LoadFromDirectory(
            packagesRoot, required, socketRootDir, warn, logSink,
            ProtocolConstants.CancelGrace, ProtocolConstants.ShutdownGrace);

    /// <summary>Same load, with the cancel/shutdown grace windows injectable. Compressed values let a
    /// test observe the full ladder without waiting out the real 5s/10s windows; production always goes
    /// through the public overload, which passes the <see cref="ProtocolConstants"/> values.</summary>
    internal static ProcessConnectorHost LoadFromDirectory(
        string packagesRoot, IReadOnlyList<ConnectorPackageRef> required, string socketRootDir,
        Action<string>? warn,
        Action<int, string, IReadOnlyDictionary<string, string>>? logSink,
        TimeSpan cancelGrace, TimeSpan shutdownGrace)
    {
        var connectorsByName = new Dictionary<string, LazyProcessConnector>(StringComparer.Ordinal);
        var rid = RuntimeInformation.RuntimeIdentifier;

        foreach (var packageRef in required)
        {
            var packageDir = Path.Combine(packagesRoot, packageRef.PackageId, packageRef.Version);
            if (!Directory.Exists(packageDir))
            {
                throw new ConnectorHostException(
                    "PZ0304",
                    $"connector package '{packageRef.PackageId}' {packageRef.Version} not found under {packagesRoot}",
                    "run 'pz restore' or check the packages layout");
            }

            var manifest = ManifestReader.TryRead(packageDir)
                ?? throw new ConnectorHostException(
                    "PZ0304",
                    $"connector package '{packageRef.PackageId}' {packageRef.Version} ships no pz.connector.json",
                    "run 'pz restore' or check the packages layout");

            if (manifest.Runtime != "process")
            {
                throw new ConnectorHostException(
                    "PZ0354",
                    $"connector package '{packageRef.PackageId}' declares runtime '{manifest.Runtime ?? "dotnet"}', which is not hosted out of process",
                    "external connectors are hosted out of process only; fix the manifest's runtime");
            }

            if (ProtocolVersion.Major < manifest.ProtocolMajorMin || ProtocolVersion.Major > manifest.ProtocolMajorMax)
            {
                throw new ConnectorHostException(
                    "PZ0306",
                    $"connector package '{packageRef.PackageId}' supports protocol majors {manifest.ProtocolMajorMin}-{manifest.ProtocolMajorMax} but this pz speaks {ProtocolVersion.Major}",
                    "upgrade pz, or pin an older connector version");
            }

            // ManifestReader.ResolveEntrypoint also ensures the resolved path is executable on Unix
            // (see its own doc comment) -- the one place both this host and `pz connector test`'s
            // target resolution go through, so neither has to remember to do it separately.
            var entrypoint = ManifestReader.ResolveEntrypoint(manifest, packageDir, rid);
            if (!File.Exists(entrypoint))
            {
                // Caught at load rather than at first spawn: a package whose manifest points at a
                // binary that is not there is broken in the same way as one shipping no binary for
                // this RID, and both should be one PZ0354 before any run work starts.
                throw new ConnectorHostException(
                    "PZ0354",
                    $"connector package '{packageRef.PackageId}' declares an entrypoint for RID '{rid}' that does not exist: '{entrypoint}'",
                    "run 'pz restore' or check the package's entrypoints for this platform");
            }

            // No falling back to the package id: an out-of-process connector's name is what its own
            // handshake has to agree with (PZ0356, in PcpClient), so a manifest that names nothing
            // leaves nothing to check the connector's claimed identity against. Same PZ0354 the manifest
            // reader already uses for a runtime:"process" package that ships no entrypoints -- both are
            // "declared runtime process, then left out something that runtime requires".
            if (manifest.Name is not { Length: > 0 } name)
            {
                throw new ConnectorHostException(
                    "PZ0354",
                    $"connector package '{packageRef.PackageId}' declares runtime 'process' but no name",
                    "add a 'name' to the package's pz.connector.json, or rebuild the connector package");
            }

            var connector = new LazyProcessConnector(
                name, packageRef, manifest, entrypoint, socketRootDir, logSink, cancelGrace, shutdownGrace);

            var dropped = connector.DeclaredCapabilities & ~connector.Capabilities;
            if (dropped != ConnectorCapabilities.None)
            {
                warn?.Invoke(
                    $"note: '{name}' declares {dropped}, which the out-of-process host does not implement; " +
                    "it will not be offered to the planner");
            }

            if (!connectorsByName.TryAdd(name, connector))
            {
                throw new ConnectorHostException(
                    "PZ0305",
                    $"connector name '{name}' is registered by more than one package",
                    "remove one of the conflicting packages");
            }
        }

        return new ProcessConnectorHost(connectorsByName);
    }

    /// <summary>Looks up a registered connector by name. Returns the lazy shim — nothing spawns here.</summary>
    public IConnector Get(string connectorName)
    {
        if (_connectorsByName.TryGetValue(connectorName, out var connector))
        {
            return connector;
        }

        var installed = string.Join(", ", _connectorsByName.Keys.OrderBy(name => name, StringComparer.Ordinal));
        throw new ConnectorHostException(
            "PZ0305",
            $"no connector named '{connectorName}' is installed",
            installed.Length == 0 ? "no connectors are installed" : $"installed connectors: {installed}");
    }

    /// <summary>All registered connectors' identities, ordered by name. Manifest- and package-derived,
    /// so this answers without a handshake; a spawned instance's Hello must agree with it (PZ0356).</summary>
    public IReadOnlyList<ConnectorInfo> Installed =>
        _connectorsByName.Values
            .Select(connector => connector.Info)
            .OrderBy(info => info.Name, StringComparer.Ordinal)
            .ToArray();

    /// <summary>Runs every spawned process through the shutdown ladder (Shutdown RPC → grace → kill the
    /// process tree) and closes its reverse channel first. Registered-but-never-spawned connectors have
    /// nothing to reap.</summary>
    public async ValueTask DisposeAsync()
    {
        foreach (var connector in _connectorsByName.Values)
        {
            try
            {
                await connector.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // Swallow-and-continue: one connector failing to tear down must not leave the rest
                // of this host's children orphaned.
            }
        }

        _connectorsByName.Clear();
    }
}

/// <summary>One registered <c>runtime: "process"</c> connector, before (and after) anything is spawned.
/// Implements both connector directions because which one a package is used as is the caller's
/// question, exactly as it is for an in-process <c>IConnector</c>.</summary>
internal sealed class LazyProcessConnector : ISourceConnector, ISinkConnector, IAsyncDisposable
{
    private readonly ConnectorPackageRef _packageRef;
    private readonly ConnectorManifest _manifest;
    private readonly string _entrypoint;
    private readonly string _socketRootDir;
    private readonly Action<int, string, IReadOnlyDictionary<string, string>>? _logSink;
    private readonly TimeSpan _cancelGrace;
    private readonly TimeSpan _shutdownGrace;
    private readonly ConcurrentBag<ProcessInstance> _instances = [];

    private string _connectionConfigSchema = string.Empty;
    private string _datasetConfigSchema = string.Empty;
    private int _opens;

    public LazyProcessConnector(
        string name, ConnectorPackageRef packageRef, ConnectorManifest manifest, string entrypoint,
        string socketRootDir, Action<int, string, IReadOnlyDictionary<string, string>>? logSink,
        TimeSpan cancelGrace, TimeSpan shutdownGrace)
    {
        _packageRef = packageRef;
        _manifest = manifest;
        _entrypoint = entrypoint;
        _socketRootDir = socketRootDir;
        _logSink = logSink;
        _cancelGrace = cancelGrace;
        _shutdownGrace = shutdownGrace;
        Info = new ConnectorInfo(name, packageRef.Version, ProtocolVersion.Major);
        DeclaredCapabilities = ParseCapabilities(manifest.Capabilities);
    }

    /// <summary>Manifest-declared, unmasked — what the package CLAIMS. Only
    /// <see cref="ProcessConnectorHost.LoadFromDirectory"/>'s warning path reads this; everything else
    /// goes through <see cref="Capabilities"/>.</summary>
    public ConnectorCapabilities DeclaredCapabilities { get; }

    public ConnectorInfo Info { get; }

    /// <summary>Manifest-declared, masked by <see cref="ProcessCapabilities"/>. The handshake is
    /// authoritative once a process exists, but <c>PcpClient</c> refuses any Hello whose capability set
    /// differs from the manifest's, so answering from the manifest before a spawn cannot disagree with
    /// what a spawn would have reported.</summary>
    public ConnectorCapabilities Capabilities => ProcessCapabilities.Mask(DeclaredCapabilities);

    /// <summary>Empty until this connector has been spawned at least once: the config schemas live in
    /// the connector's Hello, and nothing in the manifest carries them. A caller that needs the schema
    /// before an open must call <see cref="ValidateAsync"/> (which spawns) first.</summary>
    public string ConnectionConfigSchema => Volatile.Read(ref _connectionConfigSchema);

    public string DatasetConfigSchema => Volatile.Read(ref _datasetConfigSchema);

    public async ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        var instance = await SpawnAsync(config, track: false, ct).ConfigureAwait(false);
        await using (instance.ConfigureAwait(false))
        {
            return await new ProcessSourceConnector(instance.Client, instance.Process)
                .ValidateAsync(config, ct).ConfigureAwait(false);
        }
    }

    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        var instance = await SpawnAsync(config, track: false, ct).ConfigureAwait(false);
        await using (instance.ConfigureAwait(false))
        {
            return await new ProcessSourceConnector(instance.Client, instance.Process)
                .CheckConnectionAsync(config, ct).ConfigureAwait(false);
        }
    }

    async ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        var instance = await SpawnAsync(config, track: true, ct).ConfigureAwait(false);
        var source = await new ProcessSourceConnector(instance.Client, instance.Process)
            .OpenAsync(config, ct).ConfigureAwait(false);
        instance.Shim = (IGatedShim)source;
        return source;
    }

    async ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        var instance = await SpawnAsync(config, track: true, ct).ConfigureAwait(false);
        var sink = await new ProcessSinkConnector(instance.Client, instance.Process)
            .OpenAsync(config, ct).ConfigureAwait(false);
        instance.Shim = (IGatedShim)sink;
        return sink;
    }

    /// <summary>Spawn → handshake (identity, protocol and capability gates) → Configure → open the
    /// reverse channel, in that order. Every gate lives inside
    /// <see cref="PcpClient.ConnectAndConfigureAsync"/> so it runs BEFORE config values cross; this
    /// method only re-throws what that raises, since the PZ0355/PZ0356/PZ0357 taxonomy is already the
    /// right answer for each.
    ///
    /// <para>A failure at any rung reaps what exists: the client when one was built (its own ladder ends
    /// the process), otherwise the bare process. Nothing here can leave an orphan behind.</para>
    ///
    /// <para><paramref name="track"/> false is a throwaway instance (Validate/CheckConnection), disposed
    /// by its caller the moment the call returns; true hands the lifetime to this host's own
    /// <see cref="DisposeAsync"/>.</para></summary>
    private async Task<ProcessInstance> SpawnAsync(ConnectorConfig config, bool track, CancellationToken ct)
    {
        var ordinal = Interlocked.Increment(ref _opens);
        var socketDir = Path.Combine(_socketRootDir, "pcp-" + Guid.NewGuid().ToString("N")[..8]);
        var process = ConnectorProcess.Spawn(_entrypoint, socketDir, _packageRef.PackageId);
        PcpClient? client = null;
        ProcessInstance instance;
        try
        {
            var (instanceId, connectorConfig) = SplitInstanceId(config, ordinal);
            client = await PcpClient
                .ConnectAndConfigureAsync(process, _manifest, instanceId, connectorConfig, ct)
                .ConfigureAwait(false);
            client.CancelGrace = _cancelGrace;
            client.ShutdownGrace = _shutdownGrace;

            Volatile.Write(ref _connectionConfigSchema, client.Hello.ConnectionConfigSchema);
            Volatile.Write(ref _datasetConfigSchema, client.Hello.DatasetConfigSchema);

            instance = new ProcessInstance(process, client);
            // Opened once per instance, right after Configure and before the shim exists: the gate the
            // engine will hand that shim arrives later (IOperationGateAware, after OpenAsync returns),
            // which is what DeferredOperationGate bridges.
            instance.Pump = HostChannelPump.Start(
                client, process, new DeferredOperationGate(() => instance.Shim?.Gate), _logSink);
        }
        catch
        {
            // Disposing the client is what closes the channel AND ends the process; disposing the bare
            // process would leave a live GrpcChannel behind (reachable when HostChannelPump.Start throws
            // after the handshake already succeeded).
            if (client is not null)
            {
                await client.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                await process.DisposeAsync().ConfigureAwait(false);
            }

            throw;
        }

        if (track)
        {
            _instances.Add(instance);
        }

        return instance;
    }

    /// <summary>Instance id for Configure: the connection name when the caller threaded one in under
    /// <see cref="InstanceIdKey"/>, else a stable per-open id. The key is stripped from what crosses to
    /// the connector — it is host bookkeeping, not a connection option a connector's
    /// ConnectionConfigSchema ever declared.</summary>
    private (string InstanceId, ConnectorConfig Config) SplitInstanceId(ConnectorConfig config, int ordinal)
    {
        if (config.GetString(InstanceIdKey) is not { Length: > 0 } name)
        {
            return ($"{Info.Name}#{ordinal}", config);
        }

        var values = new Dictionary<string, object?>(config.Values, StringComparer.Ordinal);
        values.Remove(InstanceIdKey);
        return (name, new ConnectorConfig(values));
    }

    /// <summary>Reserved config key carrying the named connection this open belongs to. Never authored
    /// in <c>connections.yml</c> — the registry layer sets it, and it never reaches the connector.</summary>
    internal const string InstanceIdKey = "__pz_instance";

    /// <summary>Manifest capability NAMES → the flags value. Names only, matched exactly against the
    /// declared members: <see cref="Enum.TryParse{T}(string, bool, out T)"/> on its own would also accept
    /// <c>"16384"</c> or <c>"NativeScan, Merge"</c>, letting a manifest smuggle in a capability whose
    /// name it never spelled — and the manifest's capability list is what
    /// <see cref="PcpClient.ConnectAndConfigureAsync"/> compares the handshake against, so it has to mean
    /// exactly what it says. An unrecognized name is ignored rather than rejected: a newer connector
    /// naming a capability this host has never heard of is exactly the case where "this host cannot offer
    /// it" is the right, quiet answer.</summary>
    private static readonly HashSet<string> KnownCapabilityNames =
        new(Enum.GetNames<ConnectorCapabilities>(), StringComparer.Ordinal);

    private static ConnectorCapabilities ParseCapabilities(IReadOnlyList<string> names)
    {
        var flags = ConnectorCapabilities.None;
        foreach (var name in names)
        {
            if (KnownCapabilityNames.Contains(name))
            {
                flags |= Enum.Parse<ConnectorCapabilities>(name);
            }
        }

        return flags;
    }

    public async ValueTask DisposeAsync()
    {
        while (_instances.TryTake(out var instance))
        {
            try
            {
                await instance.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // One instance failing to tear down must not orphan the ones still in the bag.
            }
        }
    }

    /// <summary>One spawned connector: its process, its control-plane client, and its reverse-channel
    /// pump. Disposal order is fixed — the pump first (so nothing of it is still touching
    /// <c>client.Grpc</c>), then the client, whose own ladder ends the process.</summary>
    private sealed class ProcessInstance(ConnectorProcess process, PcpClient client) : IAsyncDisposable
    {
        public ConnectorProcess Process => process;

        public PcpClient Client => client;

        public HostChannelPump? Pump { get; set; }

        /// <summary>The opened shim, once <c>OpenAsync</c> has produced one. Only ever read for the gate
        /// it is holding — and read from the pump's own thread while the opening thread writes it, so
        /// the handover takes a real memory barrier (same reasoning as ProcessSource._gate).</summary>
        private IGatedShim? _shim;

        public IGatedShim? Shim
        {
            get => Volatile.Read(ref _shim);
            set => Volatile.Write(ref _shim, value);
        }

        public async ValueTask DisposeAsync()
        {
            if (Pump is { } pump)
            {
                await pump.DisposeAsync().ConfigureAwait(false);
            }

            await client.DisposeAsync().ConfigureAwait(false);
        }
    }
}

/// <summary>Stands in for the engine's real <see cref="IOperationGate"/> between the moment
/// <see cref="HostChannelPump"/> opens (right after Configure) and the moment the engine hands the
/// opened shim its gate (<see cref="IOperationGateAware"/>, after OpenAsync returns). Resolves the gate
/// afresh on every call, so a <c>GateAcquire</c> arriving after the handover is paced and retried by the
/// real thing; one arriving before it runs ungated, which is the only honest answer when no policy
/// exists yet.</summary>
internal sealed class DeferredOperationGate(Func<IOperationGate?> resolve) : IOperationGate
{
    public Task<T> ExecuteAsync<T>(
        string opLabel, bool idempotent, Func<CancellationToken, Task<T>> op, CancellationToken ct) =>
        resolve() is { } gate ? gate.ExecuteAsync(opLabel, idempotent, op, ct) : op(ct);

    public void ReportBudget(int remaining, DateTimeOffset resetAt) =>
        resolve()?.ReportBudget(remaining, resetAt);
}
