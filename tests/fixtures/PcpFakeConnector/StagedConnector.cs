using System.Diagnostics.Metrics;
using Apache.Arrow;
using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Sdk;

namespace PcpFakeConnector;

/// <summary>A real <see cref="LocalFilesConnector"/> behind the Abstractions interfaces, with every
/// staged misbehavior applied as a decoration: the SDK sees an ordinary connector object and answers
/// honestly from what it implements, which is exactly what makes each switch reach the wire the way
/// a real misbehaving connector would.</summary>
internal sealed class StagedConnector(FixtureOptions options, PzConnectorContext context)
    : ISourceConnector, ISinkConnector
{
    /// <summary>The one instrument this fixture records on, so a host-side test can prove that a
    /// connector's own metrics reach the collector the host named -- the meter half of the telemetry
    /// contract, which the span assertions alone leave unproven. A static, connector-authored name;
    /// nothing derived from configuration ever becomes an instrument name or a label.</summary>
    internal const string ConfigureCounterName = "pz.fixture.configure_calls";

    private readonly Counter<long> _configureCalls = context.Meter.CreateCounter<long>(ConfigureCounterName);

    /// <summary>Counts one Configure, driven from the SDK's Configure hook (wired in <c>Program.cs</c>):
    /// Configure is the SDK's own RPC handler, not a connector method, so this cannot be recorded from
    /// inside the connector object itself.</summary>
    internal void RecordConfigure() => _configureCalls.Add(1);

    /// <summary>The name this fixture registers under, distinct from the builtin's "localfiles" so a
    /// parity test can name both in one project.</summary>
    public const string ConnectorName = "localfiles-pcp";

    /// <summary>What <c>--misreport-name</c> introduces this connector as instead of
    /// <see cref="ConnectorName"/>: a connector that is not the one the manifest registers.</summary>
    private const string ImposterName = "localfiles-pcp-imposter";

    /// <summary>Written to stderr from inside the Configure RPC itself (via <c>PcpServerHooks.OnConfigure</c>,
    /// wired in <c>Program.cs</c>) -- but ONLY under <c>--misreport-name</c>, so no other mode's stderr
    /// changes shape. A host-side test asserts this line never appears after a name-mismatch handshake
    /// failure, which is how it proves Configure itself never ran, i.e. no config value crossed after
    /// an identity mismatch. Marking it from Configure directly (rather than from the first
    /// <see cref="ISourceConnector.OpenAsync"/>/<see cref="ISinkConnector.OpenAsync"/>, which the SDK
    /// reaches lazily and much later) is what keeps the marker's absence proving the strong claim
    /// instead of the merely-true-but-weaker "no source/sink was ever opened".</summary>
    internal const string ConfiguredMarker = "PcpFakeConnector: Configure ran";

    private readonly LocalFilesConnector _inner = new();

    public ConnectorInfo Info => new(
        options.MisreportName ? ImposterName : ConnectorName,
        _inner.Info.Version,
        options.WrongProtocolMajor ? ProtocolVersion.Major + 1 : _inner.Info.ProtocolMajor);

    public ConnectorCapabilities Capabilities
    {
        get
        {
            var capabilities = _inner.Capabilities;
            if (options.SyncState || options.DeclareSyncStateOnly)
            {
                // A feed connector owns one opaque token per dataset, which cannot span independent
                // partition reads -- so PartitionedRead is withdrawn together with declaring SyncState.
                capabilities = (capabilities & ~ConnectorCapabilities.PartitionedRead) | ConnectorCapabilities.SyncState;
            }

            if (options.StableIds)
            {
                capabilities |= ConnectorCapabilities.StablePartitionIds;
            }

            if (options.PruneColumns)
            {
                capabilities |= ConnectorCapabilities.ColumnPruning;
            }

            // --declare-checkpointable-reads stages a connector that claims a capability the
            // out-of-process shims do not implement, so a host test can prove the flag is masked out
            // rather than handed to the planner. --misreport-capabilities stages a manifest/handshake
            // DISAGREEMENT (a handshake failure) rather than an agreed declaration.
            if (options.MisreportCapabilities)
            {
                capabilities |= ConnectorCapabilities.Merge;
            }

            if (options.DeclareCheckpointableReads)
            {
                capabilities |= ConnectorCapabilities.CheckpointableReads;
            }

            // --native-only stages a connector with no universal read path at all: PartitionedRead is
            // withdrawn (there is no universal PlanRead to partition) together with declaring
            // NativeOnlyRead, and StagedSource's PlanReadAsync refuses every call the same way a real
            // native-only connector's does -- a host test can prove both that a conformance vector
            // gated on this flag reports Skip, and that removing that gate would make the vector FAIL
            // rather than silently pass.
            if (options.NativeOnly)
            {
                capabilities = (capabilities & ~ConnectorCapabilities.PartitionedRead) | ConnectorCapabilities.NativeOnlyRead;
            }

            // Simulates a newer SDK build whose Abstractions defines a flag this fixture's own
            // ConnectorCapabilities enum does not: an undefined bit, cast rather than named, since
            // there is no later member to reference from this build.
            if (options.ReportUnknownCapabilityBit)
            {
                capabilities |= (ConnectorCapabilities)(1 << 24);
            }

            return capabilities;
        }
    }

    public string ConnectionConfigSchema => _inner.ConnectionConfigSchema;
    public string DatasetConfigSchema => _inner.DatasetConfigSchema;

    /// <summary>The host strips its own bookkeeping key (<c>__pz_instance</c>, the connection name it
    /// names the instance after) before any config crosses to the connector. A connector is the only
    /// place that can prove it never arrived, so every config this fixture is handed is checked.</summary>
    private static void RefuseHostKeys(ConnectorConfig config)
    {
        foreach (var key in config.Values.Keys)
        {
            if (key.StartsWith("__pz", StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"fixture: host bookkeeping key '{key}' reached the connector");
            }
        }
    }

    /// <summary>Never a key a real connection ever needs; mirrors <c>ConformanceSuite</c>'s private
    /// <c>NumericOptionProbeKey</c> exactly (deliberately not "__pz"-prefixed -- see that constant's own
    /// doc comment). The two projects do not reference each other, so this is a literal, not a shared
    /// constant -- the same convention <c>StructMapping</c>'s doc comment already notes for mirroring
    /// the host's own mapping.</summary>
    private const string NumericOptionProbeKey = "pz_conformance_numeric_probe";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        RefuseHostKeys(config);

        // Cooperates with ConformanceSuite's numeric-option-fidelity vector: the probe value crosses
        // as a double (protobuf's Struct has only "number") and must come back as a long, never the
        // raw double, for this to report zero errors.
        if (config.Values.TryGetValue(NumericOptionProbeKey, out var probe))
        {
            return new ValueTask<ValidationResult>(probe is 424242L
                ? ValidationResult.Success
                : ValidationResult.Failed(
                    $"{NumericOptionProbeKey} crossed as {probe?.GetType().Name ?? "null"} ({probe})"));
        }

        return _inner.ValidateAsync(config, ct);
    }

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        RefuseHostKeys(config);
        if (options.FailCheckTransient)
        {
            throw new PzConnectorException(
                "fixture: connection check refused on purpose", isTransient: true, TimeSpan.FromMilliseconds(250),
                code: "FIXTURE_CHECK_REFUSED", hint: "retry after the cool-down");
        }

        if (options.ThrowUnhandled)
        {
            throw new InvalidOperationException("fixture: deliberate unhandled exception");
        }

        return _inner.CheckConnectionAsync(config, ct);
    }

    async ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        RefuseHostKeys(config);
        var source = await ((ISourceConnector)_inner).OpenAsync(config, ct).ConfigureAwait(false);
        return options.SyncState ? new StagedFeedSource(source, options) : new StagedSource(source, options);
    }

    async ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        RefuseHostKeys(config);
        var sink = await ((ISinkConnector)_inner).OpenAsync(config, ct).ConfigureAwait(false);
        return new StagedSink(sink, options);
    }
}

/// <summary>The source side with the read-path switches applied at plan time. Wrapped at plan time,
/// not at read time: the SDK computes the partition's <c>stable_id</c>/<c>sync_state</c> flags from
/// the planned object and later resolves reads and GetReadState against that SAME object.</summary>
internal class StagedSource(ISource inner, FixtureOptions options) : ISource, IOperationGateAware
{
    /// <summary>Static, connector-authored op label for every <c>--use-gate</c> partition read -- never
    /// a value derived from the dataset spec.</summary>
    private const string GateOpLabel = "localfiles-pcp.read_partition";

    private IOperationGate? _gate;

    public void UseOperationGate(IOperationGate gate) => _gate = gate;

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        inner.GetSchemaAsync(spec, ct);

    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan)
    {
        if (options.SyncState)
        {
            // A feed read has no SQL fragment DuckDB could scan: the token only exists once the
            // partition has been drained over the data plane.
            scan = null;
            return false;
        }

        return inner.TryGetNativeScan(spec, out scan);
    }

    public async ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(
        DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        if (options.NativeOnly)
        {
            // Same PZ0312 refusal shape every real native-only connector's PlanReadAsync stub raises
            // (Pz.Connector.S3's S3Source precedent) -- the fixture must fail exactly like the real
            // thing so a host-side test proves the conformance vector was actually gated, not merely
            // never reached.
            throw new PzConnectorException(
                $"PZ0312: dataset '{spec.Dataset}': localfiles-pcp source is native-scan only; it cannot " +
                "run on the universal tier", isTransient: false);
        }

        var planned = await inner.PlanReadAsync(spec, hints, ct).ConfigureAwait(false);
        var staged = new List<IDatasetPartition>(planned.Count);
        for (var i = 0; i < planned.Count; i++)
        {
            var partition = options.PruneColumns && hints.Columns is { Count: > 0 } columns
                ? new PruningReadPartition(planned[i], columns)
                : planned[i];
            staged.Add(Stage(partition, spec, $"{spec.Dataset}:{i}"));
        }

        return staged;
    }

    /// <summary>Innermost to outermost: endless, gated, sync-state, identified. The sync-state and
    /// identified wrappers must be outermost because the SDK decides a partition's wire flags by the
    /// interfaces the planned object implements.</summary>
    private IDatasetPartition Stage(IDatasetPartition partition, DatasetSpec spec, string stableId)
    {
        if (options.FailReadMidstreamTransient)
        {
            partition = new FailingMidstreamReadPartition(partition);
        }

        if (options.EndlessRead)
        {
            partition = new EndlessReadPartition(partition);
        }

        if (options.UseGate)
        {
            partition = new GatedReadPartition(
                partition,
                _gate ?? throw new InvalidOperationException("--use-gate needs the SDK's operation gate"),
                GateOpLabel);
        }

        if (options.SyncState)
        {
            var sync = new SyncStateReadPartition(partition, spec);
            return options.StableIds ? new IdentifiedSyncStateReadPartition(sync, stableId) : sync;
        }

        return options.StableIds ? new IdentifiedReadPartition(partition, stableId) : partition;
    }

    public ValueTask DisposeAsync() => inner.DisposeAsync();
}

/// <summary>A feed-shaped source: the only difference from <see cref="StagedSource"/> is that it
/// answers the natural read shape at all. <c>--declare-sync-state-only</c> deliberately uses the base
/// class, so the SDK answers UNIMPLEMENTED for a connector that declared SyncState and did not
/// implement it.</summary>
internal sealed class StagedFeedSource(ISource inner, FixtureOptions options)
    : StagedSource(inner, options), INaturalReadShapeSource
{
    public NaturalReadShape GetNaturalReadShape(DatasetSpec spec) => NaturalReadShape.Feed;
}

internal sealed class StagedSink(ISink inner, FixtureOptions options) : ISink
{
    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy) =>
        inner.TryGetNativeCopy(spec, out copy);

    public async ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        var session = await inner.BeginWriteAsync(spec, schema, ct).ConfigureAwait(false);
        return options.FailWriteMidstreamTransient ? new FailingMidstreamWriteSession(session) : session;
    }

    // Without the switch the wrapped sink's own declaration crosses verbatim; --report-abort-semantics-none
    // proves the field genuinely crosses the wire rather than the host echoing its own default back.
    public AbortSemantics AbortSemantics => options.ReportAbortSemanticsNone ? AbortSemantics.None : inner.AbortSemantics;

    public ValueTask DisposeAsync() => inner.DisposeAsync();
}
