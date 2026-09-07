using Apache.Arrow;
using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;

namespace PcpFakeConnector;

/// <summary>A real <see cref="LocalFilesConnector"/> behind the Abstractions interfaces, with every
/// staged misbehavior applied as a decoration: the SDK sees an ordinary connector object and answers
/// honestly from what it implements, which is exactly what makes each switch reach the wire the way
/// a real misbehaving connector would.</summary>
internal sealed class StagedConnector(FixtureOptions options) : ISourceConnector, ISinkConnector
{
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

            return capabilities;
        }
    }

    public string ConnectionConfigSchema => _inner.ConnectionConfigSchema;
    public string DatasetConfigSchema => _inner.DatasetConfigSchema;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        _inner.ValidateAsync(config, ct);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        if (options.FailCheckTransient)
        {
            throw new PzConnectorException(
                "fixture: connection check refused on purpose", isTransient: true, TimeSpan.FromMilliseconds(250));
        }

        return _inner.CheckConnectionAsync(config, ct);
    }

    async ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        var source = await ((ISourceConnector)_inner).OpenAsync(config, ct).ConfigureAwait(false);
        return options.SyncState ? new StagedFeedSource(source, options) : new StagedSource(source, options);
    }

    async ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
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
        var planned = await inner.PlanReadAsync(spec, hints, ct).ConfigureAwait(false);
        var staged = new List<IDatasetPartition>(planned.Count);
        for (var i = 0; i < planned.Count; i++)
        {
            staged.Add(Stage(planned[i], spec, $"{spec.Dataset}:{i}"));
        }

        return staged;
    }

    /// <summary>Innermost to outermost: endless, gated, sync-state, identified. The sync-state and
    /// identified wrappers must be outermost because the SDK decides a partition's wire flags by the
    /// interfaces the planned object implements.</summary>
    private IDatasetPartition Stage(IDatasetPartition partition, DatasetSpec spec, string stableId)
    {
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

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
        inner.BeginWriteAsync(spec, schema, ct);

    // Without the switch the wrapped sink's own declaration crosses verbatim; --report-abort-semantics-none
    // proves the field genuinely crosses the wire rather than the host echoing its own default back.
    public AbortSemantics AbortSemantics => options.ReportAbortSemanticsNone ? AbortSemantics.None : inner.AbortSemantics;

    public ValueTask DisposeAsync() => inner.DisposeAsync();
}
