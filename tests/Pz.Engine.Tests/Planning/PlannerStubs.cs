using System.Diagnostics.CodeAnalysis;
using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Engine.Tests.Planning;

/// <summary>Source connector whose dataset always exposes a native scan. GetSchemaAsync/PlanReadAsync
/// throw — the planner's probe (TryGetNativeScan) must never fall through to the universal read path.</summary>
internal sealed class StubNativeSource : ISourceConnector, ISource
{
    public ConnectorInfo Info => new("stub", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.NativeScan;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));
    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        throw new InvalidOperationException("planner must never call GetSchemaAsync");

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = new NativeScan("select 'SECRET_MARKER'", []) { Mechanism = "stub_scan" };
        return true;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new InvalidOperationException("planner must never call PlanReadAsync");

    public ValueTask DisposeAsync() => default;
}

/// <summary>Source connector with no native path: TryGetNativeScan always returns false. PlanReadAsync
/// throws — the planner must never attempt the universal read path itself (only the executor does).</summary>
internal sealed class StubUniversalSource : ISourceConnector, ISource
{
    public ConnectorInfo Info => new("stub", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));
    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        throw new InvalidOperationException("planner must never call GetSchemaAsync");

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new InvalidOperationException("planner must never call PlanReadAsync");

    public ValueTask DisposeAsync() => default;
}

/// <summary>Source connector declaring <see cref="ConnectorCapabilities.PartitionedRead"/> but no native
/// path -- proves the planner reads the declared "partitions" dataset option into
/// <c>PlannedNode.Partitions</c> without ever calling PlanReadAsync (which throws, like
/// <see cref="StubUniversalSource"/>).</summary>
internal sealed class StubPartitionedUniversalSource : ISourceConnector, ISource
{
    public ConnectorInfo Info => new("stub", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.PartitionedRead;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));
    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        throw new InvalidOperationException("planner must never call GetSchemaAsync");

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new InvalidOperationException("planner must never call PlanReadAsync");

    public ValueTask DisposeAsync() => default;
}

/// <summary>Source connector with test-configurable <see cref="ConnectorCapabilities"/> — unlike the
/// fixed-capability stubs above, this one lets a planner-gate test assert behavior for both a connector
/// that declares a given flag (e.g. BoundedWindow) and one that doesn't, without needing a dedicated stub
/// class per combination. No native path, same never-touches-network probe contract as
/// <see cref="StubUniversalSource"/>.</summary>
internal sealed class StubConfigurableCapabilitiesSource(ConnectorCapabilities capabilities) : ISourceConnector, ISource
{
    public ConnectorInfo Info => new("stub", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => capabilities;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));
    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        throw new InvalidOperationException("planner must never call GetSchemaAsync");

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new InvalidOperationException("planner must never call PlanReadAsync");

    public ValueTask DisposeAsync() => default;
}

/// <summary>Source connector that is both native-only-read (<see cref="INativeOnlySource"/>) AND
/// declares <see cref="ConnectorCapabilities.GatedOperations"/> connector-wide -- the exact shape of
/// azure-style connectors that adopt <see cref="IOperationGateAware"/> sink-only: the
/// GatedOperations flag alone says nothing about whether THIS (source) read path is ever opened
/// as gate-aware. Pins the PZ0317 rule: rate_limit on a source
/// backed by a connector like this must still be refused.</summary>
internal sealed class StubNativeOnlySourceWithGatedOperations : ISourceConnector, ISource, INativeOnlySource
{
    public ConnectorInfo Info => new("stub", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.NativeScan | ConnectorCapabilities.GatedOperations;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));
    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        throw new InvalidOperationException("planner must never call GetSchemaAsync");

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = new NativeScan("select 'SECRET_MARKER'", []) { Mechanism = "stub_scan" };
        return true;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new PzConnectorException("native-only source cannot use the universal read path", isTransient: false);

    public ValueTask DisposeAsync() => default;
}

/// <summary>Sink connector with no native path: TryGetNativeCopy always returns false. BeginWriteAsync
/// throws — the planner never attempts a write.</summary>
internal sealed class StubUniversalSink : ISinkConnector, ISink
{
    public ConnectorInfo Info => new("stub", "0.1.0", ProtocolVersion.Major);

    // TestDags' fixed OutputDef fixtures default to mode: replace, so this general-purpose
    // "always plans" stub must declare ReplaceWrites like every real sink connector does (PZ0324).
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.ReplaceWrites;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));
    public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

    public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
        throw new InvalidOperationException("planner must never call BeginWriteAsync");

    public ValueTask DisposeAsync() => default;
}

/// <summary>Sink connector with ONLY a native path (<see cref="INativeOnlySink"/>): TryGetNativeCopy
/// always succeeds; BeginWriteAsync always throws permanently, per the marker interface's contract.</summary>
internal sealed class StubNativeOnlySink : ISinkConnector, ISink, INativeOnlySink
{
    public ConnectorInfo Info => new("stub", "0.1.0", ProtocolVersion.Major);

    // Same reasoning as StubUniversalSink -- default OutputDef mode is
    // replace, so this stub needs ReplaceWrites to keep planning clean.
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.NativeCopy | ConnectorCapabilities.ReplaceWrites;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));
    public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

    public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = new NativeCopy("copy SECRET_MARKER", ["create secret SECRET_MARKER"]) { Mechanism = "stub_copy" };
        return true;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
        throw new PzConnectorException("native-only sink cannot use the universal write path", isTransient: false);

    public ValueTask DisposeAsync() => default;
}

/// <summary>Sink connector with test-configurable <see cref="ConnectorCapabilities"/> — the sink-side
/// mirror of <see cref="StubConfigurableCapabilitiesSource"/>, letting a planner-gate test assert
/// behavior for both a connector that declares PathTemplating and one that doesn't. No native path,
/// same never-touches-network probe contract as <see cref="StubUniversalSink"/>.</summary>
internal sealed class StubConfigurableCapabilitiesSink(ConnectorCapabilities capabilities) : ISinkConnector, ISink
{
    public ConnectorInfo Info => new("stub", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => capabilities;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));
    public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

    public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
        throw new InvalidOperationException("planner must never call BeginWriteAsync");

    public ValueTask DisposeAsync() => default;
}

/// <summary>Source connector that resolves
/// <see cref="NaturalReadShape.Feed"/> for every dataset -- the stub counterpart of HttpSource's
/// delta-link-pointer detection, for planner-gate tests that need a connector whose natural read shape
/// is Feed without depending on the real HTTP connector. No native path; test-configurable
/// <see cref="ConnectorCapabilities"/> mirrors <see cref="StubConfigurableCapabilitiesSource"/> so the
/// same stub covers both the PZ0316-conflict and the plans-clean cases.</summary>
internal sealed class StubFeedSource(ConnectorCapabilities capabilities) : ISourceConnector, ISource, INaturalReadShapeSource
{
    public ConnectorInfo Info => new("stub", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => capabilities;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));
    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        throw new InvalidOperationException("planner must never call GetSchemaAsync");

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new InvalidOperationException("planner must never call PlanReadAsync");

    public NaturalReadShape GetNaturalReadShape(DatasetSpec spec) => NaturalReadShape.Feed;

    public ValueTask DisposeAsync() => default;
}

/// <summary>Source connector with ONLY a native path (<see cref="INativeOnlySource"/>): TryGetNativeScan
/// always succeeds; PlanReadAsync always throws permanently, per the marker interface's contract.</summary>
internal sealed class StubNativeOnlySource : ISourceConnector, ISource, INativeOnlySource
{
    public ConnectorInfo Info => new("stub", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.NativeScan;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) => new(ValidationResult.Success);
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) => new(new ConnectionCheck(true));
    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        throw new InvalidOperationException("planner must never call GetSchemaAsync");

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = new NativeScan("select 'SECRET_MARKER'", []) { Mechanism = "stub_scan" };
        return true;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new PzConnectorException("native-only source cannot use the universal read path", isTransient: false);

    public ValueTask DisposeAsync() => default;
}
