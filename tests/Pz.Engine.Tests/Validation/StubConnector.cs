using Pz.Connectors.Abstractions;

namespace Pz.Engine.Tests.Validation;

/// <summary>A minimal connector double for <see cref="ConnectorConfigValidatorTests"/>: publishes
/// whatever schemas/cross-field behavior a test configures. Tier 3 opens a builtin connection offline to probe
/// it the way the plan will, so opening succeeds with a source/sink that does nothing.</summary>
internal sealed class StubConnector : ISourceConnector, ISinkConnector
{
    public string ConnectionConfigSchema { get; init; } = """{"type":"object","additionalProperties":false}""";
    public string DatasetConfigSchema { get; init; } = """{"type":"object","additionalProperties":false}""";
    public Func<ConnectorConfig, ValidationResult>? ValidateFunc { get; init; }

    public ConnectorInfo Info => new("stub", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidateFunc?.Invoke(config) ?? ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true));

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) => new(new NoopSource());

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) => new(new NoopSink());
}

/// <summary>The same double, but also offering <see cref="IOutputConfigSchema"/> -- a separate type
/// (not a StubConnector property) because the capability's whole point is that it is optional: a
/// class either implements the interface or it does not, so a StubConnector instance's
/// <c>is IOutputConfigSchema</c> check would never be false if StubConnector implemented it
/// unconditionally.</summary>
internal sealed class StubOutputSchemaConnector : ISourceConnector, ISinkConnector, IOutputConfigSchema
{
    public string ConnectionConfigSchema { get; init; } = """{"type":"object","additionalProperties":false}""";
    public string DatasetConfigSchema { get; init; } = """{"type":"object","additionalProperties":false}""";
    public string OutputConfigSchema { get; init; } = """{"type":"object","additionalProperties":false}""";

    public ConnectorInfo Info => new("stub-output-schema", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true));

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) => new(new NoopSource());

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) => new(new NoopSink());
}

/// <summary>The same double, also offering <see cref="IDatasetConfigValidator"/>: a separate type for the same
/// reason as <see cref="StubOutputSchemaConnector"/>.</summary>
internal sealed class StubDatasetValidatorConnector : ISourceConnector, IDatasetConfigValidator
{
    public string ConnectionConfigSchema { get; init; } = """{"type":"object"}""";
    public string DatasetConfigSchema { get; init; } = """{"type":"object"}""";
    public Func<ConnectorConfig, DatasetSpec, ValidationResult> ValidateDatasetFunc { get; init; } =
        (_, _) => ValidationResult.Success;

    /// <summary>Thrown by the read-shape probe tier 3 runs on the opened source, when set.</summary>
    public Func<DatasetSpec, Exception?>? ShapeThrows { get; init; }

    /// <summary>Thrown by OpenAsync, when set.</summary>
    public Func<Exception>? OpenThrows { get; init; }

    public List<DatasetSpec> Seen { get; } = [];
    public List<string> ShapeProbed { get; } = [];

    public ConnectorInfo Info => new("stub-dataset-validator", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true));

    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        OpenThrows is { } open ? throw open() : new(new ShapeSource(this));

    public ValueTask<ValidationResult> ValidateDatasetAsync(ConnectorConfig connection, DatasetSpec dataset,
        CancellationToken ct)
    {
        Seen.Add(dataset);
        return new(ValidateDatasetFunc(connection, dataset));
    }
}

internal sealed class ShapeSource(StubDatasetValidatorConnector owner) : NoopSource, INaturalReadShapeSource
{
    public NaturalReadShape GetNaturalReadShape(DatasetSpec spec)
    {
        owner.ShapeProbed.Add(spec.Dataset);
        return owner.ShapeThrows?.Invoke(spec) is { } refusal ? throw refusal : NaturalReadShape.Full;
    }
}

/// <summary>A source that is never read: tier 3 only opens it.</summary>
internal class NoopSource : ISource
{
    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        throw new InvalidOperationException("tier 3 never reads a schema");

    public bool TryGetNativeScan(DatasetSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeScan? scan) =>
        throw new InvalidOperationException("tier 3 never probes the native path");

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new InvalidOperationException("tier 3 never plans a read");

    public ValueTask DisposeAsync() => default;
}

/// <summary>A sink that is never written: tier 3 only opens it.</summary>
internal sealed class NoopSink : ISink
{
    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy) =>
        throw new InvalidOperationException("tier 3 never probes the native path");

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Apache.Arrow.Schema schema, CancellationToken ct) =>
        throw new InvalidOperationException("tier 3 never writes");

    public ValueTask DisposeAsync() => default;
}
