using Pz.Connectors.Abstractions;

namespace Pz.Engine.Tests.Validation;

/// <summary>A minimal connector double for <see cref="ConnectorConfigValidatorTests"/>: publishes
/// whatever schemas/cross-field behavior a test configures, and never actually opens a source/sink
/// (tier-3 validation never calls OpenAsync).</summary>
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

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotSupportedException("StubConnector never opens a source in tier-3 validator tests");

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotSupportedException("StubConnector never opens a sink in tier-3 validator tests");
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

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotSupportedException("StubOutputSchemaConnector never opens a source in tier-3 validator tests");

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotSupportedException("StubOutputSchemaConnector never opens a sink in tier-3 validator tests");
}
