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
