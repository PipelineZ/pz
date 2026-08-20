using Pz.Connectors.Abstractions;

// Fix 1 (review): this fixture exists solely to prove ConnectorRegistryFactory rejects a hosted
// connector whose registered name collides with a builtin's — "localfiles" is BuiltinConnectors'
// registered name for Pz.Connector.LocalFiles (src/Pz.Cli/BuiltinConnectors.cs), not a real one.
[assembly: PzConnector("localfiles", typeof(FakeBuiltinCollider.Connector))]

namespace FakeBuiltinCollider;

public sealed class Connector : ISourceConnector
{
    public ConnectorInfo Info => new("localfiles", "1.0.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
        => ValueTask.FromResult(ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
        => ValueTask.FromResult(new ConnectionCheck(true));

    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct)
        => throw new NotSupportedException(
            "fixture exists only to trigger the builtin name-collision guard; it is never actually opened");
}
