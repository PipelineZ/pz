using Pz.Connectors.Abstractions;

[assembly: PzConnector("fakeOld", typeof(FakeConnectorOld.OldConnector))]

namespace FakeConnectorOld;

public sealed class OldConnector : IConnector
{
    public ConnectorInfo Info => new("fakeOld", "0.0.1", 0);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";
    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
        => ValueTask.FromResult(ValidationResult.Success);
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
        => ValueTask.FromResult(new ConnectionCheck(true));
}
