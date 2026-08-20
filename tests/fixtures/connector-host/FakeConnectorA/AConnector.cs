using Pz.Connectors.Abstractions;

[assembly: PzConnector("fakeA", typeof(FakeConnectorA.AConnector))]

namespace FakeConnectorA;

public sealed class AConnector : IConnector
{
    public ConnectorInfo Info => new("fakeA", FakeDep.Dep.Marker, ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";
    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
        => ValueTask.FromResult(ValidationResult.Success);
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
        => ValueTask.FromResult(new ConnectionCheck(true));
}
