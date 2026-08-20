using Pz.Connectors.Abstractions;

[assembly: PzConnector("fakeB", typeof(FakeConnectorB.BConnector))]

namespace FakeConnectorB;

public sealed class BConnector : IConnector
{
    public ConnectorInfo Info => new("fakeB", FakeDep.Dep.Marker, ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";
    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
        => ValueTask.FromResult(ValidationResult.Success);
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
        => ValueTask.FromResult(new ConnectionCheck(true));
}
