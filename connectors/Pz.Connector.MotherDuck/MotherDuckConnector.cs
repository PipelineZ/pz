using Pz.Connectors.Abstractions;

[assembly: PzConnector("motherduck", typeof(Pz.Connector.MotherDuck.MotherDuckConnector))]

namespace Pz.Connector.MotherDuck;

/// <summary>MotherDuck source + sink connector — native-path-only. The engine's DuckDB session
/// attaches the MotherDuck database once per connection through the `motherduck` extension (token
/// as a session setting, attach without an alias because MotherDuck refuses aliases on owned
/// databases); every read/write is a plain statement MotherDuck executes. Zero drivers and no
/// offline probe: <see cref="CheckConnectionAsync"/> reports "not checked" and the first run
/// authenticates; `pz validate --connect`'s schema fetch works only with a declared `columns:`
/// contract. Two connections naming the same database share one attach; with different tokens the
/// last node's SET wins. Registered as "motherduck".</summary>
public sealed class MotherDuckConnector : ISourceConnector, ISinkConnector, INativeOnlySource, INativeOnlySink
{
    public ConnectorInfo Info => new("motherduck", "0.1.0", ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.NativeScan | ConnectorCapabilities.NativeCopy |
        ConnectorCapabilities.ReplaceWrites | ConnectorCapabilities.Merge |
        ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound;

    public string ConnectionConfigSchema =>
        """{ "type": "object", "required": ["database", "token"], "properties": { "database": { "type": "string" }, "token": { "type": "string" } }, "additionalProperties": false }""";

    public string DatasetConfigSchema =>
        """{ "type": "object", "properties": { "columns": { "type": "object", "additionalProperties": { "enum": ["int","bigint","double","decimal","varchar","boolean","date","timestamp"] } } }, "additionalProperties": false }""";

    /// <summary>No cross-field rules: required-ness is the schema's, and both values are ''-escaped
    /// literals in the statements that carry them.</summary>
    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true, "not checked: motherduck has no offline probe; the first run authenticates"));

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) => new(new MotherDuckSource(config));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) => new(new MotherDuckSink(config));
}
