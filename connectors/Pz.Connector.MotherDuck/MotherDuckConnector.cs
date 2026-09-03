using Pz.Connectors.Abstractions;

[assembly: PzConnector("motherduck", typeof(Pz.Connector.MotherDuck.MotherDuckConnector))]

namespace Pz.Connector.MotherDuck;

/// <summary>MotherDuck source + sink connector — native-path-only. The engine's DuckDB session
/// attaches the MotherDuck database once per connection through the `motherduck` extension (token
/// as a session setting, attach without an alias because MotherDuck refuses aliases on owned
/// databases); every read/write is a plain statement MotherDuck executes. Zero drivers and no
/// offline probe: <see cref="CheckConnectionAsync"/> reports "not checked" and the first run
/// authenticates; `pz validate --connect`'s schema fetch works only with a declared `columns:`
/// contract. The token is a session setting the extension accepts only before its first attach, and
/// the engine issues each distinct setup statement once per run — so two connections with the same
/// database and token share one attach, while a second connection with a different token fails its
/// SET as PZ0311: one MotherDuck token per run. Registered as "motherduck".</summary>
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

    /// <summary>No cross-field rules: required-ness is the schema's; the token is only ever a ''-escaped
    /// literal in the SET statement, and the database is a ''-escaped literal in the attach string and a
    /// ""-quoted identifier in every table reference.</summary>
    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true, "not checked: motherduck has no offline probe; the first run authenticates"));

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) => new(new MotherDuckSource(config));

    ValueTask<ISink> ISinkConnector.OpenAsync(ConnectorConfig config, CancellationToken ct) => new(new MotherDuckSink(config));
}
