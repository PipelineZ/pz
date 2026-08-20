namespace Pz.Connectors.Abstractions;

/// <summary>Base contract for all connectors. Implementations must have a public parameterless
/// constructor and be registered via an assembly-level <see cref="PzConnectorAttribute"/>.</summary>
public interface IConnector
{
    ConnectorInfo Info { get; }
    ConnectorCapabilities Capabilities { get; }
    /// <summary>JSON Schema (draft 2020-12) for the source/sink <c>connection:</c> block. Lets the CLI
    /// validate configs for connectors it has never seen, offline.</summary>
    string ConnectionConfigSchema { get; }
    /// <summary>JSON Schema for per-dataset / per-output options.</summary>
    string DatasetConfigSchema { get; }
    /// <summary>Offline cross-field validation. Must not touch the network. Never throws for invalid
    /// config — returns errors; throws only for engine bugs.</summary>
    ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct);
    /// <summary>Online connectivity probe. May be slow; must honor <paramref name="ct"/> promptly.</summary>
    ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct);
}
