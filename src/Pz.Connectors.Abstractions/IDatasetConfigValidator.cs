namespace Pz.Connectors.Abstractions;

/// <summary>Optional source capability: cross-field checks between a connection and one of its datasets, for
/// the problems neither <see cref="IConnector.ValidateAsync"/> (the connection alone) nor
/// <see cref="IConnector.DatasetConfigSchema"/> (the dataset alone) can see, such as a dataset option that
/// clashes with what the connection's sign-in sends. `pz validate` calls it once per dataset, after the
/// connection's own check, and reports every error it returns.
///
/// It runs offline and never opens the connection. A connection or dataset the connector cannot parse is
/// reported by those other checks, so the result for one should be success rather than a repeat of their
/// findings. A connector that does not implement this interface is validated exactly as before this
/// capability existed; its clashes surface when the read is planned. Implementing it is additive-only -- a NEW
/// capability interface, not a new member on <see cref="ISourceConnector"/>.</summary>
public interface IDatasetConfigValidator
{
    /// <summary>Checks <paramref name="dataset"/>'s options (as the author wrote them, contract columns included)
    /// against <paramref name="connection"/>. Error messages name the option but not the dataset, which the
    /// caller adds.</summary>
    ValueTask<ValidationResult> ValidateDatasetAsync(ConnectorConfig connection, DatasetSpec dataset,
        CancellationToken ct);
}
