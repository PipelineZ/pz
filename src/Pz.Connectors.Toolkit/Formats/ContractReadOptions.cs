namespace Pz.Connectors.Toolkit.Formats;

/// <summary>The unit of an epoch number read into a <c>timestamp</c> contract column.</summary>
public enum EpochUnit
{
    Seconds,
    Milliseconds,
}

/// <summary>Per-dataset reading rules for <see cref="ContractProjector"/>, beyond the column types.
/// <see cref="Default"/> reads exactly as the overloads without options do.</summary>
public sealed record ContractReadOptions
{
    public static ContractReadOptions Default { get; } = new();

    /// <summary>Column name to RFC 6901 pointer into the record: the column's value is the pointed-at
    /// node instead of the top-level key named like the column. Columns not listed read the key.</summary>
    public IReadOnlyDictionary<string, string>? Pointers { get; init; }

    /// <summary>When set, a JSON number in a <c>timestamp</c> column is epoch time in this unit. JSON
    /// strings are always parsed as text timestamps, so a digit string is never read as epoch time.</summary>
    public EpochUnit? EpochUnit { get; init; }

    /// <summary>The connector's name for the option that sets <see cref="EpochUnit"/>, used to name the
    /// next step when a JSON number reaches a timestamp column; null when the connector has none.</summary>
    public string? EpochUnitOption { get; init; }
}
