namespace Pz.Connectors.Abstractions;

/// <summary>Which validation rules an accumulator has fired.</summary>
[Flags]
public enum StatsDrop
{
    None = 0,
    NegativeOrNonFinite = 1,
    InvalidCostUnit = 2,
    SecondCostUnit = 4,
    InvalidExtraName = 8,
    TooManyExtras = 16,
}

/// <summary>An immutable snapshot of remote stats. Extras are sorted ordinally by name.</summary>
public sealed record RemoteStats(
    long Requests,
    long WireBytes,
    long RemoteMs,
    long Throttled,
    double? CostUnits,
    string? CostUnit,
    IReadOnlyList<KeyValuePair<string, double>> Extras,
    long SpansDropped)
{
    /// <summary>True when every number is 0, there is no cost unit, no extras and no dropped spans.</summary>
    public bool IsEmpty =>
        Requests == 0 && WireBytes == 0 && RemoteMs == 0 && Throttled == 0
        && CostUnit is null && Extras.Count == 0 && SpansDropped == 0;
}

/// <summary>Thread-safe <see cref="IConnectorStats"/> that validates what it is given. Counters are
/// lock-free; cost and extras are rare and share one lock.</summary>
public sealed class ConnectorStatsAccumulator : IConnectorStats
{
    private const int MaxExtras = 8;
    private const int MaxExtraNameLength = 40;
    private const int MaxCostUnitLength = 24;

    private readonly object _gate = new();
    private long _requests;
    private long _wireBytes;
    private long _remoteMs;
    private long _throttled;
    private long _spansDropped;
    private int _drops;
    private double _costUnits;
    private string? _costUnit;
    private bool _costSet;
    private Dictionary<string, double>? _extras;

    /// <summary>Union of every rule that fired so far.</summary>
    public StatsDrop Drops => (StatsDrop)Volatile.Read(ref _drops);

    public void AddRequests(long n) => AddCounter(ref _requests, n);

    public void AddWireBytes(long n) => AddCounter(ref _wireBytes, n);

    public void AddRemoteMs(long ms) => AddCounter(ref _remoteMs, ms);

    public void AddThrottled(long n) => AddCounter(ref _throttled, n);

    public void AddSpansDropped(long n) => AddCounter(ref _spansDropped, n);

    public void AddCost(double units, string unit)
    {
        if (!double.IsFinite(units) || units < 0)
        {
            Drop(StatsDrop.NegativeOrNonFinite);
            return;
        }

        if (!IsValidCostUnit(unit))
        {
            Drop(StatsDrop.InvalidCostUnit);
            return;
        }

        lock (_gate)
        {
            if (_costSet && !string.Equals(_costUnit, unit, StringComparison.Ordinal))
            {
                Drop(StatsDrop.SecondCostUnit);
                return;
            }

            _costSet = true;
            _costUnit = unit;
            _costUnits += units;
        }
    }

    public void AddExtra(string name, double value)
    {
        if (!double.IsFinite(value) || value < 0)
        {
            Drop(StatsDrop.NegativeOrNonFinite);
            return;
        }

        if (!IsValidExtraName(name))
        {
            Drop(StatsDrop.InvalidExtraName);
            return;
        }

        lock (_gate)
        {
            _extras ??= new Dictionary<string, double>(StringComparer.Ordinal);
            if (_extras.TryGetValue(name, out var current))
            {
                _extras[name] = current + value;
            }
            else if (_extras.Count >= MaxExtras)
            {
                Drop(StatsDrop.TooManyExtras);
            }
            else
            {
                _extras[name] = value;
            }
        }
    }

    /// <summary>Merges another snapshot through the same validating methods.</summary>
    public void Add(RemoteStats other)
    {
        ArgumentNullException.ThrowIfNull(other);
        if (other.Requests != 0) AddRequests(other.Requests);
        if (other.WireBytes != 0) AddWireBytes(other.WireBytes);
        if (other.RemoteMs != 0) AddRemoteMs(other.RemoteMs);
        if (other.Throttled != 0) AddThrottled(other.Throttled);
        if (other.SpansDropped != 0) AddSpansDropped(other.SpansDropped);
        if (other.CostUnit is not null) AddCost(other.CostUnits ?? 0, other.CostUnit);
        foreach (var extra in other.Extras)
        {
            AddExtra(extra.Key, extra.Value);
        }
    }

    /// <summary>Folds validation rules another party's accumulator already fired, so a reader of
    /// <see cref="Drops"/> sees them. <see cref="Add"/> does not carry drops: a snapshot has none.</summary>
    public void AddDrops(StatsDrop drops) => Drop(drops);

    public RemoteStats Snapshot()
    {
        lock (_gate)
        {
            KeyValuePair<string, double>[] extras = _extras is null
                ? []
                : [.. _extras.OrderBy(e => e.Key, StringComparer.Ordinal)];
            return new RemoteStats(
                Interlocked.Read(ref _requests),
                Interlocked.Read(ref _wireBytes),
                Interlocked.Read(ref _remoteMs),
                Interlocked.Read(ref _throttled),
                _costSet ? _costUnits : null,
                _costUnit,
                extras,
                Interlocked.Read(ref _spansDropped));
        }
    }

    private void AddCounter(ref long field, long n)
    {
        if (n < 0)
        {
            Drop(StatsDrop.NegativeOrNonFinite);
            return;
        }

        Interlocked.Add(ref field, n);
    }

    private void Drop(StatsDrop rule) => Interlocked.Or(ref _drops, (int)rule);

    // ^[a-z][a-z0-9_]*$, length 1..24 -- hand-written to stay allocation-free.
    private static bool IsValidCostUnit(string? unit)
    {
        if (unit is null || unit.Length is 0 or > MaxCostUnitLength || !IsLower(unit[0]))
        {
            return false;
        }

        foreach (var c in unit.AsSpan(1))
        {
            if (!IsLower(c) && !IsDigit(c) && c != '_')
            {
                return false;
            }
        }

        return true;
    }

    // ^[a-z][a-z0-9_]*\.[a-z][a-z0-9_.]*$, length <= 40.
    private static bool IsValidExtraName(string? name)
    {
        if (name is null || name.Length is 0 or > MaxExtraNameLength || !IsLower(name[0]))
        {
            return false;
        }

        var i = 1;
        while (i < name.Length && (IsLower(name[i]) || IsDigit(name[i]) || name[i] == '_'))
        {
            i++;
        }

        if (i >= name.Length - 1 || name[i] != '.' || !IsLower(name[i + 1]))
        {
            return false;
        }

        for (i += 2; i < name.Length; i++)
        {
            if (!IsLower(name[i]) && !IsDigit(name[i]) && name[i] != '_' && name[i] != '.')
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsLower(char c) => c is >= 'a' and <= 'z';

    private static bool IsDigit(char c) => c is >= '0' and <= '9';
}
