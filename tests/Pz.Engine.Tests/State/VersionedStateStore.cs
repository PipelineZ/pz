using Pz.Core.Validation;
using Pz.Engine.State;

namespace Pz.Engine.Tests.State;

/// <summary>The rows a versioned state server holds, shared by every run's <see cref="VersionedStateStore{T}"/>.</summary>
internal sealed class VersionedBackend<T> where T : class
{
    public readonly Dictionary<string, (T Value, int Version)> Rows = [];
}

/// <summary>The remote backends' contract (HTTP, SQL Server) in memory: a write expects the version this
/// instance last read, and a key it never read may only be created, never overwritten (PZ0520 otherwise).
/// One instance per run, the way <c>pz run</c> builds them. The local file store is looser (it overwrites
/// a key it never read), so a test that must hold on a hosted backend runs against this one.</summary>
internal sealed class VersionedStateStore<T>(VersionedBackend<T> backend) : IKeyedStateStore<T> where T : class
{
    private readonly Dictionary<string, int> _seen = [];

    public T? Get(string key, Action<string>? notice = null)
    {
        if (!backend.Rows.TryGetValue(key, out var row)) { return null; }
        _seen[key] = row.Version;
        return row.Value;
    }

    public void Set(string key, T value)
    {
        var exists = backend.Rows.TryGetValue(key, out var row);
        int? expected = _seen.TryGetValue(key, out var seen) ? seen : null;
        if (exists ? expected != row.Version : expected is not null)
        {
            throw new PzConfigException(new PzError(PzErrorCode.StateConcurrencyConflict,
                $"state key '{key}' was advanced by another run while this run was executing.", null, null, null));
        }
        var version = (exists ? row.Version : 0) + 1;
        backend.Rows[key] = (value, version);
        _seen[key] = version;
    }

    public IReadOnlyList<KeyValuePair<string, T>>? ListAll(Action<string>? notice = null) =>
        [.. backend.Rows.OrderBy(r => r.Key, StringComparer.Ordinal).Select(r => new KeyValuePair<string, T>(r.Key, r.Value.Value))];

    public void Remove(string key) => backend.Rows.Remove(key);
}
