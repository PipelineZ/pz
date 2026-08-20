using Pz.Connectors.Abstractions;

namespace Pz.Engine.State;

public static class SchemaDriftDiffer
{
    /// <summary>Kind is the wire word: "added" | "removed" | "retyped".</summary>
    public sealed record Change(string Kind, string Column, string? From, string? To);

    /// <summary>Deterministic order: added in observed order, then removed/retyped in baseline
    /// order. Names ordinal (staging columns come from one DESCRIBE, no case games); types
    /// compared as exact strings — both sides are DuckDB-rendered from the same seam.</summary>
    public static IReadOnlyList<Change> Diff(
        IReadOnlyList<SchemaColumn> baseline, IReadOnlyList<SchemaColumn> observed)
    {
        var baseTypes = baseline.ToDictionary(c => c.Name, c => c.Type, StringComparer.Ordinal);
        var seen = observed.ToDictionary(c => c.Name, c => c.Type, StringComparer.Ordinal);

        var changes = new List<Change>();
        foreach (var col in observed)
        {
            if (!baseTypes.ContainsKey(col.Name))
            {
                changes.Add(new Change("added", col.Name, null, col.Type));
            }
        }

        foreach (var col in baseline)
        {
            if (!seen.TryGetValue(col.Name, out var observedType))
            {
                changes.Add(new Change("removed", col.Name, col.Type, null));
            }
            else if (!string.Equals(observedType, col.Type, StringComparison.Ordinal))
            {
                changes.Add(new Change("retyped", col.Name, col.Type, observedType));
            }
        }

        return changes;
    }

    /// <summary>Stable digest of the effective read shape; U+001F/U+001E separators cannot occur
    /// in column names DuckDB reports or in rendered predicate SQL's semantics-relevant bytes.</summary>
    public static string HashHints(ReadHints hints) =>
        Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes(
                string.Join('', hints.Columns ?? []) + '' + (hints.PredicateSql ?? ""))));
}
