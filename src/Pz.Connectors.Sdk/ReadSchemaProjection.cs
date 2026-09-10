using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connectors.Sdk;

/// <summary>The schema a planned read's data-plane stream must open with. A source that honors
/// <see cref="ReadHints.Columns"/> yields batches carrying exactly the hinted columns, in the hint's
/// order, while <c>GetSchemaAsync</c> takes no hints and always reports the dataset's full shape --
/// so the stream header has to be derived from the hint, or every pruned batch would be written
/// under a header naming columns it does not carry, and the host would decode it by position into
/// the wrong columns.</summary>
internal static class ReadSchemaProjection
{
    /// <summary>Narrows <paramref name="declared"/> to <paramref name="hints"/>' columns, in hint
    /// order, when the connector declares <see cref="ConnectorCapabilities.ColumnPruning"/> and every
    /// hinted name is a declared field (compared case-insensitively, the way the engine compares them
    /// before restating the hint). Anything else -- no pruning capability, no hint, an empty hint, or
    /// a name the schema does not have -- returns <paramref name="declared"/> itself, since a
    /// non-pruning source ignores the hint and yields its full shape.</summary>
    internal static Schema Apply(Schema declared, ReadHints hints, ConnectorCapabilities capabilities)
    {
        if (!capabilities.HasFlag(ConnectorCapabilities.ColumnPruning) || hints.Columns is not { Count: > 0 } wanted)
        {
            return declared;
        }

        var byName = new Dictionary<string, Field>(StringComparer.OrdinalIgnoreCase);
        foreach (var field in declared.FieldsList)
        {
            byName.TryAdd(field.Name, field);
        }

        var fields = new List<Field>(wanted.Count);
        foreach (var name in wanted)
        {
            if (!byName.TryGetValue(name, out var field))
            {
                return declared;
            }

            fields.Add(field);
        }

        return new Schema(fields, declared.Metadata);
    }
}
