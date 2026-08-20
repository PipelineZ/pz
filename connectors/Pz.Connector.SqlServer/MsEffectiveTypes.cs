using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.SqlServer;

/// <summary>Per-column effective DDL type via the ratified
/// hierarchy -- declared `columns:` entry > derived from OutputSpec.MaxTextLengths with 2x bucket
/// headroom > nvarchar(4000) -- for Arrow String columns; MsDdl.DdlType for everything else.
/// Validation aggregates every bad `columns:` entry into ONE non-transient exception (error
/// philosophy: report all), offline, before any connection is opened.</summary>
internal static class MsEffectiveTypes
{
    private static readonly int[] Buckets = [16, 32, 64, 128, 256, 512, 1000, 2000, 4000];

    public static MsResolvedTypes Resolve(OutputSpec spec, Schema schema)
    {
        var declared = ParseDeclared(spec, schema);
        var types = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var field in schema.FieldsList)
        {
            types[field.Name] = declared.TryGetValue(field.Name, out var declaredType)
                ? declaredType
                : field.DataType.TypeId == ArrowTypeId.String
                    ? DeriveTextType(
                        spec.MaxTextLengths is not null && spec.MaxTextLengths.TryGetValue(field.Name, out var max)
                            ? max
                            : null)
                    : MsDdl.DdlType(field);
        }

        return new MsResolvedTypes(types, declared.Keys.ToHashSet(StringComparer.Ordinal));
    }

    /// <summary>Bucketing: smallest bucket >= min(2x observed, 4000); observed > 4000 =>
    /// nvarchar(max) (real data is never truncated to hit a bucket); no observation => the
    /// nvarchar(4000) fallback (via min(): null arrives here only through Resolve's null path).</summary>
    private static string DeriveTextType(long? observed)
    {
        if (observed is null)
        {
            return "nvarchar(4000)";
        }

        if (observed.Value > 4000)
        {
            return "nvarchar(max)";
        }

        var target = Math.Min(observed.Value * 2, 4000);
        foreach (var bucket in Buckets)
        {
            if (bucket >= target)
            {
                return $"nvarchar({bucket})";
            }
        }

        return "nvarchar(4000)";
    }

    private static Dictionary<string, string> ParseDeclared(OutputSpec spec, Schema schema)
    {
        var declared = new Dictionary<string, string>(StringComparer.Ordinal);
        if (!spec.Options.TryGetValue("columns", out var raw) || raw is null)
        {
            return declared;
        }

        if (raw is not System.Collections.IDictionary map)
        {
            throw new PzConnectorException(
                $"output '{spec.Output}': 'columns' must be a map of column name to type " +
                "(e.g. columns: { status: 'nvarchar(20)' })", isTransient: false);
        }

        var known = schema.FieldsList.Select(f => f.Name).ToHashSet(StringComparer.Ordinal);
        var errors = new List<string>();
        foreach (System.Collections.DictionaryEntry entry in map)
        {
            var name = entry.Key.ToString() ?? "";
            var typeText = entry.Value?.ToString() ?? "";
            if (!known.Contains(name))
            {
                errors.Add($"'{name}' names no column in the staged result");
                continue;
            }

            if (!MsTypeGrammar.TryParse(typeText, out var canonical, out var error))
            {
                errors.Add($"'{name}': {error}");
                continue;
            }

            declared[name] = canonical;
        }

        if (errors.Count > 0)
        {
            throw new PzConnectorException(
                $"output '{spec.Output}': invalid 'columns' entries -- {string.Join("; ", errors)}",
                isTransient: false);
        }

        return declared;
    }
}

internal sealed record MsResolvedTypes(
    IReadOnlyDictionary<string, string> Types, IReadOnlySet<string> Declared);
