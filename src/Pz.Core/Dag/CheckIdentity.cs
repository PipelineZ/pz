using Pz.Core.Model;

namespace Pz.Core.Dag;

/// <summary>The part of a check node's content hash that comes from the check itself. Two checks on
/// one pipeline with the same identity compile to the same <see cref="NodeId"/>, which a DAG cannot
/// hold twice — the loader refuses the second using this same text, so the two can never disagree.</summary>
public static class CheckIdentity
{
    public static string Canonical(CheckDef check) => string.Join('\n', check.Type,
        string.Join(',', check.Columns), CanonicalJson.Serialize(check.Options));
}
