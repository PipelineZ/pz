using Pz.Core.Validation;

namespace Pz.Core.Loading;

/// <summary>A dataset's <c>columns:</c> contract, split into the types every reader consumes and the
/// names marked <c>redact: true</c>, which only <c>pz preview</c> reads.</summary>
internal sealed record ColumnContract(Dictionary<string, string> Types, List<string> Redacted);

/// <summary>Parses a <c>columns:</c> mapping for both read surfaces (connections.yml <c>read:</c> and the
/// <c>source()</c> kwarg). An entry is a type name (<c>id: bigint</c>) or the long form
/// <c>email: { type: varchar, redact: true }</c>. Marks stay out of <see cref="ColumnContract.Types"/>, so
/// they never reach a NodeId or a connector.</summary>
internal static class ColumnContractParser
{
    private const string LongFormHint = "email: { type: varchar, redact: true }";

    /// <summary>The contract, or null when any entry was refused -- a refused contract is no contract,
    /// rather than a silently narrower one.</summary>
    internal static ColumnContract? Parse(Dictionary<string, object?> columns, string where, string file,
        List<PzError> errors)
    {
        var types = new Dictionary<string, string>();
        var redacted = new List<string>();
        var refused = false;

        foreach (var (name, value) in columns)
        {
            switch (value)
            {
                case Dictionary<string, object?> entry:
                    if (!ParseLongForm(name, entry, where, file, errors, out var type, out var redact))
                    {
                        refused = true;
                        continue;
                    }

                    types[name] = type;
                    if (redact)
                    {
                        redacted.Add(name);
                    }

                    break;
                case List<object?>:
                    errors.Add(Shape(where, file,
                        $"column '{name}' must map to a type name, or to {{ type, redact }}"));
                    refused = true;
                    break;
                default:
                    types[name] = value?.ToString() ?? string.Empty;
                    break;
            }
        }

        return refused ? null : new ColumnContract(types, redacted);
    }

    /// <summary>One error per refused column -- the first problem found -- so a column is not reported
    /// twice for the same typo.</summary>
    private static bool ParseLongForm(string name, Dictionary<string, object?> entry, string where, string file,
        List<PzError> errors, out string type, out bool redact)
    {
        type = string.Empty;
        redact = false;

        if (entry.Keys.FirstOrDefault(k => k is not ("type" or "redact")) is { } unknown)
        {
            errors.Add(Shape(where, file,
                $"column '{name}': '{unknown}' is not a column option; only 'type' and 'redact' are"));
            return false;
        }

        if (!entry.TryGetValue("type", out var rawType) || rawType is null or "" or Dictionary<string, object?>
            or List<object?>)
        {
            errors.Add(Shape(where, file, $"column '{name}' needs a 'type' in its long form"));
            return false;
        }

        if (entry.TryGetValue("redact", out var rawRedact) && rawRedact is not bool)
        {
            errors.Add(Shape(where, file, $"column '{name}': 'redact' must be true or false"));
            return false;
        }

        type = rawType.ToString()!;
        redact = rawRedact is true;
        return true;
    }

    private static PzError Shape(string where, string file, string message) =>
        new(PzErrorCode.YamlShape, $"{file}: {where}: {message}.", file, null, LongFormHint);
}
