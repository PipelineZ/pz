using Pz.Core.Validation;

namespace Pz.Core.Loading;

/// <summary>A dataset's <c>columns:</c> contract, split into the types every reader consumes, the
/// names marked <c>redact: true</c>, which only <c>pz preview</c> reads, and the columns read from a nested
/// <c>pointer</c>, which reach the connector as the <see cref="PointersOption"/> read option.</summary>
internal sealed record ColumnContract(Dictionary<string, string> Types, List<string> Redacted,
    Dictionary<string, object?> Pointers)
{
    /// <summary>The read option that carries the long form's pointers to the connector. It is an option, so
    /// it is part of the SourceLoad NodeId (a pointer changes what lands); it is present only when some
    /// column has a pointer, so a contract without one hashes as before. pz owns the key: written by hand
    /// it is refused (<see cref="RefuseHandWritten"/>), so a pointer has one way to be declared.</summary>
    internal const string PointersOption = "column_pointers";

    /// <summary>Adds <see cref="PointersOption"/> to <paramref name="options"/> when there are pointers.</summary>
    internal void AddPointersTo(Dictionary<string, object?> options)
    {
        if (Pointers.Count > 0)
        {
            options[PointersOption] = Pointers;
        }
    }

    internal static PzError? RefuseHandWritten(IEnumerable<string> keys, string where, string file) =>
        keys.Contains(PointersOption, StringComparer.Ordinal)
            ? new PzError(PzErrorCode.YamlShape,
                $"{file}: {where}: '{PointersOption}' is not a read option; pz sets it from the columns' 'pointer' entries.",
                file, null, "declare the pointer on the column: status: { type: varchar, pointer: /fields/status/name }")
            : null;
}

/// <summary>Parses a <c>columns:</c> mapping for both read surfaces (connections.yml <c>read:</c> and the
/// <c>source()</c> kwarg). An entry is a type name (<c>id: bigint</c>) or the long form
/// <c>email: { type: varchar, redact: true, pointer: /contact/email }</c>. Marks stay out of <see cref="ColumnContract.Types"/>, so
/// they never reach a NodeId or a connector.</summary>
internal static class ColumnContractParser
{
    private const string LongFormHint = "email: { type: varchar, redact: true }  or  status: { type: varchar, pointer: /fields/status/name }";

    /// <summary>The contract, or null when any entry was refused -- a refused contract is no contract,
    /// rather than a silently narrower one.</summary>
    internal static ColumnContract? Parse(Dictionary<string, object?> columns, string where, string file,
        List<PzError> errors)
    {
        var types = new Dictionary<string, string>();
        var redacted = new List<string>();
        var pointers = new Dictionary<string, object?>();
        var refused = false;

        foreach (var (name, value) in columns)
        {
            switch (value)
            {
                case Dictionary<string, object?> entry:
                    if (!ParseLongForm(name, entry, where, file, errors, out var type, out var redact,
                            out var pointer))
                    {
                        refused = true;
                        continue;
                    }

                    types[name] = type;
                    if (redact)
                    {
                        redacted.Add(name);
                    }

                    if (pointer is not null)
                    {
                        pointers[name] = pointer;
                    }

                    break;
                case List<object?>:
                    errors.Add(Shape(where, file,
                        $"column '{name}' must map to a type name, or to {{ type, redact, pointer }}"));
                    refused = true;
                    break;
                default:
                    types[name] = value?.ToString() ?? string.Empty;
                    break;
            }
        }

        return refused ? null : new ColumnContract(types, redacted, pointers);
    }

    /// <summary>One error per refused column -- the first problem found -- so a column is not reported
    /// twice for the same typo.</summary>
    private static bool ParseLongForm(string name, Dictionary<string, object?> entry, string where, string file,
        List<PzError> errors, out string type, out bool redact, out string? pointer)
    {
        type = string.Empty;
        redact = false;
        pointer = null;

        if (entry.Keys.FirstOrDefault(k => k is not ("type" or "redact" or "pointer")) is { } unknown)
        {
            errors.Add(Shape(where, file,
                $"column '{name}': '{unknown}' is not a column option; only 'type', 'redact' and 'pointer' are"));
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

        if (entry.TryGetValue("pointer", out var rawPointer) && !IsJsonPointer(rawPointer))
        {
            errors.Add(Shape(where, file,
                $"column '{name}': 'pointer' must be a JSON pointer into the record, starting with '/' " +
                "(e.g. /fields/status/name; '~0' and '~1' escape '~' and '/')"));
            return false;
        }

        type = rawType.ToString()!;
        redact = rawRedact is true;
        pointer = rawPointer as string;
        return true;
    }

    /// <summary>A non-empty RFC 6901 pointer: starts with '/', and every '~' is followed by '0' or '1'.
    /// The empty pointer (the whole record) is refused: that is the raw envelope's job.</summary>
    private static bool IsJsonPointer(object? value)
    {
        if (value is not string { Length: > 0 } text || text[0] != '/')
        {
            return false;
        }

        for (var i = 0; i < text.Length; i++)
        {
            if (text[i] == '~' && (i + 1 >= text.Length || text[i + 1] is not ('0' or '1')))
            {
                return false;
            }
        }

        return true;
    }

    private static PzError Shape(string where, string file, string message) =>
        new(PzErrorCode.YamlShape, $"{file}: {where}: {message}.", file, null, LongFormHint);
}
