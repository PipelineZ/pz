using Apache.Arrow;
using Pz.DuckDb;

namespace Pz.Engine.Preview;

/// <summary>Reads a small sample of a staging relation. Every column is cast to VARCHAR in DuckDB and a
/// redacted column is replaced by a literal there, so a redacted value never reaches this process. No
/// <c>order by</c>: it is a sample, not the first rows by any key.</summary>
public static class PreviewSampler
{
    public const string Redacted = "•••";
    public const int MaxCellChars = 200;

    public static async Task<(IReadOnlyList<PreviewColumn> Columns, IReadOnlyList<IReadOnlyList<PreviewCell>> Rows)>
        SampleAsync(IDuckSession duck, string relation, IReadOnlyList<string> redacted, int limit, CancellationToken ct)
    {
        var marks = redacted.ToHashSet(StringComparer.OrdinalIgnoreCase);
        var described = await QueryStringsAsync(duck,
            $"select column_name, column_type from (describe {relation})", ct).ConfigureAwait(false);
        var columns = described
            .Select(r => new PreviewColumn(r[0]!, r[1]!, marks.Contains(r[0]!)))
            .ToList();

        var select = string.Join(", ", columns.Select(c => c.Redacted
            ? $"'{Redacted}' as {ArrowInterop.QuoteIdentifier(c.Name)}"
            : $"cast({ArrowInterop.QuoteIdentifier(c.Name)} as varchar) as {ArrowInterop.QuoteIdentifier(c.Name)}"));
        var raw = await QueryStringsAsync(duck, $"select {select} from {relation} limit {limit}", ct)
            .ConfigureAwait(false);

        var kinds = columns.Select(c => c.Redacted ? PreviewCellKind.Text : KindOf(c.Type)).ToList();
        var rows = raw
            .Select(r => (IReadOnlyList<PreviewCell>)r.Select((value, i) => Cell(value, kinds[i])).ToList())
            .ToList();
        return (columns, rows);
    }

    /// <summary>Longer than <see cref="MaxCellChars"/>: the first 200 characters (199 when the 200th would
    /// split a surrogate pair) followed by <c>…</c>.</summary>
    internal static string Truncate(string value)
    {
        if (value.Length <= MaxCellChars)
        {
            return value;
        }

        var keep = char.IsHighSurrogate(value[MaxCellChars - 1]) ? MaxCellChars - 1 : MaxCellChars;
        return string.Concat(value.AsSpan(0, keep), "…");
    }

    internal static PreviewCellKind KindOf(string duckType)
    {
        var type = duckType.ToUpperInvariant();
        if (type.StartsWith("DECIMAL", StringComparison.Ordinal))
        {
            return PreviewCellKind.Number;
        }

        return type switch
        {
            "TINYINT" or "SMALLINT" or "INTEGER" or "BIGINT" or "HUGEINT" or "UTINYINT" or "USMALLINT"
                or "UINTEGER" or "UBIGINT" or "UHUGEINT" or "FLOAT" or "DOUBLE" => PreviewCellKind.Number,
            "BOOLEAN" => PreviewCellKind.Boolean,
            _ => PreviewCellKind.Text,
        };
    }

    private static PreviewCell Cell(string? value, PreviewCellKind kind) => value switch
    {
        null => new PreviewCell(PreviewCellKind.Null, null),
        // JSON has no NaN or infinity: keep the value, as a string.
        _ when kind == PreviewCellKind.Number && !IsFinite(value) => new PreviewCell(PreviewCellKind.Text, value),
        _ => new PreviewCell(kind, kind == PreviewCellKind.Text ? Truncate(value) : value),
    };

    private static bool IsFinite(string number) =>
        !number.Contains("nan", StringComparison.OrdinalIgnoreCase)
        && !number.Contains("inf", StringComparison.OrdinalIgnoreCase);

    /// <summary>Runs a query whose every column is VARCHAR and returns its rows as strings (null for
    /// SQL NULL).</summary>
    private static async Task<List<string?[]>> QueryStringsAsync(IDuckSession duck, string sql, CancellationToken ct)
    {
        var rows = new List<string?[]>();
        await foreach (var batch in duck.QueryArrowAsync(sql, ct: ct).ConfigureAwait(false))
        {
            using (batch)
            {
                var arrays = Enumerable.Range(0, batch.ColumnCount).Select(i => (StringArray)batch.Column(i)).ToList();
                for (var row = 0; row < batch.Length; row++)
                {
                    rows.Add([.. arrays.Select(a => a.GetString(row))]);
                }
            }
        }

        return rows;
    }
}
