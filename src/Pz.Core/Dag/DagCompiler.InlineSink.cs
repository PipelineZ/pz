using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;

namespace Pz.Core.Dag;

/// <summary>Recognizing the leading `INSERT INTO {{ sink(...) }}` statement of a pipeline and stripping it down to the query.</summary>
public static partial class DagCompiler
{
    /// <summary>
    /// Extracts the fixed `INSERT INTO {{ sink(...) }}` prefix from every pipeline carrying an inline
    /// sink() binding, replacing its entry in <paramref name="rendered"/> with the extracted-SQL
    /// RenderResult (InlineBindings carried over unchanged via `with`) so every later stage -- and the
    /// compiled pipeline node itself -- sees only the SELECT that actually runs. PZ0208 covers every way
    /// a sink() call can be malformed: more than one call to one output in a pipeline, a call on an
    /// ephemeral pipeline (which produces no node for a sink to depend on), or a call that isn't the
    /// pipeline's mandatory leading INSERT statement. Throws once, after checking every pipeline.
    /// </summary>
    private static void ExtractInlineSinkPrefixes(PzProject project, Dictionary<string, RenderResult> rendered)
    {
        var errors = new List<PzError>();
        foreach (var pipeline in project.Pipelines)
        {
            var result = rendered[pipeline.Name];
            if (result.InlineBindings.Count == 0)
            {
                continue;
            }

            if (pipeline.Materialization == "ephemeral")
            {
                var b = result.InlineBindings[0];
                errors.Add(new PzError(PzErrorCode.InvalidSinkCall,
                    $"pipeline '{pipeline.Name}' is ephemeral but calls sink('{b.Sink}', '{b.Output}') " +
                    "— ephemeral pipelines produce no node for a sink to depend on",
                    pipeline.FilePath, null, InsertFormHint));
                continue;
            }

            var duplicate = result.InlineBindings
                .GroupBy(b => (b.Sink, b.Output))
                .FirstOrDefault(g => g.Count() > 1);
            if (duplicate is not null)
            {
                errors.Add(new PzError(PzErrorCode.InvalidSinkCall,
                    $"pipeline '{pipeline.Name}' binds sink('{duplicate.Key.Sink}', '{duplicate.Key.Output}') " +
                    "more than once — list each sink output at most once",
                    pipeline.FilePath, null, InsertFormHint));
                continue;
            }

            var markers = result.InlineBindings.Select(b => b.Marker).ToList();
            var extracted = TryExtractInsertPrefix(result.Sql, markers);
            if (extracted is null)
            {
                var first = result.InlineBindings[0];
                errors.Add(new PzError(PzErrorCode.InvalidSinkCall,
                    $"pipeline '{pipeline.Name}' calls sink('{first.Sink}', '{first.Output}') but not as the " +
                    "pipeline's leading INSERT INTO {{ sink(...) }} statement",
                    pipeline.FilePath, null, InsertFormHint));
                continue;
            }

            rendered[pipeline.Name] = result with { Sql = extracted };
        }

        if (errors.Count > 0)
        {
            throw new PzValidationException(errors);
        }
    }

    private const string InsertFormHint =
        "bind the sink inline as the pipeline's leading statement: INSERT INTO {{ sink('<sink>', '<output>') }} <query>";

    /// <summary>
    /// Recognizes the INSERT-form prefix at the very start of <paramref name="sql"/>: any run
    /// of whitespace and/or full-line `--` SQL comments, then (case-insensitive) `insert`, whitespace,
    /// `into`, whitespace, then either the scalar form (a single sink() marker) or the array fan-out
    /// form `[ m1, m2, ... ]` (markers in the pipeline's recorded <see cref="InlineSinkBinding"/> render
    /// order), then whitespace separating it from the query. Names are matched by exact ordinal
    /// comparison against markers built from the bindings — deliberately not a regex with named capture
    /// groups, since sink/output names may contain underscores that would make such a regex ambiguous.
    /// Returns the query verbatim — CTEs and all, the fixed prefix ends right after the
    /// closing marker/bracket — or null if the shape doesn't match, including any marker
    /// recurring anywhere in what would be the extracted query (a malformed/duplicated call the
    /// duplicate-output check above can't catch alone).
    /// </summary>
    private static string? TryExtractInsertPrefix(string sql, IReadOnlyList<string> markers)
    {
        var i = 0;
        while (true) // leading whitespace and/or full-line `--` comments
        {
            var start = i;
            while (i < sql.Length && char.IsWhiteSpace(sql[i])) { i++; }
            if (i + 1 < sql.Length && sql[i] == '-' && sql[i + 1] == '-')
            {
                var newline = sql.IndexOf('\n', i);
                i = newline < 0 ? sql.Length : newline + 1;
            }
            if (i == start) { break; }
        }

        if (!TryMatchKeyword(sql, ref i, "insert") || !SkipWhitespace(sql, ref i) ||
            !TryMatchKeyword(sql, ref i, "into") || !SkipWhitespace(sql, ref i))
        {
            return null;
        }

        if (i < sql.Length && sql[i] == '[') // array form: [ m1, m2, ... ]
        {
            i++;
            for (var idx = 0; idx < markers.Count; idx++)
            {
                SkipWhitespace(sql, ref i);
                if (!TryMatchMarker(sql, ref i, markers[idx])) { return null; }
                SkipWhitespace(sql, ref i);
                if (idx < markers.Count - 1)
                {
                    if (i >= sql.Length || sql[i] != ',') { return null; }
                    i++;
                }
            }
            SkipWhitespace(sql, ref i);
            if (i >= sql.Length || sql[i] != ']') { return null; }
            i++;
        }
        else // scalar form: exactly one marker, no brackets
        {
            if (markers.Count != 1 || !TryMatchMarker(sql, ref i, markers[0])) { return null; }
        }

        if (!SkipWhitespace(sql, ref i)) { return null; }

        var extracted = sql[i..];
        foreach (var marker in markers)
        {
            if (extracted.Contains(marker, StringComparison.Ordinal)) { return null; }
        }
        return extracted;
    }

    private static bool TryMatchMarker(string sql, ref int i, string marker)
    {
        if (i + marker.Length > sql.Length || string.CompareOrdinal(sql, i, marker, 0, marker.Length) != 0)
        {
            return false;
        }
        i += marker.Length;
        return true;
    }

    private static bool TryMatchKeyword(string sql, ref int i, string keyword)
    {
        if (i + keyword.Length > sql.Length ||
            string.Compare(sql, i, keyword, 0, keyword.Length, StringComparison.OrdinalIgnoreCase) != 0)
        {
            return false;
        }

        i += keyword.Length;
        return true;
    }

    /// <summary>Advances past whitespace; true when at least one character was skipped.</summary>
    private static bool SkipWhitespace(string sql, ref int i)
    {
        var start = i;
        while (i < sql.Length && char.IsWhiteSpace(sql[i]))
        {
            i++;
        }

        return i > start;
    }
}
