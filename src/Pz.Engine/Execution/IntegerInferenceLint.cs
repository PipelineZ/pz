using Pz.Core.Dag;
using Pz.Core.Model;

namespace Pz.Engine.Execution;

/// <summary>DuckDB's csv/json <c>auto_detect</c> types
/// an integer column whose values exceed int64 as DOUBLE, silently landing
/// <c>12345678901234567890</c> as <c>1.2345678901234567e+19</c> — business-key corruption with no
/// signal at any tier. This lint runs after a schema-inferred native scan materializes its staging
/// table (the only tier where a contract-less read exists) and fires
/// <see cref="IRunEvents.LossyIntegerInferenceDetected"/> for every DOUBLE column whose non-null
/// values are all finite integers with at least one beyond 2^53 — the magnitude where doubles stop
/// representing integers exactly, so digits may already have been lost. Columns whose max exceeds the
/// HUGEINT range (2^127) stay quiet: no declarable integer type could hold them, so they are
/// scientific-notation floats, not corrupted keys, and the "declare a contract" nudge would mislead.
/// A warning, never a failure — genuinely floating-point data can look integral; the remediation when
/// it IS a key column is a <c>columns:</c> contract (<c>bigint</c>/<c>ubigint</c>/<c>hugeint</c>),
/// which both prunes the read and fails loudly on overflow. Column names only — never row values.</summary>
internal static class IntegerInferenceLint
{
    internal static async Task ApplyAsync(DagNode node, SourceDatasetDef def, string tableName,
        RunContext ctx, CancellationToken ct)
    {
        if (def.Dataset.Columns is { Count: > 0 })
        {
            return; // a declared contract governs the read; nothing was inferred
        }

        // Same packing discipline as SchemaDriftGate.FetchObservedAsync: chr(30) separators cannot
        // occur in DuckDB column names; tableName is a pz-generated staging identifier, never user input.
        var packed = await ctx.Duck.ScalarAsync<string>(
            "select coalesce(string_agg(name, chr(30) order by cid), '') " +
            $"from pragma_table_info('{tableName}') where lower(type) = 'double'", ct).ConfigureAwait(false);
        if (string.IsNullOrEmpty(packed))
        {
            return;
        }

        var columns = packed.Split('\u001e');
        // One aggregate pass over the staged table, one boolean verdict per DOUBLE column. NaN/inf
        // count as non-integral (their source text was never an integer); a zero-row or all-NULL
        // column's max(abs) is NULL, so the conjunction collapses to 'false' via the coalesce.
        var verdicts = columns.Select(c =>
            $"coalesce((count(*) filter (where {Quote(c)} is not null and " +
            $"(not isfinite({Quote(c)}) or {Quote(c)} != trunc({Quote(c)}))) = 0 " +
            $"and max(abs({Quote(c)})) > 9007199254740992 " +
            $"and max(abs({Quote(c)})) < 1.7014118346046923e38)::varchar, 'false')");
        var verdictPacked = await ctx.Duck.ScalarAsync<string>(
            $"select concat_ws(chr(30), {string.Join(", ", verdicts)}) from {tableName}", ct)
            .ConfigureAwait(false);

        var offending = columns
            .Zip(verdictPacked.Split('\u001e'), (name, verdict) => (name, verdict))
            .Where(pair => pair.verdict == "true")
            .Select(pair => pair.name)
            .ToList();
        if (offending.Count > 0)
        {
            ctx.Events.SafeLossyIntegerInferenceDetected(node, def.Source.Name, def.Dataset.Name, offending);
        }
    }

    private static string Quote(string name) => "\"" + name.Replace("\"", "\"\"") + "\"";
}
