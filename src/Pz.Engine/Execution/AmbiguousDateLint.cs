using Pz.Core.Dag;
using Pz.Core.Model;

namespace Pz.Engine.Execution;

/// <summary>A csv date column whose every value has
/// day AND month &le; 12 is parsed with an assumed day/month order — DuckDB's sniffer picks a
/// <c>%d/%m</c>-family format when nothing forces its hand, so a month-first (US) source is misread
/// on every row, silently. The sniffed format alone cannot distinguish "forced by a day-&gt;12 value"
/// from "assumed" (both report e.g. <c>%d/%m/%Y</c>), so this lint combines two signals: the scan's
/// sniff fragment (<see cref="Pz.Connectors.Abstractions.NativeScan.SniffFragment"/>, DuckDB's own
/// <c>sniff_csv</c>) reports the picked Date/Timestamp format, and the staged data answers whether
/// any value's day exceeds 12 — if none does, no row ever disambiguated the order and the pick was a
/// guess. Fires <see cref="IRunEvents.AmbiguousDateInferenceDetected"/> per format family; a
/// year-first format (<c>%Y-%m-%d</c>) is never ambiguous. A warning, never a failure — the data may
/// genuinely be day-first; the escape hatch when it is not is normalizing the source to ISO 8601, or
/// declaring the column <c>varchar</c> in a <c>columns:</c> contract and parsing it explicitly in
/// SQL. A probe failure is a notice, never a node failure. Column names only — never row values.</summary>
internal static class AmbiguousDateLint
{
    internal static async Task ApplyAsync(DagNode node, SourceDatasetDef def, string tableName,
        string sniffFragment, RunContext ctx, CancellationToken ct)
    {
        if (def.Dataset.Columns is { Count: > 0 })
        {
            return; // a declared contract governs the read; nothing was inferred
        }

        string dateFormat, timestampFormat;
        try
        {
            // chr(31) cannot occur in a strftime format the sniffer emits.
            var packed = await ctx.Duck.ScalarAsync<string>(
                "select coalesce(DateFormat, '') || chr(31) || coalesce(TimestampFormat, '') " +
                $"from {sniffFragment}", ct).ConfigureAwait(false);
            var sep = packed.IndexOf('\u001f');
            dateFormat = packed[..sep];
            timestampFormat = packed[(sep + 1)..];
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Best-effort lint: the scan itself already succeeded, so a failing re-read (file gone,
            // transient storage error) must never fail the node. Deliberately message-free: the raw
            // engine text could echo the sniff fragment (NativeStatementRedactor discipline).
            ctx.Notice?.Invoke(
                $"date-format probe for '{def.Source.Name}.{def.Dataset.Name}' failed; " +
                "ambiguous-date check skipped for this run");
            return;
        }

        // Year-last formats (%d/%m/…, %m/%d/…, any separator) are the ambiguous family; a year-first
        // format fixes the field order by construction.
        var kinds = new List<(string DuckType, string Format)>();
        if (IsAmbiguousFamily(dateFormat))
        {
            kinds.Add(("date", dateFormat));
        }

        if (IsAmbiguousFamily(timestampFormat))
        {
            kinds.Add(("timestamp", timestampFormat));
        }

        foreach (var (duckType, format) in kinds)
        {
            var packedCols = await ctx.Duck.ScalarAsync<string>(
                "select coalesce(string_agg(name, chr(30) order by cid), '') " +
                $"from pragma_table_info('{tableName}') where lower(type) = '{duckType}'", ct)
                .ConfigureAwait(false);
            if (string.IsNullOrEmpty(packedCols))
            {
                continue;
            }

            var columns = packedCols.Split('\u001e');
            // A parsed day > 12 means the source text itself forced the field order for the whole
            // file; all-NULL or zero-row columns collapse to 'false' via the coalesce.
            var verdicts = columns.Select(c =>
                $"coalesce((count({Quote(c)}) > 0 and max(day({Quote(c)})) <= 12)::varchar, 'false')");
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
                ctx.Events.SafeAmbiguousDateInferenceDetected(node, def.Source.Name, def.Dataset.Name,
                    offending, format);
            }
        }
    }

    private static bool IsAmbiguousFamily(string format) =>
        format.StartsWith("%d", StringComparison.Ordinal) || format.StartsWith("%m", StringComparison.Ordinal);

    private static string Quote(string name) => "\"" + name.Replace("\"", "\"\"") + "\"";
}
