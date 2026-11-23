using System.Globalization;
using Pz.Core.Dag;
using Pz.Core.Validation;
using Pz.DuckDb;

namespace Pz.Engine.Execution;

/// <summary><c>variant:</c> -- after a read lands, each named column of JSON text becomes DuckDB VARIANT, so
/// pipeline SQL reads fields as <c>col.a.b</c> and a field added or removed upstream is never schema drift.
/// It runs inside staging, after the land, so it works the same for a native scan and for rows that arrived
/// over Arrow (which cannot carry VARIANT). A column already VARIANT (a JSON file's <c>record</c>, a reused
/// staged table) is left as it is.</summary>
public static class VariantLanding
{
    /// <summary>Null when every named column is VARIANT afterwards; otherwise PZ0547, and the staged table is
    /// unchanged. Rows holding text that is not JSON are counted, never quoted.</summary>
    public static async Task<PzError?> ApplyAsync(IDuckSession duck, SourceDatasetDef def, string table,
        CancellationToken ct)
    {
        if (def.Dataset.Variant.Count == 0)
        {
            return null;
        }

        var landed = await StagedColumns.DescribeAsync(duck, table, ct).ConfigureAwait(false);
        var convert = new List<string>();
        foreach (var name in def.Dataset.Variant)
        {
            var match = landed.FirstOrDefault(c => string.Equals(c.Name, name, StringComparison.OrdinalIgnoreCase));
            if (match.Name is null)
            {
                return Error(def, $"variant: names '{name}', which the read did not land " +
                    $"(it landed {string.Join(", ", landed.Select(c => c.Name))})",
                    "name a landed column of JSON text; on a JSON file, variant: [record] lands each object whole");
            }

            switch (match.Type)
            {
                case "VARIANT":
                    continue;
                case "VARCHAR" or "JSON":
                    convert.Add(match.Name);
                    break;
                default:
                    return Error(def, $"variant: names '{match.Name}', which landed as {match.Type}, not JSON text",
                        "variant: converts columns of JSON text (VARCHAR or JSON); remove this one");
            }
        }

        if (convert.Count == 0)
        {
            return null;
        }

        // try_cast to JSON is null exactly for text that is not JSON, so a non-null value it turns into null
        // is a row the conversion would fail on.
        var counts = string.Join(", ", convert.Select(c =>
            $"count(*) filter (where {Q(c)} is not null and try_cast({Q(c)} as JSON) is null)::VARCHAR"));
        var bad = (await duck.ScalarAsync<string>(
            $"select concat_ws(',', {counts}) from {table}", ct).ConfigureAwait(false)).Split(',');
        var malformed = convert.Zip(bad, (c, n) => (Column: c, Rows: long.Parse(n, CultureInfo.InvariantCulture)))
            .Where(x => x.Rows > 0).ToList();
        if (malformed.Count > 0)
        {
            return Error(def, "variant: " + string.Join("; ", malformed.Select(m =>
                $"{m.Rows} row(s) of '{m.Column}' hold text that is not JSON")),
                "fix the source, or drop the column from variant: and parse it in the pipeline with try_cast(... as JSON)");
        }

        // A new table swapped in, inside one transaction: ALTER COLUMN ... TYPE VARIANT USING can fail on rows
        // the table no longer has.
        var scratch = table + "__pz_variant";
        var replace = string.Join(", ", convert.Select(c => $"{Q(c)}::JSON::VARIANT as {Q(c)}"));
        await duck.ExecuteTransactionAsync(
        [
            $"create or replace table {scratch} as select * replace ({replace}) from {table}",
            $"drop table {table}",
            $"alter table {scratch} rename to {Unqualified(table)}",
        ], ct).ConfigureAwait(false);
        return null;
    }

    private static PzError Error(SourceDatasetDef def, string what, string hint) =>
        new(PzErrorCode.VariantConversionFailed, $"source '{def.Source.Name}.{def.Dataset.Name}': {what}",
            def.Source.FilePath, null, hint);

    private static string Q(string column) => ArrowInterop.QuoteIdentifier(column);

    /// <summary>The table part of <c>staging.x</c> (RENAME TO takes a bare name).</summary>
    private static string Unqualified(string qualified) => qualified[(qualified.IndexOf('.', StringComparison.Ordinal) + 1)..];
}
