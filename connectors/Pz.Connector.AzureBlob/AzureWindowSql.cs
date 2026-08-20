using Pz.Connectors.Abstractions;

namespace Pz.Connector.AzureBlob;

/// <summary>Windowed-bound pushdown for native scan fragments (parquet/csv). Replicated (not shared) from
/// <c>Pz.Connector.LocalFiles.LocalFilesWindowSql.Wrap</c> -- connector projects deliberately do not
/// reference each other or Pz.Core (same precedent as the PZ0312 bare-string check), so this is the
/// source-of-truth predicate shape and escaping copied verbatim-in-spirit; keep in lockstep with
/// LocalFilesWindowSql if that shape ever changes.</summary>
internal static class AzureWindowSql
{
    /// <summary>Wraps <paramref name="fragment"/> (a bare <c>read_csv(...)</c>/<c>read_parquet(...)</c>
    /// call) in <c>(select * from &lt;fragment&gt; where "cursor" &gt; 'lo' and "cursor" &lt;= 'hi')</c>
    /// when <paramref name="spec"/> carries both a watermark cursor and an upper bound -- the only
    /// combination the engine ever stamps for a windowed dataset. Returns <paramref name="fragment"/>
    /// unchanged for every other case (no watermark fields, or a plain unwindowed lower-bound-only
    /// incremental spec), so a non-windowed dataset's fragment stays byte-identical to the unwrapped
    /// shape.</summary>
    public static string Wrap(string fragment, DatasetSpec spec)
    {
        if (spec.WatermarkCursor is null || spec.WatermarkUpperBound is null)
        {
            return fragment;
        }

        // Self-defending guard on the caller-discipline invariant this method otherwise relies on: the
        // engine always stamps WatermarkCursor/WatermarkUpperBound and WatermarkValue together for a
        // windowed dataset -- never one without the other. A null WatermarkValue here would mean that
        // pairing broke upstream; fail loudly instead of letting the `!` below turn it into a
        // NullReferenceException with no explanation.
        if (spec.WatermarkValue is null)
        {
            throw new InvalidOperationException(
                "AzureWindowSql.Wrap: WatermarkCursor/WatermarkUpperBound are set but WatermarkValue is " +
                "null -- the engine always pairs a windowed dataset's lower and upper watermark bounds together");
        }

        var cursor = QuoteIdentifier(spec.WatermarkCursor);
        var lower = EscapeSqlLiteral(spec.WatermarkValue);
        var upper = EscapeSqlLiteral(spec.WatermarkUpperBound);
        return $"(select * from {fragment} where {cursor} > '{lower}' and {cursor} <= '{upper}')";
    }

    // Same double-quote-doubling discipline the rest of the ABI's native-scan/BuildSelect code uses for
    // identifiers (mirrors LocalFilesWindowSql.QuoteIdentifier / PostgresSource.Quote).
    private static string QuoteIdentifier(string identifier) => $"\"{identifier.Replace("\"", "\"\"")}\"";

    // Single-quoted literal escaping, matching LocalFilesWindowSql.EscapeSqlLiteral.
    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");
}
