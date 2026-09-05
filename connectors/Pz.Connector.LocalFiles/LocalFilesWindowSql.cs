using Pz.Connectors.Abstractions;

namespace Pz.Connector.LocalFiles;

/// <summary>The one shared seam <see cref="CsvSource.TryGetNativeScan"/>
/// and <see cref="ParquetSource.TryGetNativeScan"/> both extend to push a bounded window down into their
/// native-scan fragment -- kept in one place so the wrapping SQL shape, identifier quoting, and literal
/// escaping never drift between the two formats.</summary>
internal static class LocalFilesWindowSql
{
    /// <summary>Wraps <paramref name="fragment"/> — a bare <c>read_csv(...)</c>/<c>read_parquet(...)</c>
    /// call, or a declared-contract cast subquery (<c>(select ... from read_xlsx(...))</c>/<c>read_avro(...)</c>)
    /// — in <c>(select * from &lt;fragment&gt; where "cursor" &gt; 'lo' and "cursor" &lt;= 'hi')</c>
    /// when <paramref name="spec"/> carries both a watermark cursor and an upper bound -- the only
    /// combination <c>SourceLoadExecutor</c> ever stamps for a windowed dataset. Returns
    /// <paramref name="fragment"/> unchanged for every other case (no watermark fields, or a plain
    /// unwindowed lower-bound-only incremental spec), so a non-windowed dataset's fragment stays
    /// byte-identical to the unwrapped shape.</summary>
    internal static string Wrap(string fragment, DatasetSpec spec)
    {
        if (spec.WatermarkCursor is null || spec.WatermarkUpperBound is null)
        {
            return fragment;
        }

        // Self-defending guard on the caller-discipline invariant this method otherwise relies on: the
        // engine (SourceLoadExecutor) always stamps WatermarkCursor/WatermarkUpperBound and WatermarkValue
        // together for a windowed dataset -- never one without the other. A null WatermarkValue here would
        // mean that pairing broke upstream; fail loudly instead of letting the `!` below turn it into a
        // NullReferenceException with no explanation.
        if (spec.WatermarkValue is null)
        {
            throw new InvalidOperationException(
                "LocalFilesWindowSql.Wrap: WatermarkCursor/WatermarkUpperBound are set but WatermarkValue is " +
                "null -- the engine always pairs a windowed dataset's lower and upper watermark bounds together");
        }

        var cursor = QuoteIdentifier(spec.WatermarkCursor);
        var lower = EscapeSqlLiteral(spec.WatermarkValue);
        var upper = EscapeSqlLiteral(spec.WatermarkUpperBound);
        return $"(select * from {fragment} where {cursor} > '{lower}' and {cursor} <= '{upper}')";
    }

    // Same double-quote-doubling discipline the rest of the ABI's native-scan/BuildSelect code uses for
    // identifiers (mirrors PostgresSource.Quote). CsvSource/ParquetSource have no identifier quoter of
    // their own -- their EscapeSqlLiteral is for SINGLE-quoted literals only.
    private static string QuoteIdentifier(string identifier) => $"\"{identifier.Replace("\"", "\"\"")}\"";

    // Single-quoted literal escaping, matching CsvSource/ParquetSource's own EscapeSqlLiteral (a private
    // duplicate rather than a shared public helper -- both call sites already have their own
    // EscapeSqlLiteral for path/column literals, and this method's escaping needs are identical but
    // logically distinct: defensive depth on an engine-canonicalized watermark value, never user input).
    private static string EscapeSqlLiteral(string value) => value.Replace("'", "''");
}
