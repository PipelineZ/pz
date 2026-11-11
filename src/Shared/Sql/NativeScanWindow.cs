using Pz.Connectors.Abstractions;

namespace Pz.Shared.Sql;

/// <summary>Pushes a windowed incremental's bounds down into a native-scan fragment, so every file
/// connector filters a window with the same predicate shape, quoting and escaping.
///
/// <para>Compiled into the localfiles, s3, gcs and azureblob connectors from this one source file:
/// connectors never reference each other.</para></summary>
internal static class NativeScanWindow
{
    /// <summary>Wraps <paramref name="fragment"/> (a bare <c>read_csv(...)</c>/<c>read_json(...)</c>/
    /// <c>read_parquet(...)</c> call, or a declared-contract cast subquery) in
    /// <c>(select * from &lt;fragment&gt; where "cursor" &gt; 'lo' and "cursor" &lt;= 'hi')</c> when
    /// <paramref name="spec"/> carries both a watermark cursor and an upper bound, the only combination
    /// the engine stamps for a windowed dataset. Every other spec (no watermark, or a lower bound
    /// only) returns <paramref name="fragment"/> unchanged, so an unwindowed fragment stays
    /// byte-identical.</summary>
    internal static string Wrap(string fragment, DatasetSpec spec)
    {
        if (spec.WatermarkCursor is null || spec.WatermarkUpperBound is null)
        {
            return fragment;
        }

        // The engine always stamps the cursor, the lower bound and the upper bound together for a
        // windowed dataset. A missing lower bound means that pairing broke upstream; fail loudly rather
        // than render an unbounded scan.
        if (spec.WatermarkValue is null)
        {
            throw new InvalidOperationException(
                "NativeScanWindow.Wrap: WatermarkCursor/WatermarkUpperBound are set but WatermarkValue is " +
                "null -- the engine always pairs a windowed dataset's lower and upper watermark bounds together");
        }

        var cursor = DuckDbSqlText.QuoteIdentifier(spec.WatermarkCursor);
        var lower = DuckDbSqlText.EscapeLiteral(spec.WatermarkValue);
        var upper = DuckDbSqlText.EscapeLiteral(spec.WatermarkUpperBound);
        return $"(select * from {fragment} where {cursor} > '{lower}' and {cursor} <= '{upper}')";
    }
}
