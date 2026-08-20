namespace Pz.Core.Dag;

/// <summary>The name a SourceLoad's staging relation goes by inside the run's DuckDB — the one place
/// <c>src_&lt;source&gt;__&lt;dataset&gt;</c> is spelled, so the renderer, the compiler, the watermark
/// inference, and the executors can never drift apart on it.
///
/// This is load-bearing: an entity is named the way its own system names it, so <c>dbo.orders</c> is
/// a perfectly ordinary dataset name. Interpolated raw,
/// its dot would turn <c>staging.src_erp__dbo.orders</c> into a four-part qualified name and DuckDB
/// would go looking for a catalog called <c>staging</c>. Quoting instead of folding was rejected: the
/// name is concatenated downstream (<c>+ "__deletes"</c>), and a suffix past a closing quote is not a
/// name at all.
///
/// Folding is many-to-one, so <c>dbo.orders</c> and <c>dbo_orders</c> in one source would land in the
/// same staging table. <c>DagCompiler</c> refuses that pair (PZ0110) rather than letting the second
/// load overwrite the first.</summary>
public static class StagingName
{
    /// <summary>The staging relation name (unqualified — callers prepend <c>staging.</c>) for one
    /// dataset of one source.</summary>
    public static string ForSourceLoad(string sourceName, string datasetName) =>
        $"src_{sourceName}__{Fold(datasetName)}";

    /// <summary>Every character outside <c>[A-Za-z0-9_]</c> becomes <c>_</c>, so the result is a legal
    /// unquoted SQL identifier whatever the remote system's naming allows — dots, and the slashes and
    /// dashes of path and endpoint entities.</summary>
    private static string Fold(string name) =>
        string.Create(name.Length, name, static (span, source) =>
        {
            for (var i = 0; i < source.Length; i++)
            {
                span[i] = char.IsAsciiLetterOrDigit(source[i]) || source[i] == '_' ? source[i] : '_';
            }
        });
}
