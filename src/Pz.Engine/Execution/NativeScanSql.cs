using Pz.Core.Dag;

namespace Pz.Engine.Execution;

/// <summary>Builds the statement that stages a native scan. The compiler's read hints narrow what DuckDB
/// reads from the fragment, so columns and rows the pipeline never uses are not materialised.</summary>
internal static class NativeScanSql
{
    internal static string Staging(string fragment, ReadHintPlan? hints)
    {
        var projection = hints?.Columns is { Count: > 0 } columns
            ? string.Join(", ", columns.Select(Quote))
            : "*";
        var sql = $"select {projection} from {fragment}";
        return string.IsNullOrWhiteSpace(hints?.PredicateSql) ? sql : $"{sql} where ({hints.PredicateSql})";
    }

    private static string Quote(string name) => "\"" + name.Replace("\"", "\"\"") + "\"";
}
