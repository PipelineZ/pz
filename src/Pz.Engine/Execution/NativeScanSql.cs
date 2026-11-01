using Pz.Core.Dag;

namespace Pz.Engine.Execution;

/// <summary>Builds the statement that stages a native scan. The compiler's read hints narrow what DuckDB
/// reads from the fragment, so columns and rows the pipeline never uses are not materialised.</summary>
internal static class NativeScanSql
{
    /// <summary><paramref name="limit"/> (a fresh preview's slice) caps the rows staged.</summary>
    internal static string Staging(string fragment, ReadHintPlan? hints, long? limit = null)
    {
        var projection = hints?.Columns is { Count: > 0 } columns
            ? string.Join(", ", columns.Select(Quote))
            : "*";
        var sql = $"select {projection} from {fragment}";
        sql = string.IsNullOrWhiteSpace(hints?.PredicateSql) ? sql : $"{sql} where ({hints.PredicateSql})";
        return limit is { } rows ? $"{sql} limit {rows.ToString(System.Globalization.CultureInfo.InvariantCulture)}" : sql;
    }

    private static string Quote(string name) => "\"" + name.Replace("\"", "\"\"") + "\"";
}
