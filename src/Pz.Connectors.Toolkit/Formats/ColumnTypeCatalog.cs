using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connectors.Toolkit.Formats;

/// <summary>The one `columns:` contract type matrix every connector shares: the fixed v0 set (int,
/// bigint, double, decimal, varchar, boolean, date, timestamp) mapped to an Arrow type (for a
/// contract-is-the-schema answer) and to a DuckDB type name (for a native scan's strict
/// <c>columns = {…}</c> map or a cast projection). Connectors never reference each other, so the matrix
/// lives here rather than in any one of them.</summary>
public static class ColumnTypeCatalog
{
    /// <summary>The dataset's declared `columns:` contract (column name to contract type name), or
    /// null when it declares none.</summary>
    public static IReadOnlyDictionary<string, string>? ContractColumns(DatasetSpec spec) =>
        spec.Options.TryGetValue("columns", out var value) &&
        value is IReadOnlyDictionary<string, string> { Count: > 0 } columns
            ? columns
            : null;

    /// <summary>Resolves one contract type name to its Arrow type. Throws a permanent
    /// <see cref="PzConnectorException"/> naming <paramref name="columnName"/> for an unknown type name.</summary>
    public static IArrowType ToArrowType(string typeName, string columnName) => typeName switch
    {
        "int" => Int32Type.Default,
        "bigint" => Int64Type.Default,
        "double" => DoubleType.Default,
        "decimal" => new Decimal128Type(38, 9),
        "varchar" => StringType.Default,
        "boolean" => BooleanType.Default,
        "date" => Date32Type.Default,
        "timestamp" => new TimestampType(TimeUnit.Microsecond, "+00:00"),
        _ => throw new PzConnectorException(
            $"column '{columnName}': unknown columns: contract type '{typeName}'", isTransient: false, code: PzConnectorErrorCodes.Config),
    };

    public static Field ToArrowField(string columnName, string typeName) =>
        new(columnName, ToArrowType(typeName, columnName), nullable: true);

    /// <summary>Resolves one contract type name to its DuckDB type name for a native scan's
    /// <c>columns = {…}</c> map or cast projection. Throws a permanent
    /// <see cref="PzConnectorException"/> naming <paramref name="columnName"/> for an unknown type name.</summary>
    public static string ToDuckDbName(string typeName, string columnName) => typeName switch
    {
        "int" => "INTEGER",
        "bigint" => "BIGINT",
        "double" => "DOUBLE",
        "decimal" => "DECIMAL(38,9)",
        "varchar" => "VARCHAR",
        "boolean" => "BOOLEAN",
        "date" => "DATE",
        "timestamp" => "TIMESTAMP",
        _ => throw new PzConnectorException(
            $"column '{columnName}': unknown columns: contract type '{typeName}'", isTransient: false, code: PzConnectorErrorCodes.Config),
    };
}
