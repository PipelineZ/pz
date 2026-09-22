using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connectors.Toolkit.Formats;

/// <summary>The one `columns:` contract type matrix shared by every file-place connector
/// (localfiles, s3, gcs, azureblob): fixed v0 set -- int, bigint, double, decimal, varchar, boolean,
/// date, timestamp -- mapped to an Arrow type (for a contract-is-the-schema answer) and to a DuckDB
/// type name (for a native scan's strict <c>columns = {…}</c> map or a cast projection). Previously
/// replicated per connector (LocalFiles' <c>TypeNameMap</c>, <c>AzureTypeNameMap</c>,
/// <c>GcsTypeNameMap</c>, <c>S3TypeNameMap</c>) under the no-cross-connector-reference rule; that rule
/// is about connectors not referencing EACH OTHER, and does not require re-deriving something every
/// one of them already gets from this shared toolkit, the same way they already share
/// <see cref="FileFormatCatalog"/> itself.</summary>
public static class ColumnTypeCatalog
{
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
            $"column '{columnName}': unknown columns: contract type '{typeName}'", isTransient: false),
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
            $"column '{columnName}': unknown columns: contract type '{typeName}'", isTransient: false),
    };
}
