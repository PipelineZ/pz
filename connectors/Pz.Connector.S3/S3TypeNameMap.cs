using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.S3;

/// <summary>Maps the `columns:` contract's type names to Arrow types (for
/// <see cref="S3Source.GetSchemaAsync"/>'s contract-is-the-schema answer) and to DuckDB native-scan
/// type names (for the strict <c>columns = {…}</c> map). Replicated (not shared) from
/// <c>Pz.Connector.AzureBlob.AzureTypeNameMap</c> per the no-cross-connector-reference rule — the
/// fixed v0 type matrix, kept in lockstep with LocalFiles/Azure if it ever grows.</summary>
internal static class S3TypeNameMap
{
    internal static Field ToArrowField(string columnName, string typeName) =>
        new(columnName, ToArrowType(typeName, columnName), nullable: true);

    private static IArrowType ToArrowType(string typeName, string columnName) => typeName switch
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

    internal static string ToDuckDbName(string typeName, string columnName) => typeName switch
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
