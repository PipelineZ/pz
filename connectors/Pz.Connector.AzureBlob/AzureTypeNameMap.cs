using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.AzureBlob;

/// <summary>Maps the `columns:` contract's type names to Arrow types, and to DuckDB native-scan type
/// names. Replicated (not shared) from <c>Pz.Connector.LocalFiles.TypeNameMap</c> -- connector projects
/// deliberately do not reference each other or Pz.Core, so this is the source-of-truth v0 type matrix
/// (int, bigint, double, decimal, varchar, boolean, date, timestamp) copied verbatim-in-spirit; keep in
/// lockstep with LocalFiles' TypeNameMap if that matrix ever grows.</summary>
internal static class AzureTypeNameMap
{
    /// <summary>Resolves one contract type name to its Arrow type. Throws a permanent
    /// <see cref="PzConnectorException"/> naming <paramref name="columnName"/> for an unknown type name.</summary>
    internal static IArrowType ToArrowType(string typeName, string columnName) => typeName switch
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

    internal static Field ToArrowField(string columnName, string typeName) =>
        new(columnName, ToArrowType(typeName, columnName), nullable: true);

    /// <summary>Resolves one contract type name to its DuckDB type name for native <c>read_csv</c>
    /// columns. Throws a permanent <see cref="PzConnectorException"/> naming <paramref name="columnName"/>
    /// for an unknown type name.</summary>
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
