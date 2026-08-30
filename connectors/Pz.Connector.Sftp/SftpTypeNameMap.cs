using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Sftp;

/// <summary>Maps the `columns:` contract's type names to Arrow types (for
/// <see cref="SftpSource.GetSchemaAsync"/>'s contract-is-the-schema answer). Replicated
/// from <c>Pz.Connector.S3.S3TypeNameMap</c> per the no-cross-connector-reference rule — the
/// fixed v0 type matrix, kept in lockstep with LocalFiles/Azure if it ever grows.</summary>
internal static class SftpTypeNameMap
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
}
