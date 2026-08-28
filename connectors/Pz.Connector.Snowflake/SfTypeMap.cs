using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Snowflake;

internal static class SfTypeMap
{
    public static string ToSnowflakeDdl(IArrowType type) => type switch
    {
        Int64Type => "BIGINT",
        Int32Type => "INTEGER",
        DoubleType => "DOUBLE",
        Decimal128Type d => $"NUMBER({d.Precision},{d.Scale})",
        StringType => "VARCHAR",
        BooleanType => "BOOLEAN",
        Date32Type => "DATE",
        TimestampType => "TIMESTAMP_NTZ(6)",
        _ => throw new PzConnectorException(
            $"Snowflake type mapping not implemented for Arrow type {type.Name}",
            isTransient: false)
    };
}
