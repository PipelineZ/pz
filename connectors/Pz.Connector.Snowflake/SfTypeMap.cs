using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Snowflake;

/// <summary>Snowflake ↔ Arrow v0-matrix type mapping. Read side accepts both the driver's logical
/// type spellings (FIXED, TEXT, REAL) and SQL spellings (NUMBER, VARCHAR, DOUBLE) because
/// DbDataReader metadata exposes the logical names while `columns:`-style declarations use SQL names.</summary>
internal static class SfTypeMap
{
    public static bool TryResolve(string snowflakeTypeName, short precision, short scale, out IArrowType? arrowType)
    {
        arrowType = snowflakeTypeName.ToUpperInvariant() switch
        {
            "FIXED" or "NUMBER" or "DECIMAL" or "NUMERIC" => scale == 0 && precision is > 0 and <= 9
                ? Int32Type.Default
                : scale == 0 && precision is > 0 and <= 18
                    ? Int64Type.Default
                    : new Decimal128Type(precision > 0 ? precision : 38, scale),
            "REAL" or "FLOAT" or "DOUBLE" => DoubleType.Default,
            "TEXT" or "VARCHAR" or "STRING" or "CHAR" => StringType.Default,
            "BOOLEAN" => BooleanType.Default,
            "DATE" => Date32Type.Default,
            "TIMESTAMP_NTZ" or "TIMESTAMP_LTZ" or "TIMESTAMP_TZ" or "DATETIME" =>
                new TimestampType(TimeUnit.Microsecond, (string?)null),
            _ => null,
        };
        return arrowType is not null;
    }

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
            $"arrow type '{type.Name}' has no snowflake DDL mapping -- outside the v0 matrix",
            isTransient: false),
    };
}
