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

    /// <summary>The canonical <c>information_schema.columns</c> spelling Snowflake reports back for a
    /// column declared via <see cref="ToSnowflakeDdl"/> -- the sink's <c>schema_policy:
    /// fail_on_change</c> drift check compares this against what the catalog actually has (see
    /// <see cref="SfDdl.EnsureTargetAsync"/>). Constraints this mapping relies on, verified against
    /// Snowflake's own documented type system:
    /// <list type="bullet">
    /// <item>Snowflake's catalog does not distinguish INTEGER from BIGINT -- both are literal
    /// synonyms for <c>NUMBER(38,0)</c>, so <see cref="Int32Type"/> and <see cref="Int64Type"/> map
    /// to the identical display and are indistinguishable once stored.</item>
    /// <item><see cref="Decimal128Type"/> reports <c>DATA_TYPE='NUMBER'</c> with its actual
    /// precision/scale in <c>NUMERIC_PRECISION</c>/<c>NUMERIC_SCALE</c>.</item>
    /// <item><see cref="DoubleType"/> reports <c>DATA_TYPE='FLOAT'</c> with no numeric
    /// precision/scale -- Snowflake's floating type has none.</item>
    /// <item><see cref="StringType"/> reports <c>DATA_TYPE='TEXT'</c>. This sink never declares a
    /// sized VARCHAR (<see cref="ToSnowflakeDdl"/> always emits unsized <c>VARCHAR</c>), so
    /// <c>CHARACTER_MAXIMUM_LENGTH</c> plays no part in the comparison.</item>
    /// <item><see cref="Date32Type"/> reports <c>DATA_TYPE='DATE'</c>.</item>
    /// <item><see cref="TimestampType"/> reports <c>DATA_TYPE='TIMESTAMP_NTZ'</c> at
    /// <c>DATETIME_PRECISION</c> 6 -- the microsecond precision <see cref="ToSnowflakeDdl"/>
    /// always declares.</item>
    /// </list></summary>
    public static string ToInformationSchemaDisplay(IArrowType type) => type switch
    {
        Int64Type or Int32Type => "NUMBER(38,0)",
        Decimal128Type d => $"NUMBER({d.Precision},{d.Scale})",
        DoubleType => "FLOAT",
        StringType => "TEXT",
        BooleanType => "BOOLEAN",
        Date32Type => "DATE",
        TimestampType => "TIMESTAMP_NTZ(6)",
        _ => throw new PzConnectorException(
            $"arrow type '{type.Name}' has no snowflake DDL mapping -- outside the v0 matrix",
            isTransient: false),
    };
}
