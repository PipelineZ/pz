using Apache.Arrow.Types;

namespace Pz.Engine.Validation;

/// <summary>Contract type name -> DuckDB DDL type name (the fixed v0 matrix: int, bigint,
/// double, decimal, varchar, boolean, date, timestamp). Deliberately independent of
/// connectors/Pz.Connector.LocalFiles/TypeNameMap, which maps the same eight names for CSV native
/// scans -- Pz.Engine must not depend on a connector project.</summary>
public static class ContractTypes
{
    public static string ToDuckDdl(string contractType) => contractType switch
    {
        "int" => "integer",
        "bigint" => "bigint",
        "double" => "double",
        "decimal" => "decimal(38,9)",
        "varchar" => "varchar",
        "boolean" => "boolean",
        "date" => "date",
        "timestamp" => "timestamp",
        _ => throw new ArgumentException($"unknown columns: contract type '{contractType}'", nameof(contractType)),
    };

    /// <summary>Contract type name -> the Arrow type tier 5 (<see cref="ConnectivityValidator"/>) expects
    /// a connector's <c>GetSchemaAsync</c> to report for that column, mirroring
    /// <see cref="Pz.Connectors.Abstractions.Batches.ArrowBatchBuilder"/>'s matrix. The
    /// timestamp timezone string ("+00:00") matches every connector's own Arrow construction
    /// (<c>DataReaderSource</c>, <c>TypeNameMap</c>, the in-memory reference source) rather than the
    /// literal string "UTC" -- same offset, the codebase's one actual convention.</summary>
    public static IArrowType ToArrowExpectation(string contractType) => contractType switch
    {
        "int" => Int32Type.Default,
        "bigint" => Int64Type.Default,
        "double" => DoubleType.Default,
        "decimal" => new Decimal128Type(38, 9),
        "varchar" => StringType.Default,
        "boolean" => BooleanType.Default,
        "date" => Date32Type.Default,
        "timestamp" => new TimestampType(TimeUnit.Microsecond, "+00:00"),
        _ => throw new ArgumentException($"unknown columns: contract type '{contractType}'", nameof(contractType)),
    };

    /// <summary>Human-readable rendering of an Arrow type for PZ0331 messages and the schemas.json cache
    /// -- Int32, Int64, Double, Decimal128(38,9), Utf8, Boolean, Date32, Timestamp(Microsecond, +00:00)
    /// rather than the .NET type's own <c>ToString()</c>/<c>Name</c>,
    /// which are inconsistent across these types (some lowercase, some fully-qualified type names).</summary>
    public static string Describe(IArrowType type) => type switch
    {
        Int32Type => "Int32",
        Int64Type => "Int64",
        DoubleType => "Double",
        Decimal128Type d => $"Decimal128({d.Precision},{d.Scale})",
        StringType => "Utf8",
        BooleanType => "Boolean",
        Date32Type => "Date32",
        TimestampType t => $"Timestamp({t.Unit}, {t.Timezone})",
        _ => type.Name,
    };

    /// <summary>Value-equality for two Arrow types, since <see cref="Decimal128Type"/> and
    /// <see cref="TimestampType"/> do not override <see cref="object.Equals(object?)"/> --
    /// comparing the parameters that actually matter (precision/scale, unit/timezone)
    /// rather than reference identity.</summary>
    public static bool ArrowTypesEqual(IArrowType expected, IArrowType actual)
    {
        if (expected.TypeId != actual.TypeId)
        {
            return false;
        }

        return (expected, actual) switch
        {
            (Decimal128Type e, Decimal128Type a) => e.Precision == a.Precision && e.Scale == a.Scale,
            (TimestampType e, TimestampType a) =>
                e.Unit == a.Unit && string.Equals(e.Timezone, a.Timezone, StringComparison.Ordinal),
            _ => true,
        };
    }
}
