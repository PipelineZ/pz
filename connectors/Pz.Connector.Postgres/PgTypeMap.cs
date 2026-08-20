namespace Pz.Connector.Postgres;

/// <summary>Documents the v0 postgres → ADO.NET(Npgsql) CLR → Arrow type matrix. <see
/// cref="Pz.Connectors.Abstractions.Batches.DataReaderSource"/> maps generically off
/// <c>reader.GetFieldType(i)</c> — it never inspects postgres type names — so nothing here is
/// consulted at runtime; this table exists purely so the mapping is documented in one place and the
/// container type-matrix test (<c>PgTypeMatrixTests</c>) can seed/assert against it without
/// hardcoding the same pg-type ↔ CLR-type pairing twice.
///
/// Notes on the two non-obvious rows:
/// - <c>numeric</c> has no fixed precision/scale in postgres; Npgsql surfaces it as CLR <see
///   cref="decimal"/>, which <see cref="Pz.Connectors.Abstractions.Batches.DataReaderSource"/> always
///   widens to <c>decimal128(38,9)</c>. A postgres <c>numeric</c> value with more than 9
///   fractional digits is a HARD ERROR, not silent rounding: <see
///   cref="Pz.Connectors.Abstractions.Batches.DataReaderSource.ReadBatchesAsync"/> catches the
///   <see cref="OverflowException"/> the Arrow builder throws for it and rethrows a permanent <see
///   cref="Pz.Connectors.Abstractions.PzConnectorException"/> naming the offending column and the remedy
///   (cast the column in <c>query:</c>, e.g. <c>::numeric(38,9)</c> or <c>::text</c>, or declare it
///   <c>varchar</c> in <c>columns:</c>). Values within 9 fractional digits and 38 total significant
///   digits roundtrip exactly; nothing on this path silently loses precision.
/// - <c>timestamp</c> (no time zone) surfaces as CLR <see cref="DateTime"/> with
///   <see cref="DateTimeKind.Unspecified"/>; <see
///   cref="Pz.Connectors.Abstractions.Batches.DataReaderSource"/>'s Normalize step reinterprets
///   (not converts) it as UTC via <c>DateTime.SpecifyKind(..., Utc)</c> — i.e. postgres
///   <c>timestamp</c> values are trusted to already be UTC wall-clock values.
///   <c>timestamptz</c> surfaces as CLR <see cref="DateTimeOffset"/> already normalized to UTC by
///   Npgsql itself (postgres always stores <c>timestamptz</c> as UTC internally and Npgsql's default
///   session time zone is UTC), so no additional conversion happens for it.</summary>
public static class PgTypeMap
{
    public sealed record Entry(string PgType, Type ClrType, string ArrowDescription);

    public static readonly IReadOnlyList<Entry> Matrix =
    [
        new("integer", typeof(int), "int32"),
        new("bigint", typeof(long), "int64"),
        new("double precision", typeof(double), "float64"),
        new("numeric(38,9)", typeof(decimal), "decimal128(38,9)"),
        new("text", typeof(string), "utf8"),
        new("boolean", typeof(bool), "bool"),
        new("date", typeof(DateOnly), "date32"),
        new("timestamp", typeof(DateTime), "timestamp(us, UTC)"),
        new("timestamptz", typeof(DateTimeOffset), "timestamp(us, UTC)"),
    ];
}
