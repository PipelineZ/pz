using Apache.Arrow.Types;

namespace Pz.Connector.SqlServer;

internal enum MsColumnKind
{
    Int32, Int32FromByte, Int32FromInt16, Int64, Double, DoubleFromFloat, Decimal,
    Utf8, Utf8FromGuid, Bool, Date, TimestampFromDateTime, TimestampFromDateTimeOffset,
}

internal sealed record MsColumn(IArrowType ArrowType, MsColumnKind Kind);

/// <summary>SQL Server → Arrow type matrix: canonical types plus widening acceptance
/// (tinyint/smallint/real/money/char-family/uniqueidentifier/datetime/smalldatetime), keyed by
/// DbColumn.DataTypeName. datetime-family values carry no offset on the wire and are trusted-UTC.</summary>
internal static class MsTypeMap
{
    private static readonly TimestampType TimestampUtc = new(Apache.Arrow.Types.TimeUnit.Microsecond, "+00:00");
    private static readonly Decimal128Type Decimal38x9 = new(38, 9);

    private static readonly Dictionary<string, MsColumn> Matrix = new(StringComparer.OrdinalIgnoreCase)
    {
        ["int"] = new(Int32Type.Default, MsColumnKind.Int32),
        ["tinyint"] = new(Int32Type.Default, MsColumnKind.Int32FromByte),
        ["smallint"] = new(Int32Type.Default, MsColumnKind.Int32FromInt16),
        ["bigint"] = new(Int64Type.Default, MsColumnKind.Int64),
        ["float"] = new(DoubleType.Default, MsColumnKind.Double),
        ["real"] = new(DoubleType.Default, MsColumnKind.DoubleFromFloat),
        ["decimal"] = new(Decimal38x9, MsColumnKind.Decimal),
        ["numeric"] = new(Decimal38x9, MsColumnKind.Decimal),
        ["money"] = new(Decimal38x9, MsColumnKind.Decimal),
        ["smallmoney"] = new(Decimal38x9, MsColumnKind.Decimal),
        ["nvarchar"] = new(StringType.Default, MsColumnKind.Utf8),
        ["varchar"] = new(StringType.Default, MsColumnKind.Utf8),
        ["char"] = new(StringType.Default, MsColumnKind.Utf8),
        ["nchar"] = new(StringType.Default, MsColumnKind.Utf8),
        ["text"] = new(StringType.Default, MsColumnKind.Utf8),
        ["ntext"] = new(StringType.Default, MsColumnKind.Utf8),
        ["uniqueidentifier"] = new(StringType.Default, MsColumnKind.Utf8FromGuid),
        ["bit"] = new(BooleanType.Default, MsColumnKind.Bool),
        ["date"] = new(Date32Type.Default, MsColumnKind.Date),
        ["datetime2"] = new(TimestampUtc, MsColumnKind.TimestampFromDateTime),
        ["datetime"] = new(TimestampUtc, MsColumnKind.TimestampFromDateTime),
        ["smalldatetime"] = new(TimestampUtc, MsColumnKind.TimestampFromDateTime),
        ["datetimeoffset"] = new(TimestampUtc, MsColumnKind.TimestampFromDateTimeOffset),
    };

    public static bool TryResolve(string dataTypeName, out MsColumn? column)
    {
        var paren = dataTypeName.IndexOf('(');
        var normalized = paren >= 0 ? dataTypeName[..paren] : dataTypeName;
        var found = Matrix.TryGetValue(normalized.Trim(), out var value);
        column = found ? value : null;
        return found;
    }
}
