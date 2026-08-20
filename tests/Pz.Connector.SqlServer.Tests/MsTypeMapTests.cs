using Apache.Arrow.Types;

namespace Pz.Connector.SqlServer.Tests;

public class MsTypeMapTests
{
    // MsColumnKind is `internal`; InternalsVisibleTo makes it usable inside this assembly's method
    // bodies, but a *public* Theory method may not declare an internal type in its signature (CS0051 --
    // this is a hard C# accessibility-domain rule, not relaxed by InternalsVisibleTo). The kind is
    // therefore passed through InlineData as its underlying int and cast back inside the test body.
    [Theory]
    [InlineData("int", (int)MsColumnKind.Int32)]
    [InlineData("tinyint", (int)MsColumnKind.Int32FromByte)]
    [InlineData("smallint", (int)MsColumnKind.Int32FromInt16)]
    [InlineData("bigint", (int)MsColumnKind.Int64)]
    [InlineData("float", (int)MsColumnKind.Double)]
    [InlineData("real", (int)MsColumnKind.DoubleFromFloat)]
    [InlineData("decimal", (int)MsColumnKind.Decimal)]
    [InlineData("numeric", (int)MsColumnKind.Decimal)]
    [InlineData("money", (int)MsColumnKind.Decimal)]
    [InlineData("smallmoney", (int)MsColumnKind.Decimal)]
    [InlineData("nvarchar", (int)MsColumnKind.Utf8)]
    [InlineData("varchar", (int)MsColumnKind.Utf8)]
    [InlineData("char", (int)MsColumnKind.Utf8)]
    [InlineData("nchar", (int)MsColumnKind.Utf8)]
    [InlineData("text", (int)MsColumnKind.Utf8)]
    [InlineData("ntext", (int)MsColumnKind.Utf8)]
    [InlineData("uniqueidentifier", (int)MsColumnKind.Utf8FromGuid)]
    [InlineData("bit", (int)MsColumnKind.Bool)]
    [InlineData("date", (int)MsColumnKind.Date)]
    [InlineData("datetime2", (int)MsColumnKind.TimestampFromDateTime)]
    [InlineData("datetime", (int)MsColumnKind.TimestampFromDateTime)]
    [InlineData("smalldatetime", (int)MsColumnKind.TimestampFromDateTime)]
    [InlineData("datetimeoffset", (int)MsColumnKind.TimestampFromDateTimeOffset)]
    public void Resolves_supported_types(string name, int kind)
    {
        Assert.True(MsTypeMap.TryResolve(name, out var column));
        Assert.Equal((MsColumnKind)kind, column!.Kind);
    }

    [Fact]
    public void Normalizes_case_and_length_suffix()
    {
        Assert.True(MsTypeMap.TryResolve("NVARCHAR(50)", out var column));
        Assert.Equal(MsColumnKind.Utf8, column!.Kind);
    }

    [Theory]
    [InlineData("varbinary")]
    [InlineData("time")]
    [InlineData("sql_variant")]
    [InlineData("xml")]
    [InlineData("geography")]
    public void Rejects_unsupported_types(string name) =>
        Assert.False(MsTypeMap.TryResolve(name, out _));

    [Fact]
    public void Decimal_maps_to_decimal128_38_9()
    {
        Assert.True(MsTypeMap.TryResolve("decimal", out var column));
        var t = Assert.IsType<Decimal128Type>(column!.ArrowType);
        Assert.Equal(38, t.Precision);
        Assert.Equal(9, t.Scale);
    }
}
