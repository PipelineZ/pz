namespace Pz.Connector.Postgres.Tests;

/// <summary>Pure unit coverage of <see cref="PgTypeMap.Matrix"/> -- the documentation-only pg-type
/// &lt;-&gt; CLR-type &lt;-&gt; Arrow-description table (nothing here is consulted at runtime, see the
/// class's own summary). <see cref="PgTypeMatrixTests"/> exercises the same matrix end-to-end through a
/// real postgres container but SKIPs without docker, so this file is the offline-safe guarantee that the
/// table itself stays correct and internally consistent.</summary>
public sealed class PgTypeMapTests
{
    [Theory]
    [InlineData("integer", typeof(int), "int32")]
    [InlineData("bigint", typeof(long), "int64")]
    [InlineData("double precision", typeof(double), "float64")]
    [InlineData("numeric(38,9)", typeof(decimal), "decimal128(38,9)")]
    [InlineData("text", typeof(string), "utf8")]
    [InlineData("boolean", typeof(bool), "bool")]
    [InlineData("date", typeof(DateOnly), "date32")]
    [InlineData("timestamp", typeof(DateTime), "timestamp(us, UTC)")]
    [InlineData("timestamptz", typeof(DateTimeOffset), "timestamp(us, UTC)")]
    public void Matrix_entry_maps_pg_type_to_expected_clr_and_arrow_description(
        string pgType, Type clrType, string arrowDescription)
    {
        var entry = Assert.Single(PgTypeMap.Matrix, e => e.PgType == pgType);

        Assert.Equal(clrType, entry.ClrType);
        Assert.Equal(arrowDescription, entry.ArrowDescription);
    }

    [Fact]
    public void Matrix_has_exactly_the_documented_nine_rows() =>
        Assert.Equal(9, PgTypeMap.Matrix.Count);

    [Fact]
    public void Matrix_pg_types_are_unique() =>
        Assert.Equal(PgTypeMap.Matrix.Count, PgTypeMap.Matrix.Select(e => e.PgType).Distinct().Count());

    [Fact]
    public void Timestamp_and_timestamptz_share_the_same_arrow_description_but_differ_in_clr_type()
    {
        var timestamp = PgTypeMap.Matrix.Single(e => e.PgType == "timestamp");
        var timestamptz = PgTypeMap.Matrix.Single(e => e.PgType == "timestamptz");

        Assert.Equal(timestamp.ArrowDescription, timestamptz.ArrowDescription);
        Assert.NotEqual(timestamp.ClrType, timestamptz.ClrType);
    }
}
