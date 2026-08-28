using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Snowflake.Tests;

public class SfTypeMapTests
{
    [Theory]
    [InlineData("FIXED", 9, 0, ArrowTypeId.Int32)]
    [InlineData("NUMBER", 18, 0, ArrowTypeId.Int64)]
    [InlineData("NUMBER", 38, 0, ArrowTypeId.Decimal128)]
    [InlineData("NUMBER", 10, 2, ArrowTypeId.Decimal128)]
    [InlineData("REAL", 0, 0, ArrowTypeId.Double)]
    [InlineData("TEXT", 0, 0, ArrowTypeId.String)]
    [InlineData("BOOLEAN", 0, 0, ArrowTypeId.Boolean)]
    [InlineData("DATE", 0, 0, ArrowTypeId.Date32)]
    [InlineData("TIMESTAMP_NTZ", 0, 0, ArrowTypeId.Timestamp)]
    [InlineData("TIMESTAMP_TZ", 0, 0, ArrowTypeId.Timestamp)]
    public void Resolves_supported_types(string name, short p, short s, ArrowTypeId expected)
    {
        Assert.True(SfTypeMap.TryResolve(name, p, s, out var arrow));
        Assert.Equal(expected, arrow!.TypeId);
    }

    [Fact]
    public void Timestamps_are_microsecond()
    {
        Assert.True(SfTypeMap.TryResolve("TIMESTAMP_NTZ", 0, 9, out var arrow));
        Assert.Equal(TimeUnit.Microsecond, ((TimestampType)arrow!).Unit);
    }

    [Fact]
    public void Decimal_carries_precision_and_scale()
    {
        Assert.True(SfTypeMap.TryResolve("NUMBER", 10, 2, out var arrow));
        var dec = (Decimal128Type)arrow!;
        Assert.Equal(10, dec.Precision);
        Assert.Equal(2, dec.Scale);
    }

    [Theory]
    [InlineData("VARIANT")]
    [InlineData("ARRAY")]
    [InlineData("OBJECT")]
    [InlineData("GEOGRAPHY")]
    [InlineData("BINARY")]
    [InlineData("TIME")]
    public void Unmapped_types_return_false(string name) =>
        Assert.False(SfTypeMap.TryResolve(name, 0, 0, out _));

    [Theory]
    [InlineData(ArrowTypeId.Int64, "BIGINT")]
    [InlineData(ArrowTypeId.Int32, "INTEGER")]
    [InlineData(ArrowTypeId.Double, "DOUBLE")]
    [InlineData(ArrowTypeId.String, "VARCHAR")]
    [InlineData(ArrowTypeId.Boolean, "BOOLEAN")]
    [InlineData(ArrowTypeId.Date32, "DATE")]
    public void Ddl_maps_simple_types(ArrowTypeId id, string expected)
    {
        IArrowType t = id switch
        {
            ArrowTypeId.Int64 => Int64Type.Default, ArrowTypeId.Int32 => Int32Type.Default,
            ArrowTypeId.Double => DoubleType.Default, ArrowTypeId.String => StringType.Default,
            ArrowTypeId.Boolean => BooleanType.Default, ArrowTypeId.Date32 => Date32Type.Default,
            _ => throw new InvalidOperationException(),
        };
        Assert.Equal(expected, SfTypeMap.ToSnowflakeDdl(t));
    }

    [Fact]
    public void Ddl_maps_decimal_and_timestamp()
    {
        Assert.Equal("NUMBER(10,2)", SfTypeMap.ToSnowflakeDdl(new Decimal128Type(10, 2)));
        Assert.Equal("TIMESTAMP_NTZ(6)", SfTypeMap.ToSnowflakeDdl(new TimestampType(TimeUnit.Microsecond, (string?)null)));
    }
}
