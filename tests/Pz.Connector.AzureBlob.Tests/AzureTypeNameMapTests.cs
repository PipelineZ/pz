using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.AzureBlob.Tests;

/// <summary>Pure type-mapping coverage of <see cref="AzureTypeNameMap"/> -- the `columns:` contract type
/// name matrix replicated (not shared) from <c>Pz.Connector.LocalFiles.TypeNameMap</c> (see the class's
/// own summary). Exercises every known type name plus the unknown-type error path for all three exposed
/// members.</summary>
public sealed class AzureTypeNameMapTests
{
    [Fact]
    public void ToArrowType_maps_each_known_type_to_the_expected_arrow_type()
    {
        Assert.IsType<Int32Type>(AzureTypeNameMap.ToArrowType("int", "c"));
        Assert.IsType<Int64Type>(AzureTypeNameMap.ToArrowType("bigint", "c"));
        Assert.IsType<DoubleType>(AzureTypeNameMap.ToArrowType("double", "c"));
        Assert.IsType<StringType>(AzureTypeNameMap.ToArrowType("varchar", "c"));
        Assert.IsType<BooleanType>(AzureTypeNameMap.ToArrowType("boolean", "c"));
        Assert.IsType<Date32Type>(AzureTypeNameMap.ToArrowType("date", "c"));

        var decimalType = Assert.IsType<Decimal128Type>(AzureTypeNameMap.ToArrowType("decimal", "c"));
        Assert.Equal(38, decimalType.Precision);
        Assert.Equal(9, decimalType.Scale);

        var timestampType = Assert.IsType<TimestampType>(AzureTypeNameMap.ToArrowType("timestamp", "c"));
        Assert.Equal(TimeUnit.Microsecond, timestampType.Unit);
        Assert.Equal("+00:00", timestampType.Timezone);
    }

    [Fact]
    public void ToArrowType_unknown_type_throws_permanent_error_naming_column_and_type()
    {
        var ex = Assert.Throws<PzConnectorException>(() => AzureTypeNameMap.ToArrowType("nope", "weird_col"));

        Assert.Contains("weird_col", ex.Message, StringComparison.Ordinal);
        Assert.Contains("nope", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void ToArrowField_wraps_the_arrow_type_as_a_nullable_field()
    {
        var field = AzureTypeNameMap.ToArrowField("amount", "double");

        Assert.Equal("amount", field.Name);
        Assert.IsType<DoubleType>(field.DataType);
        Assert.True(field.IsNullable);
    }

    [Fact]
    public void ToArrowField_unknown_type_throws() =>
        Assert.Throws<PzConnectorException>(() => AzureTypeNameMap.ToArrowField("c", "nope"));

    [Theory]
    [InlineData("int", "INTEGER")]
    [InlineData("bigint", "BIGINT")]
    [InlineData("double", "DOUBLE")]
    [InlineData("decimal", "DECIMAL(38,9)")]
    [InlineData("varchar", "VARCHAR")]
    [InlineData("boolean", "BOOLEAN")]
    [InlineData("date", "DATE")]
    [InlineData("timestamp", "TIMESTAMP")]
    public void ToDuckDbName_maps_every_known_type(string typeName, string expected) =>
        Assert.Equal(expected, AzureTypeNameMap.ToDuckDbName(typeName, "col"));

    [Fact]
    public void ToDuckDbName_unknown_type_throws_permanent_error_naming_column_and_type()
    {
        var ex = Assert.Throws<PzConnectorException>(() => AzureTypeNameMap.ToDuckDbName("nope", "weird_col"));

        Assert.Contains("weird_col", ex.Message, StringComparison.Ordinal);
        Assert.Contains("nope", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
    }
}
