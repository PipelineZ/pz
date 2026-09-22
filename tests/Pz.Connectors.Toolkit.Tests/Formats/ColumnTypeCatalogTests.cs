using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Formats;

namespace Pz.Connectors.Toolkit.Tests.Formats;

/// <summary>Characterization test for the `columns:` contract's fixed v0 type matrix (int, bigint,
/// double, decimal, varchar, boolean, date, timestamp), pinning it BEFORE it becomes the one shared
/// implementation every file-place connector (localfiles, s3, gcs, azureblob) uses in place of its own
/// copy (<c>TypeNameMap</c>/<c>S3TypeNameMap</c>/<c>GcsTypeNameMap</c>/<c>AzureTypeNameMap</c> --
/// CONN-6). Those four copies were textually identical, byte for byte, including the unknown-type
/// error message -- this is that one matrix, verified against exactly what all four already agreed
/// on, so consolidating them changes no connector's observable behavior.</summary>
public sealed class ColumnTypeCatalogTests
{
    [Fact]
    public void ToArrowType_maps_each_known_type_to_the_expected_arrow_type()
    {
        Assert.IsType<Int32Type>(ColumnTypeCatalog.ToArrowType("int", "c"));
        Assert.IsType<Int64Type>(ColumnTypeCatalog.ToArrowType("bigint", "c"));
        Assert.IsType<DoubleType>(ColumnTypeCatalog.ToArrowType("double", "c"));
        Assert.IsType<StringType>(ColumnTypeCatalog.ToArrowType("varchar", "c"));
        Assert.IsType<BooleanType>(ColumnTypeCatalog.ToArrowType("boolean", "c"));
        Assert.IsType<Date32Type>(ColumnTypeCatalog.ToArrowType("date", "c"));

        var decimalType = Assert.IsType<Decimal128Type>(ColumnTypeCatalog.ToArrowType("decimal", "c"));
        Assert.Equal(38, decimalType.Precision);
        Assert.Equal(9, decimalType.Scale);

        var timestampType = Assert.IsType<TimestampType>(ColumnTypeCatalog.ToArrowType("timestamp", "c"));
        Assert.Equal(TimeUnit.Microsecond, timestampType.Unit);
        Assert.Equal("+00:00", timestampType.Timezone);
    }

    [Fact]
    public void ToArrowType_unknown_type_throws_permanent_error_naming_column_and_type()
    {
        var ex = Assert.Throws<PzConnectorException>(() => ColumnTypeCatalog.ToArrowType("nope", "weird_col"));

        Assert.Contains("weird_col", ex.Message, StringComparison.Ordinal);
        Assert.Contains("nope", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void ToArrowField_wraps_the_arrow_type_as_a_nullable_field()
    {
        var field = ColumnTypeCatalog.ToArrowField("amount", "double");

        Assert.Equal("amount", field.Name);
        Assert.IsType<DoubleType>(field.DataType);
        Assert.True(field.IsNullable);
    }

    [Fact]
    public void ToArrowField_unknown_type_throws() =>
        Assert.Throws<PzConnectorException>(() => ColumnTypeCatalog.ToArrowField("c", "nope"));

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
        Assert.Equal(expected, ColumnTypeCatalog.ToDuckDbName(typeName, "col"));

    [Fact]
    public void ToDuckDbName_unknown_type_throws_permanent_error_naming_column_and_type()
    {
        var ex = Assert.Throws<PzConnectorException>(() => ColumnTypeCatalog.ToDuckDbName("nope", "weird_col"));

        Assert.Contains("weird_col", ex.Message, StringComparison.Ordinal);
        Assert.Contains("nope", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsTransient);
    }
}
