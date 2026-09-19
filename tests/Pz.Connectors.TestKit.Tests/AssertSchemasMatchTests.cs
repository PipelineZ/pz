using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.TestKit;

/// <summary><see cref="SourceConnectorAcceptanceTests.AssertSchemasMatch"/>: the acceptance suite's own
/// schema/batch equality check, driven directly rather than through a subclassed connector that would
/// have to misbehave on purpose to exercise a mismatch.</summary>
public sealed class AssertSchemasMatchTests
{
    private static Field F(string name, IArrowType type) => new(name, type, nullable: true);

    private static Schema S(params Field[] fields) => new(fields, null);

    [Fact]
    public void A_TypeId_matching_but_structurally_different_list_item_type_fails()
    {
        var expected = S(F("tags", new ListType(Int32Type.Default)));
        var actual = S(F("tags", new ListType(StringType.Default)));

        Assert.ThrowsAny<Exception>(() => SourceConnectorAcceptanceTests.AssertSchemasMatch(expected, actual));
    }

    [Fact]
    public void A_decimal_precision_mismatch_fails()
    {
        var expected = S(F("amount", new Decimal128Type(10, 2)));
        var actual = S(F("amount", new Decimal128Type(18, 4)));

        Assert.ThrowsAny<Exception>(() => SourceConnectorAcceptanceTests.AssertSchemasMatch(expected, actual));
    }

    [Fact]
    public void A_timestamp_unit_mismatch_fails()
    {
        var expected = S(F("ts", new TimestampType(TimeUnit.Second, (string?)null)));
        var actual = S(F("ts", new TimestampType(TimeUnit.Microsecond, (string?)null)));

        Assert.ThrowsAny<Exception>(() => SourceConnectorAcceptanceTests.AssertSchemasMatch(expected, actual));
    }

    [Fact]
    public void Structurally_equal_schemas_pass()
    {
        var expected = S(F("id", Int64Type.Default), F("tags", new ListType(StringType.Default)));
        var actual = S(F("id", Int64Type.Default), F("tags", new ListType(StringType.Default)));

        SourceConnectorAcceptanceTests.AssertSchemasMatch(expected, actual);
    }
}
