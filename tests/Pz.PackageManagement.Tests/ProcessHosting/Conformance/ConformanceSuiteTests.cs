using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.PackageManagement.ProcessHosting.Conformance;

namespace Pz.PackageManagement.Tests.ProcessHosting.Conformance;

/// <summary><see cref="ConformanceSuite.CompareSchemas"/>: the schema/batch-equality vector's own
/// per-field type check, driven directly rather than through a live connector whose data-plane batches
/// would have to disagree with its own declared schema on purpose.</summary>
public sealed class ConformanceSuiteTests
{
    private static Field F(string name, IArrowType type) => new(name, type, nullable: true);

    private static Schema S(params Field[] fields) => new(fields, null);

    [Fact]
    public void A_TypeId_matching_but_structurally_different_list_item_type_is_a_mismatch()
    {
        var declared = S(F("tags", new ListType(Int32Type.Default)));
        var batch = S(F("tags", new ListType(StringType.Default)));

        var verdict = ConformanceSuite.CompareSchemas(declared, batch);

        Assert.NotNull(verdict);
        Assert.Equal(ConformanceOutcome.Failed, verdict.Value.Outcome);
        Assert.Contains("list<int32>", verdict.Value.Detail, StringComparison.Ordinal);
        Assert.Contains("list<utf8>", verdict.Value.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public void A_decimal_precision_mismatch_is_a_mismatch()
    {
        var declared = S(F("amount", new Decimal128Type(10, 2)));
        var batch = S(F("amount", new Decimal128Type(18, 4)));

        var verdict = ConformanceSuite.CompareSchemas(declared, batch);

        Assert.NotNull(verdict);
        Assert.Equal(ConformanceOutcome.Failed, verdict.Value.Outcome);
    }

    [Fact]
    public void A_timestamp_unit_mismatch_is_a_mismatch()
    {
        var declared = S(F("ts", new TimestampType(TimeUnit.Second, (string?)null)));
        var batch = S(F("ts", new TimestampType(TimeUnit.Microsecond, (string?)null)));

        var verdict = ConformanceSuite.CompareSchemas(declared, batch);

        Assert.NotNull(verdict);
        Assert.Equal(ConformanceOutcome.Failed, verdict.Value.Outcome);
    }

    [Fact]
    public void Structurally_equal_schemas_are_not_a_mismatch()
    {
        var declared = S(F("id", Int64Type.Default), F("tags", new ListType(StringType.Default)));
        var batch = S(F("id", Int64Type.Default), F("tags", new ListType(StringType.Default)));

        Assert.Null(ConformanceSuite.CompareSchemas(declared, batch));
    }

    [Fact]
    public void A_field_name_mismatch_is_still_a_mismatch()
    {
        var declared = S(F("id", Int64Type.Default));
        var batch = S(F("identifier", Int64Type.Default));

        var verdict = ConformanceSuite.CompareSchemas(declared, batch);

        Assert.NotNull(verdict);
        Assert.Equal(ConformanceOutcome.Failed, verdict.Value.Outcome);
        Assert.Contains("field name", verdict.Value.Detail, StringComparison.Ordinal);
    }
}
