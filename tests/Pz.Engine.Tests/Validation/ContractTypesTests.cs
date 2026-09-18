using Apache.Arrow.Types;
using Pz.Engine.Validation;

namespace Pz.Engine.Tests.Validation;

/// <summary><see cref="ContractTypes.ArrowTypesEqual"/> used to compare only <c>TypeId</c> plus a
/// hand-picked pair of parameterized types (decimal128, timestamp) -- anything else whose Arrow
/// <c>TypeId</c> matched but whose nested/parameterized shape did not (a list of the wrong item type,
/// a decimal32/64/256, a fixed-size type of the wrong width) was wrongly accepted as equal. These
/// facts pin cases <c>ToArrowExpectation</c> never produces today but the API is public and general;
/// the deep comparison must not regress just because today's only caller happens not to exercise
/// them.</summary>
public sealed class ContractTypesTests
{
    [Fact]
    public void List_item_type_mismatch_is_not_equal()
    {
        Assert.False(ContractTypes.ArrowTypesEqual(
            new ListType(Int32Type.Default), new ListType(StringType.Default)));
    }

    [Fact]
    public void Decimal32_precision_mismatch_is_not_equal()
    {
        Assert.False(ContractTypes.ArrowTypesEqual(
            new Decimal32Type(9, 2), new Decimal32Type(5, 2)));
    }

    [Fact]
    public void Fixed_size_binary_width_mismatch_is_not_equal()
    {
        Assert.False(ContractTypes.ArrowTypesEqual(
            new FixedSizeBinaryType(16), new FixedSizeBinaryType(8)));
    }

    [Fact]
    public void Timestamp_unit_mismatch_is_still_caught()
    {
        Assert.False(ContractTypes.ArrowTypesEqual(
            new TimestampType(TimeUnit.Second, (string?)null), new TimestampType(TimeUnit.Microsecond, (string?)null)));
    }

    [Fact]
    public void Matching_structural_types_are_equal()
    {
        Assert.True(ContractTypes.ArrowTypesEqual(
            new ListType(Int32Type.Default), new ListType(Int32Type.Default)));
        Assert.True(ContractTypes.ArrowTypesEqual(new Decimal128Type(38, 9), new Decimal128Type(38, 9)));
    }
}
