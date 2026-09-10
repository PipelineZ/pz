using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Arrow;

namespace Pz.Connectors.Sdk.Tests;

public sealed class ArrowSchemaShapeTests
{
    private static Field F(string name, IArrowType type) => new(name, type, nullable: true);

    private static Schema S(params Field[] fields) => new(fields, null);

    public static TheoryData<string, IArrowType, IArrowType> DifferentShapes => new()
    {
        { "list item type", new ListType(Int32Type.Default), new ListType(StringType.Default) },
        { "large list item type", new LargeListType(Int32Type.Default), new LargeListType(Int64Type.Default) },
        { "nested list item type", new ListType(new ListType(Int32Type.Default)), new ListType(new ListType(StringType.Default)) },
        { "struct child count", new StructType([F("a", Int32Type.Default)]), new StructType([F("a", Int32Type.Default), F("b", Int32Type.Default)]) },
        { "struct child type", new StructType([F("a", Int32Type.Default)]), new StructType([F("a", StringType.Default)]) },
        { "map value type", new MapType(StringType.Default, Int32Type.Default), new MapType(StringType.Default, StringType.Default) },
        { "fixed size list size", new FixedSizeListType(Int32Type.Default, 3), new FixedSizeListType(Int32Type.Default, 4) },
        { "fixed size list item type", new FixedSizeListType(Int32Type.Default, 3), new FixedSizeListType(FloatType.Default, 3) },
        { "fixed size binary width", new FixedSizeBinaryType(16), new FixedSizeBinaryType(8) },
        { "decimal128 precision", new Decimal128Type(10, 2), new Decimal128Type(38, 2) },
        { "decimal128 scale", new Decimal128Type(10, 2), new Decimal128Type(10, 0) },
        { "decimal256 precision", new Decimal256Type(40, 2), new Decimal256Type(76, 2) },
        { "timestamp unit", new TimestampType(TimeUnit.Microsecond, (string?)null), new TimestampType(TimeUnit.Nanosecond, (string?)null) },
        { "timestamp timezone", new TimestampType(TimeUnit.Microsecond, "UTC"), new TimestampType(TimeUnit.Microsecond, (string?)null) },
        { "timestamp different timezones", new TimestampType(TimeUnit.Microsecond, "UTC"), new TimestampType(TimeUnit.Microsecond, "Europe/Berlin") },
        { "time32 unit", new Time32Type(TimeUnit.Second), new Time32Type(TimeUnit.Millisecond) },
        { "time64 unit", new Time64Type(TimeUnit.Microsecond), new Time64Type(TimeUnit.Nanosecond) },
        { "duration unit", DurationType.FromTimeUnit(TimeUnit.Millisecond), DurationType.FromTimeUnit(TimeUnit.Second) },
        { "interval unit", new IntervalType(IntervalUnit.YearMonth), new IntervalType(IntervalUnit.DayTime) },
        { "dictionary value type", new DictionaryType(Int32Type.Default, StringType.Default, ordered: false), new DictionaryType(Int32Type.Default, Int64Type.Default, ordered: false) },
        { "dictionary index type", new DictionaryType(Int32Type.Default, StringType.Default, ordered: false), new DictionaryType(Int8Type.Default, StringType.Default, ordered: false) },
        { "dictionary ordered", new DictionaryType(Int32Type.Default, StringType.Default, ordered: false), new DictionaryType(Int32Type.Default, StringType.Default, ordered: true) },
        { "union mode", new UnionType([F("a", Int32Type.Default)], [0], UnionMode.Dense), new UnionType([F("a", Int32Type.Default)], [0], UnionMode.Sparse) },
        { "union type ids", new UnionType([F("a", Int32Type.Default)], [0], UnionMode.Dense), new UnionType([F("a", Int32Type.Default)], [5], UnionMode.Dense) },
        { "union child type", new UnionType([F("a", Int32Type.Default)], [0], UnionMode.Dense), new UnionType([F("a", StringType.Default)], [0], UnionMode.Dense) },
        { "primitive", Int32Type.Default, Int64Type.Default },
        { "primitive vs list of it", Int32Type.Default, new ListType(Int32Type.Default) },
    };

    public static TheoryData<string, IArrowType, IArrowType> SameShapes => new()
    {
        { "primitive", Int64Type.Default, Int64Type.Default },
        { "list", new ListType(Int32Type.Default), new ListType(Int32Type.Default) },
        { "list item renamed", new ListType(F("item", Int32Type.Default)), new ListType(F("element", Int32Type.Default)) },
        { "struct children renamed", new StructType([F("a", Int32Type.Default)]), new StructType([F("b", Int32Type.Default)]) },
        { "struct child nullability", new StructType([new Field("a", Int32Type.Default, nullable: true)]), new StructType([new Field("a", Int32Type.Default, nullable: false)]) },
        { "map", new MapType(StringType.Default, Int32Type.Default), new MapType(StringType.Default, Int32Type.Default) },
        { "decimal128", new Decimal128Type(10, 2), new Decimal128Type(10, 2) },
        { "timestamp", new TimestampType(TimeUnit.Microsecond, "UTC"), new TimestampType(TimeUnit.Microsecond, "UTC") },
        { "timestamp empty vs null timezone", new TimestampType(TimeUnit.Microsecond, ""), new TimestampType(TimeUnit.Microsecond, (string?)null) },
        { "fixed size binary", new FixedSizeBinaryType(16), new FixedSizeBinaryType(16) },
        { "dictionary", new DictionaryType(Int32Type.Default, StringType.Default, ordered: false), new DictionaryType(Int32Type.Default, StringType.Default, ordered: false) },
        { "union", new UnionType([F("a", Int32Type.Default)], [0], UnionMode.Dense), new UnionType([F("a", Int32Type.Default)], [0], UnionMode.Dense) },
    };

    [Theory]
    [MemberData(nameof(DifferentShapes))]
    public void Structurally_different_types_differ(string label, IArrowType expected, IArrowType actual)
    {
        Assert.False(ArrowSchemaShape.SameType(expected, actual), label);
        Assert.False(ArrowSchemaShape.Same(S(F("c", expected)), S(F("c", actual))), label);
    }

    [Theory]
    [MemberData(nameof(SameShapes))]
    public void Structurally_equal_types_are_the_same(string label, IArrowType expected, IArrowType actual)
    {
        Assert.True(ArrowSchemaShape.SameType(expected, actual), label);
        Assert.True(ArrowSchemaShape.Same(S(F("c", expected)), S(F("c", actual))), label);
    }

    [Fact]
    public void Top_level_names_nullability_and_metadata_never_mismatch()
    {
        var expected = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);
        var actual = new Schema([new Field("ID", Int64Type.Default, nullable: true)],
            new Dictionary<string, string> { ["origin"] = "test" });

        Assert.True(ArrowSchemaShape.Same(expected, actual));
    }

    [Fact]
    public void Field_count_differences_mismatch()
    {
        var two = S(F("a", Int64Type.Default), F("b", StringType.Default));
        var one = S(F("a", Int64Type.Default));

        Assert.False(ArrowSchemaShape.Same(two, one));
        Assert.False(ArrowSchemaShape.Same(one, two));
    }

    [Fact]
    public void An_extension_type_differs_from_its_storage_type_and_from_another_extension()
    {
        var storage = new FixedSizeBinaryType(16);
        var guid = new GuidType();

        Assert.False(ArrowSchemaShape.SameType(guid, storage));
        Assert.False(ArrowSchemaShape.SameType(storage, guid));
        Assert.True(ArrowSchemaShape.SameType(guid, new GuidType()));
    }

    [Theory]
    [InlineData("int64", "int64")]
    [InlineData("utf8", "utf8")]
    [InlineData("list", "list<int32>")]
    [InlineData("large_list", "large_list<utf8>")]
    [InlineData("fixed_size_list", "fixed_size_list<int32>[3]")]
    [InlineData("struct", "struct<a:int32, b:utf8>")]
    [InlineData("map", "map<utf8, int32>")]
    [InlineData("decimal128", "decimal128(10, 2)")]
    [InlineData("timestamp", "timestamp[us]")]
    [InlineData("timestamp_tz", "timestamp[ns, tz=UTC]")]
    [InlineData("time32", "time32[ms]")]
    [InlineData("duration", "duration[s]")]
    [InlineData("fixed_size_binary", "fixed_size_binary[16]")]
    [InlineData("dictionary", "dictionary<int32, utf8>")]
    [InlineData("ordered_dictionary", "dictionary<int32, utf8, ordered>")]
    [InlineData("dense_union", "dense_union<a:int32=0>")]
    [InlineData("sparse_union", "sparse_union<a:int32=0>")]
    [InlineData("nested", "list<struct<a:list<int32>>>")]
    public void Describe_spells_out_the_structure(string key, string expected)
    {
        IArrowType type = key switch
        {
            "int64" => Int64Type.Default,
            "utf8" => StringType.Default,
            "list" => new ListType(Int32Type.Default),
            "large_list" => new LargeListType(StringType.Default),
            "fixed_size_list" => new FixedSizeListType(Int32Type.Default, 3),
            "struct" => new StructType([F("a", Int32Type.Default), F("b", StringType.Default)]),
            "map" => new MapType(StringType.Default, Int32Type.Default),
            "decimal128" => new Decimal128Type(10, 2),
            "timestamp" => new TimestampType(TimeUnit.Microsecond, (string?)null),
            "timestamp_tz" => new TimestampType(TimeUnit.Nanosecond, "UTC"),
            "time32" => new Time32Type(TimeUnit.Millisecond),
            "duration" => DurationType.FromTimeUnit(TimeUnit.Second),
            "fixed_size_binary" => new FixedSizeBinaryType(16),
            "dictionary" => new DictionaryType(Int32Type.Default, StringType.Default, ordered: false),
            "ordered_dictionary" => new DictionaryType(Int32Type.Default, StringType.Default, ordered: true),
            "dense_union" => new UnionType([F("a", Int32Type.Default)], [0], UnionMode.Dense),
            "sparse_union" => new UnionType([F("a", Int32Type.Default)], [0], UnionMode.Sparse),
            "nested" => new ListType(new StructType([F("a", new ListType(Int32Type.Default))])),
            _ => throw new ArgumentOutOfRangeException(nameof(key)),
        };

        Assert.Equal(expected, ArrowSchemaShape.Describe(type));
    }

    [Fact]
    public void Describe_schema_lists_name_and_structure_per_column()
    {
        var schema = S(F("id", Int64Type.Default), F("tags", new ListType(StringType.Default)));

        Assert.Equal("id:int64, tags:list<utf8>", ArrowSchemaShape.Describe(schema));
    }
}
