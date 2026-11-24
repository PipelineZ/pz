using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions.Batches;
using Pz.Connectors.Abstractions.Formats;
using Pz.Connectors.Abstractions.Memory;

namespace Pz.Connectors.Abstractions.Tests.Batches;

/// <summary><see cref="ArrowBatchBuilder"/> with nested columns: boxed .NET values (enumerables, dictionaries,
/// positional arrays) and Arrow cells (<see cref="ArrowBatchBuilder.AppendFrom"/>) both build list, fixed-size
/// list, struct and map columns at any depth, nulls kept at every level, into pooled memory that is all
/// returned when the batch is disposed.</summary>
public sealed class ArrowBatchBuilderNestedTests
{
    private static readonly StructType PersonType = new([
        new Field("name", StringType.Default, true),
        new Field("tags", new ListType(new Field("item", StringType.Default, true)), true),
    ]);

    private static readonly Schema NestedSchema = new([
        new Field("id", Int64Type.Default, false),
        new Field("person", PersonType, true),
        new Field("scores", new ListType(new Field("item", Int32Type.Default, true)), true),
        new Field("pair", new FixedSizeListType(new Field("item", DoubleType.Default, true), 2), true),
        new Field("attrs", new MapType(StringType.Default, Int64Type.Default), true),
        new Field("events", new ListType(new Field("item", new StructType([
            new Field("at", new TimestampType(TimeUnit.Microsecond, "+00:00"), true),
            new Field("amount", new Decimal128Type(38, 9), true),
        ]), true)), true),
    ], null);

    private static readonly object?[][] Rows =
    [
        [
            1L,
            new Dictionary<string, object?> { ["name"] = "ann", ["tags"] = new[] { "a", null } },
            new List<int?> { 1, null, 3 },
            new[] { 1.5, 2.5 },
            new Dictionary<string, long?> { ["k"] = 1, ["z"] = null },
            new object?[] { new object?[] { new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero), 12.5m }, null },
        ],
        [2L, null, null, null, null, null],
        [
            3L,
            new object?[] { null, System.Array.Empty<string>() },
            new List<int>(),
            new double?[] { null, 4 },
            new Dictionary<string, long>(),
            new List<object?>(),
        ],
    ];

    private const string ExpectedJson =
        "{\"id\":1,\"person\":{\"name\":\"ann\",\"tags\":[\"a\",null]},\"scores\":[1,null,3],\"pair\":[1.5,2.5]," +
        "\"attrs\":{\"k\":1,\"z\":null},\"events\":[{\"at\":\"2026-01-02T03:04:05.000000Z\",\"amount\":12.500000000},null]}\n" +
        "{\"id\":2,\"person\":null,\"scores\":null,\"pair\":null,\"attrs\":null,\"events\":null}\n" +
        "{\"id\":3,\"person\":{\"name\":null,\"tags\":[]},\"scores\":[],\"pair\":[null,4],\"attrs\":{},\"events\":[]}\n";

    private static async Task<string> JsonAsync(RecordBatch batch)
    {
        using var stream = new MemoryStream();
        await NdjsonCodec.WriteAsync(batch, stream, CancellationToken.None);
        return Encoding.UTF8.GetString(stream.ToArray());
    }

    [Fact]
    public async Task Boxed_nested_values_build_every_nested_type_with_nulls_at_every_level()
    {
        var allocator = new PooledNativeAllocator();
        var builder = new ArrowBatchBuilder(NestedSchema, allocator: allocator);
        foreach (var row in Rows)
        {
            builder.AppendRow(row);
        }

        using (var batch = builder.Flush()!)
        {
            Assert.Equal(ExpectedJson, await JsonAsync(batch));
            Assert.True(allocator.RentedBytes > 0);
        }

        Assert.Equal(0, allocator.RentedBytes);
    }

    [Fact]
    public async Task AppendFrom_copies_nested_cells_and_the_builder_is_reusable_across_batches()
    {
        var source = new ArrowBatchBuilder(NestedSchema);
        foreach (var row in Rows)
        {
            source.AppendRow(row);
        }

        using var original = source.Flush()!;
        var columns = Enumerable.Range(0, original.ColumnCount).Select(original.Column).ToArray();
        var copy = new ArrowBatchBuilder(NestedSchema);
        for (var round = 0; round < 2; round++)
        {
            foreach (var row in new[] { 2, 0, 1 })
            {
                copy.AppendFrom(columns, row);
            }

            using var batch = copy.Flush()!;
            var lines = ExpectedJson.Split('\n');
            Assert.Equal(lines[2] + "\n" + lines[0] + "\n" + lines[1] + "\n", await JsonAsync(batch));
        }
    }

    [Theory]
    [InlineData(1, "\"abc\"", "a string is not a list value")]
    [InlineData(2, "[1.0]", "fixed-size list of 2; a value has 1 items")]
    [InlineData(0, "42", "pass an IReadOnlyDictionary<string, object?> or an object?[]")]
    [InlineData(3, "[1]", "is a map: pass an IDictionary")]
    public void A_value_of_the_wrong_shape_names_the_column(int nestedColumn, string kind, string message)
    {
        object? value = kind switch
        {
            "\"abc\"" => "abc",
            "[1.0]" => new[] { 1.0 },
            "42" => 42,
            _ => new[] { 1 },
        };
        var row = new object?[] { 1L, null, null, null, null, null };
        row[nestedColumn + 1] = value;

        var ex = Assert.Throws<ArgumentException>(() => new ArrowBatchBuilder(NestedSchema).AppendRow(row));

        Assert.Contains($"column '{NestedSchema.FieldsList[nestedColumn + 1].Name}'", ex.Message, StringComparison.Ordinal);
        Assert.Contains(message, ex.Message, StringComparison.Ordinal);
    }
}
