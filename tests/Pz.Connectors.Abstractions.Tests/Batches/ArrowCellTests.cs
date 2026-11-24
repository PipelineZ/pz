using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connectors.Abstractions.Tests.Batches;

/// <summary>A sink reads a nested cell as JSON or as boxed values: a struct in field order, a list, a map in entry
/// order with non-string keys kept, and nulls at every level.</summary>
public sealed class ArrowCellTests
{
    private static RecordBatch Batch()
    {
        var schema = new Schema(
        [
            new Field("rec", new StructType(
            [
                new Field("name", StringType.Default, true),
                new Field("tags", new ListType(new Field("element", StringType.Default, true)), true),
            ]), true),
            new Field("m", new MapType(new Field("key", Int64Type.Default, false), new Field("value", DoubleType.Default, true)), true),
        ], null);
        var builder = new ArrowBatchBuilder(schema, int.MaxValue, maxRowsPerBatch: int.MaxValue);
        builder.AppendRow([new object?[] { "ann", new object?[] { "a", null } }, new Dictionary<long, double?> { [2] = 1.5, [1] = null }]);
        builder.AppendRow([null, null]);
        return builder.Flush()!;
    }

    [Fact]
    public void Nested_cells_read_as_boxed_values()
    {
        using var batch = Batch();

        var rec = Assert.IsType<Dictionary<string, object?>>(ArrowCell.GetValue(batch.Column(0), 0));
        Assert.Equal(["name", "tags"], rec.Keys);
        Assert.Equal("ann", rec["name"]);
        Assert.Equal(["a", null], Assert.IsType<List<object?>>(rec["tags"]));
        var map = Assert.IsType<List<KeyValuePair<object, object?>>>(ArrowCell.GetValue(batch.Column(1), 0));
        Assert.Equal([new KeyValuePair<object, object?>(2L, 1.5), new KeyValuePair<object, object?>(1L, null)], map);
        Assert.Null(ArrowCell.GetValue(batch.Column(0), 1));
    }

    [Fact]
    public void Nested_cells_read_as_JSON()
    {
        using var batch = Batch();

        Assert.Equal("{\"name\":\"ann\",\"tags\":[\"a\",null]}", ArrowCell.ToJson(batch.Column(0), 0));
        Assert.Equal("{\"2\":1.5,\"1\":null}", ArrowCell.ToJson(batch.Column(1), 0));
        Assert.Equal("null", ArrowCell.ToJson(batch.Column(1), 1));
    }

    [Fact]
    public void A_map_whose_keys_share_a_text_is_refused_rather_than_written_with_a_duplicate_property()
    {
        // Arrow does not make map keys unique, so a connector-built map can hold one key twice.
        var type = new MapType(new Field("key", StringType.Default, false), new Field("value", Int64Type.Default, true));
        var keys = new StringArray.Builder().Append("k").Append("k").Build();
        var values = new Int64Array.Builder().Append(1).Append(2).Build();
        var entries = new StructArray((StructType)type.Fields[0].DataType, 2, [keys, values], ArrowBuffer.Empty);
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(2).Build();
        var map = new MapArray(type, 1, offsets, entries, ArrowBuffer.Empty);

        var ex = Assert.Throws<PzConnectorException>(() => ArrowCell.ToJson(map, 0));

        Assert.Contains("two entries whose keys have the same text", ex.Message, StringComparison.Ordinal);
    }
}
