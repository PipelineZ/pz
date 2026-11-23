using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions.Formats;

namespace Pz.Connectors.Abstractions.Tests.Formats;

/// <summary>Nested cells as JSON: a struct is an object, a list an array, a map an object keyed by the map
/// keys' text, at any depth. Arrays sliced by Apache.Arrow keep their own child indexing (a struct's children
/// are sliced with it, a list's values are not), so a slice writes exactly its own rows.</summary>
public class NdjsonCodecNestedWriteTests
{
    private static async Task<string> WriteAsync(RecordBatch batch)
    {
        using var stream = new MemoryStream();
        await NdjsonCodec.WriteAsync(batch, stream, CancellationToken.None);
        return Encoding.UTF8.GetString(stream.ToArray());
    }

    [Fact]
    public async Task Sliced_struct_and_list_columns_write_only_their_own_rows()
    {
        var ints = new Int32Array.Builder().AppendRange([10, 11, 12, 13]).Build();
        var names = new StringArray.Builder().AppendRange(["a", "b", "c", "d"]).Build();
        var structType = new StructType([new Field("n", Int32Type.Default, true), new Field("s", StringType.Default, true)]);
        var @struct = new StructArray(structType, 4, [ints, names], ArrowBuffer.Empty);

        var listBuilder = new ListArray.Builder(Int64Type.Default);
        var values = (Int64Array.Builder)listBuilder.ValueBuilder;
        for (var row = 0; row < 4; row++)
        {
            listBuilder.Append();
            values.Append(row);
            values.Append(row * 10);
        }

        var list = listBuilder.Build();
        var schema = new Schema([new Field("rec", structType, true), new Field("xs", list.Data.DataType, true)], null);
        using var batch = new RecordBatch(schema, [@struct.Slice(2, 2), list.Slice(2, 2)], 2);

        Assert.Equal(
            "{\"rec\":{\"n\":12,\"s\":\"c\"},\"xs\":[2,20]}\n{\"rec\":{\"n\":13,\"s\":\"d\"},\"xs\":[3,30]}\n",
            await WriteAsync(batch));
    }

    [Fact]
    public async Task Map_keys_of_any_type_become_property_names_and_nulls_stay_null()
    {
        var mapBuilder = new MapArray.Builder(new MapType(Int64Type.Default, StringType.Default));
        var keys = (Int64Array.Builder)mapBuilder.KeyBuilder;
        var items = (StringArray.Builder)mapBuilder.ValueBuilder;
        mapBuilder.Append();
        keys.Append(1);
        items.Append("one");
        keys.Append(2);
        items.AppendNull();
        mapBuilder.AppendNull();
        var map = mapBuilder.Build();
        using var batch = new RecordBatch(new Schema([new Field("m", map.Data.DataType, true)], null), [map], 2);

        Assert.Equal("{\"m\":{\"1\":\"one\",\"2\":null}}\n{\"m\":null}\n", await WriteAsync(batch));
    }
}
