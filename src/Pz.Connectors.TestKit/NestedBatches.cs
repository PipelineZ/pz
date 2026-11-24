using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Connectors.TestKit;

/// <summary>The nested-columns vector's batch: <c>id</c>, <c>rec struct&lt;n int32, tags list&lt;utf8&gt;&gt;</c>,
/// <c>xs list&lt;int64&gt;</c>, <c>pair fixed_size_list&lt;int32&gt;[2]</c>, <c>m map&lt;utf8, int32&gt;</c> and
/// <c>items list&lt;struct&lt;k utf8, v float64&gt;&gt;</c>, four rows with a null at every level: row 0 is fully
/// populated, row 1 has null nested columns, row 2 empty lists and null leaves, row 3 null struct fields and
/// elements.</summary>
internal static class NestedBatches
{
    public const int Rows = 4;

    public static RecordBatch Build()
    {
        var ids = new Int64Array.Builder().AppendRange([0, 1, 2, 3]).Build();

        // rec: {n: 7, tags: [a, null]}, null, {n: null, tags: []}, {n: 9, tags: null}
        var tags = new ListArray.Builder(StringType.Default);
        var tagValues = (StringArray.Builder)tags.ValueBuilder;
        tags.Append();
        tagValues.Append("a");
        tagValues.AppendNull();
        tags.AppendNull(); // under the null struct
        tags.Append();
        tags.AppendNull();
        var recType = new StructType([
            new Field("n", Int32Type.Default, true),
            new Field("tags", new ListType(new Field("item", StringType.Default, true)), true),
        ]);
        var rec = new StructArray(recType, Rows,
            [new Int32Array.Builder().Append(7).AppendNull().AppendNull().Append(9).Build(), tags.Build()],
            new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Append(true).Append(true).Build(), nullCount: 1);

        // xs: [1, 2], null, [], [null]
        var xs = new ListArray.Builder(Int64Type.Default);
        var xValues = (Int64Array.Builder)xs.ValueBuilder;
        xs.Append();
        xValues.Append(1);
        xValues.Append(2);
        xs.AppendNull();
        xs.Append();
        xs.Append();
        xValues.AppendNull();
        var xsArray = xs.Build();

        // pair: [1, 2], null, [null, 4], [5, 6]
        var pairType = new FixedSizeListType(new Field("item", Int32Type.Default, true), 2);
        var pairValues = new Int32Array.Builder().Append(1).Append(2).Append(0).Append(0).AppendNull().Append(4).Append(5).Append(6).Build();
        var pair = new FixedSizeListArray(new ArrayData(pairType, Rows, 1, 0,
            [new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Append(true).Append(true).Build()], [pairValues.Data]));

        // m: {k1: 1, k2: null}, null, {}, {k3: 3}
        var map = new MapArray.Builder(new MapType(StringType.Default, Int32Type.Default));
        var keys = (StringArray.Builder)map.KeyBuilder;
        var values = (Int32Array.Builder)map.ValueBuilder;
        map.Append();
        keys.Append("k1");
        values.Append(1);
        keys.Append("k2");
        values.AppendNull();
        map.AppendNull();
        map.Append();
        map.Append();
        keys.Append("k3");
        values.Append(3);
        var mapArray = map.Build();

        // items: [{k: x, v: 1.5}], null, [], [null, {k: null, v: 2.5}]
        var itemType = new StructType([new Field("k", StringType.Default, true), new Field("v", DoubleType.Default, true)]);
        var itemStructs = new StructArray(itemType, 3,
            [new StringArray.Builder().Append("x").AppendNull().AppendNull().Build(),
             new DoubleArray.Builder().Append(1.5).AppendNull().Append(2.5).Build()],
            new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Append(true).Build(), nullCount: 1);
        var itemsType = new ListType(new Field("item", itemType, true));
        var items = new ListArray(itemsType, Rows,
            new ArrowBuffer.Builder<int>().AppendRange([0, 1, 1, 1, 3]).Build(), itemStructs,
            new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Append(true).Append(true).Build(), nullCount: 1);

        var schema = new Schema([
            new Field("id", Int64Type.Default, false),
            new Field("rec", recType, true),
            new Field("xs", xsArray.Data.DataType, true),
            new Field("pair", pairType, true),
            new Field("m", mapArray.Data.DataType, true),
            new Field("items", itemsType, true),
        ], null);
        return new RecordBatch(schema, [ids, rec, xsArray, pair, mapArray, items], Rows);
    }
}
