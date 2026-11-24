using System.Collections;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Pz.Connectors.Abstractions.Batches;

/// <summary>One column of <see cref="ArrowBatchBuilder"/>: how to append a boxed value (null included), how
/// many bytes a non-null value costs, how to copy one cell (null included) from a same-typed Arrow array, and
/// how to build the accumulated values into an array and start over.</summary>
internal sealed record ColumnBuilder(
    Action<object?> Append,
    Func<object?, double> WidthOf,
    Func<IArrowArray, int, double> CopyFrom,
    Func<IArrowArray> Build);

/// <summary>Builders for nested columns: list, fixed-size list, struct and map of any carried type, at any
/// depth. Each keeps its own validity bitmap (and offsets, for a list or map) and recurses into child
/// builders; every buffer is built through the batch builder's allocator. Boxed values: a list or fixed-size
/// list takes any non-string <see cref="IEnumerable"/>; a struct takes an
/// <see cref="IReadOnlyDictionary{TKey,TValue}"/> of field name to value (a missing field is null) or an
/// <c>object?[]</c> in field order; a map takes an <see cref="IDictionary"/> (a .NET dictionary of any key
/// and value types). Copying from Arrow follows Apache.Arrow's accessors: a struct's children are indexed
/// like the struct, a list's values through its offsets, a fixed-size list's from <c>(offset + row) * size</c>.</summary>
internal static class NestedColumns
{
    public static ColumnBuilder? TryCreate(
        Field field, MemoryAllocator allocator, Func<Field, ColumnBuilder> create) => field.DataType switch
    {
        MapType map => Map(field, map, allocator, create),
        ListType list => List(field, list, allocator, create),
        FixedSizeListType list => FixedSizeList(field, list, allocator, create),
        StructType @struct => Struct(field, @struct, allocator, create),
        _ => null,
    };

    private static ColumnBuilder List(Field field, ListType type, MemoryAllocator allocator, Func<Field, ColumnBuilder> create)
    {
        var child = create(type.ValueField);
        var shape = new ListShape(type, allocator);

        void Append(object? value)
        {
            if (value is null)
            {
                shape.AppendNull();
                return;
            }

            foreach (var item in Items(field, value))
            {
                child.Append(item);
                shape.Children++;
            }

            shape.AppendValid();
        }

        double CopyFrom(IArrowArray source, int row)
        {
            if (source.IsNull(row))
            {
                shape.AppendNull();
                return 0d;
            }

            var list = (ListArray)source;
            var bytes = 4d;
            for (var i = list.ValueOffsets[row]; i < list.ValueOffsets[row + 1]; i++)
            {
                bytes += child.CopyFrom(list.Values, i) + 0.125d;
                shape.Children++;
            }

            shape.AppendValid();
            return bytes;
        }

        return new ColumnBuilder(Append,
            value => 4d + Items(field, value!).Sum(item => item is null ? 0.125d : child.WidthOf(item) + 0.125d),
            CopyFrom,
            () => shape.Build(child.Build()));
    }

    private static ColumnBuilder Map(Field field, MapType type, MemoryAllocator allocator, Func<Field, ColumnBuilder> create)
    {
        var keys = create(type.KeyField);
        var values = create(type.ValueField);
        var entriesType = (StructType)type.Fields[0].DataType;
        var shape = new ListShape(type, allocator);

        void Append(object? value)
        {
            if (value is null)
            {
                shape.AppendNull();
                return;
            }

            foreach (DictionaryEntry entry in Entries(field, value))
            {
                keys.Append(entry.Key);
                values.Append(entry.Value);
                shape.Children++;
            }

            shape.AppendValid();
        }

        double CopyFrom(IArrowArray source, int row)
        {
            if (source.IsNull(row))
            {
                shape.AppendNull();
                return 0d;
            }

            var map = (MapArray)source;
            var entries = map.KeyValues;
            var bytes = 4d;
            for (var i = map.ValueOffsets[row]; i < map.ValueOffsets[row + 1]; i++)
            {
                bytes += keys.CopyFrom(entries.Fields[0], i) + values.CopyFrom(entries.Fields[1], i) + 0.25d;
                shape.Children++;
            }

            shape.AppendValid();
            return bytes;
        }

        IArrowArray Build()
        {
            var keyArray = keys.Build();
            var valueArray = values.Build();
            var entries = new StructArray(entriesType, keyArray.Length, [keyArray, valueArray], ArrowBuffer.Empty, nullCount: 0);
            return shape.Build(entries);
        }

        return new ColumnBuilder(Append,
            value =>
            {
                var bytes = 4d;
                foreach (DictionaryEntry entry in Entries(field, value!))
                {
                    bytes += keys.WidthOf(entry.Key) + (entry.Value is null ? 0d : values.WidthOf(entry.Value)) + 0.25d;
                }

                return bytes;
            },
            CopyFrom,
            Build);
    }

    private static ColumnBuilder FixedSizeList(
        Field field, FixedSizeListType type, MemoryAllocator allocator, Func<Field, ColumnBuilder> create)
    {
        var child = create(type.ValueField);
        var size = type.ListSize;
        var validity = new Validity(allocator);

        void Append(object? value)
        {
            if (value is null)
            {
                for (var i = 0; i < size; i++)
                {
                    child.Append(null);
                }

                validity.Append(false);
                return;
            }

            var count = 0;
            foreach (var item in Items(field, value))
            {
                if (++count > size)
                {
                    break;
                }

                child.Append(item);
            }

            if (count != size)
            {
                throw new ArgumentException(
                    $"column '{field.Name}' is a fixed-size list of {size}; a value has {(count > size ? "more" : count.ToString(System.Globalization.CultureInfo.InvariantCulture))} items");
            }

            validity.Append(true);
        }

        double CopyFrom(IArrowArray source, int row)
        {
            var list = (FixedSizeListArray)source;
            var start = (list.Offset + row) * size;
            var bytes = 0d;
            for (var i = start; i < start + size; i++)
            {
                bytes += child.CopyFrom(list.Values, i) + 0.125d;
            }

            validity.Append(!source.IsNull(row));
            return bytes;
        }

        IArrowArray Build()
        {
            var values = child.Build();
            var (bitmap, nullCount, length) = validity.Build();
            return new FixedSizeListArray(new ArrayData(type, length, nullCount, 0, [bitmap], [values.Data]));
        }

        return new ColumnBuilder(Append,
            value => Items(field, value!).Sum(item => item is null ? 0.125d : child.WidthOf(item) + 0.125d),
            CopyFrom,
            Build);
    }

    private static ColumnBuilder Struct(Field field, StructType type, MemoryAllocator allocator, Func<Field, ColumnBuilder> create)
    {
        var children = type.Fields.Select(create).ToArray();
        var validity = new Validity(allocator);

        object? FieldValue(object value, int k) => value switch
        {
            IReadOnlyDictionary<string, object?> named => named.TryGetValue(type.Fields[k].Name, out var v) ? v : null,
            object?[] positional when positional.Length == children.Length => positional[k],
            object?[] positional => throw new ArgumentException(
                $"column '{field.Name}' is a struct of {children.Length} fields; a positional value has {positional.Length}"),
            _ => throw new ArgumentException(
                $"column '{field.Name}' is a struct: pass an IReadOnlyDictionary<string, object?> or an object?[] in field order, not {value.GetType().Name}"),
        };

        void Append(object? value)
        {
            if (value is null)
            {
                foreach (var child in children)
                {
                    child.Append(null);
                }

                validity.Append(false);
                return;
            }

            for (var k = 0; k < children.Length; k++)
            {
                children[k].Append(FieldValue(value, k));
            }

            validity.Append(true);
        }

        double CopyFrom(IArrowArray source, int row)
        {
            var @struct = (StructArray)source;
            var bytes = 0d;
            for (var k = 0; k < children.Length; k++)
            {
                bytes += children[k].CopyFrom(@struct.Fields[k], row) + 0.125d;
            }

            validity.Append(!source.IsNull(row));
            return bytes;
        }

        IArrowArray Build()
        {
            var arrays = children.Select(c => c.Build()).ToArray();
            var (bitmap, nullCount, length) = validity.Build();
            return new StructArray(type, length, arrays, bitmap, nullCount);
        }

        return new ColumnBuilder(Append,
            value =>
            {
                var bytes = 0d;
                for (var k = 0; k < children.Length; k++)
                {
                    var item = FieldValue(value!, k);
                    bytes += (item is null ? 0d : children[k].WidthOf(item)) + 0.125d;
                }

                return bytes;
            },
            CopyFrom,
            Build);
    }

    private static IEnumerable<object?> Items(Field field, object value) => value switch
    {
        string => throw new ArgumentException($"column '{field.Name}' is a list; a string is not a list value"),
        IEnumerable items => items.Cast<object?>(),
        _ => throw new ArgumentException($"column '{field.Name}' is a list: pass an IEnumerable, not {value.GetType().Name}"),
    };

    private static IDictionary Entries(Field field, object value) => value as IDictionary
        ?? throw new ArgumentException($"column '{field.Name}' is a map: pass an IDictionary, not {value.GetType().Name}");

    /// <summary>A validity bitmap plus the count of values appended, built through the allocator.</summary>
    private sealed class Validity(MemoryAllocator allocator)
    {
        private ArrowBuffer.BitmapBuilder _bits = new();

        public int Length => _bits.Length;

        public void Append(bool valid) => _bits.Append(valid);

        public (ArrowBuffer Bitmap, int NullCount, int Length) Build()
        {
            var nullCount = _bits.UnsetBitCount;
            var length = _bits.Length;
            var bitmap = nullCount == 0 ? ArrowBuffer.Empty : _bits.Build(allocator);
            _bits = new ArrowBuffer.BitmapBuilder();
            return (bitmap, nullCount, length);
        }
    }

    /// <summary>A list's or map's validity and value offsets: <see cref="Children"/> counts the child values
    /// appended so far, and each row records where its values end.</summary>
    private sealed class ListShape(NestedType type, MemoryAllocator allocator)
    {
        private readonly Validity _validity = new(allocator);
        private ArrowBuffer.Builder<int> _offsets = new ArrowBuffer.Builder<int>().Append(0);

        public int Children { get; set; }

        public void AppendNull()
        {
            _validity.Append(false);
            _offsets.Append(Children);
        }

        public void AppendValid()
        {
            _validity.Append(true);
            _offsets.Append(Children);
        }

        public IArrowArray Build(IArrowArray values)
        {
            var (bitmap, nullCount, length) = _validity.Build();
            var offsets = _offsets.Build(allocator);
            _offsets = new ArrowBuffer.Builder<int>().Append(0);
            Children = 0;
            return ArrowArrayFactory.BuildArray(
                new ArrayData(type, length, nullCount, 0, [bitmap, offsets], [values.Data]));
        }
    }
}
