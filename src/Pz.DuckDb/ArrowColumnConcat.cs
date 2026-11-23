using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Pz.DuckDb;

/// <summary>A row range of one column: <see cref="Length"/> rows starting at logical row
/// <see cref="Offset"/> of <see cref="Array"/> (on top of the array's own <see cref="ArrayData.Offset"/>).</summary>
internal readonly record struct ColumnSlice(IArrowArray Array, int Offset, int Length);

/// <summary>Concatenates row ranges of v0-matrix columns (int32, int64, double, decimal128, utf8, bool,
/// date32, timestamp) by writing every output buffer directly into memory rented from a
/// <see cref="MemoryAllocator"/>: fixed-width values and utf8 data are block copies, utf8 offsets are
/// rebased, and validity and boolean bitmaps are bit-shift copies when a range does not start on a byte
/// boundary. Nothing is staged on the managed heap, so a large column costs no large-object allocation.
///
/// Every output buffer is freshly rented and owned by the returned array (disposing it returns the memory
/// to the allocator); none aliases an input. A column type outside the matrix throws
/// <see cref="NotSupportedException"/> — there is no fallback.</summary>
internal static class ArrowColumnConcat
{
    /// <summary>Throws <see cref="UnsupportedArrowTypeException"/> naming every column pz's Arrow path does
    /// not carry (see <see cref="IsCarried(Field)"/>).</summary>
    public static void EnsureSupported(Schema schema)
    {
        var unsupported = schema.FieldsList
            .Where(f => !IsCarried(f))
            .Select(f => (f.Name, f.DataType.Name))
            .ToList();
        if (unsupported.Count > 0)
        {
            throw new UnsupportedArrowTypeException(unsupported, "egress concatenation");
        }
    }

    private static bool IsSupported(ArrowTypeId type) =>
        type is ArrowTypeId.Boolean or ArrowTypeId.String || FixedWidth(type) > 0;

    /// <summary>The Arrow extension a field carries, from its metadata; DuckDB marks VARIANT this way.</summary>
    private const string ExtensionNameKey = "ARROW:extension:name";

    /// <summary>A field pz's Arrow path carries: a v0-matrix leaf, or a list, fixed-size list, struct or map
    /// whose children are carried, at any depth. An extension type (VARIANT's binary encoding) and a union
    /// are not: a sink could not interpret either.</summary>
    internal static bool IsCarried(Field field) =>
        field.Metadata?.ContainsKey(ExtensionNameKey) != true && IsCarried(field.DataType);

    internal static bool IsCarried(IArrowType type) => type switch
    {
        ListType list => IsCarried(list.ValueField),
        FixedSizeListType list => IsCarried(list.ValueField),
        MapType map => IsCarried(map.KeyField) && IsCarried(map.ValueField),
        StructType @struct => @struct.Fields.Count > 0 && @struct.Fields.All(IsCarried),
        // DuckDB's TIMESTAMP is microseconds; TIMESTAMP_NS/_MS/_S export as other units pz does not map.
        TimestampType timestamp => timestamp.Unit == TimeUnit.Microsecond,
        _ => IsSupported(type.TypeId),
    };

    /// <summary>True for a list, fixed-size list, struct or map: a column with child arrays.</summary>
    internal static bool IsNested(IArrowType type) =>
        type is ListType or FixedSizeListType or MapType or StructType;

    /// <summary>Bytes per value of a fixed-width matrix type; 0 for every other type.</summary>
    private static int FixedWidth(ArrowTypeId type) => type switch
    {
        ArrowTypeId.Int32 or ArrowTypeId.Date32 => 4,
        ArrowTypeId.Int64 or ArrowTypeId.Double or ArrowTypeId.Timestamp => 8,
        ArrowTypeId.Decimal128 => 16,
        _ => 0,
    };

    /// <summary>Concatenates <paramref name="slices"/> (all of type <paramref name="type"/>, together
    /// <paramref name="length"/> rows) into one new array whose buffers come from
    /// <paramref name="allocator"/>. On failure every buffer rented so far is returned before the exception
    /// propagates.</summary>
    public static IArrowArray Concatenate(
        IArrowType type, ReadOnlySpan<ColumnSlice> slices, int length, MemoryAllocator allocator)
    {
        if (IsNested(type))
        {
            return ConcatenateNested(type, slices, length, allocator);
        }

        ArrowBuffer validity = ArrowBuffer.Empty;
        ArrowBuffer values = ArrowBuffer.Empty;
        ArrowBuffer data = ArrowBuffer.Empty;
        try
        {
            validity = ConcatenateValidity(slices, length, allocator, out var nullCount);
            ArrowBuffer[] buffers;
            if (type.TypeId == ArrowTypeId.Boolean)
            {
                values = ConcatenateBits(slices, bufferIndex: 1, length, allocator);
                buffers = [validity, values];
            }
            else if (type.TypeId == ArrowTypeId.String)
            {
                (values, data) = ConcatenateUtf8(slices, length, allocator);
                buffers = [validity, values, data];
            }
            else if (FixedWidth(type.TypeId) is var width and > 0)
            {
                values = ConcatenateFixedWidth(slices, width, length, allocator);
                buffers = [validity, values];
            }
            else
            {
                throw new NotSupportedException(
                    $"egress concatenation does not support Arrow type '{type}'");
            }

            return ArrowArrayFactory.BuildArray(new ArrayData(type, length, nullCount, 0, buffers));
        }
        catch
        {
            validity.Dispose();
            values.Dispose();
            data.Dispose();
            throw;
        }
    }

    /// <summary>A nested column: its own validity (and, for a list or map, rebased offsets) written like a
    /// leaf's, then each child concatenated recursively over the child rows the slices cover. Every buffer at
    /// every depth is rented from <paramref name="allocator"/>; none aliases an input. (Apache.Arrow's own
    /// concatenator returns its input for a single slice and reuses child buffers, which would leave an
    /// output pointing into a chunk the caller is about to release.)</summary>
    private static IArrowArray ConcatenateNested(
        IArrowType type, ReadOnlySpan<ColumnSlice> slices, int length, MemoryAllocator allocator)
    {
        ArrowBuffer validity = ArrowBuffer.Empty;
        ArrowBuffer offsets = ArrowBuffer.Empty;
        var children = new List<IArrowArray>();
        try
        {
            validity = ConcatenateValidity(slices, length, allocator, out var nullCount);
            ArrowBuffer[] buffers;
            switch (type)
            {
                case StructType @struct:
                    for (var k = 0; k < @struct.Fields.Count; k++)
                    {
                        // A struct's children are indexed like the struct itself, its offset included.
                        var childSlices = new ColumnSlice[slices.Length];
                        for (var i = 0; i < slices.Length; i++)
                        {
                            var parent = slices[i].Array.Data;
                            childSlices[i] = new ColumnSlice(
                                ArrowArrayFactory.BuildArray(parent.Children[k]), parent.Offset + slices[i].Offset, slices[i].Length);
                        }

                        children.Add(Concatenate(@struct.Fields[k].DataType, childSlices, length, allocator));
                    }

                    buffers = [validity];
                    break;
                case FixedSizeListType fixedList:
                {
                    var size = fixedList.ListSize;
                    var childSlices = new ColumnSlice[slices.Length];
                    for (var i = 0; i < slices.Length; i++)
                    {
                        var parent = slices[i].Array.Data;
                        childSlices[i] = new ColumnSlice(ArrowArrayFactory.BuildArray(parent.Children[0]),
                            checked((parent.Offset + slices[i].Offset) * size), checked(slices[i].Length * size));
                    }

                    children.Add(Concatenate(fixedList.ValueDataType, childSlices, checked(length * size), allocator));
                    buffers = [validity];
                    break;
                }

                case ListType or MapType:
                {
                    // A map is a list of key/value structs: the same offsets, one struct child.
                    offsets = Rent(checked((length + 1) * sizeof(int)), allocator, out var offsetsSpan);
                    var outOffsets = MemoryMarshal.Cast<byte, int>(offsetsSpan);
                    var childSlices = new ColumnSlice[slices.Length];
                    var row = 0;
                    var at = 0;
                    IArrowType? childType = null;
                    for (var i = 0; i < slices.Length; i++)
                    {
                        var parent = slices[i].Array.Data;
                        var source = MemoryMarshal.Cast<byte, int>(parent.Buffers[1].Span)
                            .Slice(parent.Offset + slices[i].Offset, slices[i].Length + 1);
                        var start = source[0];
                        var count = source[slices[i].Length] - start;
                        for (var r = 0; r < slices[i].Length; r++)
                        {
                            outOffsets[row + r] = source[r] - start + at;
                        }

                        var child = ArrowArrayFactory.BuildArray(parent.Children[0]);
                        childType ??= child.Data.DataType;
                        childSlices[i] = new ColumnSlice(child, start, count);
                        row += slices[i].Length;
                        at = checked(at + count);
                    }

                    outOffsets[length] = at;
                    childType ??= type is ListType list ? list.ValueDataType : ((MapType)type).Fields[0].DataType;
                    children.Add(Concatenate(childType, childSlices, at, allocator));
                    buffers = [validity, offsets];
                    break;
                }

                default:
                    throw new NotSupportedException($"egress concatenation does not support Arrow type '{type}'");
            }

            return ArrowArrayFactory.BuildArray(new ArrayData(type, length, nullCount, 0, buffers,
                children.Select(c => c.Data)));
        }
        catch
        {
            validity.Dispose();
            offsets.Dispose();
            foreach (var child in children)
            {
                child.Dispose();
            }

            throw;
        }
    }

    /// <summary>The output validity bitmap, or <see cref="ArrowBuffer.Empty"/> (null count 0) when no row in
    /// any slice is null.</summary>
    private static ArrowBuffer ConcatenateValidity(
        ReadOnlySpan<ColumnSlice> slices, int length, MemoryAllocator allocator, out int nullCount)
    {
        nullCount = 0;
        var anyNulls = false;
        foreach (var slice in slices)
        {
            if (slice.Length > 0 && slice.Array.NullCount > 0)
            {
                anyNulls = true;
                break;
            }
        }

        if (!anyNulls)
        {
            return ArrowBuffer.Empty;
        }

        var bitmap = Rent(BitmapBytes(length), allocator, out var span);
        try
        {
            span.Clear(); // the bit copies OR into the target
            var at = 0;
            foreach (var slice in slices)
            {
                var source = slice.Array.Data;
                if (source.Buffers[0].IsEmpty || slice.Array.NullCount == 0)
                {
                    SetBits(span, at, slice.Length);
                }
                else
                {
                    CopyBits(source.Buffers[0].Span, source.Offset + slice.Offset, span, at, slice.Length);
                }

                at += slice.Length;
            }

            // Bits past `length` were cleared and never set, so whole bytes count exactly.
            nullCount = length - PopCount(span[..BitmapBytes(length)]);
        }
        catch
        {
            bitmap.Dispose();
            throw;
        }

        if (nullCount == 0)
        {
            // The slices' arrays have nulls, but none falls inside the concatenated ranges.
            bitmap.Dispose();
            return ArrowBuffer.Empty;
        }

        return bitmap;
    }

    private static ArrowBuffer ConcatenateBits(
        ReadOnlySpan<ColumnSlice> slices, int bufferIndex, int length, MemoryAllocator allocator)
    {
        var bitmap = Rent(BitmapBytes(length), allocator, out var span);
        try
        {
            span.Clear(); // the bit copies OR into the target
            var at = 0;
            foreach (var slice in slices)
            {
                var source = slice.Array.Data;
                CopyBits(source.Buffers[bufferIndex].Span, source.Offset + slice.Offset, span, at, slice.Length);
                at += slice.Length;
            }
        }
        catch
        {
            bitmap.Dispose();
            throw;
        }

        return bitmap;
    }

    private static ArrowBuffer ConcatenateFixedWidth(
        ReadOnlySpan<ColumnSlice> slices, int width, int length, MemoryAllocator allocator)
    {
        var buffer = Rent(checked(length * width), allocator, out var span);
        try
        {
            var at = 0;
            foreach (var slice in slices)
            {
                var source = slice.Array.Data;
                var bytes = slice.Length * width;
                source.Buffers[1].Span.Slice((source.Offset + slice.Offset) * width, bytes).CopyTo(span.Slice(at, bytes));
                at += bytes;
            }
        }
        catch
        {
            buffer.Dispose();
            throw;
        }

        return buffer;
    }

    private static (ArrowBuffer Offsets, ArrowBuffer Data) ConcatenateUtf8(
        ReadOnlySpan<ColumnSlice> slices, int length, MemoryAllocator allocator)
    {
        var dataBytes = 0;
        foreach (var slice in slices)
        {
            var offsets = SourceOffsets(slice);
            dataBytes = checked(dataBytes + (offsets[slice.Length] - offsets[0]));
        }

        var offsetsBuffer = Rent(checked((length + 1) * sizeof(int)), allocator, out var offsetsSpan);
        var dataBuffer = ArrowBuffer.Empty;
        try
        {
            var dataSpan = Span<byte>.Empty;
            if (dataBytes > 0)
            {
                dataBuffer = Rent(dataBytes, allocator, out dataSpan);
            }

            var outOffsets = MemoryMarshal.Cast<byte, int>(offsetsSpan);
            var row = 0;
            var at = 0;
            foreach (var slice in slices)
            {
                var offsets = SourceOffsets(slice);
                var start = offsets[0];
                var bytes = offsets[slice.Length] - start;
                var rebase = at - start;
                for (var i = 0; i < slice.Length; i++)
                {
                    outOffsets[row + i] = offsets[i] + rebase;
                }

                slice.Array.Data.Buffers[2].Span.Slice(start, bytes).CopyTo(dataSpan.Slice(at, bytes));
                row += slice.Length;
                at += bytes;
            }

            outOffsets[length] = at;
        }
        catch
        {
            offsetsBuffer.Dispose();
            dataBuffer.Dispose();
            throw;
        }

        return (offsetsBuffer, dataBuffer);
    }

    /// <summary>The slice's <c>Length + 1</c> utf8 offsets (absolute positions in its data buffer).</summary>
    private static ReadOnlySpan<int> SourceOffsets(ColumnSlice slice)
    {
        var source = slice.Array.Data;
        return MemoryMarshal.Cast<byte, int>(source.Buffers[1].Span).Slice(source.Offset + slice.Offset, slice.Length + 1);
    }

    private static int BitmapBytes(int bits) => (bits + 7) >> 3;

    /// <summary>Rents a buffer of <paramref name="length"/> bytes (padded to a 64-byte multiple,
    /// as Arrow's own builders pad) and returns it wrapped as an <see cref="ArrowBuffer"/> that owns the
    /// memory, plus a writable span over the first <paramref name="length"/> bytes.</summary>
    private static ArrowBuffer Rent(int length, MemoryAllocator allocator, out Span<byte> span)
    {
        var padded = checked((length + 63) & ~63);
        var owner = allocator.Allocate(padded);
        try
        {
            var memory = owner.Memory.Span;
            memory[length..].Clear(); // padding is never written, and an allocator need not zero it
            span = memory[..length];
            return WrapOwned(owner);
        }
        catch
        {
            owner.Dispose();
            throw;
        }
    }

    /// <summary>The ownership-transferring <see cref="ArrowBuffer"/> constructor Arrow's own builders use
    /// for allocator memory: disposing the buffer disposes <paramref name="owner"/>. It is not public, and
    /// the public <see cref="ArrowBuffer(ReadOnlyMemory{byte})"/> never releases its memory. An unsafe
    /// accessor (unlike reflection) is trim- and Native-AOT-safe; if a future Arrow drops the constructor,
    /// every concatenation throws <see cref="MissingMethodException"/>.</summary>
    [UnsafeAccessor(UnsafeAccessorKind.Constructor)]
    private static extern ArrowBuffer WrapOwned(IMemoryOwner<byte> owner);

    /// <summary>ORs <paramref name="count"/> bits of <paramref name="source"/> starting at bit
    /// <paramref name="sourceBit"/> into <paramref name="target"/> at bit <paramref name="targetBit"/>;
    /// the target bits must be zero on entry.</summary>
    internal static void CopyBits(ReadOnlySpan<byte> source, int sourceBit, Span<byte> target, int targetBit, int count)
    {
        while (count > 0 && (targetBit & 7) != 0)
        {
            CopyBit(source, sourceBit++, target, targetBit++);
            count--;
        }

        var bytes = count >> 3;
        if (bytes > 0)
        {
            var from = source[(sourceBit >> 3)..];
            var to = target.Slice(targetBit >> 3, bytes);
            var shift = sourceBit & 7;
            if (shift == 0)
            {
                from[..bytes].CopyTo(to);
            }
            else
            {
                // Output byte i takes the high bits of source byte i and the low bits of byte i + 1.
                var i = 0;
                for (; i + 8 <= bytes && i + 9 <= from.Length; i += 8)
                {
                    var low = BinaryPrimitives.ReadUInt64LittleEndian(from[i..]);
                    var high = (ulong)from[i + 8];
                    BinaryPrimitives.WriteUInt64LittleEndian(to[i..], (low >> shift) | (high << (64 - shift)));
                }

                for (; i < bytes; i++)
                {
                    to[i] = (byte)((from[i] >> shift) | (from[i + 1] << (8 - shift)));
                }
            }

            sourceBit += bytes << 3;
            targetBit += bytes << 3;
            count -= bytes << 3;
        }

        while (count > 0)
        {
            CopyBit(source, sourceBit++, target, targetBit++);
            count--;
        }
    }

    private static void CopyBit(ReadOnlySpan<byte> source, int sourceBit, Span<byte> target, int targetBit)
    {
        if ((source[sourceBit >> 3] & (1 << (sourceBit & 7))) != 0)
        {
            target[targetBit >> 3] |= (byte)(1 << (targetBit & 7));
        }
    }

    /// <summary>Sets <paramref name="count"/> bits of <paramref name="target"/> starting at bit
    /// <paramref name="targetBit"/>.</summary>
    private static void SetBits(Span<byte> target, int targetBit, int count)
    {
        while (count > 0 && (targetBit & 7) != 0)
        {
            target[targetBit >> 3] |= (byte)(1 << (targetBit & 7));
            targetBit++;
            count--;
        }

        var bytes = count >> 3;
        target.Slice(targetBit >> 3, bytes).Fill(0xFF);
        targetBit += bytes << 3;
        count -= bytes << 3;

        while (count > 0)
        {
            target[targetBit >> 3] |= (byte)(1 << (targetBit & 7));
            targetBit++;
            count--;
        }
    }

    private static int PopCount(ReadOnlySpan<byte> bytes)
    {
        var total = 0;
        var words = MemoryMarshal.Cast<byte, ulong>(bytes);
        foreach (var word in words)
        {
            total += BitOperations.PopCount(word);
        }

        foreach (var b in bytes[(words.Length * sizeof(ulong))..])
        {
            total += BitOperations.PopCount(b);
        }

        return total;
    }
}
