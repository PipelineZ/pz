using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Sftp;

/// <summary>One declared contract column's accumulator: parses cells straight out of the CSV reader's
/// char buffer and lays the values down in Arrow's own memory layout, emitting a finished
/// <see cref="IArrowArray"/> per batch.
///
/// This exists because Arrow's array builders are the read side's remaining per-cell cost once the
/// string-per-cell boxing is gone. <c>Int64Array.Builder.Append(long)</c>
/// is not one array store: it is a capacity check and span re-derivation on the value buffer, plus a
/// second capacity check and a read-modify-write on the validity bitmap builder, behind two delegate
/// invocations from the column plan — measured at ~30-60 ns/cell, which on a 4-column 5M-row file is
/// 0.6-1.2 s of a 4.2 s read.
///
/// The writers below keep the same two buffers the builder does but write them as plain managed arrays
/// (grown by doubling, reused across batches) and copy them into pooled native Arrow buffers once per
/// batch — the per-cell path becomes a bounds check and a store. Output is byte-identical to the builder
/// path, validity buffer included: like Arrow's own builders, a column with no nulls emits
/// <see cref="ArrowBuffer.Empty"/> rather than an all-ones bitmap.
///
/// <para><b>Only where the parse is cheap.</b> int/bigint/double/varchar/boolean get the hand-written
/// path; decimal, date and timestamp stay on Arrow's builders (<see cref="BuilderWriter{TArray,TBuilder}"/>).
/// Their parses cost hundreds of nanoseconds and their layouts (128-bit two's-complement scaled decimals,
/// epoch-relative calendar arithmetic) are exactly the kind of conversion that is a correctness risk to
/// re-derive for a few percent — the same line <c>CsvWriteCodec</c> draws on the write side.</para>
///
/// Replicated from connectors/Pz.Connector.LocalFiles/CsvColumnWriter.cs per the no-cross-connector-reference rule; keep in lockstep.</summary>
internal abstract class CsvColumnWriter
{
    /// <summary>Parses and appends one cell, returning the value's estimated width in EIGHTHS of a byte.
    /// Eighths (rather than the <c>double</c> the boxed path accumulated) because a validity bit is 1/8
    /// of a byte and every other width is a whole number of bytes, so integer arithmetic reproduces the
    /// old estimate exactly while costing an add. <paramref name="line"/> is only ever read to build a
    /// parse-failure message.</summary>
    public abstract int Append(ReadOnlySpan<char> value, long line);

    /// <summary>Emits the accumulated values as one Arrow array and resets for the next batch.</summary>
    public abstract IArrowArray BuildAndReset();

    /// <summary>Builds the writer for one declared contract column. <paramref name="typeName"/> is the
    /// `columns:` contract's type name, not the Arrow type — the contract is the source of truth for how
    /// a cell is parsed; this path does no inference.</summary>
    public static CsvColumnWriter Create(
        Field field, string typeName, string path, MemoryAllocator allocator) => typeName switch
    {
        "int" => new Int32Writer(field.Name, typeName, path, allocator),
        "bigint" => new Int64Writer(field.Name, typeName, path, allocator),
        "double" => new DoubleWriter(field.Name, typeName, path, allocator),
        "varchar" => new Utf8Writer(allocator),
        "boolean" => new BooleanWriter(field.Name, typeName, path, allocator),
        "decimal" => new BuilderWriter<Decimal128Array, Decimal128Array.Builder>(
            new Decimal128Array.Builder((Decimal128Type)field.DataType), allocator, 16 * 8,
            (b, v, line) => b.Append(decimal.TryParse(v, NumberStyles.Number, CultureInfo.InvariantCulture, out var parsed)
                ? parsed
                : throw Invalid(path, line, field.Name, v, typeName))),
        "date" => new BuilderWriter<Date32Array, Date32Array.Builder>(
            new Date32Array.Builder(), allocator, 4 * 8,
            (b, v, line) => b.Append(DateOnly.TryParse(v, CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed)
                ? parsed
                : throw Invalid(path, line, field.Name, v, typeName))),
        "timestamp" => new BuilderWriter<TimestampArray, TimestampArray.Builder>(
            new TimestampArray.Builder((TimestampType)field.DataType), allocator, 8 * 8,
            (b, v, line) => b.Append(DateTimeOffset.TryParse(v, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed)
                ? parsed
                : throw Invalid(path, line, field.Name, v, typeName))),
        _ => throw new PzConnectorException(
            $"column '{field.Name}': unknown columns: contract type '{typeName}'", isTransient: false),
    };

    private static PzConnectorException Invalid(
        string path, long line, string column, ReadOnlySpan<char> value, string typeName) =>
        new($"csv file '{path}' line {line}: column '{column}' value '{value}' is not a valid {typeName}",
            isTransient: false);

    /// <summary>A growable bit vector in Arrow's little-endian bit order, used for both validity bitmaps
    /// and boolean values. Bits are only ever SET, so the backing array is cleared once per batch rather
    /// than per value — <see cref="Array.Resize{T}"/> zero-fills what it adds, so growth needs no clear
    /// of its own.</summary>
    private sealed class BitVector
    {
        private readonly ReusableArrowBuffer<byte> _arrow = new();
        private byte[] _bits = new byte[1024];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(int index)
        {
            var slot = index >> 3;
            if (slot >= _bits.Length)
            {
                System.Array.Resize(ref _bits, Math.Max(slot + 1, _bits.Length * 2));
            }

            _bits[slot] |= (byte)(1 << (index & 7));
        }

        /// <summary>Reserves room for <paramref name="index"/> without setting it — the null case, which
        /// leaves the bit clear but must still not fall off the end of a later <see cref="Build"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reserve(int index)
        {
            var slot = index >> 3;
            if (slot >= _bits.Length)
            {
                System.Array.Resize(ref _bits, Math.Max(slot + 1, _bits.Length * 2));
            }
        }

        public ArrowBuffer Build(int length, MemoryAllocator allocator) =>
            _arrow.Build(_bits.AsSpan(0, (length + 7) / 8), allocator);

        public void Reset(int length) => System.Array.Clear(_bits, 0, Math.Min(_bits.Length, (length + 7) / 8));
    }

    /// <summary>Copies one finished column buffer into pooled native Arrow memory, through an Arrow
    /// buffer builder that is kept and <c>Clear()</c>ed rather than constructed per batch.
    ///
    /// Constructing one per batch is the obvious way to write this and quietly allocates a
    /// batch-sized managed array every time — on the 5M-row probe, 184 MiB of LOH churn across the read,
    /// where the Arrow array builders this class replaced allocated almost nothing in steady state
    /// because they reuse their own scratch across batches. Keeping the builder keeps
    /// that property.</summary>
    private sealed class ReusableArrowBuffer<T>
        where T : struct
    {
        private readonly ArrowBuffer.Builder<T> _builder = new();

        public ArrowBuffer Build(ReadOnlySpan<T> values, MemoryAllocator allocator)
        {
            if (values.IsEmpty)
            {
                return ArrowBuffer.Empty;
            }

            _builder.Append(values);
            var buffer = _builder.Build(allocator);
            _builder.Clear();
            return buffer;
        }
    }

    /// <summary>Shared bookkeeping for the hand-written writers: the value count, the validity bitmap,
    /// and the null count — which is what decides whether a validity buffer is emitted at all.</summary>
    private abstract class BufferedWriter(MemoryAllocator allocator) : CsvColumnWriter
    {
        private readonly BitVector _validity = new();

        protected MemoryAllocator Allocator { get; } = allocator;

        protected int Length { get; private set; }

        protected int NullCount { get; private set; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void MarkValid() => _validity.Set(Length++);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected void MarkNull()
        {
            _validity.Reserve(Length++);
            NullCount++;
        }

        /// <summary>Arrow's own builders emit <see cref="ArrowBuffer.Empty"/> for a column with no nulls
        /// (verified against Arrow 23), and the C data interface exports that as a null validity pointer —
        /// so matching it keeps the hand-written path byte-identical to the builder path downstream.</summary>
        protected ArrowBuffer BuildValidity() =>
            NullCount == 0 ? ArrowBuffer.Empty : _validity.Build(Length, Allocator);

        protected void ResetCounts()
        {
            _validity.Reset(Length);
            Length = 0;
            NullCount = 0;
        }
    }

    /// <summary>Fixed-width writer: one managed value array plus the shared validity bitmap. A null still
    /// occupies its slot (Arrow requires <c>length</c> values regardless of validity), written as
    /// <c>default</c>.</summary>
    private abstract class PrimitiveWriter<T>(MemoryAllocator allocator, int widthEighths)
        : BufferedWriter(allocator)
        where T : struct
    {
        private readonly ReusableArrowBuffer<T> _arrow = new();
        private T[] _values = new T[4096];

        public sealed override int Append(ReadOnlySpan<char> value, long line)
        {
            if (Length == _values.Length)
            {
                System.Array.Resize(ref _values, _values.Length * 2);
            }

            if (value.IsEmpty)
            {
                _values[Length] = default;
                MarkNull();
                return 0;
            }

            _values[Length] = Parse(value, line);
            MarkValid();
            return widthEighths;
        }

        public sealed override IArrowArray BuildAndReset()
        {
            var length = Length;
            var array = Wrap(
                _arrow.Build(_values.AsSpan(0, length), Allocator), BuildValidity(), length, NullCount);
            ResetCounts();
            return array;
        }

        protected abstract T Parse(ReadOnlySpan<char> value, long line);

        protected abstract IArrowArray Wrap(ArrowBuffer values, ArrowBuffer validity, int length, int nullCount);
    }

    private sealed class Int32Writer(string name, string typeName, string path, MemoryAllocator allocator)
        : PrimitiveWriter<int>(allocator, 4 * 8)
    {
        protected override int Parse(ReadOnlySpan<char> value, long line) =>
            int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
                ? parsed
                : throw Invalid(path, line, name, value, typeName);

        protected override IArrowArray Wrap(ArrowBuffer values, ArrowBuffer validity, int length, int nullCount) =>
            new Int32Array(values, validity, length, nullCount, 0);
    }

    private sealed class Int64Writer(string name, string typeName, string path, MemoryAllocator allocator)
        : PrimitiveWriter<long>(allocator, 8 * 8)
    {
        protected override long Parse(ReadOnlySpan<char> value, long line) =>
            long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
                ? parsed
                : throw Invalid(path, line, name, value, typeName);

        protected override IArrowArray Wrap(ArrowBuffer values, ArrowBuffer validity, int length, int nullCount) =>
            new Int64Array(values, validity, length, nullCount, 0);
    }

    private sealed class DoubleWriter(string name, string typeName, string path, MemoryAllocator allocator)
        : PrimitiveWriter<double>(allocator, 8 * 8)
    {
        protected override double Parse(ReadOnlySpan<char> value, long line) =>
            double.TryParse(value, NumberStyles.Float | NumberStyles.AllowThousands,
                CultureInfo.InvariantCulture, out var parsed)
                ? parsed
                : throw Invalid(path, line, name, value, typeName);

        protected override IArrowArray Wrap(ArrowBuffer values, ArrowBuffer validity, int length, int nullCount) =>
            new DoubleArray(values, validity, length, nullCount, 0);
    }

    /// <summary>Boolean's values are themselves a bitmap, so it gets its own writer rather than riding
    /// <see cref="PrimitiveWriter{T}"/>'s value array.</summary>
    private sealed class BooleanWriter(string name, string typeName, string path, MemoryAllocator allocator)
        : BufferedWriter(allocator)
    {
        private readonly BitVector _values = new();

        public override int Append(ReadOnlySpan<char> value, long line)
        {
            if (value.IsEmpty)
            {
                _values.Reserve(Length);
                MarkNull();
                return 0;
            }

            if (!bool.TryParse(value, out var parsed))
            {
                throw Invalid(path, line, name, value, typeName);
            }

            if (parsed)
            {
                _values.Set(Length);
            }
            else
            {
                _values.Reserve(Length);
            }

            MarkValid();
            return 1;
        }

        public override IArrowArray BuildAndReset()
        {
            var length = Length;
            var array = new BooleanArray(
                _values.Build(length, Allocator), BuildValidity(), length, NullCount, 0);
            _values.Reset(length);
            ResetCounts();
            return array;
        }
    }

    /// <summary>varchar: UTF-8 bytes are encoded straight from the reader's char span into the growing
    /// value buffer at its current end, so a cell is transcoded exactly once and never lands on the heap
    /// — neither as a string nor as a bounce through a scratch array on the way into Arrow's builder.</summary>
    private sealed class Utf8Writer(MemoryAllocator allocator) : BufferedWriter(allocator)
    {
        private readonly ReusableArrowBuffer<int> _offsetsArrow = new();
        private readonly ReusableArrowBuffer<byte> _bytesArrow = new();
        private int[] _offsets = new int[4097];
        private byte[] _bytes = new byte[64 * 1024];
        private int _byteLength;

        public override int Append(ReadOnlySpan<char> value, long line)
        {
            if (Length + 2 > _offsets.Length)
            {
                System.Array.Resize(ref _offsets, _offsets.Length * 2);
            }

            if (value.IsEmpty)
            {
                _offsets[Length + 1] = _byteLength;
                MarkNull();
                return 0;
            }

            var maximum = Encoding.UTF8.GetMaxByteCount(value.Length);
            if (_byteLength + maximum > _bytes.Length)
            {
                System.Array.Resize(ref _bytes, Math.Max(_byteLength + maximum, _bytes.Length * 2));
            }

            var written = Encoding.UTF8.GetBytes(value, _bytes.AsSpan(_byteLength));
            _byteLength += written;
            _offsets[Length + 1] = _byteLength;
            MarkValid();
            return (4 + written) * 8;
        }

        public override IArrowArray BuildAndReset()
        {
            var length = Length;
            var array = new StringArray(
                length,
                _offsetsArrow.Build(_offsets.AsSpan(0, length + 1), Allocator),
                _bytesArrow.Build(_bytes.AsSpan(0, _byteLength), Allocator),
                BuildValidity(),
                NullCount,
                0);
            ResetCounts();
            _byteLength = 0;
            return array;
        }
    }

    private delegate void AppendTyped<in TBuilder>(TBuilder builder, ReadOnlySpan<char> value, long line);

    /// <summary>The decimal/date/timestamp path: Arrow's own builder. Their parses dominate their
    /// appends, and their memory layouts are conversions worth borrowing rather than re-deriving (see
    /// the class doc comment).</summary>
    private sealed class BuilderWriter<TArray, TBuilder>(
        TBuilder builder, MemoryAllocator allocator, int widthEighths, AppendTyped<TBuilder> appendValue)
        : CsvColumnWriter
        where TArray : IArrowArray
        where TBuilder : class, IArrowArrayBuilder<TArray, TBuilder>
    {
        public override int Append(ReadOnlySpan<char> value, long line)
        {
            if (value.IsEmpty)
            {
                builder.AppendNull();
                return 0;
            }

            appendValue(builder, value, line);
            return widthEighths;
        }

        public override IArrowArray BuildAndReset()
        {
            var array = builder.Build(allocator);
            builder.Clear();
            return array;
        }
    }
}
