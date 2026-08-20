using System.Text;
using Apache.Arrow;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions.Memory;

namespace Pz.Connectors.Abstractions.Batches;

/// <summary>Pivots values into columnar Arrow <see cref="RecordBatch"/>es, via either of two entry
/// points sharing the same underlying column builders/byte accounting: row-major
/// <see cref="AppendRow"/> (a boxed <c>object?[]</c> row) or, for a source that is already columnar
/// Arrow (DuckDB's own egress batches), the typed non-boxing <see cref="AppendFrom"/>. v0 supports a fixed
/// type matrix (int32/int64/double/decimal128(38,9)/utf8/bool/date32/timestamp-µs-UTC,
/// all nullable); any other schema field type fails fast in the constructor. Byte accounting is an
/// estimate (fixed-width columns count exactly, utf8 counts UTF-8 bytes + a 4-byte offset, every column
/// adds one validity bit per value) used only to decide when to emit a batch.
///
/// Final batch buffers (validity/offsets/value) are allocated through an
/// <see cref="Apache.Arrow.Memory.MemoryAllocator"/> — <see cref="PooledNativeAllocator.Shared"/> by
/// default — so they are pooled, off-heap native memory instead of managed arrays; builder scratch
/// stays managed (Arrow's own model, and the only seam its builders expose) but is reused across
/// batches via each column builder's <c>Clear()</c> instead of being discarded and recreated,
/// avoiding steady-state LOH churn.</summary>
public sealed class ArrowBatchBuilder
{
    private readonly Schema _schema;
    private readonly int _targetBatchBytes;
    private readonly int _maxRowsPerBatch;
    private readonly Action<object?>[] _appenders;
    private readonly Action<IArrowArray, int>[] _copiers;
    private readonly Func<IArrowArray>[] _builders;
    private double _bytesEstimate;

    public ArrowBatchBuilder(
        Schema schema,
        int targetBatchBytes = 32 * 1024 * 1024,
        MemoryAllocator? allocator = null,
        int? maxRowsPerBatch = null)
    {
        ArgumentNullException.ThrowIfNull(schema);
        _schema = schema;
        _targetBatchBytes = targetBatchBytes;
        _maxRowsPerBatch = maxRowsPerBatch ?? BatchOptions.Default.MaxRowsPerBatch;
        allocator ??= PooledNativeAllocator.Shared;

        var fields = schema.FieldsList;
        _appenders = new Action<object?>[fields.Count];
        _copiers = new Action<IArrowArray, int>[fields.Count];
        _builders = new Func<IArrowArray>[fields.Count];

        for (var i = 0; i < fields.Count; i++)
        {
            var field = fields[i];
            var (append, build, widthOf, copyFrom) = field.DataType.TypeId switch
            {
                ArrowTypeId.Int32 => MakeColumn<Int32Array, Int32Array.Builder>(
                    () => new Int32Array.Builder(),
                    static (b, v) => b.Append((int)v),
                    static _ => 4d,
                    static (b, src, row) => { b.Append(((Int32Array)src).GetValue(row)!.Value); return 4d; },
                    allocator),
                ArrowTypeId.Int64 => MakeColumn<Int64Array, Int64Array.Builder>(
                    () => new Int64Array.Builder(),
                    static (b, v) => b.Append((long)v),
                    static _ => 8d,
                    static (b, src, row) => { b.Append(((Int64Array)src).GetValue(row)!.Value); return 8d; },
                    allocator),
                ArrowTypeId.Double => MakeColumn<DoubleArray, DoubleArray.Builder>(
                    () => new DoubleArray.Builder(),
                    static (b, v) => b.Append((double)v),
                    static _ => 8d,
                    static (b, src, row) => { b.Append(((DoubleArray)src).GetValue(row)!.Value); return 8d; },
                    allocator),
                ArrowTypeId.Boolean => MakeColumn<BooleanArray, BooleanArray.Builder>(
                    () => new BooleanArray.Builder(),
                    static (b, v) => b.Append((bool)v),
                    static _ => 0.125d,
                    static (b, src, row) => { b.Append(((BooleanArray)src).GetValue(row)!.Value); return 0.125d; },
                    allocator),
                ArrowTypeId.Date32 => MakeColumn<Date32Array, Date32Array.Builder>(
                    () => new Date32Array.Builder(),
                    static (b, v) => b.Append((DateOnly)v),
                    static _ => 4d,
                    static (b, src, row) => { b.Append(((Date32Array)src).GetDateOnly(row)!.Value); return 4d; },
                    allocator),
                ArrowTypeId.Timestamp => MakeColumn<TimestampArray, TimestampArray.Builder>(
                    () => new TimestampArray.Builder((TimestampType)field.DataType),
                    static (b, v) => b.Append((DateTimeOffset)v),
                    static _ => 8d,
                    static (b, src, row) => { b.Append(((TimestampArray)src).GetTimestamp(row)!.Value); return 8d; },
                    allocator),
                ArrowTypeId.Decimal128 => MakeColumn<Decimal128Array, Decimal128Array.Builder>(
                    () => new Decimal128Array.Builder((Decimal128Type)field.DataType),
                    static (b, v) => b.Append((decimal)v),
                    static _ => 16d,
                    static (b, src, row) => { b.Append(((Decimal128Array)src).GetValue(row)!.Value); return 16d; },
                    allocator),
                ArrowTypeId.String => MakeColumn<StringArray, StringArray.Builder>(
                    () => new StringArray.Builder(),
                    static (b, v) => b.Append((string)v),
                    static v => 4d + Encoding.UTF8.GetByteCount((string)v!),
                    static (b, src, row) =>
                    {
                        var s = ((StringArray)src).GetString(row)!;
                        b.Append(s);
                        return 4d + Encoding.UTF8.GetByteCount(s);
                    },
                    allocator),
                _ => throw new NotSupportedException(
                    $"ArrowBatchBuilder v0 does not support column '{field.Name}' with Arrow type '{field.DataType}'"),
            };

            _builders[i] = build;
            _appenders[i] = value =>
            {
                append(value);
                _bytesEstimate += (value is null ? 0d : widthOf(value)) + 0.125d;
            };
            _copiers[i] = (source, row) =>
            {
                _bytesEstimate += copyFrom(source, row) + 0.125d;
            };
        }
    }

    public int PendingRows { get; private set; }

    public long PendingBytes => (long)Math.Ceiling(_bytesEstimate);

    /// <summary>Appends one row. <paramref name="values"/> must have exactly one entry per schema
    /// field, in field order; a null entry means SQL NULL for that column regardless of type.</summary>
    public void AppendRow(object?[] values)
    {
        if (values.Length != _appenders.Length)
        {
            throw new ArgumentException(
                $"expected {_appenders.Length} values (one per schema field), got {values.Length}", nameof(values));
        }

        for (var i = 0; i < _appenders.Length; i++)
        {
            _appenders[i](values[i]);
        }

        PendingRows++;
    }

    /// <summary>Bulk analogue of <see cref="AppendRow"/>: appends one row by reading each column
    /// directly out of an already-built Arrow batch's arrays (<paramref name="sourceColumns"/>, one per
    /// schema field, same order) at <paramref name="sourceRow"/>, using each column's typed
    /// (non-boxing) <c>Append</c> overload instead of round-tripping the value through
    /// <c>object</c>/<c>object?[]</c> the way <see cref="AppendRow"/> does. Exists for re-batching an
    /// already-columnar source (e.g. DuckDB's own Arrow export) toward this builder's
    /// <see cref="PendingBytes"/> target without paying a struct-boxing allocation per cell — the whole
    /// point of a coalescing step whose input is already Arrow, not row-major values. Null handling
    /// mirrors <see cref="AppendRow"/>: a null source cell (<see cref="IArrowArray.IsNull"/>) becomes
    /// SQL NULL for that column regardless of type; the type-specific copy delegate is never invoked for
    /// one.</summary>
    public void AppendFrom(IReadOnlyList<IArrowArray> sourceColumns, int sourceRow)
    {
        if (sourceColumns.Count != _copiers.Length)
        {
            throw new ArgumentException(
                $"expected {_copiers.Length} columns (one per schema field), got {sourceColumns.Count}",
                nameof(sourceColumns));
        }

        for (var i = 0; i < _copiers.Length; i++)
        {
            _copiers[i](sourceColumns[i], sourceRow);
        }

        PendingRows++;
    }

    /// <summary>Emits and resets the pending batch once <see cref="PendingBytes"/> reaches the target
    /// (or row count reaches the configured max rows per batch), else returns false.</summary>
    public bool TryTakeBatch(out RecordBatch? batch)
    {
        if (PendingRows > 0 && (PendingBytes >= _targetBatchBytes || PendingRows >= _maxRowsPerBatch))
        {
            batch = BuildAndReset();
            return true;
        }

        batch = null;
        return false;
    }

    /// <summary>Emits whatever is pending regardless of size, or null when nothing is pending.</summary>
    public RecordBatch? Flush() => PendingRows > 0 ? BuildAndReset() : null;

    private RecordBatch BuildAndReset()
    {
        var arrays = new IArrowArray[_builders.Length];
        for (var i = 0; i < _builders.Length; i++)
        {
            arrays[i] = _builders[i]();
        }

        var batch = new RecordBatch(_schema, arrays, PendingRows);
        PendingRows = 0;
        _bytesEstimate = 0;
        return batch;
    }

    /// <summary>Builds the append/build-and-reset/copy-from closures for one column, deferring
    /// null-handling and byte estimation to the caller so every branch above only supplies the
    /// type-specific bits: how to construct a fresh builder, how to append one non-null boxed value, how
    /// many bytes a boxed value costs, and how to append one non-null value read directly out of a
    /// same-typed source <see cref="IArrowArray"/> at a row index (the non-boxing bulk-copy path — see
    /// <see cref="AppendFrom"/>; <paramref name="copyFrom"/> is only ever invoked for a non-null source
    /// cell, mirroring <paramref name="appendValue"/>, and returns the byte width it consumed). The SAME
    /// builder instance is reused across batches (<c>Clear()</c> after <c>Build(allocator)</c> instead of
    /// discarding it for a fresh one via <paramref name="factory"/>) — Arrow 23's <c>Clear()</c> resets
    /// length to zero without releasing the builder's already-grown managed scratch buffer, so
    /// steady-state batches stop reallocating/regrowing that scratch from empty every time,
    /// which is what keeps that scratch out of the LOH.</summary>
    private static (
        Action<object?> Append,
        Func<IArrowArray> Build,
        Func<object?, double> WidthOf,
        Func<IArrowArray, int, double> CopyFrom) MakeColumn<TArray, TBuilder>(
        Func<TBuilder> factory,
        Action<TBuilder, object> appendValue,
        Func<object?, double> widthOf,
        Func<TBuilder, IArrowArray, int, double> copyFrom,
        MemoryAllocator allocator)
        where TArray : IArrowArray
        where TBuilder : class, IArrowArrayBuilder<TArray, TBuilder>
    {
        var builder = factory();

        void Append(object? value)
        {
            if (value is null)
            {
                builder.AppendNull();
            }
            else
            {
                appendValue(builder, value);
            }
        }

        double CopyFrom(IArrowArray source, int row)
        {
            if (source.IsNull(row))
            {
                builder.AppendNull();
                return 0d;
            }

            return copyFrom(builder, source, row);
        }

        IArrowArray BuildAndReset()
        {
            var array = builder.Build(allocator);
            builder.Clear();
            return array;
        }

        return (Append, BuildAndReset, widthOf, CopyFrom);
    }
}
