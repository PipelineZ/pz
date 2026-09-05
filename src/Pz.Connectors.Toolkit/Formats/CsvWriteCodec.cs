using System.Buffers;
using System.Runtime.CompilerServices;
using System.Globalization;
using System.Text;
using Apache.Arrow;

namespace Pz.Connectors.Toolkit.Formats;

/// <summary>The toolkit's CSV write surface: Arrow <see cref="RecordBatch"/> → RFC-4180 CSV, shared by
/// every file connector's universal tier (the go-forward home, mirroring
/// <see cref="NdjsonWriteCodec"/>).
///
/// Output carries no UTF-8 BOM, deliberately: DuckDB's native <c>COPY ... (FORMAT csv)</c> never writes
/// one, the planner — not the author — picks the tier, and the two tiers' outputs must be
/// interchangeable byte-for-byte. Header row from the Arrow schema, invariant formatting per the v0 type
/// matrix, delimiter/quote/CR/LF (comma by default) fields quoted with doubled internal quotes, LF line
/// endings.
///
/// The obvious shape — format each cell to a <c>string</c> (re-allocated again by quoting), append it to
/// a per-row <c>StringBuilder</c>, transcode UTF-16 → UTF-8 through a <c>StreamWriter</c>, one
/// <c>await</c> on <c>WriteLineAsync</c> per row — measures ~7.7× DuckDB's native <c>COPY</c> on the
/// same data, and the cost is structural: several allocations per cell, one await per row, and one Arrow
/// accessor call per cell (<c>IsNull</c>/<c>GetValue</c>/<c>GetBytes</c>), each of which re-derives a
/// <see cref="Span{T}"/> from <c>ArrowBuffer.Memory</c> — for a pooled, natively-allocated buffer
/// that walks a <see cref="System.Buffers.MemoryManager{T}"/> every time, ~25 ns/cell, more than
/// formatting a <c>long</c> costs.
///
/// So this writer: formats values straight into a pooled UTF-8 buffer via
/// <c>TryFormat(Span&lt;byte&gt;, …)</c>; copies string cells from the Arrow value buffer's UTF-8 bytes
/// without ever decoding them; awaits once per buffer flush rather than once per row; and pins each
/// column's buffers once per batch so a cell read is a pointer dereference. Decimal, date and timestamp
/// cells deliberately stay on Arrow's own accessors — their formatting dominates their access, and
/// re-deriving Arrow's calendar semantics here would be a correctness risk for no measurable gain.
///
/// A row whose worst-case encoding cannot fit the buffer is written through a slower per-cell path
/// (<see cref="WriteOversizedRowAsync"/>), so buffer memory stays bounded no matter how wide a single
/// value is.</summary>
public sealed class CsvWriteCodec : IAsyncDisposable
{
    /// <summary>Bytes reserved for one fixed-width cell. The widest v0 fixed-width rendering is a
    /// decimal (up to 31 characters) or a round-trip <see cref="DateTimeOffset"/> ("O", 33), both ASCII;
    /// 64 leaves room without a per-value length calculation.</summary>
    private const int FixedCellReserve = 64;

    /// <summary>Buffer size, and therefore the largest row the fast path handles. Big enough that a flush
    /// costs one write per ~2000 typical rows, small enough to stay pooled and out of the LOH's way per
    /// concurrent sink session.</summary>
    private const int BufferBytes = 256 * 1024;

    private readonly Stream _stream;
    private readonly bool _leaveOpen;
    private readonly string _sinkLabel;
    private readonly byte _delimiter;

    /// <summary>The four characters RFC 4180 requires a field to be quoted for: the configured
    /// delimiter (comma by default), the quote character, and CR/LF. All are ASCII, so scanning the raw
    /// UTF-8 bytes can never collide with a multi-byte sequence's continuation bytes (those are all ≥
    /// 0x80).</summary>
    private readonly SearchValues<byte> _quoteTriggers;
    private byte[]? _buffer;
    private int _length;
    private Column[] _columns = [];
    private int _columnCount;

    /// <summary>The constant worst-case row size for a schema with no string column, or 0 when at least
    /// one column is a string and the bound must be computed per row.</summary>
    private int _fixedRowBytes;

    /// <summary>Opens a writer over <paramref name="destination"/> and buffers the header row for
    /// <paramref name="schema"/>. Nothing reaches <paramref name="destination"/> until the first flush.
    /// <paramref name="sinkLabel"/> names the connector in the "unsupported array type" message.
    /// <paramref name="delimiter"/> must be ASCII, since the fast paths scan raw UTF-8 bytes for it
    /// alongside the quote and CR/LF triggers.</summary>
    public CsvWriteCodec(Stream destination, Schema schema, string sinkLabel, bool leaveOpen = false, char delimiter = ',')
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(schema);
        if (!char.IsAscii(delimiter) || delimiter is '"' or '\n' or '\r')
        {
            throw new ArgumentOutOfRangeException(
                nameof(delimiter), delimiter, "csv delimiter must be ASCII and not a quote, newline or carriage return");
        }

        _stream = destination;
        _leaveOpen = leaveOpen;
        _sinkLabel = sinkLabel;
        _delimiter = (byte)delimiter;
        _quoteTriggers = SearchValues.Create([(byte)delimiter, (byte)'"', (byte)'\n', (byte)'\r']);

        var header = new StringBuilder();
        for (var i = 0; i < schema.FieldsList.Count; i++)
        {
            if (i > 0)
            {
                header.Append(delimiter);
            }

            header.Append(QuoteText(schema.FieldsList[i].Name, delimiter));
        }

        header.Append('\n');

        var headerBytes = Encoding.UTF8.GetBytes(header.ToString());
        _buffer = ArrayPool<byte>.Shared.Rent(Math.Max(BufferBytes, headerBytes.Length));
        headerBytes.CopyTo(_buffer, 0);
        _length = headerBytes.Length;
    }

    /// <summary>Appends every row of <paramref name="batch"/>. Nothing from the batch's buffers is
    /// referenced after this returns (the engine recycles its pooled buffers as soon as it disposes
    /// the batch): the pins taken on entry are released in the
    /// finally, and every value has been copied into this writer's buffer or written to the destination
    /// by then.</summary>
    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ObjectDisposedException.ThrowIf(_buffer is null, this);
        if (batch.Length == 0)
        {
            return;
        }

        PinColumns(batch);
        try
        {
            var row = 0;
            while (row < batch.Length)
            {
                ct.ThrowIfCancellationRequested();

                row = WriteRowsThatFit(row, batch.Length);
                if (row >= batch.Length)
                {
                    break;
                }

                await FlushBufferAsync(ct).ConfigureAwait(false);
                if (WorstCaseRowBytes(row) > _buffer!.Length)
                {
                    await WriteOversizedRowAsync(row, ct).ConfigureAwait(false);
                    row++;
                }
            }
        }
        finally
        {
            ReleaseColumns();
        }
    }

    /// <summary>Pushes everything buffered so far to the destination stream and flushes it.</summary>
    public async ValueTask FlushAsync(CancellationToken ct)
    {
        await FlushBufferAsync(ct).ConfigureAwait(false);
        await _stream.FlushAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Flushes and (unless constructed with <c>leaveOpen</c>) disposes the destination stream.
    /// Idempotent.</summary>
    public async ValueTask DisposeAsync()
    {
        var buffer = _buffer;
        if (buffer is null)
        {
            return;
        }

        try
        {
            await FlushAsync(CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            _buffer = null;
            ArrayPool<byte>.Shared.Return(buffer);

            if (!_leaveOpen)
            {
                await _stream.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>Resolves each column to its kind and pins the Arrow buffers the pointer paths read
    /// (validity, values, and a string column's offsets). One pin per buffer per batch replaces one
    /// <c>Memory.Span</c> walk per cell.</summary>
    private unsafe void PinColumns(RecordBatch batch)
    {
        if (_columns.Length < batch.ColumnCount)
        {
            _columns = new Column[batch.ColumnCount];
        }

        _columnCount = batch.ColumnCount;
        var strings = false;
        try
        {
            for (var col = 0; col < batch.ColumnCount; col++)
            {
                _columns[col] = Column.Pin(batch.Column(col), _sinkLabel);
                strings |= _columns[col].Kind == CellKind.String;
            }
        }
        catch
        {
            ReleaseColumns();
            throw;
        }

        _fixedRowBytes = strings ? 0 : (batch.ColumnCount * (FixedCellReserve + 1)) + 1;
    }

    private unsafe void ReleaseColumns()
    {
        for (var col = 0; col < _columnCount; col++)
        {
            _columns[col].Release();
        }

        _columnCount = 0;
    }

    /// <summary>Writes rows from <paramref name="row"/> while each one provably fits the buffer's
    /// remaining space, and returns the first row it did not write. Entirely synchronous: the caller
    /// awaits a flush only when this stops short. The buffer, its fill mark and the column table are
    /// hoisted into locals for the whole run of rows — at a few nanoseconds per cell, reloading them as
    /// fields is a measurable share of what a cell costs.</summary>
    private unsafe int WriteRowsThatFit(int row, int end)
    {
        var buffer = _buffer!;
        var columns = _columns;
        var count = _columnCount;
        var length = _length;
        var delimiter = _delimiter;
        var quoteTriggers = _quoteTriggers;

        // With no string column in the schema every row has the same bound, so the per-row sizing pass
        // collapses to one comparison.
        var fixedBound = _fixedRowBytes;
        while (row < end)
        {
            var needed = fixedBound > 0 ? fixedBound : WorstCaseRowBytes(columns, count, row);
            if (length + needed > buffer.Length)
            {
                break;
            }

            for (var col = 0; col < count; col++)
            {
                if (col > 0)
                {
                    buffer[length++] = delimiter;
                }

                length += WriteCell(ref columns[col], row, buffer.AsSpan(length), quoteTriggers);
            }

            buffer[length++] = (byte)'\n';
            row++;
        }

        _length = length;
        return row;
    }

    /// <summary>An upper bound on the UTF-8 bytes row <paramref name="row"/> can occupy: every
    /// fixed-width cell's reserve, every string cell's actual byte length doubled (worst case: the value
    /// is entirely quote characters, each doubled) plus its two enclosing quotes, one separator per
    /// column and the line feed. A string cell's length is two reads from the pinned offsets buffer, not
    /// a decode.</summary>
    private unsafe int WorstCaseRowBytes(Column[] columns, int count, int row)
    {
        var total = count + 1;
        for (var col = 0; col < count; col++)
        {
            ref var column = ref columns[col];
            total += column.Kind == CellKind.String && column.IsValid(row)
                ? (2 * column.Utf8Length(row)) + 2
                : FixedCellReserve;
        }

        return total;
    }

    private unsafe int WorstCaseRowBytes(int row) => WorstCaseRowBytes(_columns, _columnCount, row);

    /// <summary>The slow path for a row whose worst case exceeds the whole buffer — in practice a single
    /// very wide string value. Fixed-width cells still go through the buffer (with a flush before each);
    /// a wide string is copied out in buffer-sized chunks, so peak memory stays at one buffer regardless
    /// of the value's size.</summary>
    private async ValueTask WriteOversizedRowAsync(int row, CancellationToken ct)
    {
        for (var col = 0; col < _columnCount; col++)
        {
            if (col > 0)
            {
                await EnsureAsync(1, ct).ConfigureAwait(false);
                _buffer![_length++] = _delimiter;
            }

            if (_columns[col].Kind != CellKind.String)
            {
                await EnsureAsync(FixedCellReserve, ct).ConfigureAwait(false);
                _length += WriteCell(ref _columns[col], row, _buffer.AsSpan(_length));
                continue;
            }

            if (!_columns[col].IsValid(row))
            {
                continue;
            }

            var length = _columns[col].Utf8Length(row);
            if (!NeedsQuoting(col, row))
            {
                await CopyChunkedAsync(col, row, 0, length, ct).ConfigureAwait(false);
                continue;
            }

            await EnsureAsync(1, ct).ConfigureAwait(false);
            _buffer![_length++] = (byte)'"';

            var start = 0;
            while (start < length)
            {
                var quote = IndexOfQuote(col, row, start);
                if (quote < 0)
                {
                    await CopyChunkedAsync(col, row, start, length - start, ct).ConfigureAwait(false);
                    break;
                }

                await CopyChunkedAsync(col, row, start, quote - start + 1, ct).ConfigureAwait(false);
                await EnsureAsync(1, ct).ConfigureAwait(false);
                _buffer![_length++] = (byte)'"';
                start = quote + 1;
            }

            await EnsureAsync(1, ct).ConfigureAwait(false);
            _buffer![_length++] = (byte)'"';
        }

        await EnsureAsync(1, ct).ConfigureAwait(false);
        _buffer![_length++] = (byte)'\n';
    }

    private unsafe bool NeedsQuoting(int col, int row) => _columns[col].Utf8(row).IndexOfAny(_quoteTriggers) >= 0;

    private unsafe int IndexOfQuote(int col, int row, int start)
    {
        var found = _columns[col].Utf8(row)[start..].IndexOf((byte)'"');
        return found < 0 ? -1 : start + found;
    }

    /// <summary>Copies <paramref name="count"/> bytes of one string cell into the buffer, flushing as
    /// many times as it takes. The span is re-derived after every flush rather than held across the
    /// await.</summary>
    private async ValueTask CopyChunkedAsync(int col, int row, int start, int count, CancellationToken ct)
    {
        var offset = 0;
        while (offset < count)
        {
            if (_length == _buffer!.Length)
            {
                await FlushBufferAsync(ct).ConfigureAwait(false);
            }

            var take = Math.Min(count - offset, _buffer.Length - _length);
            _columns[col].Utf8(row).Slice(start + offset, take).CopyTo(_buffer.AsSpan(_length));
            _length += take;
            offset += take;
        }
    }

    private ValueTask EnsureAsync(int bytes, CancellationToken ct) =>
        _length + bytes > _buffer!.Length ? FlushBufferAsync(ct) : ValueTask.CompletedTask;

    private async ValueTask FlushBufferAsync(CancellationToken ct)
    {
        if (_length == 0)
        {
            return;
        }

        var pending = _length;
        _length = 0;
        await _stream.WriteAsync(_buffer.AsMemory(0, pending), ct).ConfigureAwait(false);
    }

    /// <summary>Formats one non-null cell (or nothing at all, for a null one) at the buffer's current
    /// position, which the caller has already sized. Every rendering here must stay byte-for-byte what
    /// <c>ToString(format, InvariantCulture)</c> produces — the <c>TryFormat</c> overloads used are the
    /// UTF-8 equivalents of exactly those calls.</summary>
    private unsafe int WriteCell(ref Column column, int row, Span<byte> destination) =>
        WriteCell(ref column, row, destination, _quoteTriggers);

    /// <summary>Same cell formatting as the field-reading overload above, but takes the quote-trigger set
    /// as a parameter so a hot per-cell caller (<see cref="WriteRowsThatFit"/>) that has already hoisted
    /// it into a local passes it straight through instead of re-reading <see cref="_quoteTriggers"/> on
    /// every string cell.</summary>
    private unsafe int WriteCell(ref Column column, int row, Span<byte> destination, SearchValues<byte> quoteTriggers)
    {
        if (!column.IsValid(row))
        {
            return 0;
        }

        int written;
        switch (column.Kind)
        {
            case CellKind.Int32:
                column.Int32At(row).TryFormat(destination, out written, default, CultureInfo.InvariantCulture);
                break;
            case CellKind.Int64:
                column.Int64At(row).TryFormat(destination, out written, default, CultureInfo.InvariantCulture);
                break;
            case CellKind.Double:
                column.DoubleAt(row).TryFormat(destination, out written, "R", CultureInfo.InvariantCulture);
                break;
            case CellKind.Boolean:
                var flag = column.BooleanAt(row) ? "True"u8 : "False"u8;
                flag.CopyTo(destination);
                written = flag.Length;
                break;
            case CellKind.Decimal128:
                ((Decimal128Array)column.Array).GetValue(row)!.Value
                    .TryFormat(destination, out written, default, CultureInfo.InvariantCulture);
                break;
            case CellKind.Date32:
                ((Date32Array)column.Array).GetDateTime(row)!.Value
                    .TryFormat(destination, out written, "O", CultureInfo.InvariantCulture);
                break;
            case CellKind.Timestamp:
                ((TimestampArray)column.Array).GetTimestamp(row)!.Value
                    .TryFormat(destination, out written, "O", CultureInfo.InvariantCulture);
                break;
            default:
                return WriteStringCell(ref column, row, destination, quoteTriggers);
        }

        return written;
    }

    /// <summary>Copies one string cell's UTF-8 bytes straight from the pinned Arrow value buffer,
    /// quoting only when RFC 4180 requires it. Never decodes to UTF-16, so a value's bytes reach the
    /// file exactly as the source produced them.</summary>
    private static unsafe int WriteStringCell(ref Column column, int row, Span<byte> destination, SearchValues<byte> quoteTriggers)
    {
        var value = column.Utf8(row);
        if (value.IndexOfAny(quoteTriggers) < 0)
        {
            value.CopyTo(destination);
            return value.Length;
        }

        destination[0] = (byte)'"';
        var written = 1;
        int quote;
        while ((quote = value.IndexOf((byte)'"')) >= 0)
        {
            value[..(quote + 1)].CopyTo(destination[written..]);
            written += quote + 1;
            destination[written++] = (byte)'"';
            value = value[(quote + 1)..];
        }

        value.CopyTo(destination[written..]);
        written += value.Length;
        destination[written++] = (byte)'"';
        return written;
    }

    private static string QuoteText(string? value, char delimiter)
    {
        if (value is null)
        {
            return string.Empty;
        }

        return value.IndexOfAny([delimiter, '"', '\n', '\r']) >= 0
            ? "\"" + value.Replace("\"", "\"\"") + "\""
            : value;
    }

    private enum CellKind : byte
    {
        Int32,
        Int64,
        Double,
        Decimal128,
        Boolean,
        Date32,
        Timestamp,
        String,
    }

    /// <summary>One column's pinned view for the lifetime of a single
    /// <see cref="WriteBatchAsync"/> call: the kind to dispatch on, pointers into the Arrow validity /
    /// values / offsets buffers, and the array itself for the accessor-based kinds. Arrow arrays can be
    /// slices of a larger buffer, so every pointer read adds <see cref="_offset"/> — the same
    /// adjustment Arrow's own accessors make internally.</summary>
    private unsafe struct Column
    {
        private Pins? _pins;
        private byte* _validity;
        private byte* _values;
        private int* _offsets;
        private int _offset;

        public CellKind Kind { get; private init; }

        public IArrowArray Array { get; private init; }

        public static Column Pin(IArrowArray array, string sinkLabel)
        {
            var column = new Column
            {
                Kind = KindOf(array, sinkLabel),
                Array = array,
            };

            var data = ((Apache.Arrow.Array)array).Data;
            column._offset = data.Offset;

            // Buffer 0 is the validity bitmap for every Arrow layout used here; it is empty when the
            // column has no nulls, in which case IsValid short-circuits on the null pointer.
            var pins = new Pins();
            column._pins = pins;
            if (data.NullCount != 0 && data.Buffers.Length > 0 && !data.Buffers[0].IsEmpty)
            {
                pins.Validity = data.Buffers[0].Memory.Pin();
                column._validity = (byte*)pins.Validity.Pointer;
            }

            switch (column.Kind)
            {
                case CellKind.Int32:
                case CellKind.Int64:
                case CellKind.Double:
                case CellKind.Boolean:
                    pins.Values = data.Buffers[1].Memory.Pin();
                    column._values = (byte*)pins.Values.Pointer;
                    break;
                case CellKind.String:
                    pins.Offsets = data.Buffers[1].Memory.Pin();
                    column._offsets = (int*)pins.Offsets.Pointer;
                    pins.Values = data.Buffers[2].Memory.Pin();
                    column._values = (byte*)pins.Values.Pointer;
                    break;
                default:
                    // Decimal/date/timestamp read through Arrow's accessors — nothing to pin.
                    break;
            }

            return column;
        }

        public void Release()
        {
            if (_pins is not { } pins)
            {
                return;
            }

            pins.Validity.Dispose();
            pins.Values.Dispose();
            pins.Offsets.Dispose();
            this = default;
        }

        /// <summary>The pins themselves, one allocation per column per batch, kept off the hot struct:
        /// a <see cref="MemoryHandle"/> is 24 bytes and nothing in the row loop ever reads one.</summary>
        private sealed class Pins
        {
            public MemoryHandle Validity;
            public MemoryHandle Values;
            public MemoryHandle Offsets;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool IsValid(int row) =>
            _validity is null || (_validity[(_offset + row) >> 3] & (1 << ((_offset + row) & 7))) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly int Int32At(int row) => ((int*)_values)[_offset + row];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly long Int64At(int row) => ((long*)_values)[_offset + row];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly double DoubleAt(int row) => ((double*)_values)[_offset + row];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly bool BooleanAt(int row) =>
            (_values[(_offset + row) >> 3] & (1 << ((_offset + row) & 7))) != 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly int Utf8Length(int row) => _offsets[_offset + row + 1] - _offsets[_offset + row];

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly ReadOnlySpan<byte> Utf8(int row)
        {
            var start = _offsets[_offset + row];
            return new ReadOnlySpan<byte>(_values + start, _offsets[_offset + row + 1] - start);
        }

        private static CellKind KindOf(IArrowArray array, string sinkLabel) => array switch
        {
            Int32Array => CellKind.Int32,
            Int64Array => CellKind.Int64,
            DoubleArray => CellKind.Double,
            Decimal128Array => CellKind.Decimal128,
            BooleanArray => CellKind.Boolean,
            Date32Array => CellKind.Date32,
            TimestampArray => CellKind.Timestamp,
            StringArray => CellKind.String,
            _ => throw new NotSupportedException($"{sinkLabel} does not support array type '{array.GetType()}'"),
        };
    }
}
