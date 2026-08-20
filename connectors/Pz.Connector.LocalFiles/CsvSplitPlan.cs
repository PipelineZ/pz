using System.Buffers;
using System.Text;

namespace Pz.Connector.LocalFiles;

/// <summary>Where a large CSV file can be cut into byte ranges that each parse independently, so the
/// universal (Arrow) read can run several readers concurrently instead of one.
///
/// The universal read's two halves already run concurrently in <c>SourceLoadExecutor</c> — the
/// connector's reader feeds a bounded channel that <c>IngestArrowAsync</c> drains — so the node costs
/// <c>max(read, ingest)</c>. With one reader the parse is the larger half by roughly 2x on a 4-column
/// file, so the whole node pays for a parser that a second core could be helping with. Splitting the file
/// makes the reader the smaller half, and the node settles at the ingest floor.
///
/// <para><b>Why a scan, and why it refuses.</b> A byte offset chosen blind can land inside a quoted
/// field, where a newline is data rather than a record terminator — splitting there would silently drop
/// or duplicate rows, which is the one outcome worth spending a whole pass over the file to rule out.
/// So the plan is computed by actually walking the bytes with the same rules Sylvan parses by (verified
/// against it: a quote only opens a quoted field at a field start, so <c>1"2,3</c> is a literal quote and
/// NOT a quoted field), and it REFUSES — returns null, leaving a single whole-file partition — for
/// anything it cannot prove:</para>
/// <list type="bullet">
/// <item><description>a file too small to be worth more than one reader;</description></item>
/// <item><description>a header that is not exactly its own field names joined with commas, which is what
/// proves the delimiter really is a comma. Sylvan auto-detects the delimiter and does not report what it
/// picked, and field starts (hence which quotes are quoting) cannot be found without knowing it;</description></item>
/// <item><description>a file whose quoting does not close — the scan ending inside a quoted field means
/// the bytes did not parse the way this scanner assumed, so none of its boundaries are trustworthy.</description></item>
/// </list>
/// <para>The scan costs one sequential pass (~150 ms for 146 MB, warm) and leaves the page cache warm for
/// the readers that follow it.</para></summary>
internal sealed record CsvSplitPlan(byte[] Header, IReadOnlyList<CsvSplit> Splits);

/// <summary>One reader's byte range, plus what to add to that reader's own 1-based data-row number to
/// name the row an author counting from the top of the file would name. Every range but the first is
/// read with <see cref="CsvSplitPlan.Header"/> spliced in front of it, so each reader sees a normal
/// headed CSV and resolves its own column ordinals.</summary>
internal readonly record struct CsvSplit(long Start, long End, long RowNumberOffset);

internal static class CsvSplitPlanner
{
    /// <summary>How much file each extra reader has to be worth. Splitting is not free — a whole
    /// sequential scan up front, plus a file handle and a batch buffer per reader — and below this the
    /// read is over before the fan-out pays for itself.</summary>
    internal const long MinBytesPerPartition = 32L * 1024 * 1024;

    private const int ChunkBytes = 1024 * 1024;
    private const byte Quote = (byte)'"';
    private const byte Newline = (byte)'\n';
    private const byte Return = (byte)'\r';
    private const byte Comma = (byte)',';

    private enum ScanState
    {
        /// <summary>Outside any quoted field: a newline here terminates a record.</summary>
        Unquoted,

        /// <summary>Inside a quoted field: newlines are data, only a quote can end it.</summary>
        Quoted,

        /// <summary>Just consumed a quote while inside a quoted field — the next byte decides whether it
        /// was an escaped quote (<c>""</c>) or the field's closing quote. Kept as an explicit state so a
        /// chunk boundary can fall between the two bytes.</summary>
        QuotedQuote,
    }

    /// <summary>Plans up to <paramref name="maxPartitions"/> byte ranges over <paramref name="path"/>, or
    /// returns null when the file cannot be proven safe to split (see <see cref="CsvSplitPlan"/>).
    /// <paramref name="headerNames"/> is the file's own header as the CSV reader resolved it.
    /// <paramref name="minBytesPerPartition"/> is a seam for tests, which need the splitting behaviour on
    /// files small enough to write out and check by hand; production always takes the default.</summary>
    internal static CsvSplitPlan? TryPlan(
        string path,
        IReadOnlyList<string> headerNames,
        int maxPartitions,
        long minBytesPerPartition = MinBytesPerPartition)
    {
        var info = new FileInfo(path);
        if (!info.Exists)
        {
            return null;
        }

        var length = info.Length;
        var partitions = (int)Math.Min(maxPartitions, length / minBytesPerPartition);
        if (partitions < 2)
        {
            return null;
        }

        var targets = new long[partitions - 1];
        for (var i = 0; i < targets.Length; i++)
        {
            targets[i] = length * (i + 1) / partitions;
        }

        if (!Scan(path, targets, out var headerEnd, out var boundaries, out var recordsBefore))
        {
            return null;
        }

        var header = ReadHeaderBytes(path, headerEnd);
        if (!HeaderIsCommaDelimited(header, headerNames))
        {
            return null;
        }

        var splits = new List<CsvSplit>(boundaries.Count + 1);
        var start = headerEnd;
        var offset = 0L;
        for (var i = 0; i < boundaries.Count; i++)
        {
            splits.Add(new CsvSplit(start, boundaries[i], offset));
            start = boundaries[i];
            offset = recordsBefore[i] - 1;
        }

        splits.Add(new CsvSplit(start, length, offset));
        return splits.Count < 2 ? null : new CsvSplitPlan(header, splits);
    }

    /// <summary>Walks every byte once, tracking quoted-field state, and reports the first record boundary
    /// at or after each requested target offset along with how many records (header included) precede it.
    /// Returns false when the file ends inside a quoted field.</summary>
    private static bool Scan(
        string path, long[] targets, out long headerEnd, out List<long> boundaries, out List<long> recordsBefore)
    {
        headerEnd = -1;
        boundaries = [];
        recordsBefore = [];

        var state = ScanState.Unquoted;
        var previous = Newline; // start-of-file is a record start, so a leading quote opens a field
        var records = 0L;
        var next = 0;
        var basePosition = 0L;

        var buffer = ArrayPool<byte>.Shared.Rent(ChunkBytes);
        try
        {
            using var file = new FileStream(path, new FileStreamOptions
            {
                Mode = FileMode.Open,
                Access = FileAccess.Read,
                Share = FileShare.Read,
                Options = FileOptions.SequentialScan,
            });

            while (true)
            {
                var read = file.Read(buffer, 0, ChunkBytes);
                if (read <= 0)
                {
                    break;
                }

                var span = buffer.AsSpan(0, read);
                var position = 0;
                while (position < read)
                {
                    switch (state)
                    {
                        case ScanState.Quoted:
                        {
                            var found = span[position..].IndexOf(Quote);
                            if (found < 0)
                            {
                                position = read;
                                break;
                            }

                            position += found + 1;
                            state = ScanState.QuotedQuote;
                            break;
                        }

                        case ScanState.QuotedQuote:
                        {
                            if (span[position] == Quote)
                            {
                                position++;
                                state = ScanState.Quoted;
                            }
                            else
                            {
                                state = ScanState.Unquoted;
                                previous = Quote;
                            }

                            break;
                        }

                        default:
                        {
                            var found = span[position..].IndexOfAny(Quote, Newline);
                            if (found < 0)
                            {
                                previous = span[read - 1];
                                position = read;
                                break;
                            }

                            var at = position + found;
                            if (span[at] == Newline)
                            {
                                records++;
                                var absolute = basePosition + at;
                                if (headerEnd < 0)
                                {
                                    headerEnd = absolute + 1;
                                }
                                else
                                {
                                    // One boundary per target, and never two targets on the same
                                    // boundary -- a zero-row partition would be pointless work and an
                                    // off-by-one waiting to happen in the row-number offsets.
                                    while (next < targets.Length && targets[next] <= absolute)
                                    {
                                        if (boundaries.Count == 0 || boundaries[^1] != absolute + 1)
                                        {
                                            boundaries.Add(absolute + 1);
                                            recordsBefore.Add(records);
                                        }

                                        next++;
                                    }
                                }

                                previous = Newline;
                                position = at + 1;
                            }
                            else
                            {
                                // A quote only opens a quoted field at a field start; anywhere else it is
                                // a literal character, which is how Sylvan reads it too.
                                var before = at > 0 ? span[at - 1] : previous;
                                state = before is Comma or Newline or Return ? ScanState.Quoted : ScanState.Unquoted;
                                previous = Quote;
                                position = at + 1;
                            }

                            break;
                        }
                    }
                }

                basePosition += read;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        return state != ScanState.Quoted && headerEnd > 0 && boundaries.Count > 0;
    }

    private static byte[] ReadHeaderBytes(string path, long headerEnd)
    {
        using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        var header = new byte[headerEnd];
        file.ReadExactly(header);
        return header;
    }

    /// <summary>Proves the delimiter is a comma: the header line, decoded, must be exactly its own field
    /// names joined with commas. Any other delimiter, or a quoted header field, fails this and the file
    /// is left unsplit — see <see cref="CsvSplitPlan"/> for why the delimiter has to be known.</summary>
    private static bool HeaderIsCommaDelimited(byte[] header, IReadOnlyList<string> headerNames)
    {
        var text = Encoding.UTF8.GetString(header).TrimEnd('\n').TrimEnd('\r');
        if (text.StartsWith('\uFEFF'))
        {
            text = text[1..];
        }

        return string.Equals(text, string.Join(",", headerNames), StringComparison.Ordinal);
    }
}

/// <summary>Presents one <see cref="CsvSplit"/> as a normal headed CSV stream: the file's header bytes,
/// then the split's byte range. Splicing the header in front is what lets every split partition run an
/// ordinary <c>HasHeaders = true</c> reader — it resolves its own column ordinals, and Sylvan's delimiter
/// auto-detection sees the same header the whole-file read would, so a split read cannot drift from an
/// unsplit one on either.</summary>
internal sealed class CsvSliceStream : Stream
{
    private readonly byte[] _header;
    private readonly FileStream _file;
    private int _headerPosition;
    private long _remaining;

    public CsvSliceStream(string path, byte[] header, long start, long end)
    {
        _header = header;
        _remaining = end - start;
        _file = new FileStream(path, new FileStreamOptions
        {
            Mode = FileMode.Open,
            Access = FileAccess.Read,
            Share = FileShare.Read,
            Options = FileOptions.SequentialScan,
        });
        _file.Seek(start, SeekOrigin.Begin);
    }

    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

    public override int Read(Span<byte> buffer)
    {
        if (_headerPosition < _header.Length)
        {
            var take = Math.Min(buffer.Length, _header.Length - _headerPosition);
            _header.AsSpan(_headerPosition, take).CopyTo(buffer);
            _headerPosition += take;
            return take;
        }

        if (_remaining <= 0)
        {
            return 0;
        }

        var read = _file.Read(buffer[..(int)Math.Min(buffer.Length, _remaining)]);
        _remaining -= read;
        return read;
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
    {
        if (_headerPosition < _header.Length)
        {
            var take = Math.Min(buffer.Length, _header.Length - _headerPosition);
            _header.AsSpan(_headerPosition, take).CopyTo(buffer.Span);
            _headerPosition += take;
            return take;
        }

        if (_remaining <= 0)
        {
            return 0;
        }

        var read = await _file.ReadAsync(buffer[..(int)Math.Min(buffer.Length, _remaining)], ct).ConfigureAwait(false);
        _remaining -= read;
        return read;
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct) =>
        ReadAsync(buffer.AsMemory(offset, count), ct).AsTask();

    public override void Flush()
    {
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _file.Dispose();
        }

        base.Dispose(disposing);
    }
}
