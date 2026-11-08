using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http;

/// <summary>Caps the decompressed bytes read from a download, summed across every stream it wraps
/// (all zip members of one partition share one budget). Sizes claimed by archive headers are never
/// consulted; only bytes actually delivered by a read count.</summary>
internal sealed class DecompressionBudget(long limitBytes, string label)
{
    private const long MiB = 1024 * 1024;
    private long _used;

    /// <summary>A read-only view of <paramref name="decompressed"/> that charges every byte read to
    /// this budget and throws a permanent error once the total passes the limit. Disposing it disposes
    /// <paramref name="decompressed"/>.</summary>
    public Stream Wrap(Stream decompressed) => new BudgetStream(decompressed, this);

    private void Charge(int bytes)
    {
        _used += bytes;
        if (_used > limitBytes)
        {
            throw new PzConnectorException(
                $"{label}: decompressed content exceeds 20 × 'max_download_mb' ({limitBytes / MiB} MiB)",
                isTransient: false);
        }
    }

    private sealed class BudgetStream(Stream inner, DecompressionBudget budget) : Stream
    {
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
            var n = inner.Read(buffer);
            budget.Charge(n);
            return n;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var n = await inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
            budget.Charge(n);
            return n;
        }

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
                inner.Dispose();
            }

            base.Dispose(disposing);
        }
    }
}
