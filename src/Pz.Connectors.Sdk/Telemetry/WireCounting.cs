using System.Net;

namespace Pz.Connectors.Sdk;

/// <summary>Counts the bytes that pass through a stream into a call's wire-byte total. Counting never
/// throws, and it keeps working after the call's span has ended: the call captured its stats handle when
/// it started.</summary>
internal sealed class CountingStream(Stream inner, RemoteCall call) : Stream
{
    public override bool CanRead => inner.CanRead;

    public override bool CanSeek => false;

    public override bool CanWrite => inner.CanWrite;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Flush() => inner.Flush();

    public override Task FlushAsync(CancellationToken cancellationToken) => inner.FlushAsync(cancellationToken);

    public override int Read(byte[] buffer, int offset, int count) => Count(inner.Read(buffer, offset, count));

    public override int Read(Span<byte> buffer) => Count(inner.Read(buffer));

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        Count(await inner.ReadAsync(buffer.AsMemory(offset, count), cancellationToken).ConfigureAwait(false));

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
        Count(await inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false));

    public override void Write(byte[] buffer, int offset, int count)
    {
        inner.Write(buffer, offset, count);
        Count(count);
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        inner.Write(buffer);
        Count(buffer.Length);
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        await inner.WriteAsync(buffer.AsMemory(offset, count), cancellationToken).ConfigureAwait(false);
        Count(count);
    }

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        await inner.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
        Count(buffer.Length);
    }

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            inner.Dispose();
        }

        base.Dispose(disposing);
    }

    public override ValueTask DisposeAsync() => inner.DisposeAsync();

    private int Count(int n)
    {
        if (n > 0)
        {
            try
            {
                call.WireBytes(n);
            }
            catch
            {
                // Reporting statistics must never fail the transfer it observes.
            }
        }

        return n;
    }
}

/// <summary>A request body of unknown length that counts the bytes it actually serializes.</summary>
internal sealed class CountingRequestContent : HttpContent
{
    private readonly HttpContent _inner;
    private readonly RemoteCall _call;

    public CountingRequestContent(HttpContent inner, RemoteCall call)
    {
        _inner = inner;
        _call = call;
        foreach (var header in inner.Headers)
        {
            Headers.TryAddWithoutValidation(header.Key, header.Value);
        }
    }

    protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context) =>
        SerializeToStreamAsync(stream, context, CancellationToken.None);

    protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context, CancellationToken cancellationToken)
    {
        // The counter only forwards writes; the transport's stream stays open.
        await using var counting = new CountingStream(new NonOwningStream(stream), _call);
        await _inner.CopyToAsync(counting, context, cancellationToken).ConfigureAwait(false);
    }

    protected override bool TryComputeLength(out long length)
    {
        length = 0;
        return false;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _inner.Dispose();
        }

        base.Dispose(disposing);
    }
}

/// <summary>A response body of unknown length that counts the bytes the caller reads from it. Disposing
/// it disposes the original response content too.</summary>
internal sealed class CountingResponseContent : StreamContent
{
    private readonly HttpContent _original;

    public CountingResponseContent(HttpContent original, Stream body, RemoteCall call)
        : base(new CountingStream(body, call))
    {
        _original = original;
        foreach (var header in original.Headers)
        {
            Headers.TryAddWithoutValidation(header.Key, header.Value);
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _original.Dispose();
        }

        base.Dispose(disposing);
    }
}

/// <summary>Forwards writes to a stream without taking ownership of it.</summary>
internal sealed class NonOwningStream(Stream inner) : Stream
{
    public override bool CanRead => false;

    public override bool CanSeek => false;

    public override bool CanWrite => true;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Flush() => inner.Flush();

    public override Task FlushAsync(CancellationToken cancellationToken) => inner.FlushAsync(cancellationToken);

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);

    public override void Write(ReadOnlySpan<byte> buffer) => inner.Write(buffer);

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        inner.WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) =>
        inner.WriteAsync(buffer, cancellationToken);
}
