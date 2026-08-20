using System.Buffers;

namespace Pz.Connectors.Abstractions.Memory;

/// <summary>The <see cref="IMemoryOwner{T}"/> <see cref="PooledNativeAllocator"/> hands out: a
/// <see cref="MemoryManager{T}"/> over an aligned native buffer, shaped exactly like Apache.Arrow's
/// own internal <c>NativeMemoryManager</c> (same <see cref="GetSpan"/>/<see cref="Pin"/>/
/// <see cref="Unpin"/> contract) so it pins and releases correctly across the C Data Interface export
/// path DuckDB ingest uses. <see cref="Memory{T}.Length"/> is always exactly the
/// length the allocator was asked for — never the (possibly larger) pooled size-class capacity —
/// because <c>Apache.Arrow.ArrowBuffer.Length</c> reads straight through to this owner's
/// <see cref="Memory"/>, with no separate length field of its own.
///
/// Dispose returns the buffer to <see cref="PooledNativeAllocator"/>'s free list for its size class
/// (oversize/unpooled buffers are freed immediately instead). In DEBUG builds, disposing twice or
/// touching <see cref="GetSpan"/>/<see cref="Memory"/> after Dispose throws
/// <see cref="InvalidOperationException"/>; RELEASE builds skip both checks — the underlying pointer
/// swap that makes Dispose idempotent is needed either way, so skipping the checks removes the guard
/// without adding a hot-path branch.</summary>
internal sealed class PooledMemoryOwner : MemoryManager<byte>
{
    private readonly PooledNativeAllocator _owner;
    private readonly int _length;
    private readonly int _sizeClassIndex; // -1 == oversize passthrough, never pooled
    private nint _ptr;
    private int _pinCount;
#if DEBUG
    private int _disposed;
#endif

    internal PooledMemoryOwner(PooledNativeAllocator owner, nint ptr, int length, int sizeClassIndex)
    {
        _owner = owner;
        _ptr = ptr;
        _length = length;
        _sizeClassIndex = sizeClassIndex;
    }

    public override unsafe Span<byte> GetSpan()
    {
#if DEBUG
        ThrowIfDisposed();
#endif
        return new Span<byte>((void*)_ptr, _length);
    }

    public override unsafe MemoryHandle Pin(int elementIndex = 0)
    {
#if DEBUG
        ThrowIfDisposed();
#endif
        if ((uint)elementIndex > (uint)_length)
        {
            throw new ArgumentOutOfRangeException(nameof(elementIndex));
        }

        Interlocked.Increment(ref _pinCount);
        return new MemoryHandle((void*)(_ptr + elementIndex), default, this);
    }

    public override void Unpin() => Interlocked.Decrement(ref _pinCount);

    protected override void Dispose(bool disposing)
    {
#if DEBUG
        if (disposing && Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            throw new InvalidOperationException(
                "PooledMemoryOwner disposed more than once — pooled batch buffers must be disposed exactly once.");
        }
#endif

        var ptr = Interlocked.Exchange(ref _ptr, 0);
        if (ptr == 0)
        {
            return; // idempotent: already returned (RELEASE double-dispose, or finalizer after an explicit Dispose).
        }

        if (_pinCount > 0)
        {
            _ptr = ptr; // restore — this attempt did not consume the buffer.
            throw new InvalidOperationException(
                "cannot return pooled native memory to the pool while it is still pinned");
        }

        _owner.Return(ptr, _length, _sizeClassIndex);
    }

#if DEBUG
    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _ptr) == 0)
        {
            throw new InvalidOperationException("pooled buffer accessed after Dispose.");
        }
    }
#endif
}
