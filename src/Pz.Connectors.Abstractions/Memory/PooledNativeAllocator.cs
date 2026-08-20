using System.Buffers;
using System.Collections.Concurrent;
using System.Numerics;
using System.Runtime.InteropServices;

namespace Pz.Connectors.Abstractions.Memory;

/// <summary>Pools batch buffer memory in power-of-two size classes (64KB..64MB; larger requests
/// pass through unpooled). Thread-safe. Dispose returns memory to the pool; in DEBUG builds,
/// double-dispose and post-dispose access throw.
///
/// This is the sanctioned seam Apache.Arrow 23 exposes for pooling: every v0-matrix builder's
/// <c>Build(MemoryAllocator?)</c> copies its final validity/offsets/value buffers through the
/// allocator passed in — only those final buffers, never the builders' own managed scratch.
/// <see cref="Pz.Connectors.Abstractions.Batches.ArrowBatchBuilder"/> uses <see cref="Shared"/> by
/// default.</summary>
public sealed class PooledNativeAllocator : Apache.Arrow.Memory.MemoryAllocator
{
    /// <summary>Smallest pooled size class: 64KB. Requests at or below this round up to it.</summary>
    public const int MinSizeClass = 64 * 1024;

    /// <summary>Largest pooled size class: 64MB. Requests larger than this pass through unpooled —
    /// allocated and freed directly on rent/dispose, never retained in a free list.</summary>
    public const int MaxSizeClass = 64 * 1024 * 1024;

    /// <summary>Matches <c>Apache.Arrow.Memory.MemoryAllocator.DefaultAlignment</c> so pooled buffers
    /// are aligned exactly as Arrow's own native allocator aligns them.</summary>
    private const int NativeAlignment = 64;

    private const int MinExponent = 16; // 1 << 16 == 64KB
    private const int MaxExponent = 26; // 1 << 26 == 64MB
    private const int ClassCount = MaxExponent - MinExponent + 1;

    /// <summary>Process-wide default, used by <see cref="ArrowBatchBuilder"/> when no allocator is
    /// supplied explicitly.</summary>
    public static PooledNativeAllocator Shared { get; } = new();

    private readonly ConcurrentQueue<nint>[] _freeLists;
    private long _rentedBytes;
    private long _pooledBytes;

    public PooledNativeAllocator()
    {
        _freeLists = new ConcurrentQueue<nint>[ClassCount];
        for (var i = 0; i < ClassCount; i++)
        {
            _freeLists[i] = new ConcurrentQueue<nint>();
        }
    }

    /// <summary>Total bytes currently rented out to live, undisposed owners (size-class-rounded for
    /// pooled requests; exact for oversize passthrough requests).</summary>
    public long RentedBytes => Interlocked.Read(ref _rentedBytes);

    /// <summary>Total bytes currently sitting in free lists, available for reuse without a fresh
    /// native allocation.</summary>
    public long PooledBytes => Interlocked.Read(ref _pooledBytes);

    protected override unsafe IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
    {
        if (length > MaxSizeClass)
        {
            var oversizePtr = (nint)NativeMemory.AlignedAlloc((nuint)length, NativeAlignment);
            NativeMemory.Clear((void*)oversizePtr, (nuint)length);
            bytesAllocated = length;
            Interlocked.Add(ref _rentedBytes, length);
            return new PooledMemoryOwner(this, oversizePtr, length, sizeClassIndex: -1);
        }

        var exponent = SizeClassExponent(length);
        var sizeClass = 1 << exponent;
        var index = exponent - MinExponent;

        if (!_freeLists[index].TryDequeue(out var ptr))
        {
            ptr = (nint)NativeMemory.AlignedAlloc((nuint)sizeClass, NativeAlignment);
        }
        else
        {
            Interlocked.Add(ref _pooledBytes, -sizeClass);
        }

        // Only the requested length needs to be deterministic (the ArrowBuffer built from this owner
        // never reads past it) but zeroing exactly `length` — not the whole size-class capacity —
        // guarantees a batch never observes another batch's bytes that happened to share this
        // recycled native block, at the cost of a partial fill instead of a full one.
        NativeMemory.Clear((void*)ptr, (nuint)length);

        Interlocked.Add(ref _rentedBytes, sizeClass);
        bytesAllocated = sizeClass;
        return new PooledMemoryOwner(this, ptr, length, index);
    }

    /// <summary>Called by <see cref="PooledMemoryOwner.Dispose"/>. Oversize (<paramref name="sizeClassIndex"/>
    /// &lt; 0) buffers are freed immediately; pooled ones go back on their size class's free list for reuse.</summary>
    internal unsafe void Return(nint ptr, int length, int sizeClassIndex)
    {
        if (sizeClassIndex < 0)
        {
            NativeMemory.AlignedFree((void*)ptr);
            Interlocked.Add(ref _rentedBytes, -length);
            return;
        }

        var sizeClass = 1 << (sizeClassIndex + MinExponent);
        Interlocked.Add(ref _rentedBytes, -sizeClass);
        Interlocked.Add(ref _pooledBytes, sizeClass);
        _freeLists[sizeClassIndex].Enqueue(ptr);
    }

    private static int SizeClassExponent(int length)
    {
        var rounded = BitOperations.RoundUpToPowerOf2((uint)length);
        var exponent = BitOperations.Log2(rounded);
        return Math.Clamp(exponent, MinExponent, MaxExponent);
    }
}
