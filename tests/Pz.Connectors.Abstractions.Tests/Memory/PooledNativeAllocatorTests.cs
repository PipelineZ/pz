using Pz.Connectors.Abstractions.Memory;

namespace Pz.Connectors.Abstractions.Tests.Memory;

/// <summary>Each test uses a fresh <see cref="PooledNativeAllocator"/>
/// instance (never <see cref="PooledNativeAllocator.Shared"/>) so rented/pooled byte assertions are
/// exact and independent of whatever else in the suite might be exercising the shared instance.</summary>
public class PooledNativeAllocatorTests
{
    [Fact]
    public void Rent_returns_size_class_rounded_memory()
    {
        var allocator = new PooledNativeAllocator();

        // Apache.Arrow's ArrowBuffer.Length reads straight through to the owner's Memory.Length, with
        // no separate length field of its own (verified against Apache.Arrow 23.0.0's decompiled IL)
        // — so the caller must see EXACTLY what it asked for, never the padded size-class
        // capacity kept around internally for reuse.
        using var owner = allocator.Allocate(1000);
        Assert.Equal(1000, owner.Memory.Length);

        // The internal accounting, however, IS size-class-rounded: a 1000-byte request rents a whole
        // 64KB (MinSizeClass) block from the pool.
        Assert.Equal(PooledNativeAllocator.MinSizeClass, allocator.RentedBytes);
    }

    [Fact]
    public void Dispose_returns_to_pool_and_reuses()
    {
        var allocator = new PooledNativeAllocator();

        var first = allocator.Allocate(70_000); // rounds up to the 128KB size class
        first.Dispose();

        var pooledAfterFirstReturn = allocator.PooledBytes;
        Assert.True(pooledAfterFirstReturn > 0);
        Assert.Equal(0, allocator.RentedBytes);

        // Renting the same size class again must come out of the free list, not a fresh native
        // allocation — PooledBytes drops back to 0 while it's rented out.
        var second = allocator.Allocate(70_000);
        Assert.Equal(0, allocator.PooledBytes);
        Assert.Equal(pooledAfterFirstReturn, allocator.RentedBytes);

        second.Dispose();

        // Same class, reused, no growth: pooled bytes settle back at exactly what they were before.
        Assert.Equal(pooledAfterFirstReturn, allocator.PooledBytes);
    }

    [Fact]
    public void Oversize_requests_pass_through_unpooled()
    {
        var allocator = new PooledNativeAllocator();
        const int oversize = PooledNativeAllocator.MaxSizeClass + 1024;

        var owner = allocator.Allocate(oversize);
        Assert.Equal(oversize, owner.Memory.Length);
        Assert.Equal(oversize, allocator.RentedBytes);
        Assert.Equal(0, allocator.PooledBytes);

        owner.Dispose();

        Assert.Equal(0, allocator.RentedBytes);
        Assert.Equal(0, allocator.PooledBytes); // freed straight back to the OS, never pooled
    }

#if DEBUG
    [Fact]
    public void Double_dispose_throws_in_debug()
    {
        var allocator = new PooledNativeAllocator();
        var owner = allocator.Allocate(4096);
        owner.Dispose();

        Assert.Throws<InvalidOperationException>(() => owner.Dispose());
    }

    [Fact]
    public void Access_after_dispose_throws_in_debug()
    {
        var allocator = new PooledNativeAllocator();
        var owner = allocator.Allocate(4096);
        owner.Dispose();

        Assert.Throws<InvalidOperationException>(() => _ = owner.Memory);
    }
#endif
}
