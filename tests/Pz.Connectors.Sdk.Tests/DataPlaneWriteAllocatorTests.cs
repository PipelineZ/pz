using System.Diagnostics;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Memory;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

/// <summary>Proves the write-path data-plane listener pools its incoming batch buffers through
/// <see cref="PooledNativeAllocator"/> -- exactly as the host's own read path already does -- instead of
/// letting <see cref="Apache.Arrow.Ipc.ArrowStreamReader"/> fall back to the managed heap (where a large
/// enough batch lands on the LOH). Each test uses a fresh allocator instance, never
/// <see cref="PooledNativeAllocator.Shared"/>, so rented/pooled byte counts are exact.</summary>
public sealed class DataPlaneWriteAllocatorTests
{
    private static readonly ActivitySource Source = new("test");

    [Fact]
    public async Task Write_stream_batches_use_pooled_native_memory_and_are_returned_after_each_batch()
    {
        using var input = await BuildWriteInputAsync(rows: 3);
        var allocator = new PooledNativeAllocator();
        var sink = new RecordingSinkWriteSession(allocator);
        var session = new WriteSessionState("session-1", "op-1", sink);
        var ticket = new WriteTicket(session, default);

        await DataPlaneListener.ServeWriteAsync(input, ticket, Source, allocator, CancellationToken.None);

        Assert.Equal(3, sink.RentedDuringCall.Count);

        // Every batch rents from the same (smallest) size class. If the previous batch's buffer were
        // not already disposed and returned to the free list by the time the next one is read, later
        // calls would observe RentedBytes climbing instead of repeating the same value -- proving
        // disposal keeps pace with reads rather than lagging behind or never happening.
        Assert.All(sink.RentedDuringCall, rented => Assert.Equal(sink.RentedDuringCall[0], rented));
        Assert.True(sink.RentedDuringCall[0] > 0);

        // Fully drained: the last batch's buffer is back in the pool -- not held by the sink (which
        // never retains it) and not leaked to the managed heap.
        Assert.Equal(0, allocator.RentedBytes);
        Assert.True(allocator.PooledBytes > 0);
        Assert.True(session.Drained.Task.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task A_sink_that_throws_mid_batch_does_not_leak_the_batch_it_was_handed()
    {
        using var input = await BuildWriteInputAsync(rows: 3);
        var allocator = new PooledNativeAllocator();
        var sink = new RecordingSinkWriteSession(allocator, failOnCall: 2);
        var session = new WriteSessionState("session-1", "op-1", sink);
        var ticket = new WriteTicket(session, default);

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            DataPlaneListener.ServeWriteAsync(input, ticket, Source, allocator, CancellationToken.None));

        // The failing batch's own buffer must still come back -- the `using` around WriteBatchAsync in
        // ServeWriteAsync disposes on every exit, including this throw -- and reading never continued
        // past it, so nothing later was rented either.
        Assert.Equal(0, allocator.RentedBytes);
        Assert.Equal(2, sink.RentedDuringCall.Count);

        // The control plane's CommitWrite awaiter must observe the same failure, not a hang or a
        // silently-committed prefix.
        var faulted = await Assert.ThrowsAsync<InvalidOperationException>(() => session.Drained.Task);
        Assert.Same(thrown, faulted);
    }

    private static async Task<MemoryStream> BuildWriteInputAsync(int rows)
    {
        var partition = new CountingPartition(rows, prior: null);
        var input = new MemoryStream();
        await DataPlaneListener.ServeReadAsync(
            input,
            new ReadTicket(
                PlainSource.RowSchema, partition, BatchOptions.Default, CancellationToken.None,
                new SyncStateCapture(), new StreamFailureCapture(), default),
            Source, CancellationToken.None);
        input.Position = 0;
        return input;
    }

    /// <summary>Records the allocator's <see cref="PooledNativeAllocator.RentedBytes"/> as observed
    /// during each call -- before this session has had any chance to dispose anything -- and, if
    /// <paramref name="failOnCall"/> matches, throws instead of returning, to model a connector that
    /// dies partway through a batch.</summary>
    private sealed class RecordingSinkWriteSession(PooledNativeAllocator allocator, int failOnCall = -1)
        : ISinkWriteSession
    {
        public List<long> RentedDuringCall { get; } = [];

        public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
        {
            RentedDuringCall.Add(allocator.RentedBytes);
            if (RentedDuringCall.Count == failOnCall)
            {
                throw new InvalidOperationException("synthetic mid-batch sink failure");
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask<WriteResult> CommitAsync(CancellationToken ct) =>
            throw new NotSupportedException("not exercised by these tests -- the data plane never commits");

        public ValueTask AbortAsync(CancellationToken ct) =>
            throw new NotSupportedException("not exercised by these tests -- the data plane never aborts");

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
