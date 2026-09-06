using Apache.Arrow.Ipc;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

public sealed class SyncStateCaptureTests
{
    [Fact]
    public async Task A_clean_drain_captures_the_token_once_before_end_of_stream()
    {
        var partition = new SyncPartition(3, prior: "0+3");
        var capture = new SyncStateCapture();
        using var stream = new MemoryStream();

        await DataPlaneListener.ServeReadAsync(
            stream, new ReadTicket(PlainSource.RowSchema, partition, BatchOptions.Default, CancellationToken.None, capture),
            CancellationToken.None);

        Assert.True(capture.TryGet(out var token));
        Assert.Equal("0+3+3", token);
        Assert.Equal(1, partition.Polls);

        // The stream is a complete Arrow IPC stream: schema, three batches, end-of-stream.
        stream.Position = 0;
        using var reader = new ArrowStreamReader(stream);
        var batches = 0;
        while (await reader.ReadNextRecordBatchAsync() is { } batch)
        {
            batch.Dispose();
            batches++;
        }

        Assert.Equal(3, batches);
    }

    [Fact]
    public async Task A_partition_that_throws_mid_stream_captures_nothing()
    {
        var partition = new ThrowingSyncPartition();
        var capture = new SyncStateCapture();
        using var stream = new MemoryStream();

        await Assert.ThrowsAsync<InvalidOperationException>(() => DataPlaneListener.ServeReadAsync(
            stream, new ReadTicket(PlainSource.RowSchema, partition, BatchOptions.Default, CancellationToken.None, capture),
            CancellationToken.None));

        Assert.False(capture.TryGet(out _));
        Assert.Equal(0, partition.Polls);
    }

    [Fact]
    public async Task A_cancelled_drain_captures_nothing()
    {
        using var cts = new CancellationTokenSource();
        var partition = new SyncPartition(3, prior: null);
        var capture = new SyncStateCapture();
        cts.Cancel();
        using var stream = new MemoryStream();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => DataPlaneListener.ServeReadAsync(
            stream, new ReadTicket(PlainSource.RowSchema, partition, BatchOptions.Default, cts.Token, capture), cts.Token));

        Assert.False(capture.TryGet(out _));
    }

    [Fact]
    public async Task A_plain_partition_is_never_polled()
    {
        var partition = new CountingPartition(2, prior: null);
        var capture = new SyncStateCapture();
        using var stream = new MemoryStream();

        await DataPlaneListener.ServeReadAsync(
            stream, new ReadTicket(PlainSource.RowSchema, partition, BatchOptions.Default, CancellationToken.None, capture),
            CancellationToken.None);

        Assert.False(capture.TryGet(out _));
    }

    private sealed class ThrowingSyncPartition : IDatasetPartition, ISyncStatePartition
    {
        public int Polls;

        public async IAsyncEnumerable<Apache.Arrow.RecordBatch> ReadAsync(
            BatchOptions options, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();
            yield return new Apache.Arrow.RecordBatch(
                PlainSource.RowSchema, [new Apache.Arrow.Int64Array.Builder().Append(1).Build()], 1);
            throw new InvalidOperationException("torn");
        }

        public bool TryGetSyncStateCandidate(out string? candidate)
        {
            Polls++;
            candidate = "never";
            return true;
        }
    }
}
