using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.DuckDb;

namespace Pz.DuckDb.Tests;

// Not parallelised with other classes: the pool thread-count assertion would see their threads.
[CollectionDefinition(nameof(GateWaitTests), DisableParallelization = true)]
public sealed class GateWaitCollection;

[Collection(nameof(GateWaitTests))]
public sealed class GateWaitTests : IDisposable
{
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(30);
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));

    public GateWaitTests() => Directory.CreateDirectory(_dir);
    public void Dispose() { try { Directory.Delete(_dir, recursive: true); } catch { } }

    private DuckSession Open() => DuckSession.Open(Path.Combine(_dir, "t.duckdb"));

    /// <summary>Takes one batch from an egress and stops advancing: the producer then blocks on its
    /// hand-off channel while still holding the gate.</summary>
    private static async Task<IAsyncEnumerator<RecordBatch>> HoldGateAsync(DuckSession duck)
    {
        await duck.ExecuteAsync("create table main.big as select range as v from range(200000)");
        var egress = duck.QueryArrowAsync("select v from main.big", targetBatchBytes: 1024).GetAsyncEnumerator();
        Assert.True(await egress.MoveNextAsync().AsTask().WaitAsync(Guard));
        egress.Current.Dispose();
        return egress;
    }

    [Fact]
    public async Task Append_with_an_already_cancelled_token_disposes_the_batch_and_leaves_the_gate_free()
    {
        await using var duck = Open();
        var schema = new Schema([new Field("x", Int64Type.Default, nullable: true)], null);
        await duck.CreateEmptyTableAsync("main.t", schema);

        var disposed = false;
        var batch = new TrackingBatch(
            schema, [new Int64Array.Builder().Append(1).Build()], 1, () => disposed = true);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => duck.AppendArrowBatchAsync("main.t", batch, ct: cts.Token).WaitAsync(Guard));

        Assert.True(disposed);
        Assert.Equal(1L, await duck.ScalarAsync<long>("select 1").WaitAsync(Guard));
        Assert.Equal(0L, await duck.ScalarAsync<long>("select count(*) from main.t").WaitAsync(Guard));
    }

    [Fact]
    public async Task Statement_cancelled_while_waiting_for_the_gate_leaves_the_gate_free()
    {
        await using var duck = Open();
        var egress = await HoldGateAsync(duck);
        try
        {
            using var cts = new CancellationTokenSource();
            var waiting = duck.ExecuteAsync("select 1", cts.Token);
            Assert.False(waiting.IsCompleted);

            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting.WaitAsync(Guard));
        }
        finally
        {
            await egress.DisposeAsync();
        }

        await duck.ExecuteAsync("select 1").WaitAsync(Guard);
        Assert.Equal(1L, await duck.ScalarAsync<long>("select 1").WaitAsync(Guard));
    }

    // Limit: this discriminates only when the queued waiters outnumber the pool's idle threads. If earlier
    // tests left 64 or more idle pool threads, blocking waiters would reuse them and the pool would not grow.
    [Fact]
    public async Task Waiting_for_the_gate_does_not_occupy_a_pool_thread()
    {
        await using var duck = Open();
        var egress = await HoldGateAsync(duck);
        try
        {
            var before = ThreadPool.ThreadCount;
            var waiters = Enumerable.Range(0, 64).Select(_ => duck.ExecuteAsync("select 1")).ToArray();

            // Blocking waiters would each pin a pool thread, so this probe could only run once the
            // pool injected a thread for it, and the pool would have grown to serve the waiters.
            var probe = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _ = Task.Run(() => probe.SetResult());
            await probe.Task.WaitAsync(Guard);
            var growth = ThreadPool.ThreadCount - before;

            Assert.All(waiters, w => Assert.False(w.IsCompleted));
            Assert.True(growth < 16, $"pool grew by {growth} threads while 64 callers waited for the gate");

            await egress.DisposeAsync();
            await Task.WhenAll(waiters).WaitAsync(Guard);
        }
        finally
        {
            await egress.DisposeAsync();
        }
    }

    private sealed class TrackingBatch(Schema schema, IEnumerable<IArrowArray> arrays, int length, Action onDispose)
        : RecordBatch(schema, arrays, length)
    {
        protected override void Dispose(bool disposing)
        {
            onDispose();
            base.Dispose(disposing);
        }
    }
}
