using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Resilience;

namespace Pz.Engine.Tests.Execution;

/// <summary>Partition fault isolation (a connector-side read fault records and
/// lets siblings finish), aggregated node failure (every isolated fault becomes ONE thrown
/// <see cref="PzConnectorException"/>), the engine-fault self-cancel promptness
/// guarantee, the sync done-skip exception, and streaming sources joining partition mode.
/// Shares <see cref="IdentifiedStubPartition"/>/<see cref="ListStubSource"/>/<see cref="ListStubConnector"/>
/// (from <see cref="PartitionModeTests"/>) and <see cref="StreamingStubSource"/>/
/// <see cref="StreamingStubConnector"/>/<see cref="StubPartition"/>/<see cref="StubSchema"/> (from
/// <see cref="StreamingSourceDrainTests"/>) — all already <c>internal</c> in this same assembly/namespace,
/// so no promotion to a separate stubs file was needed to reuse them here.
///
/// Collected into <c>partition-fault-timing</c> (see
/// <see cref="TimingSensitiveCollection"/>): <see cref="Engine_fault_in_one_partition_cancels_gated_sibling_promptly"/>
/// asserts wall-clock promptness of a self-cancel under a bounded wait, which needs the ThreadPool free
/// of full-suite pressure to be reliable -- kept on the whole class since every fact here shares the
/// DuckSession-backed fixture and none of the others are parallelism-sensitive enough to be worth
/// splitting out.</summary>
[Collection("partition-fault-timing")]
public sealed class PartitionFaultTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private RunContext Context(ISourceConnector connector)
    {
        var reg = new ConnectorRegistry();
        reg.AddSource("liststub", connector);
        reg.AddSource("streamstub", connector);
        return new RunContext(_duck, reg, new RunPaths(_dir, "test-run"), NullRunEvents.Instance);
    }

    // An explicitly declared `sync: {mode: auto}` block. Combined with a feed-shaped source stub
    // (ListStubSource's feedShaped: true), ReadShapeResolver resolves this to Feed, which is what
    // engages the sync-state machinery.
    private static DagNode Node(bool isSync = false)
    {
        var source = new ConnectionDef("mem", "liststub", new Dictionary<string, object?>(),
            [new DatasetDef("numbers", new Dictionary<string, object?>(), null,
                isSync ? new SyncModeDef(SyncMode.Auto, null) : null)], "sources/mem.yml");
        return new DagNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_mem__numbers",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    private static KindDispatchingExecutor RetryTestExecutor(int maxAttempts) =>
        new(new RetryPolicy(maxAttempts, TimeSpan.Zero, TimeSpan.Zero), new FixedRandom(0.5),
            delay: (_, _) => Task.CompletedTask);

    [Fact]
    public async Task Sibling_completes_when_one_partition_faults()
    {
        var release = new TaskCompletionSource();
        var faulted = new TaskCompletionSource();
        var source = new ListStubSource(
        [
            // 'slow' parks until 'bad' has faulted, then completes — proving the fault did not
            // cancel it -- faults are isolated per partition.
            new IdentifiedStubPartition("slow", [1, 2], gate: async ct =>
            {
                await faulted.Task.WaitAsync(ct);
                await release.Task.WaitAsync(ct);
            }),
            new IdentifiedStubPartition("bad", [9], fault: () =>
            {
                faulted.TrySetResult();
                return new PzConnectorException("boom", isTransient: true, retryAfter: TimeSpan.FromSeconds(7));
            }),
        ]);
        var ctx = Context(new ListStubConnector(source, ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds));

        var pending = new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None);
        await faulted.Task.WaitAsync(TimeSpan.FromSeconds(180));
        release.SetResult();

        var thrown = await Assert.ThrowsAsync<PzConnectorException>(() => pending.WaitAsync(TimeSpan.FromSeconds(180)));
        Assert.True(thrown.IsTransient);
        Assert.Equal(TimeSpan.FromSeconds(7), thrown.RetryAfter);
        Assert.StartsWith("1 of 2 partitions failed; first: boom", thrown.Message, StringComparison.Ordinal);
        Assert.Equal(1L, await _duck.ScalarAsync<long>(
            "select count(*) from pz_meta.partitions_done where node_id = 'aaaaaaaaaaaaaaaa'", default));
        Assert.Equal(2L, await _duck.ScalarAsync<long>("select count(*) from staging.src_mem__numbers", default));
    }

    [Fact]
    public async Task Aggregate_is_transient_only_when_every_failure_is()
    {
        var source = new ListStubSource(
        [
            new IdentifiedStubPartition("t", [1], fault: () => new PzConnectorException("t", isTransient: true)),
            new IdentifiedStubPartition("p", [2], fault: () => new PzConnectorException("p", isTransient: false)),
        ]);
        var ctx = Context(new ListStubConnector(source, ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds));

        var thrown = await Assert.ThrowsAsync<PzConnectorException>(
            () => new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None));
        Assert.False(thrown.IsTransient);
        Assert.StartsWith("2 of 2 partitions failed", thrown.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Foreign_read_exception_aggregates_as_non_transient()
    {
        var source = new ListStubSource(
            [new IdentifiedStubPartition("x", [1], fault: () => new InvalidOperationException("weird"))]);
        var ctx = Context(new ListStubConnector(source, ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds));

        var thrown = await Assert.ThrowsAsync<PzConnectorException>(
            () => new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None));
        Assert.False(thrown.IsTransient);
    }

    [Fact]
    public async Task Run_cancellation_propagates_as_cancellation()
    {
        using var cts = new CancellationTokenSource();
        var started = new TaskCompletionSource();
        var source = new ListStubSource(
        [
            new IdentifiedStubPartition("x", [1], gate: async ct =>
            {
                started.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }),
        ]);
        var ctx = Context(new ListStubConnector(source, ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds));

        var pending = new SourceLoadExecutor().ExecuteAsync(Node(), ctx, cts.Token);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(180));
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending.WaitAsync(TimeSpan.FromSeconds(180)));
    }

    [Fact]
    public async Task Node_attempt_loop_retries_the_aggregate_and_second_attempt_succeeds()
    {
        var failOnce = true;
        var aReads = 0;
        var source = new ListStubSource(
        [
            new IdentifiedStubPartition("a", [1, 2], onRead: () => aReads++),
            new IdentifiedStubPartition("b", [3], fault: () =>
            {
                if (!failOnce) { return null; }
                failOnce = false;
                return new PzConnectorException("blip", isTransient: true);
            }),
        ]);
        var ctx = Context(new ListStubConnector(source, ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds));

        // Zero-delay attempt loop: follow the ctor/delay-injection pattern the existing
        // KindDispatchingExecutor retry tests use (RetryingExecutionTests' NoDelayExecutor +
        // FixedRandom(0.5)).
        var executor = RetryTestExecutor(maxAttempts: 2);
        var result = await executor.ExecuteAsync(Node(), ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(3, result.RowsMoved);
        Assert.Equal(1, aReads);
    }

    [Fact]
    public async Task Streaming_partitions_join_partition_mode_with_isolation()
    {
        var source = new StreamingStubSource(ct => Stream(ct));
        var ctx = Context(new StreamingStubConnector(source,
            ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StreamingPartitions | ConnectorCapabilities.StablePartitionIds));

        var thrown = await Assert.ThrowsAsync<PzConnectorException>(
            () => new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None));
        Assert.StartsWith("1 of 3 partitions failed", thrown.Message, StringComparison.Ordinal);
        Assert.Equal(2L, await _duck.ScalarAsync<long>(
            "select count(*) from pz_meta.partitions_done where node_id = 'aaaaaaaaaaaaaaaa'", default));

        static async IAsyncEnumerable<IDatasetPartition> Stream([EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();
            yield return new IdentifiedStubPartition("s1", [1]);
            yield return new IdentifiedStubPartition("s2", [2], fault: () => new PzConnectorException("x", isTransient: true));
            yield return new IdentifiedStubPartition("s3", [3]);
        }
    }

    [Fact]
    public async Task Streaming_run_cancellation_before_any_admission_propagates_as_cancellation()
    {
        // Regression guard: if the run's own `ct` is cancelled while the streaming enumerator itself
        // is still blocked (zero partitions admitted yet), `tasks` is empty -- Task.WhenAll([]) does
        // NOT throw, so a naive "swallow the enumerator's OCE, then await Task.WhenAll(tasks)" would
        // silently turn a genuine cancellation into a spurious success. This must propagate as
        // cancellation instead, exactly like the list path's Run_cancellation_propagates_as_cancellation.
        using var cts = new CancellationTokenSource();
        var started = new TaskCompletionSource();
        var source = new StreamingStubSource(ct => Stream(ct));
        var ctx = Context(new StreamingStubConnector(source,
            ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StreamingPartitions | ConnectorCapabilities.StablePartitionIds));

        var pending = new SourceLoadExecutor().ExecuteAsync(Node(), ctx, cts.Token);
        await started.Task.WaitAsync(TimeSpan.FromSeconds(180));
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending.WaitAsync(TimeSpan.FromSeconds(180)));

        async IAsyncEnumerable<IDatasetPartition> Stream([EnumeratorCancellation] CancellationToken ct)
        {
            started.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            yield break;
        }
    }

    [Fact]
    public async Task Streaming_identity_violation_fails_with_PZ0319()
    {
        var source = new StreamingStubSource(ct => Stream(ct));
        var ctx = Context(new StreamingStubConnector(source,
            ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StreamingPartitions | ConnectorCapabilities.StablePartitionIds));

        var result = await new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None);
        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal("PZ0319", result.Error!.Code);

        static async IAsyncEnumerable<IDatasetPartition> Stream([EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();
            yield return new StubPartition(1); // not IIdentifiedPartition
        }
    }

    // --- Engine-fault teardown must be genuinely prompt. ---

    [Fact]
    public async Task Engine_fault_in_one_partition_cancels_gated_sibling_promptly()
    {
        // 'bad' yields a batch whose schema deliberately mismatches the part table DuckDB actually
        // created (an extra column) -- this is a genuine ENGINE-side fault (duckdb_append_data_chunk
        // rejecting the chunk), NOT a connector-authored `fault:` exception, so it exercises
        // RunPartitionAsync's self-cancelling outer catch rather than the isolated read-fault path.
        // 'slow' parks on a TaskCompletionSource that the test never releases -- the ONLY way its gate
        // unblocks is loadCts being cancelled from inside 'bad's task the moment the append fails. If
        // that self-cancel did not happen promptly (i.e. only after Task.WhenAll observed the fault),
        // this test would hang until its own bounded wait trips.
        var neverReleased = new TaskCompletionSource();
        var gateObservedCancellation = new TaskCompletionSource();
        // Ordering gate: 'bad' must not yield its
        // poisoned batch until 'slow' has actually PARKED inside its read gate. Without this, a
        // loaded dispatcher can run 'bad' to its append failure while 'slow' is still in its
        // part-table DDL — the cancellation then surfaces from duck.ExecuteAsync instead of the
        // gate delegate, gateObservedCancellation never resolves, and the test times out at ANY
        // bound. Gate-based determinism, not timing assumptions (house rule).
        var slowParked = new TaskCompletionSource();
        var source = new ListStubSource(
        [
            new IdentifiedStubPartition("slow", [1], gate: async ct =>
            {
                slowParked.TrySetResult();
                try
                {
                    await neverReleased.Task.WaitAsync(ct);
                }
                catch (OperationCanceledException)
                {
                    gateObservedCancellation.TrySetResult();
                    throw;
                }
            }),
            new SchemaMismatchStubPartition("bad", gate: ct => slowParked.Task.WaitAsync(ct)),
        ]);
        var ctx = Context(new ListStubConnector(source, ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds));

        var pending = new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None);

        // Promptness proof: 'slow's gate awaits a TaskCompletionSource this test never completes --
        // 'gateObservedCancellation' can resolve ONLY via loadCts being cancelled from INSIDE 'bad's
        // task the moment its append fails, never on its own. This bounded wait resolving at all
        // (instead of throwing WaitAsync's own TimeoutException) IS the promptness assertion; a
        // fail-fast that only cancelled once every task had ALREADY finished -- impossible while 'slow'
        // is still parked -- would hang until the wait's own timeout fired. Do NOT weaken this to
        // `ThrowsAnyAsync<Exception>` on the wait itself: that very TimeoutException satisfies it,
        // making the test vacuous. Bound VERY generously (not tightly timed): the discrimination is
        // finite-vs-infinite (broken code parks 'slow' forever), and whole-solution runs execute ~16
        // test processes concurrently — observed
        // ~19x wall-clock dilation on this assembly (38s vs 2s solo) put a 30s bound right at the
        // contention edge (in-assembly serialization via the partition-fault-timing collection cannot
        // shield against cross-process CPU contention).
        await gateObservedCancellation.Task.WaitAsync(TimeSpan.FromSeconds(180));

        // Promptness is now proven; this just drains the (by now essentially finished) node attempt and
        // asserts WHAT it throws: the raw ENGINE fault (duckdb_append_data_chunk rejecting 'bad's
        // mismatched chunk), never a TimeoutException (which would mean this bound -- not the self-cancel
        // above -- was what let the node attempt finish) and never the isolated-failure aggregate.
        var thrown = await Assert.ThrowsAnyAsync<Exception>(() => pending.WaitAsync(TimeSpan.FromSeconds(180)));
        var engineFault = Assert.IsType<InvalidOperationException>(thrown);
        Assert.Contains("duckdb_append_data_chunk", engineFault.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Mixed_fault_surfaces_engine_error_and_notices_recorded_failures()
    {
        // An engine-side fault self-cancelling loadCts must not let an
        // ALREADY-recorded sibling read failure vanish without a trace. 'read-fault' isolates a
        // connector-authored fault (recorded into `failures`, its task then completes normally) BEFORE
        // 'engine-fault' is allowed to proceed to its schema-mismatched append -- the TCS handshake
        // below sequences that ordering deterministically, no sleeps.
        var readFaultRecorded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var notices = new List<string>();
        var source = new ListStubSource(
        [
            new IdentifiedStubPartition("read-fault", [1], fault: () =>
            {
                var ex = new PzConnectorException("boom", isTransient: true);
                readFaultRecorded.TrySetResult();
                return ex;
            }),
            new SchemaMismatchStubPartition("engine-fault", gate: ct => readFaultRecorded.Task.WaitAsync(ct)),
        ]);
        var connector = new ListStubConnector(source, ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds);
        var ctx = Context(connector) with { Notice = notices.Add };

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None));

        // The propagated exception is the raw ENGINE fault -- never the isolated-failure aggregate
        // (which would be a PzConnectorException reading "N of M partitions failed").
        Assert.Contains("duckdb_append_data_chunk", thrown.Message, StringComparison.Ordinal);

        // ...but the read-fault recorded before teardown was not silently discarded: it surfaced as
        // exactly one notice.
        var notice = Assert.Single(notices);
        Assert.Contains("partition failure(s) were recorded", notice, StringComparison.Ordinal);
        Assert.Contains("boom", notice, StringComparison.Ordinal);
    }

    // --- Sync datasets never skip-reuse a done partition. ---

    [Fact]
    public async Task Sync_dataset_never_skip_reuses_a_done_partition()
    {
        var reads = 0;
        var source = new ListStubSource(
            [new IdentifiedStubPartition("only", [1, 2], onRead: () => reads++)], feedShaped: true);
        var connector = new ListStubConnector(source, ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds);
        var ctx = Context(connector);
        var node = Node(isSync: true);

        var first = await new SourceLoadExecutor().ExecuteAsync(node, ctx, CancellationToken.None);
        Assert.Equal(NodeStatus.Success, first.Status);
        Assert.Equal(1, reads);
        Assert.Equal(1L, await _duck.ScalarAsync<long>(
            "select count(*) from pz_meta.partitions_done where node_id = 'aaaaaaaaaaaaaaaa'", default));

        // A node-attempt-loop retry (same node id, same run) re-enters the executor: the partition
        // is already `done`, but being a sync dataset it must be reset and re-read live -- never
        // skip-reused -- so the read counter increments again.
        var second = await new SourceLoadExecutor().ExecuteAsync(node, ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, second.Status);
        Assert.Equal(2, reads);
        Assert.Equal(2L, second.RowsMoved);
        Assert.Equal(2L, await _duck.ScalarAsync<long>("select count(*) from staging.src_mem__numbers", default));

        // No duplicate rows and no duplicate/stale ledger entries -- exactly one done row survives.
        Assert.Equal(1L, await _duck.ScalarAsync<long>(
            "select count(*) from pz_meta.partitions_done where node_id = 'aaaaaaaaaaaaaaaa'", default));
    }
}

/// <summary>Partition whose batch DELIBERATELY mismatches the part table's declared schema (an extra
/// column) -- triggers a genuine DuckDB engine-side APPEND failure (duckdb_append_data_chunk), distinct
/// from a connector-authored <c>fault:</c> exception, so a test built on this stub exercises
/// <c>PartitionModeLoader.RunPartitionAsync</c>'s self-cancelling ENGINE-fault path rather than the
/// isolated connector read-fault path.</summary>
internal sealed class SchemaMismatchStubPartition(string id, Func<CancellationToken, Task>? gate = null) : IIdentifiedPartition
{
    public string PartitionId => id;

    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options,
        [EnumeratorCancellation] CancellationToken ct)
    {
        await Task.Yield();
        if (gate is not null)
        {
            await gate(ct).ConfigureAwait(false);
        }

        var schema = new Schema([new Field("id", Int64Type.Default, nullable: false),
            new Field("unexpected_extra_column", Int64Type.Default, nullable: false)], null);
        var idBuilder = new Int64Array.Builder();
        idBuilder.Append(1);
        var extraBuilder = new Int64Array.Builder();
        extraBuilder.Append(2);
        yield return new RecordBatch(schema, [idBuilder.Build(), extraBuilder.Build()], 1);
    }
}
