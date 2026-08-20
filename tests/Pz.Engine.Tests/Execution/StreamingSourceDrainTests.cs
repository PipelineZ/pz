using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Dispatch;

namespace Pz.Engine.Tests.Execution;

/// <summary><see cref="SourceLoadExecutor"/> drains an <see cref="IStreamingSource"/> lazily under a
/// concurrency cap, while a plain <see cref="ISource"/> keeps the list-based path. These three tests
/// pin the load-bearing properties: laziness (the async
/// enumerable is never materialized up front), the non-streaming regression guard, and fail-fast (a
/// faulting partition cancels siblings and fails the run without hanging). All gate-based — no wall-clock
/// sleeps.</summary>
public sealed class StreamingSourceDrainTests : IAsyncLifetime
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
        reg.AddSource("streamstub", connector);
        return new RunContext(_duck, reg, new RunPaths(_dir, "test-run"), NullRunEvents.Instance);
    }

    private static DagNode Node()
    {
        var source = new ConnectionDef("mem", "streamstub", new Dictionary<string, object?>(),
            [new DatasetDef("numbers", new Dictionary<string, object?>(), null)], "sources/mem.yml");
        return new DagNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_mem__numbers",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    /// <summary>Lazy async enumerable over a fixed partition list: increments <paramref name="onProduce"/>
    /// just before each partition is yielded, so the test can observe how many partitions the pump has
    /// actually pulled at any moment. Honors <paramref name="ct"/> so a sibling fault (which cancels the
    /// pump's token) tears down enumeration too, exactly like a real streaming source would.</summary>
    private static async IAsyncEnumerable<IDatasetPartition> Lazily(
        IReadOnlyList<IDatasetPartition> partitions, Action onProduce, [EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var partition in partitions)
        {
            await Task.Yield();
            ct.ThrowIfCancellationRequested();
            onProduce();
            yield return partition;
        }
    }

    [Fact]
    public async Task Streaming_source_is_drained_lazily_and_all_rows_land()
    {
        // The engine caps in-flight streaming partition reads at Environment.ProcessorCount (the same value
        // the executor uses). With p slots and n = 3p partitions that each park in ReadAsync until released,
        // a LAZY, gated drain pulls at most p+1 partitions from the enumerable before the (p+1)-th gate
        // WaitAsync blocks -- so `produced` sits at <= p+1 while p partitions are in flight. An EAGER drain
        // (materializing the whole enumerable up front, as the list path does via PlanReadAsync) would have
        // already pulled all n before reading any, making `produced == n` observable here -- which is the
        // regression this test exists to catch.
        var p = Environment.ProcessorCount;
        var n = p * 3;
        var produced = 0;
        var started = 0;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gateFull = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var partitions = new List<IDatasetPartition>(n);
        for (var i = 0; i < n; i++)
        {
            partitions.Add(new StubPartition(i,
                onRead: () => { if (Interlocked.Increment(ref started) == p) gateFull.TrySetResult(); },
                gate: ct => release.Task.WaitAsync(ct)));
        }

        var source = new StreamingStubSource(ct => Lazily(partitions, () => Interlocked.Increment(ref produced), ct));
        var ctx = Context(new StreamingStubConnector(source, ConnectorCapabilities.StreamingPartitions));

        var execute = new KindDispatchingExecutor().ExecuteAsync(Node(), ctx, default);

        // Wait until exactly a gate's worth of partitions are actively being read (all slots occupied).
        await gateFull.Task.WaitAsync(TimeSpan.FromSeconds(30));
        var producedAtGate = Volatile.Read(ref produced);
        Assert.True(producedAtGate <= p + 1,
            $"expected a lazy drain to have pulled at most {p + 1} partitions while {p} are in flight, but " +
            $"{producedAtGate} of {n} were already produced -- the async enumerable was materialized eagerly");
        Assert.True(producedAtGate < n,
            $"expected fewer than all {n} partitions to be produced, but saw {producedAtGate} -- not lazy");

        release.SetResult();
        var result = await execute.WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal((long)n, result.RowsMoved);
        Assert.Equal((long)n, await _duck.ScalarAsync<long>("select count(*) from staging.src_mem__numbers"));
    }

    [Fact]
    public async Task Non_streaming_source_drains_via_PlanReadAsync()
    {
        // Regression guard: a plain ISource (no IStreamingSource) must keep the existing list-based path --
        // PlanReadAsync is called exactly once and every partition's rows land.
        var partitions = new List<IDatasetPartition> { new StubPartition(1), new StubPartition(2), new StubPartition(3) };
        var source = new NonStreamingStubSource(partitions);
        var ctx = Context(new StreamingStubConnector(source, ConnectorCapabilities.None));

        var result = await new KindDispatchingExecutor().ExecuteAsync(Node(), ctx, default)
            .WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(3, result.RowsMoved);
        Assert.Equal(1, source.PlanReadCalls);
        Assert.Equal(3, await _duck.ScalarAsync<long>("select count(*) from staging.src_mem__numbers"));
    }

    [Fact]
    public async Task Streaming_partition_fault_cancels_siblings_and_fails_fast()
    {
        // The first partition faults on read; the rest block in ReadAsync on a release that never comes but
        // honors ct -- so they can only unwind via the pump's cancellation. If the faulting partition did
        // not cancel its siblings, any sibling that started would block forever and this test would hang
        // (the 15s timeout would trip). Completing with a Failed result therefore proves fail-fast:
        // siblings are torn down and the genuine fault surfaces, never a masking cancellation.
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var partitions = new List<IDatasetPartition> { new StubPartition(0, fault: true) };
        for (var i = 1; i <= 4; i++)
        {
            partitions.Add(new StubPartition(i, gate: ct => release.Task.WaitAsync(ct)));
        }

        var source = new StreamingStubSource(ct => Lazily(partitions, () => { }, ct));
        var ctx = Context(new StreamingStubConnector(source, ConnectorCapabilities.StreamingPartitions));

        var execute = new KindDispatchingExecutor().ExecuteAsync(Node(), ctx, default);
        var result = await execute.WaitAsync(TimeSpan.FromSeconds(15));

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Error);
        Assert.Equal("PZ0501", result.Error!.Code);
    }

    [Fact]
    public async Task Streaming_enumerator_fault_cancels_siblings_and_fails_fast()
    {
        // Distinct from a single partition's ReadAsync faulting (covered by
        // Streaming_partition_fault_cancels_siblings_and_fails_fast above), this fault comes from the async
        // ENUMERATOR itself throwing mid-enumeration (e.g. a connector's listing-page failure).
        // PumpPartitionAsync's catch never runs for this kind of fault, so PumpStreamingPartitionsAsync's
        // own catch must cancel pumpCts first -- otherwise the already-launched sibling below (blocked in
        // ReadAsync on a ct-honoring gate that never releases) runs forever and this test's bounded
        // WaitAsync trips.
        //
        // Deliberately exactly ONE blocking partition, not several: the enumerator (below) can only reach its
        // post-loop throw once the consumer's `await foreach` has pulled every yielded partition through a
        // *successful* `gate.WaitAsync` -- with more than one blocking partition that would require the gate
        // (sized Environment.ProcessorCount) to admit all of them, which hangs on a low-core-count runner. One
        // partition always fits (the gate always starts with capacity >= 1), so this stays correct at any
        // Environment.ProcessorCount while still exercising a real already-launched, blocked sibling task.
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var blocking = new List<IDatasetPartition>
        {
            new StubPartition(0, onRead: () => firstStarted.TrySetResult(), gate: ct => release.Task.WaitAsync(ct)),
        };

        var source = new StreamingStubSource(ct => EnumeratorFaultsAfterYielding(blocking, firstStarted.Task, ct));
        var ctx = Context(new StreamingStubConnector(source, ConnectorCapabilities.StreamingPartitions));

        var execute = new KindDispatchingExecutor().ExecuteAsync(Node(), ctx, default);
        var result = await execute.WaitAsync(TimeSpan.FromSeconds(15));

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Error);
    }

    /// <summary>Yields every partition in <paramref name="partitions"/> (each of which blocks in
    /// <c>ReadAsync</c> until its own gate is released -- never here), then, once <paramref name="firstStarted"/>
    /// signals that at least one already-yielded partition's read has actually started (proving one or more
    /// siblings are now in flight and pending on the pump), throws a non-OCE fault from the enumerator itself --
    /// distinct from any partition's own <c>ReadAsync</c> faulting.</summary>
    private static async IAsyncEnumerable<IDatasetPartition> EnumeratorFaultsAfterYielding(
        IReadOnlyList<IDatasetPartition> partitions, Task firstStarted, [EnumeratorCancellation] CancellationToken ct)
    {
        foreach (var partition in partitions)
        {
            await Task.Yield();
            ct.ThrowIfCancellationRequested();
            yield return partition;
        }

        await firstStarted.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
        throw new InvalidOperationException("injected streaming enumerator failure (not a partition ReadAsync fault)");
    }

    [Fact]
    public async Task Streaming_source_with_many_partitions_beyond_concurrency_cap_lands_all_rows()
    {
        // Functional proof, not a memory probe: drives N partitions, N well beyond
        // Environment.ProcessorCount, through the gated drain so the tasks-list prune
        // (`tasks.RemoveAll(t => t.IsCompletedSuccessfully)` in PumpStreamingPartitionsAsync) fires
        // repeatedly on the happy path. All N partitions complete near-instantly (no gate/park), so the
        // prune should reclaim virtually every completed task as the drain proceeds. This asserts every
        // partition's row still lands and the run succeeds -- proving the prune never drops unobserved work.
        // Exact bounded retention (tasks staying at ~ProcessorCount entries) is deliberately NOT asserted
        // by a memory-size probe here, since that would be flaky.
        var n = Environment.ProcessorCount * 50;
        var partitions = new List<IDatasetPartition>(n);
        for (var i = 0; i < n; i++)
        {
            partitions.Add(new StubPartition(i));
        }

        var source = new StreamingStubSource(ct => Lazily(partitions, () => { }, ct));
        var ctx = Context(new StreamingStubConnector(source, ConnectorCapabilities.StreamingPartitions));

        var result = await new KindDispatchingExecutor().ExecuteAsync(Node(), ctx, default)
            .WaitAsync(TimeSpan.FromSeconds(30));

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal((long)n, result.RowsMoved);
        Assert.Equal((long)n, await _duck.ScalarAsync<long>("select count(*) from staging.src_mem__numbers"));
    }
}

/// <summary>Single int64 <c>id</c> column -- the minimal schema these stubs stage.</summary>
internal static class StubSchema
{
    public static readonly Schema IdSchema = new([new Field("id", Int64Type.Default, nullable: false)], null);

    public static RecordBatch BuildBatch(long id)
    {
        var builder = new Int64Array.Builder();
        builder.Append(id);
        return new RecordBatch(IdSchema, [builder.Build()], 1);
    }
}

/// <summary>One partition yielding a single one-row batch (id = <paramref name="id"/>). Optional hooks let
/// a test observe when the read starts (<paramref name="onRead"/>), park it until released
/// (<paramref name="gate"/>, ct-honoring), or fault it (<paramref name="fault"/>).</summary>
internal sealed class StubPartition(
    long id, Action? onRead = null, Func<CancellationToken, Task>? gate = null, bool fault = false) : IDatasetPartition
{
    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        await Task.Yield();
        onRead?.Invoke();
        if (gate is not null)
        {
            await gate(ct).ConfigureAwait(false);
        }

        if (fault)
        {
            throw new PzConnectorException("injected streaming partition read failure", isTransient: false);
        }

        yield return StubSchema.BuildBatch(id);
    }
}

/// <summary>Source implementing BOTH <see cref="ISource"/> and <see cref="IStreamingSource"/>: the engine
/// must prefer the streaming path, so <see cref="PlanReadAsync"/> throws to catch any fallthrough.</summary>
internal sealed class StreamingStubSource(Func<CancellationToken, IAsyncEnumerable<IDatasetPartition>> stream)
    : ISource, IStreamingSource
{
    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        new(new DatasetSchema(StubSchema.IdSchema));

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        throw new InvalidOperationException("streaming source: the engine must drain PlanReadStreamingAsync, not PlanReadAsync");

    public IAsyncEnumerable<IDatasetPartition> PlanReadStreamingAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        stream(ct);

    public ValueTask DisposeAsync() => default;
}

/// <summary>Plain <see cref="ISource"/> (no <see cref="IStreamingSource"/>) that counts PlanReadAsync
/// calls -- the non-streaming regression guard.</summary>
internal sealed class NonStreamingStubSource(IReadOnlyList<IDatasetPartition> partitions) : ISource
{
    private int _planReadCalls;

    public int PlanReadCalls => Volatile.Read(ref _planReadCalls);

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        new(new DatasetSchema(StubSchema.IdSchema));

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        Interlocked.Increment(ref _planReadCalls);
        return new(partitions);
    }

    public ValueTask DisposeAsync() => default;
}

/// <summary>Source connector returning a pre-built source, advertising a test-chosen capability set --
/// lets a test flip <see cref="ConnectorCapabilities.StreamingPartitions"/> on or off.</summary>
internal sealed class StreamingStubConnector(ISource source, ConnectorCapabilities capabilities) : ISourceConnector
{
    public ConnectorInfo Info => new("streamstub", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => capabilities;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true));

    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(source);
}
