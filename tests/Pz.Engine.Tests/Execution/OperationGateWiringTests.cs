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
using Pz.Engine.Dispatch;

namespace Pz.Engine.Tests.Execution;

/// <summary>Proves the engine actually hands a gate-aware connector its
/// <see cref="OperationGate"/> at the two documented sites -- immediately after
/// <c>OpenAsync</c>, before any plan/read call -- and that the returned <see cref="OpStats"/> lands on
/// <see cref="NodeResult.Ops"/> ONLY for the universal-tier success path. Every test here uses a minimal
/// gate-aware fake source in-file (mirroring <c>RetryingExecutionTests</c>/<c>StreamingSourceDrainTests</c>'
/// stub-connector arrangement) rather than the shared <c>InMemorySource</c> reference connector, since
/// that connector has no gate-aware variant and this task must not touch it.</summary>
public sealed class OperationGateWiringTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;
    private RecordingEvents _events = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
        _events = new RecordingEvents();
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private RunContext Context(ISourceConnector connector, RateLimiterRegistry? rateLimiters = null)
    {
        var reg = new ConnectorRegistry();
        reg.AddSource("gatestub", connector);
        return new RunContext(_duck, reg, new RunPaths(_dir, "test-run"), _events, RateLimiters: rateLimiters);
    }

    private static DagNode Node(string dataset = "numbers", RetryDef? datasetRetry = null)
    {
        var source = new ConnectionDef("gatemem", "gatestub", new Dictionary<string, object?>(),
            [new DatasetDef(dataset, new Dictionary<string, object?>(), null, Retry: datasetRetry)],
            "sources/gatemem.yml");
        return new DagNode(new NodeId("eeeeeeeeeeeeeeee"), NodeKind.SourceLoad, $"src_gatemem__{dataset}",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    [Fact]
    public async Task Gate_reaches_gate_aware_source()
    {
        var source = new GateAwareFakeSource();
        var ctx = Context(new GateStubConnector(source), new RateLimiterRegistry(TimeProvider.System));

        var result = await new KindDispatchingExecutor().ExecuteAsync(Node(), ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.NotNull(source.ObservedGate);
        Assert.Equal(new OpStats(1, 0, 0), result.Ops);
    }

    [Fact]
    public async Task Non_gate_aware_source_untouched()
    {
        var source = new PlainFakeSource();
        var ctx = Context(new GateStubConnector(source), new RateLimiterRegistry(TimeProvider.System));

        var result = await new KindDispatchingExecutor().ExecuteAsync(Node(), ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Null(result.Ops);
        Assert.Equal(1, source.OpRuns);
    }

    [Fact]
    public async Task Null_registry_still_gates()
    {
        var source = new GateAwareFakeSource();
        var ctx = Context(new GateStubConnector(source)); // RateLimiters left at its test default: null

        Assert.Null(ctx.RateLimiters);

        var result = await new KindDispatchingExecutor().ExecuteAsync(Node(), ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.NotNull(source.ObservedGate); // gate still created and handed over
        Assert.Equal(new OpStats(1, 0, 0), result.Ops);
    }

    [Fact]
    public async Task Op_exhaustion_is_one_node_failure()
    {
        // Zero delay on both sides: the node-level delay is the injected no-op below; the op-level gate
        // (constructed inside SourceLoadExecutor with the real Task.Delay -- see its doc comment) is kept
        // fast by a zero base/max delay on the resolved policy, not by a test seam.
        var source = new GateAwareFakeSource(alwaysFailTransient: true);
        var ctx = Context(new GateStubConnector(source));
        var node = Node(datasetRetry: new RetryDef(2, TimeSpan.Zero, TimeSpan.Zero));
        var executor = new KindDispatchingExecutor(jitter: new FixedRandom(0.5), delay: (_, _) => Task.CompletedTask);

        var result = await executor.ExecuteAsync(node, ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Equal("PZ0501", result.Error!.Code);
        Assert.Equal(4, source.OpRuns); // 2 op attempts x 2 node attempts
        Assert.Single(_events.RetryScheduledCalls);
    }

    private sealed record RetryCall(int Attempt, int MaxAttempts, TimeSpan Delay, string Reason);

    private sealed class RecordingEvents : IRunEvents
    {
        public List<RetryCall> RetryScheduledCalls { get; } = [];

        public void RunStarted(string runId, string projectName, int nodeCount) { }
        public void NodeStarted(DagNode node) { }
        public void NodeProgress(DagNode node, long rowsSoFar, long bytesSoFar, long batchesSoFar) { }

        public void RetryScheduled(DagNode node, int attempt, int maxAttempts, TimeSpan delay, string reason) =>
            RetryScheduledCalls.Add(new RetryCall(attempt, maxAttempts, delay, reason));

        public void BreakerStateChanged(string instance, string oldState, string newState, string trigger,
            TimeSpan coolDown) { }

        public void SourceDriftDetected(DagNode node, string connection, string entity, string policy,
            IReadOnlyList<Pz.Engine.State.SchemaDriftDiffer.Change> changes,
            IReadOnlyList<Pz.Engine.State.SchemaColumn> observed, string hintsHash) { }
        public void MergeKeyDuplicatesDetected(DagNode node, string output, IReadOnlyList<string> keys,
            long duplicateGroups, long extraRows) { }
        public void LossyIntegerInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns) { }
        public void AmbiguousDateInferenceDetected(DagNode node, string connection, string entity,
            IReadOnlyList<string> columns, string format) { }

        public void NodeCompleted(NodeResult result) { }
        public void RunCompleted(string runId, RunStatus status, int succeeded, int failed, int skipped, TimeSpan duration) { }
    }
}

/// <summary>Single int64 <c>id</c> column -- the minimal schema the fakes below stage.</summary>
internal static class GateFakeSchema
{
    public static readonly Schema Schema = new([new Field("id", Int64Type.Default, nullable: false)], null);

    public static RecordBatch BuildBatch(long id)
    {
        var builder = new Int64Array.Builder();
        builder.Append(id);
        return new RecordBatch(Schema, [builder.Build()], 1);
    }
}

/// <summary>Source connector wrapping a pre-built <see cref="ISource"/> -- mirrors
/// <c>StreamingSourceDrainTests.StreamingStubConnector</c>'s idiom. Declares
/// <see cref="ConnectorCapabilities.GatedOperations"/> unconditionally; whether the wrapped source
/// actually implements <see cref="IOperationGateAware"/> is what each test varies.</summary>
internal sealed class GateStubConnector(ISource source) : ISourceConnector
{
    public ConnectorInfo Info => new("gatestub", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.GatedOperations;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true));

    public ValueTask<ISource> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(source);
}

/// <summary>Gate-aware fake: implements <see cref="IOperationGateAware"/> and its one partition wraps its
/// (in-memory) batch production in exactly one <c>gate.ExecuteAsync</c> call per partition read. When
/// <paramref name="alwaysFailTransient"/> is set, the wrapped op always throws a transient
/// <see cref="PzConnectorException"/> -- used by <c>Op_exhaustion_is_one_node_failure</c> to prove op
/// exhaustion inside the gate surfaces as ONE node-attempt failure, never a native exception escaping
/// unwrapped.</summary>
internal sealed class GateAwareFakeSource(bool alwaysFailTransient = false) : ISource, IOperationGateAware
{
    private int _opRuns;

    public IOperationGate? ObservedGate { get; private set; }

    public int OpRuns => Volatile.Read(ref _opRuns);

    public void UseOperationGate(IOperationGate gate) => ObservedGate = gate;

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        new(new DatasetSchema(GateFakeSchema.Schema));

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        new(new IDatasetPartition[] { new GatedPartition(this) });

    public ValueTask DisposeAsync() => default;

    internal async Task<RecordBatch> RunOpAsync(CancellationToken ct)
    {
        Interlocked.Increment(ref _opRuns);
        if (alwaysFailTransient)
        {
            throw new PzConnectorException("fake gated op failure", isTransient: true);
        }

        await Task.Yield();
        return GateFakeSchema.BuildBatch(1);
    }

    private sealed class GatedPartition(GateAwareFakeSource source) : IDatasetPartition
    {
        public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
        {
            var gate = source.ObservedGate;
            var batch = gate is not null
                ? await gate.ExecuteAsync("fake.read", idempotent: true, source.RunOpAsync, ct).ConfigureAwait(false)
                : await source.RunOpAsync(ct).ConfigureAwait(false);
            yield return batch;
        }
    }
}

/// <summary>Plain <see cref="ISource"/> (no <see cref="IOperationGateAware"/>) -- the "existing connector,
/// untouched" regression fixture.</summary>
internal sealed class PlainFakeSource : ISource
{
    private int _opRuns;

    public int OpRuns => Volatile.Read(ref _opRuns);

    public ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct) =>
        new(new DatasetSchema(GateFakeSchema.Schema));

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct) =>
        new(new IDatasetPartition[] { new PlainPartition(this) });

    public ValueTask DisposeAsync() => default;

    internal Task<RecordBatch> RunOpAsync(CancellationToken ct)
    {
        Interlocked.Increment(ref _opRuns);
        return Task.FromResult(GateFakeSchema.BuildBatch(1));
    }

    private sealed class PlainPartition(PlainFakeSource source) : IDatasetPartition
    {
        public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
        {
            yield return await source.RunOpAsync(ct).ConfigureAwait(false);
        }
    }
}
