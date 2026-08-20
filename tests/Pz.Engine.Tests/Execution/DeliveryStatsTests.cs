using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Resilience;

namespace Pz.Engine.Tests.Execution;

/// <summary>A failed universal write on a BestEffort/None sink carries
/// DeliveryStats on the terminal Failed NodeResult ("delivery stopped; N rows already
/// visible"), a DiscardsAll failure carries nothing, and a retry that eventually succeeds
/// clears the slot. Harness mirrors NodeExecutorTests (real DuckSession + stub registry);
/// TargetBatchBytes is shrunk so a small table drains as several batches.</summary>
public sealed class DeliveryStatsTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-delivery-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        _duck = DuckSession.Open(Path.Combine(_dir, "staging.duckdb"));
        await _duck.ExecuteAsync("create schema if not exists staging");
        await _duck.ExecuteAsync("create table staging.stg_orders as select range as id from range(200)");
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    private RunContext Context(FlakySinkConnector sink) => new(
        _duck, Registry(sink), new RunPaths(_dir, "test-run"), NullRunEvents.Instance,
        Batch: new BatchOptions(TargetBatchBytes: 256));

    private static ConnectorRegistry Registry(FlakySinkConnector sink)
    {
        var reg = new ConnectorRegistry();
        reg.AddSink("flaky", sink);
        return reg;
    }

    private static DagNode SinkNode()
    {
        var sink = new ConnectionDef("api", "flaky", new Dictionary<string, object?>(), [],
            "sinks/api.yml") { Outputs = [new OutputDef("out", "stg_orders", "append", "fail_on_change", new Dictionary<string, object?>())] };
        return new DagNode(new NodeId("dddddddddddddddd"), NodeKind.SinkWrite, "api.out",
            [], null, new SinkOutputDef(sink, sink.Outputs[0]));
    }

    [Fact]
    public async Task None_sink_failure_carries_delivery_stats()
    {
        var sink = new FlakySinkConnector(AbortSemantics.None, failAtBatch: 2, transient: false);
        var result = await new KindDispatchingExecutor().ExecuteAsync(SinkNode(), Context(sink), default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Delivery);
        Assert.Equal("none", result.Delivery!.AbortSemantics);
        // Upper-bound meaning: RowsVisible is rows handed to WriteBatchAsync
        // INCLUDING the batch that was in flight when it failed, not just the prior successful
        // batches -- a non-checkpointing sink offers no acknowledgment channel, so "up to N" must
        // count the batch the sink may already be partway through processing.
        Assert.Equal(sink.RowsWrittenBeforeFailure + sink.FailingBatchLength, result.Delivery.RowsVisible);
        Assert.True(result.Delivery.RowsVisible > 0);
        Assert.True(sink.FailingBatchLength > 0);
        Assert.Equal(0, result.Delivery.ResumedRows);
        Assert.Equal(1, sink.AbortCalls); // abort still runs (a None abort is an honest no-op)
    }

    [Fact]
    public async Task DiscardsAll_failure_carries_no_delivery_stats()
    {
        var sink = new FlakySinkConnector(AbortSemantics.DiscardsAll, failAtBatch: 2, transient: false);
        var result = await new KindDispatchingExecutor().ExecuteAsync(SinkNode(), Context(sink), default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.Null(result.Delivery);
    }

    [Fact]
    public async Task Transient_failure_then_success_clears_the_slot()
    {
        var sink = new FlakySinkConnector(AbortSemantics.None, failAtBatch: 2, transient: true, failSessions: 1);
        var policy = new RetryPolicy(MaxAttempts: 2, BaseDelay: TimeSpan.Zero, MaxDelay: TimeSpan.Zero);
        var ctx = Context(sink);

        var result = await new KindDispatchingExecutor(policy, delay: (_, _) => Task.CompletedTask)
            .ExecuteAsync(SinkNode(), ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Null(result.Delivery); // no resume machinery on this path => success carries nothing
        Assert.Empty(ctx.DeliveryFailures);
    }
}

/// <summary>Configurable-abort flaky sink: fails WriteBatchAsync at a given batch ordinal for
/// the first <c>failSessions</c> sessions, then succeeds. Counts rows handed over and abort
/// calls so tests can pin RowsVisible to what actually crossed the ABI.</summary>
internal sealed class FlakySinkConnector(
    AbortSemantics semantics, int failAtBatch, bool transient, int failSessions = int.MaxValue)
    : ISinkConnector, ISink
{
    private int _sessionsOpened;
    public long RowsWrittenBeforeFailure { get; private set; }
    public long FailingBatchLength { get; private set; }
    public int AbortCalls { get; private set; }

    public ConnectorInfo Info => new("flaky", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";
    public AbortSemantics AbortSemantics => semantics;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        new(ValidationResult.Success);
    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        new(new ConnectionCheck(true));
    public ValueTask<ISink> OpenAsync(ConnectorConfig config, CancellationToken ct) => new(this);

    public bool TryGetNativeCopy(OutputSpec spec, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct) =>
        new(new Session(this, ++_sessionsOpened <= failSessions ? failAtBatch : int.MaxValue, transient));

    public ValueTask DisposeAsync() => default;

    private sealed class Session(FlakySinkConnector owner, int failAtBatch, bool transient) : ISinkWriteSession
    {
        private int _batches;
        private long _rows;

        public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
        {
            if (++_batches == failAtBatch)
            {
                owner.FailingBatchLength = batch.Length;
                throw new PzConnectorException("flaky sink write failure", isTransient: transient);
            }

            _rows += batch.Length;
            owner.RowsWrittenBeforeFailure = _rows;
            return ValueTask.CompletedTask;
        }

        public ValueTask<WriteResult> CommitAsync(CancellationToken ct) =>
            new(new WriteResult(_rows, _batches - 1));

        public ValueTask AbortAsync(CancellationToken ct)
        {
            owner.AbortCalls++;
            return ValueTask.CompletedTask;
        }

        public ValueTask DisposeAsync() => default;
    }
}
