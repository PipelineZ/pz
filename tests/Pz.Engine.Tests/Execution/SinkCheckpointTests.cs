using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Resilience;

namespace Pz.Engine.Tests.Execution;

/// <summary>Checkpointing sink drain — deterministic order-by-all
/// drain, acknowledgment polling, teardown-time ledger persistence, offset resume on the next
/// attempt, post-commit clear, and the scratch paths (connector declines; fingerprint
/// mismatch). DeliveryStatsTests' harness shape; the stub sink acknowledges every batch on
/// 2xx-analog success and fails mid-stream on command.</summary>
public sealed class SinkCheckpointTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-sink-ckpt-tests", Guid.NewGuid().ToString("N"));
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

    private RunContext Context(ISinkConnector sink) => new(
        _duck, Registry(sink), new RunPaths(_dir, "test-run"), NullRunEvents.Instance,
        Batch: new BatchOptions(TargetBatchBytes: 256));

    private static ConnectorRegistry Registry(ISinkConnector sink)
    {
        var reg = new ConnectorRegistry();
        reg.AddSink("ckpt", sink);
        return reg;
    }

    private static DagNode SinkNode()
    {
        var sink = new ConnectionDef("api", "ckpt", new Dictionary<string, object?>(), [],
            "sinks/api.yml") { Outputs = [new OutputDef("out", "stg_orders", "append", "fail_on_change", new Dictionary<string, object?>())] };
        return new DagNode(new NodeId("eeeeeeeeeeeeeeee"), NodeKind.SinkWrite, "api.out",
            [], null, new SinkOutputDef(sink, sink.Outputs[0]));
    }

    private static KindDispatchingExecutor TwoAttempts() =>
        new(new RetryPolicy(MaxAttempts: 2, BaseDelay: TimeSpan.Zero, MaxDelay: TimeSpan.Zero),
            delay: (_, _) => Task.CompletedTask);

    [Fact]
    public async Task Retry_resumes_past_the_acknowledged_prefix()
    {
        var sink = new CheckpointingSinkConnector(failAtBatch: 3, failSessions: 1);
        var ctx = Context(sink);

        var result = await TwoAttempts().ExecuteAsync(SinkNode(), ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(200, result.RowsMoved); // commit counts the resumed prefix, not just this attempt's rows

        var firstAck = sink.Sessions[0].AcknowledgedAtFailure;
        Assert.True(firstAck > 0);
        Assert.Equal(firstAck, sink.Sessions[1].ResumedFrom);      // engine offered the prefix
        Assert.Equal(200 - firstAck, sink.Sessions[1].RowsReceived); // and delivered only the suffix
        Assert.Equal(200, sink.Sessions.SelectMany(s => s.Ids).Concat(
            Enumerable.Range(0, (int)firstAck).Select(i => (long)i)).Distinct().Count());

        Assert.NotNull(result.Delivery); // success + resume => observability payload
        Assert.Equal(firstAck, result.Delivery!.ResumedRows);
        Assert.Null(await SinkDeliveryLedger.ReadAsync(_duck, SinkNode().Id.Value, default)); // cleared
    }

    [Fact]
    public async Task Declined_resume_redelivers_from_zero_and_clears_the_row()
    {
        var sink = new CheckpointingSinkConnector(failAtBatch: 3, failSessions: 1, acceptResume: false);
        var ctx = Context(sink);

        var result = await TwoAttempts().ExecuteAsync(SinkNode(), ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(0, sink.Sessions[1].ResumedFrom ?? 0);
        Assert.Equal(200, sink.Sessions[1].RowsReceived);
        Assert.Null(result.Delivery); // scratch success carries nothing
    }

    [Fact]
    public async Task Fingerprint_mismatch_scratches_instead_of_resuming()
    {
        var sink = new CheckpointingSinkConnector(failAtBatch: 3, failSessions: 1);
        var ctx = Context(sink);

        // Attempt 1 (direct executor call): fails mid-stream, persists the ledger row.
        await Assert.ThrowsAsync<PzConnectorException>(
            () => new SinkWriteExecutor().ExecuteAsync(SinkNode(), ctx, default));
        Assert.NotNull(await SinkDeliveryLedger.ReadAsync(_duck, SinkNode().Id.Value, default));

        // Relation content changes between attempts => the recorded prefix means nothing.
        await _duck.ExecuteAsync("insert into staging.stg_orders values (999)");

        var result = await new KindDispatchingExecutor().ExecuteAsync(SinkNode(), ctx, default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(0, sink.Sessions[1].ResumedFrom ?? 0);
        Assert.Equal(201, sink.Sessions[1].RowsReceived);
    }

    [Fact]
    public async Task Failed_checkpointing_sink_reports_acknowledged_not_handed_rows()
    {
        var sink = new CheckpointingSinkConnector(failAtBatch: 3, failSessions: int.MaxValue,
            acknowledgeEveryBatch: false); // acks lag one batch behind writes
        var ctx = Context(sink);

        // Single attempt, deterministically: CheckpointingSession always throws isTransient:true, and
        // this node has no configured RetryDef, so an unbounded KindDispatchingExecutor() would resolve
        // RetryPolicy.Default (3 attempts, REAL 1s/2s backoff via Task.Delay) -- a wall-clock sleep this
        // project's tests never take, and a false premise for this assertion besides: each retry resumes
        // past the previous attempt's acknowledged prefix (by design, proven by
        // Retry_resumes_past_the_acknowledged_prefix above), so the acknowledged count compounds
        // attempt-over-attempt and would no longer equal Sessions[0]'s alone. Pinning MaxAttempts to 1
        // isolates exactly what this test is about: a single failed attempt's honesty side-band reports
        // the acknowledged count, not the handed-over one.
        var policy = new RetryPolicy(MaxAttempts: 1, BaseDelay: TimeSpan.Zero, MaxDelay: TimeSpan.Zero);
        var result = await new KindDispatchingExecutor(policy, delay: (_, _) => Task.CompletedTask)
            .ExecuteAsync(SinkNode(), ctx, default);

        Assert.Equal(NodeStatus.Failed, result.Status);
        Assert.NotNull(result.Delivery);
        Assert.Equal(sink.Sessions[0].AcknowledgedAtFailure, result.Delivery!.RowsVisible);
        Assert.True(result.Delivery.RowsVisible < sink.Sessions[0].RowsReceived);
    }

    [Fact]
    public async Task Failure_polls_acknowledged_rows_once_more_before_recording_and_persisting()
    {
        // A stub whose acknowledged count rises DURING the failing
        // WriteBatchAsync call itself (ack rises, then throw) -- modeling a checkpointing
        // session whose durable-delivery confirmation channel (e.g. HTTP chunk responses)
        // advanced right up to the moment the batch call failed. Only the post-failure poll
        // this test targets can observe that rise; the last poll taken during the loop (after
        // the PRIOR successful batch) predates it.
        const long ackDuringFailingWrite = 137;
        var sink = new AckDuringFailureSinkConnector(failAtBatch: 2, ackDuringFailingWrite);
        var ctx = Context(sink);
        var node = SinkNode();

        var thrown = await Assert.ThrowsAsync<PzConnectorException>(
            () => new SinkWriteExecutor().ExecuteAsync(node, ctx, default));
        Assert.Contains("ack-during-failure", thrown.Message);

        Assert.True(ctx.DeliveryFailures.TryGetValue(node.Id, out var stats));
        Assert.Equal(ackDuringFailingWrite, stats!.RowsVisible);

        var persisted = await SinkDeliveryLedger.ReadAsync(_duck, node.Id.Value, default);
        Assert.NotNull(persisted);
        Assert.Equal(ackDuringFailingWrite, persisted!.AcknowledgedRows);
    }

    [Fact]
    public async Task Post_commit_ledger_clear_failure_does_not_fail_the_node()
    {
        // The stub cancels the very token the executor was called with, from
        // inside CommitAsync, AFTER computing its (already-successful) result -- so by the time
        // SinkWriteExecutor reaches the post-commit SinkDeliveryLedger.ClearAsync, ct is
        // pre-cancelled. Pre-fix, ClearAsync propagates that as an OperationCanceledException
        // straight out of ExecuteAsync (no try/catch guarded it), turning a committed sink into a
        // thrown failure. Post-fix, the clear runs on CancellationToken.None and is
        // try/catch-suppressed, so the node still reports Success.
        using var cts = new CancellationTokenSource();
        var sink = new CancelOnCommitSinkConnector(cts);
        var ctx = Context(sink);
        var node = SinkNode();

        var result = await new SinkWriteExecutor().ExecuteAsync(node, ctx, cts.Token);

        Assert.Equal(NodeStatus.Success, result.Status);
    }
}

/// <summary>Checkpointing stub sink (AbortSemantics.None): records per-session received rows
/// and ids, acknowledges cumulatively (every batch, or lagging one batch when
/// <c>acknowledgeEveryBatch</c> is false), fails WriteBatchAsync at a batch ordinal for the
/// first <c>failSessions</c> sessions, and accepts/declines resume on command. Commit returns
/// resumed-prefix + delivered rows per the ABI contract.</summary>
internal sealed class CheckpointingSinkConnector(
    int failAtBatch, int failSessions, bool acceptResume = true, bool acknowledgeEveryBatch = true)
    : ISinkConnector, ISink
{
    public List<CheckpointingSession> Sessions { get; } = [];

    public ConnectorInfo Info => new("ckpt", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.CheckpointableWrites;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";
    public AbortSemantics AbortSemantics => AbortSemantics.None;

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

    public ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        var session = new CheckpointingSession(
            Sessions.Count < failSessions ? failAtBatch : int.MaxValue, acceptResume, acknowledgeEveryBatch);
        Sessions.Add(session);
        return new(session);
    }

    public ValueTask DisposeAsync() => default;
}

internal sealed class CheckpointingSession(int failAtBatch, bool acceptResume, bool acknowledgeEveryBatch)
    : ICheckpointingSinkSession
{
    private int _batches;
    private long _acknowledged;
    private long _lastReported = -1;

    public long? ResumedFrom { get; private set; }
    public long RowsReceived { get; private set; }
    public long AcknowledgedAtFailure { get; private set; }
    public List<long> Ids { get; } = [];

    public bool TryResumeFrom(long acknowledgedRows)
    {
        if (!acceptResume)
        {
            return false;
        }

        ResumedFrom = acknowledgedRows;
        _acknowledged = acknowledgedRows;
        return true;
    }

    public bool TryGetAcknowledgedRows(out long acknowledgedRows)
    {
        acknowledgedRows = _acknowledged;
        if (_acknowledged == _lastReported)
        {
            return false;
        }

        _lastReported = _acknowledged;
        return true;
    }

    public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        if (++_batches == failAtBatch)
        {
            AcknowledgedAtFailure = _acknowledged;
            throw new PzConnectorException("checkpointing sink write failure", isTransient: true);
        }

        var ids = (Int64Array)batch.Column(0);
        for (var i = 0; i < batch.Length; i++)
        {
            Ids.Add(ids.GetValue(i)!.Value);
        }

        RowsReceived += batch.Length;
        // acknowledgeEveryBatch=false models a connector whose durable-delivery confirmation
        // lags what it has accepted -- acks trail by one batch.
        _acknowledged = acknowledgeEveryBatch
            ? (ResumedFrom ?? 0) + RowsReceived
            : (ResumedFrom ?? 0) + RowsReceived - batch.Length;
        AcknowledgedAtFailure = _acknowledged;
        return ValueTask.CompletedTask;
    }

    public ValueTask<WriteResult> CommitAsync(CancellationToken ct) =>
        new(new WriteResult((ResumedFrom ?? 0) + RowsReceived, _batches));

    public ValueTask AbortAsync(CancellationToken ct) => ValueTask.CompletedTask;

    public ValueTask DisposeAsync() => default;
}

/// <summary>A checkpointing sink whose acknowledged count rises DURING
/// the failing <see cref="WriteBatchAsync"/> call itself, immediately before it throws --
/// isolating the one-more-poll-in-the-catch behavior from everything else the checkpoint
/// machinery does.</summary>
internal sealed class AckDuringFailureSinkConnector(int failAtBatch, long ackDuringFailingWrite)
    : ISinkConnector, ISink
{
    public ConnectorInfo Info => new("ack-during-failure", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.CheckpointableWrites;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";
    public AbortSemantics AbortSemantics => AbortSemantics.None;

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
        new(new Session(failAtBatch, ackDuringFailingWrite));

    public ValueTask DisposeAsync() => default;

    private sealed class Session(int failAtBatch, long ackDuringFailingWrite) : ICheckpointingSinkSession
    {
        private int _batches;
        private long _acknowledged;
        private long _lastReported = -1;

        public bool TryResumeFrom(long acknowledgedRows) => false;

        public bool TryGetAcknowledgedRows(out long acknowledgedRows)
        {
            acknowledgedRows = _acknowledged;
            if (_acknowledged == _lastReported)
            {
                return false;
            }

            _lastReported = _acknowledged;
            return true;
        }

        public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
        {
            if (++_batches == failAtBatch)
            {
                // The rise happens here, inside the failing call -- a poll taken only after the
                // PRIOR successful batch would never see it.
                _acknowledged = ackDuringFailingWrite;
                throw new PzConnectorException("ack-during-failure sink write failure", isTransient: true);
            }

            return ValueTask.CompletedTask;
        }

        public ValueTask<WriteResult> CommitAsync(CancellationToken ct) => new(new WriteResult(_acknowledged, _batches));

        public ValueTask AbortAsync(CancellationToken ct) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync() => default;
    }
}

/// <summary>Cancels the caller's own token from inside
/// <see cref="CommitAsync"/>, right after computing an already-successful result -- simulating a
/// cancellation that races in between a durable commit and the engine's post-commit ledger
/// cleanup.</summary>
internal sealed class CancelOnCommitSinkConnector(CancellationTokenSource cancelAtCommit)
    : ISinkConnector, ISink
{
    public ConnectorInfo Info => new("cancel-on-commit", "0.1.0", ProtocolVersion.Major);
    public ConnectorCapabilities Capabilities => ConnectorCapabilities.CheckpointableWrites;
    public string ConnectionConfigSchema => "{}";
    public string DatasetConfigSchema => "{}";
    public AbortSemantics AbortSemantics => AbortSemantics.None;

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
        new(new Session(cancelAtCommit));

    public ValueTask DisposeAsync() => default;

    private sealed class Session(CancellationTokenSource cancelAtCommit) : ICheckpointingSinkSession
    {
        private long _rows;
        private long _batches;

        public bool TryResumeFrom(long acknowledgedRows) => false;

        public bool TryGetAcknowledgedRows(out long acknowledgedRows)
        {
            acknowledgedRows = _rows;
            return false;
        }

        public ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
        {
            _rows += batch.Length;
            _batches++;
            return ValueTask.CompletedTask;
        }

        public ValueTask<WriteResult> CommitAsync(CancellationToken ct)
        {
            var result = new WriteResult(_rows, _batches);
            // The commit itself has already durably succeeded (the result above is computed);
            // the cancellation races in only now, exactly the "after CommitAsync succeeded"
            // window I-2 is about.
            cancelAtCommit.Cancel();
            return new ValueTask<WriteResult>(result);
        }

        public ValueTask AbortAsync(CancellationToken ct) => ValueTask.CompletedTask;

        public ValueTask DisposeAsync() => default;
    }
}
