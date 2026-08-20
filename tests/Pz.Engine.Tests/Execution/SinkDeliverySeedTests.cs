using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

/// <summary>pz retry seeds a prior FAILED sink's delivery row into the
/// fresh run's ledger -- guards in order (local row wins; prior readable; fingerprint match
/// against the freshly rebuilt relation), silent scratch on any failure, DETACH always.
/// Prior staging DBs are hand-built like PartialReuseTests'.</summary>
public sealed class SinkDeliverySeedTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-sink-seed-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        var currentPaths = new RunPaths(_dir, "current");
        Directory.CreateDirectory(currentPaths.RunDir);
        _duck = DuckSession.Open(currentPaths.StagingDbPath);
        await _duck.ExecuteAsync("create schema if not exists staging");
        // The freshly rebuilt relation this retry drains: ids 0..199.
        await _duck.ExecuteAsync("create table staging.stg_orders as select range as id from range(200)");
    }

    public async Task DisposeAsync()
    {
        await _duck.DisposeAsync();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    private static DagNode SinkNode()
    {
        var sink = new ConnectionDef("api", "ckpt", new Dictionary<string, object?>(), [],
            "sinks/api.yml") { Outputs = [new OutputDef("out", "stg_orders", "append", "fail_on_change", new Dictionary<string, object?>())] };
        return new DagNode(new NodeId("eeeeeeeeeeeeeeee"), NodeKind.SinkWrite, "api.out",
            [], null, new SinkOutputDef(sink, sink.Outputs[0]));
    }

    private RunContext Context(CheckpointingSinkConnector sink)
    {
        var reg = new ConnectorRegistry();
        reg.AddSink("ckpt", sink);
        return new RunContext(_duck, reg, new RunPaths(_dir, "current"), NullRunEvents.Instance,
            Batch: new BatchOptions(TargetBatchBytes: 256),
            Reuse: new ReuseManifest(
                new Dictionary<NodeId, ReuseEntry>(), null,
                new Dictionary<NodeId, DeliveryResumeEntry>
                {
                    [new NodeId("eeeeeeeeeeeeeeee")] = new(new RunPaths(_dir, "prior").StagingDbPath),
                }));
    }

    /// <summary>Prior staging DB: sink_deliveries says 80 rows delivered against a relation
    /// fingerprint the mutator can distort. matchCurrent=true records the CURRENT relation's
    /// real fingerprint (content-identical retry, the resume case).</summary>
    private async Task SeedPriorAsync(bool matchCurrent, long acknowledged = 80)
    {
        var priorPaths = new RunPaths(_dir, "prior");
        Directory.CreateDirectory(priorPaths.RunDir);
        var fp = matchCurrent
            ? await SinkDeliveryLedger.FingerprintAsync(_duck, "staging.stg_orders", default)
            : new SinkDeliveryLedger.Fingerprint(200, "not-the-real-hash");
        await using var prior = DuckSession.Open(priorPaths.StagingDbPath);
        await SinkDeliveryLedger.EnsureAsync(prior, default);
        await prior.ExecuteTransactionAsync(
            SinkDeliveryLedger.UpsertStatements("eeeeeeeeeeeeeeee", acknowledged, fp), default);
    }

    [Fact]
    public async Task Matching_prior_row_seeds_and_the_sink_resumes()
    {
        await SeedPriorAsync(matchCurrent: true);
        var sink = new CheckpointingSinkConnector(failAtBatch: int.MaxValue, failSessions: 0);

        var result = await new SinkWriteExecutor().ExecuteAsync(SinkNode(), Context(sink), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(200, result.RowsMoved);
        Assert.Equal(80, sink.Sessions[0].ResumedFrom);
        Assert.Equal(120, sink.Sessions[0].RowsReceived);
        Assert.Equal(80, result.Delivery!.ResumedRows);
    }

    [Fact]
    public async Task Fingerprint_mismatch_scratches_silently()
    {
        await SeedPriorAsync(matchCurrent: false);
        var sink = new CheckpointingSinkConnector(failAtBatch: int.MaxValue, failSessions: 0);

        var result = await new SinkWriteExecutor().ExecuteAsync(SinkNode(), Context(sink), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Null(sink.Sessions[0].ResumedFrom);
        Assert.Equal(200, sink.Sessions[0].RowsReceived);
    }

    [Fact]
    public async Task Local_row_wins_over_the_prior_run()
    {
        await SeedPriorAsync(matchCurrent: true, acknowledged: 80);
        // A local attempt already committed further progress this run: local says 120.
        await SinkDeliveryLedger.EnsureAsync(_duck, default);
        var localFp = await SinkDeliveryLedger.FingerprintAsync(_duck, "staging.stg_orders", default);
        await _duck.ExecuteTransactionAsync(
            SinkDeliveryLedger.UpsertStatements("eeeeeeeeeeeeeeee", 120, localFp), default);
        var sink = new CheckpointingSinkConnector(failAtBatch: int.MaxValue, failSessions: 0);

        var result = await new SinkWriteExecutor().ExecuteAsync(SinkNode(), Context(sink), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(120, sink.Sessions[0].ResumedFrom);
        Assert.Equal(80, sink.Sessions[0].RowsReceived);
    }

    [Fact]
    public async Task Missing_prior_staging_scratches_silently()
    {
        // No SeedPriorAsync: the manifest names a path that does not exist.
        var sink = new CheckpointingSinkConnector(failAtBatch: int.MaxValue, failSessions: 0);

        var result = await new SinkWriteExecutor().ExecuteAsync(SinkNode(), Context(sink), default);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Null(sink.Sessions[0].ResumedFrom);
        Assert.Equal(200, sink.Sessions[0].RowsReceived);
    }
}
