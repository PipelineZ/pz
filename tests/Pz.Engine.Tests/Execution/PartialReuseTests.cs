using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

/// <summary>Partial reuse of a FAILED prior SourceLoad's completed partitions --
/// unlike full reuse (<see cref="StagingReuseTests"/>, prior run succeeded, whole table copied), here
/// the prior run's staging DB has partition-level pz_meta accounting (some partitions done, maybe one
/// checkpointed) and the executor pre-populates this run's main table + ledger from it so
/// <see cref="PartitionModeLoader"/> only re-extracts what is left. Fixture mirrors
/// <see cref="PartitionModeTests"/> (real <see cref="DuckSession"/>, <see cref="ListStubSource"/>/
/// <see cref="ListStubConnector"/>/<see cref="IdentifiedStubPartition"/>/<see cref="CheckpointingStubPartition"/>)
/// plus <see cref="StagingReuseTests"/>'s hand-built prior staging DB pattern.</summary>
public sealed class PartialReuseTests : IAsyncLifetime
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-partial-reuse-tests", Guid.NewGuid().ToString("N"));
    private DuckSession _duck = null!;

    public async Task InitializeAsync()
    {
        Directory.CreateDirectory(_dir);
        var currentPaths = new RunPaths(_dir, "current");
        Directory.CreateDirectory(currentPaths.RunDir);
        _duck = DuckSession.Open(currentPaths.StagingDbPath);
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
        return new RunContext(_duck, reg, new RunPaths(_dir, "current"), NullRunEvents.Instance);
    }

    private static DagNode Node()
    {
        var source = new ConnectionDef("mem", "liststub", new Dictionary<string, object?>(),
            [new DatasetDef("numbers", new Dictionary<string, object?>(), null)], "sources/mem.yml");
        return new DagNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_mem__numbers",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    /// <summary>Same node id/table as <see cref="Node"/>, but `sync:` -- the done-skip exception
    /// applies to its sole partition regardless of where a "done" ledger row came from.</summary>
    private static DagNode SyncNode()
    {
        var source = new ConnectionDef("mem", "liststub", new Dictionary<string, object?>(),
            [new DatasetDef("numbers", new Dictionary<string, object?>(), null, new SyncModeDef(SyncMode.Auto, null))], "sources/mem.yml");
        return new DagNode(new NodeId("aaaaaaaaaaaaaaaa"), NodeKind.SourceLoad, "src_mem__numbers",
            [], null, new SourceDatasetDef(source, source.Datasets[0]));
    }

    private async Task SeedPriorAsync(Action<string>? mutate = null)
    {
        var priorPaths = new RunPaths(_dir, "prior");
        Directory.CreateDirectory(priorPaths.RunDir);
        await using var prior = DuckSession.Open(priorPaths.StagingDbPath);
        await prior.ExecuteAsync("create schema staging");
        await prior.ExecuteAsync("create schema pz_meta");
        await prior.ExecuteAsync(
            "create table pz_meta.partitions_done (node_id varchar not null, partition_id varchar not null, " +
            "rows bigint not null, primary key (node_id, partition_id))");
        await prior.ExecuteAsync(
            "create table pz_meta.partition_checkpoints (node_id varchar not null, partition_id varchar not null, " +
            "checkpoint varchar not null, rows bigint not null, primary key (node_id, partition_id))");
        await prior.ExecuteAsync(
            "create table pz_meta.node_window (node_id varchar not null, lower varchar not null, " +
            "upper varchar not null, primary key (node_id))");
        // Partitions a (ids 1,2) and b (id 3) completed in the failed prior run.
        await prior.ExecuteAsync("create table staging.src_mem__numbers as select * from range(1, 4) t(id)");
        await prior.ExecuteAsync("insert into pz_meta.partitions_done values ('aaaaaaaaaaaaaaaa', 'a', 2)");
        await prior.ExecuteAsync("insert into pz_meta.partitions_done values ('aaaaaaaaaaaaaaaa', 'b', 1)");
        mutate?.Invoke(priorPaths.StagingDbPath);
    }

    private ReuseManifest PartialManifest() => new(
        new Dictionary<NodeId, ReuseEntry>(),
        new Dictionary<NodeId, PartialReuseEntry>
        {
            [new NodeId("aaaaaaaaaaaaaaaa")] = new(new RunPaths(_dir, "prior").StagingDbPath),
        });

    [Fact]
    public async Task Partial_reuse_skips_done_partitions_and_extracts_the_rest()
    {
        await SeedPriorAsync();
        int aReads = 0, bReads = 0, cReads = 0;
        var source = new ListStubSource(
        [
            new IdentifiedStubPartition("a", [1, 2], onRead: () => aReads++),
            new IdentifiedStubPartition("b", [3], onRead: () => bReads++),
            new IdentifiedStubPartition("c", [4], onRead: () => cReads++),
        ]);
        var ctx = Context(new ListStubConnector(source,
            ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds)) with
        { Reuse = PartialManifest() };

        var result = await new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(4, result.RowsMoved);
        Assert.Equal(new PartitionStats(3, 3, 2, 0), result.Partitions);
        Assert.Equal(0, aReads);
        Assert.Equal(0, bReads);
        Assert.Equal(1, cReads);
        Assert.Null(result.Provenance); // it genuinely extracted the remainder, so nothing was reused
        // cast(...as bigint): DuckDB widens SUM(bigint) to HUGEINT, which ScalarAsync<long>'s
        // Convert.ChangeType can't marshal (same fix CheckpointResumeTests already applies).
        Assert.Equal(10L, await _duck.ScalarAsync<long>("select cast(sum(id) as bigint) from staging.src_mem__numbers", default));
    }

    [Fact]
    public async Task Prior_without_ledger_falls_back_silently()
    {
        var priorPaths = new RunPaths(_dir, "prior");
        Directory.CreateDirectory(priorPaths.RunDir);
        await using (var prior = DuckSession.Open(priorPaths.StagingDbPath))
        {
            await prior.ExecuteAsync("create schema staging");
            await prior.ExecuteAsync("create table staging.src_mem__numbers as select * from range(3) t(id)");
        }

        var notices = new List<string>();
        var reads = 0;
        var source = new ListStubSource([new IdentifiedStubPartition("a", [1], onRead: () => reads++)]);
        var ctx = Context(new ListStubConnector(source,
            ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds)) with
        { Reuse = PartialManifest(), Notice = notices.Add };

        var result = await new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(1, reads);
        Assert.Equal(new PartitionStats(1, 1, 0, 0), result.Partitions);
        Assert.Empty(notices); // legacy failed node: silent skip, not noise
    }

    [Fact]
    public async Task Ledger_row_count_mismatch_falls_back_with_notice()
    {
        await SeedPriorAsync();
        // Corrupt: ledger claims 3 rows total, table has 2.
        var priorPath = new RunPaths(_dir, "prior").StagingDbPath;
        await using (var prior = DuckSession.Open(priorPath))
        {
            await prior.ExecuteAsync("delete from staging.src_mem__numbers where id = 3");
        }

        var notices = new List<string>();
        var reads = new List<string>();
        var source = new ListStubSource(
        [
            new IdentifiedStubPartition("a", [1, 2], onRead: () => reads.Add("a")),
            new IdentifiedStubPartition("b", [3], onRead: () => reads.Add("b")),
        ]);
        var ctx = Context(new ListStubConnector(source,
            ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds)) with
        { Reuse = PartialManifest(), Notice = notices.Add };

        var result = await new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(["a", "b"], reads.Order().ToList());
        Assert.Contains(notices, n => n.Contains("re-extracting", StringComparison.Ordinal));
        Assert.Equal(new PartitionStats(2, 2, 0, 0), result.Partitions);
    }

    [Fact]
    public async Task Done_ids_outside_the_fresh_plan_fall_back_with_notice()
    {
        await SeedPriorAsync();
        var notices = new List<string>();
        var source = new ListStubSource(
            [new IdentifiedStubPartition("a", [1, 2]), new IdentifiedStubPartition("z", [9])]); // 'b' gone
        var ctx = Context(new ListStubConnector(source,
            ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds)) with
        { Reuse = PartialManifest(), Notice = notices.Add };

        var result = await new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Contains(notices, n => n.Contains("no longer match", StringComparison.Ordinal));
        Assert.Equal(new PartitionStats(2, 2, 0, 0), result.Partitions);
        // Full re-extraction of the CURRENT plan (a: 1+2, z: 9) = 12 -- the guard rejects the copy
        // before anything is written locally, so nothing carries over from the stale prior partition 'b'.
        Assert.Equal(12L, await _duck.ScalarAsync<long>("select cast(sum(id) as bigint) from staging.src_mem__numbers", default));
    }

    [Fact]
    public async Task Window_drift_falls_back_with_notice()
    {
        await SeedPriorAsync();
        await using (var prior = DuckSession.Open(new RunPaths(_dir, "prior").StagingDbPath))
        {
            await prior.ExecuteAsync("insert into pz_meta.node_window values ('aaaaaaaaaaaaaaaa', '1', '5')");
        }

        // Current run is unwindowed (Node() has no incremental def) — windowed prior ⇒ drift.
        var notices = new List<string>();
        var source = new ListStubSource(
            [new IdentifiedStubPartition("a", [1, 2]), new IdentifiedStubPartition("b", [3])]);
        var ctx = Context(new ListStubConnector(source,
            ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds)) with
        { Reuse = PartialManifest(), Notice = notices.Add };

        var result = await new SourceLoadExecutor().ExecuteAsync(Node(), ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Contains(notices, n => n.Contains("different window", StringComparison.Ordinal));
        Assert.Equal(new PartitionStats(2, 2, 0, 0), result.Partitions);
    }

    [Fact]
    public async Task Checkpointed_prefix_is_copied_and_resumed_cross_run()
    {
        await SeedPriorAsync();
        var node = Node();
        var nodeKey = PartitionLedger.NodeKey(node);
        var partTable = PartitionLedger.PartTable(nodeKey, "c");
        await using (var prior = DuckSession.Open(new RunPaths(_dir, "prior").StagingDbPath))
        {
            await prior.ExecuteAsync($"create table {partTable} as select * from range(100, 102) t(id)");
            await prior.ExecuteAsync("insert into pz_meta.partition_checkpoints values ('aaaaaaaaaaaaaaaa', 'c', 'tok-2', 2)");
        }

        var c = new CheckpointingStubPartition("c", [100, 101, 102, 103]);
        var source = new ListStubSource(
            [new IdentifiedStubPartition("a", [1, 2]), new IdentifiedStubPartition("b", [3]), c]);
        var ctx = Context(new ListStubConnector(source, ConnectorCapabilities.PartitionedRead |
            ConnectorCapabilities.StablePartitionIds | ConnectorCapabilities.CheckpointableReads)) with
        { Reuse = PartialManifest() };

        var result = await new SourceLoadExecutor().ExecuteAsync(node, ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(["tok-2"], c.ResumeCalls);
        Assert.Equal(new PartitionStats(3, 3, 2, 1), result.Partitions);
        Assert.Equal(7, result.RowsMoved); // 3 prior + 4 from partition c
    }

    /// <summary><see cref="PartitionModeLoader.TryCopyPartialAsync"/> is invoked
    /// from <see cref="SourceLoadExecutor"/> on EVERY node attempt because
    /// <see cref="KindDispatchingExecutor"/>'s retry loop re-runs the whole executor over the SAME
    /// ctx/staging DB -- simulated here by calling <see cref="SourceLoadExecutor.ExecuteAsync"/> twice
    /// over one shared <see cref="RunContext"/>. Attempt 1 copies prior partitions a+b, extracts c
    /// cleanly, then a fresh partition d faults transiently, aggregating into a thrown
    /// <see cref="PzConnectorException"/>. Attempt 2's re-entry into <c>TryCopyPartialAsync</c> must NOT
    /// re-run the create-or-replace copy: that would wipe the local ledger and main table and discard
    /// attempt 1's already-committed partition c (which would then redundantly re-extract) -- a
    /// violation of the ledger retry-safety contract. This test proves
    /// c is read exactly once across both attempts.</summary>
    [Fact]
    public async Task Retry_attempt_does_not_reapply_the_partial_copy()
    {
        await SeedPriorAsync();

        int aReads = 0, bReads = 0, cReads = 0, dReads = 0;
        Exception? dFault = new PzConnectorException("transient blip", isTransient: true);
        var source = new ListStubSource(
        [
            new IdentifiedStubPartition("a", [1, 2], onRead: () => aReads++),
            new IdentifiedStubPartition("b", [3], onRead: () => bReads++),
            new IdentifiedStubPartition("c", [4, 5], onRead: () => cReads++),
            new IdentifiedStubPartition("d", [6, 7, 8], onRead: () => dReads++, fault: () =>
            {
                var thrown = dFault;
                dFault = null;
                return thrown;
            }),
        ]);
        var connector = new ListStubConnector(source,
            ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds);
        var notices = new List<string>();
        var node = Node();
        var ctx = Context(connector) with { Reuse = PartialManifest(), Notice = notices.Add };

        // Attempt 1: local ledger is empty, so the partial copy fires and reuses a+b; c extracts
        // cleanly and commits; d's transient fault aggregates into a thrown PzConnectorException that
        // escapes ExecuteAsync raw (only KindDispatchingExecutor's retry loop would normally catch it).
        var thrown = await Assert.ThrowsAsync<PzConnectorException>(
            () => new SourceLoadExecutor().ExecuteAsync(node, ctx, CancellationToken.None));
        Assert.True(thrown.IsTransient);
        Assert.Equal(0, aReads);
        Assert.Equal(0, bReads);
        Assert.Equal(1, cReads);
        Assert.Equal(1, dReads);
        Assert.Equal(1, notices.Count(n => n.Contains("reusing 2 completed partition(s)", StringComparison.Ordinal)));

        // Attempt 2 (the retry, same ctx/staging DB): the local ledger now already has a, b, AND c --
        // the partial copy must bail out silently instead of reapplying, or it would wipe c's
        // already-committed progress and force a redundant re-extraction (the regression being fixed).
        var result = await new SourceLoadExecutor().ExecuteAsync(node, ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(0, aReads);
        Assert.Equal(0, bReads);
        Assert.Equal(1, cReads); // unchanged -- attempt 1's commit survived, not re-extracted
        Assert.Equal(2, dReads); // fault attempt + this clean attempt
        Assert.Equal(8, result.RowsMoved); // a(2) + b(1) + c(2) + d(3)
        Assert.Equal(1, notices.Count(n => n.Contains("reusing 2 completed partition(s)", StringComparison.Ordinal)));
        // Documented side effect: Reused reports 0 even though a/b's surviving done rows originated
        // from attempt 1's copy -- stats are informational and scoped to THIS successful attempt.
        Assert.Equal(new PartitionStats(4, 4, 0, 0), result.Partitions);
    }

    /// <summary>Sync-interplay guard: a Feed-shaped dataset's sole partition is NEVER skip-reused within
    /// a run -- LoadAsync unconditionally resets and re-reads it live even if a copied-in
    /// ledger row said "done". Without excluding feed datasets from the partial-copy attempt, the copy
    /// would still succeed (this prior's single done partition 'a' matches the fresh plan exactly) and
    /// report PartitionStats.Reused = 1, but the sync-reset immediately discards that copy and
    /// re-extracts live -- an over-reported, meaningless "reused" count for a partition that was NOT
    /// actually reused. The executor excludes feed datasets from the partial-copy attempt entirely
    /// (SourceLoadExecutor's `shape != ResolvedReadShape.Feed` guard), so the prior staging DB below is
    /// never even ATTACHed: no notice fires, Reused is honestly 0, and the live read proceeds exactly as
    /// it would with no manifest entry at all.</summary>
    [Fact]
    public async Task Sync_dataset_sole_partition_never_attempts_partial_copy()
    {
        var priorPaths = new RunPaths(_dir, "prior");
        Directory.CreateDirectory(priorPaths.RunDir);
        await using (var prior = DuckSession.Open(priorPaths.StagingDbPath))
        {
            await prior.ExecuteAsync("create schema staging");
            await prior.ExecuteAsync("create schema pz_meta");
            await prior.ExecuteAsync(
                "create table pz_meta.partitions_done (node_id varchar not null, partition_id varchar not null, " +
                "rows bigint not null, primary key (node_id, partition_id))");
            await prior.ExecuteAsync(
                "create table pz_meta.partition_checkpoints (node_id varchar not null, partition_id varchar not null, " +
                "checkpoint varchar not null, rows bigint not null, primary key (node_id, partition_id))");
            await prior.ExecuteAsync(
                "create table pz_meta.node_window (node_id varchar not null, lower varchar not null, " +
                "upper varchar not null, primary key (node_id))");
            // The sole partition 'a' completed in the failed prior run -- were the copy attempted, it
            // would succeed cleanly (matches the fresh plan exactly, main count == ledger sum).
            await prior.ExecuteAsync("create table staging.src_mem__numbers as select * from range(1, 3) t(id)");
            await prior.ExecuteAsync("insert into pz_meta.partitions_done values ('aaaaaaaaaaaaaaaa', 'a', 2)");
        }

        var notices = new List<string>();
        var reads = 0;
        var source = new ListStubSource(
            [new IdentifiedStubPartition("a", [1, 2], onRead: () => reads++)], feedShaped: true);
        var ctx = Context(new ListStubConnector(source,
            ConnectorCapabilities.PartitionedRead | ConnectorCapabilities.StablePartitionIds)) with
        { Reuse = PartialManifest(), Notice = notices.Add };

        var result = await new SourceLoadExecutor().ExecuteAsync(SyncNode(), ctx, CancellationToken.None);

        Assert.Equal(NodeStatus.Success, result.Status);
        Assert.Equal(1, reads); // the sole partition was read live, not skip-reused
        Assert.Equal(new PartitionStats(1, 1, 0, 0), result.Partitions); // Reused honestly 0
        Assert.Empty(notices); // no ATTACH ever attempted -- no guard notice of any kind
        Assert.Equal(2L, await _duck.ScalarAsync<long>("select count(*) from staging.src_mem__numbers", default));
    }
}
