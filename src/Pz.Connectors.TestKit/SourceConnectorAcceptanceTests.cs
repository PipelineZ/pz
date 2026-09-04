using System.Globalization;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Xunit;

namespace Pz.Connectors.TestKit;

/// <summary>The optional docker-backed fixture the change-capture
/// acceptance facts run against. Returning null from <see
/// cref="SourceConnectorAcceptanceTests.ChangeCaptureFixture"/> (the default) makes every fact below
/// Skip — subclasses (InMemory, LocalFiles, and any source that has not opted into cdc)
/// need no changes to keep compiling and passing. Methods are async
/// because every implementation does real server-side setup (publication/slot prereqs for
/// postgres, a primed sp_cdc_enable_table capture instance for sql server) that cannot run inside a
/// constructor or a synchronous property getter.</summary>
public interface IChangeCaptureFixture
{
    /// <summary>A <see cref="DatasetSpec"/> whose server-side change-capture state is already fully
    /// prepared (first call does the one-time setup; later calls on the SAME fixture instance return
    /// the identical spec) — ready for a first-run (<see cref="DatasetSpec.PriorSyncState"/> null)
    /// snapshot read. The underlying table is seeded with rows whose keys <see cref="MutateAsync"/>
    /// never touches, so the snapshot row count is stable across a test.</summary>
    Task<DatasetSpec> CdcSpecAsync();

    /// <summary>Applies exactly <paramref name="inserts"/> brand-new rows, <paramref name="updates"/>
    /// updates, and <paramref name="deletes"/> deletes to the <see cref="CdcSpecAsync"/> table, using
    /// fresh/not-yet-touched keys for every call on this fixture instance (an update or delete never
    /// targets a row a previous call on the SAME instance already deleted). Returns only once the
    /// mutations are durably visible to a subsequent poll read (e.g. waits out SQL Server's async
    /// capture job) — the caller never needs its own bounded wait.</summary>
    Task MutateAsync(int inserts, int updates, int deletes);

    /// <summary>The connector's own canonical sync-state token for "now" (e.g. the current WAL head for
    /// postgres, the current <c>max_lsn</c> for sql server) — same string form as <see
    /// cref="DatasetSpec.PriorSyncState"/>/<see cref="ISyncStatePartition.TryGetSyncStateCandidate"/>.
    /// Used to derive a synthetically stale prior position (same length, all zeros) for the
    /// retention-honesty fact. Null if the connector cannot report one (the fact then Skips).</summary>
    Task<string?> ServerPositionAsync();
}

/// <summary>Acceptance contract every <see cref="ISourceConnector"/> must satisfy. Connector authors
/// subclass this, supplying fixtures via the abstract members below; the provided <c>[Fact]</c>s are the
/// executable spec — do not override them.</summary>
public abstract class SourceConnectorAcceptanceTests
{
    protected abstract ISourceConnector CreateSource();
    protected abstract ConnectorConfig ValidConfig { get; }

    /// <summary>A dataset yielding >= 2 batches under a 4KB batch target and >= 100 rows.</summary>
    protected abstract DatasetSpec SmallDataset { get; }

    /// <summary>A dataset large enough that cancelling mid-read is observable (>= 100k rows), or null to skip.</summary>
    protected virtual DatasetSpec? LargeDataset => null;

    /// <summary>Config/spec that triggers a TRANSIENT failure, or null to skip the classification check.</summary>
    protected virtual DatasetSpec? TransientFailureDataset => null;

    /// <summary>Return a spec equivalent to <see cref="SmallDataset"/> but forced to the given partition
    /// count; return null if your connector cannot control partitioning -- the fact then falls back to a
    /// weaker re-plan idempotency check.</summary>
    protected virtual DatasetSpec? GetSpecWithPartitionOverride(int partitions) => null;

    /// <summary>Maximum batches the suite tolerates after cancellation is signaled. The default suits
    /// per-row/per-batch cancellation checks; raise it if your connector legitimately prefetches batches.</summary>
    protected virtual int CancellationBatchTolerance => 2;

    /// <summary>A dataset spec, over this connector's fixed cursor column, already stamped
    /// with <see cref="DatasetSpec.WatermarkCursor"/>/<see cref="DatasetSpec.WatermarkValue"/>="3"/
    /// <see cref="DatasetSpec.WatermarkUpperBound"/>="7" against a seed producing cursor values 0..10 —
    /// or null (the default) if the connector does not declare <see cref="ConnectorCapabilities.BoundedWindow"/>.
    /// Null mirrors <see cref="MergeOutput"/>'s null-hook precedent: the <c>BoundedWindow_*</c> fact below
    /// becomes a Skip-free no-op, so source subclasses that have not opted in keep compiling and
    /// passing.</summary>
    protected virtual DatasetSpec? BoundedWindowDataset => null;

    /// <summary>A dataset the suite reads with its small batch target
    /// (<see cref="SmallBatchTargetBytes"/>) so the connector's own paging/checkpointing kicks in. The
    /// partition must offer its first checkpoint (<see cref="ICheckpointingPartition.TryGetCheckpoint"/>)
    /// BEFORE the final batch -- a token that only appears once the read is already exhausted fails the
    /// <c>Checkpoint_resume_yields_strictly_after_the_token</c> fact, which treats the checkpoint's
    /// position as the mid-read boundary between a "prefix" read and a "resume" read. Row values in
    /// <see cref="CheckpointKeyColumn"/> must be unique across the whole dataset -- they are the row
    /// identity the fact uses to prove the prefix and resumed row sets are disjoint and together equal
    /// the full read. <see cref="ICheckpointingPartition.TryResumeFrom"/> must return false, never throw,
    /// when handed a garbage/unrecognized token. Null (the default) when the connector does not declare
    /// <see cref="ConnectorCapabilities.CheckpointableReads"/>, in which case the fact Skips.</summary>
    protected virtual DatasetSpec? CheckpointDataset => null;

    /// <summary>Column index the checkpoint-resume fact uses as row identity when comparing the
    /// prefix and resumed row sets. Values must be unique per row in <see cref="CheckpointDataset"/>.</summary>
    protected virtual int CheckpointKeyColumn => 0;

    /// <summary>The change-capture acceptance facts' fixture. Null (the
    /// default) makes every <c>ChangeCapture_*</c> fact below Skip; docker-backed subclasses whose
    /// connector declares <see cref="ConnectorCapabilities.ChangeCapture"/> (e.g. Postgres, SqlServer)
    /// override this.</summary>
    protected virtual IChangeCaptureFixture? ChangeCaptureFixture => null;

    /// <summary>Invoked first by every <c>[SkippableFact]</c> below. No-op by
    /// default (InMemory/LocalFiles subclasses need no change); docker-backed subclasses (e.g.
    /// <c>PostgresSourceAcceptance</c>) override this with <c>DockerFacts.SkipUnlessDocker()</c> so the
    /// suite SKIPs cleanly instead of failing when docker is absent. It receives nothing identifying the
    /// caller, so it can only skip the suite as a whole — override <see cref="ShouldRun"/> to skip a
    /// subset.</summary>
    protected virtual void GateFact()
    {
    }

    /// <summary>Per-fact opt-out, by fact method name. Every fact runs by default, so an existing
    /// subclass is unaffected. Override to skip a SUBSET — a connector that cannot satisfy one fact for
    /// a structural reason no capability flag expresses would otherwise have to skip the whole suite
    /// through <see cref="GateFact"/> and lose the facts it does satisfy.</summary>
    protected virtual bool ShouldRun(string fact) => true;

    /// <summary>What each fact actually calls: the suite-wide <see cref="GateFact"/> first (so an
    /// override that skips on absent docker still runs first), then this fact's own
    /// <see cref="ShouldRun"/> verdict. <paramref name="fact"/> is filled in by the compiler with the
    /// calling fact's method name.</summary>
    private void Gate([CallerMemberName] string fact = "")
    {
        GateFact();
        Skip.IfNot(ShouldRun(fact), $"subclass excluded '{fact}' via ShouldRun");
    }

    /// <summary>Skips the calling fact for a connector whose read path is a DuckDB-native scan and whose
    /// <see cref="ISource.PlanReadAsync"/> therefore always throws (<see cref="INativeOnlySource"/>).
    /// Every data-plane fact below reads through PlanReadAsync, so without this such a connector could
    /// not satisfy the suite at all — not because it violates the contract, but because the suite only
    /// knew one way to ask for rows. What it must satisfy instead is the <c>NativeScan_*</c> group.</summary>
    private static void SkipIfNativeOnly(ISourceConnector connector) =>
        Skip.If(connector is INativeOnlySource,
            "connector is INativeOnlySource: it has no universal read path for this fact to read through");

    /// <summary>The dataset the <c>NativeScan_*</c> facts ask for a scan of. Defaults to
    /// <see cref="SmallDataset"/> — for most connectors the same dataset answers both paths. Override
    /// when the scan-able dataset is a different one (a format the connector can only read natively,
    /// say).</summary>
    protected virtual DatasetSpec NativeScanDataset => SmallDataset;

    private const int SmallBatchTargetBytes = 4096;

    [SkippableFact]
    public async Task Validate_accepts_valid_config()
    {
        Gate();
        var connector = CreateSource();
        var result = await connector.ValidateAsync(ValidConfig, CancellationToken.None);

        Assert.True(result.IsValid);
    }

    [SkippableFact]
    public async Task Schema_matches_produced_batches()
    {
        Gate();
        var connector = CreateSource();
        SkipIfNativeOnly(connector);
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var declared = await source.GetSchemaAsync(SmallDataset, CancellationToken.None);

        var partitions = await source.PlanReadAsync(SmallDataset, ReadHints.None, CancellationToken.None);
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(
                new BatchOptions(TargetBatchBytes: SmallBatchTargetBytes), CancellationToken.None))
            {
                AssertSchemasMatch(declared.Schema, batch.Schema);
                batch.Dispose();
            }
        }
    }

    [SkippableFact]
    public async Task Read_is_deterministic_across_two_reads()
    {
        Gate();
        var connector = CreateSource();
        SkipIfNativeOnly(connector);
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var (rowCount1, column1) = await ReadFirstColumn(source, SmallDataset);
        var (rowCount2, column2) = await ReadFirstColumn(source, SmallDataset);

        Assert.Equal(rowCount1, rowCount2);
        Assert.Equal(column1, column2);
    }

    [SkippableFact]
    public async Task Partitions_union_equals_single_partition_read()
    {
        Gate();
        var connector = CreateSource();
        SkipIfNativeOnly(connector);
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var partitions = await source.PlanReadAsync(SmallDataset, ReadHints.None, CancellationToken.None);
        if (partitions.Count == 1)
        {
            Assert.True(true);
            return;
        }

        var singlePartitionSpec = GetSpecWithPartitionOverride(1);
        if (singlePartitionSpec is null)
        {
            // WEAK FALLBACK: the connector cannot force a single-partition read of the same logical
            // dataset (GetSpecWithPartitionOverride returned null), so the strongest check available is
            // re-plan idempotency -- this does NOT catch a deterministic duplicate/dropped row at a
            // partition boundary, since both sides replay the identical plan shape.
            var unionRows = await CountRows(partitions);
            var replanned = await source.PlanReadAsync(SmallDataset, ReadHints.None, CancellationToken.None);
            var replannedTotal = await CountRows(replanned);

            Assert.Equal(replannedTotal, unionRows);
            return;
        }

        // GROUND-TRUTH CHECK: compare the multi-partition read against a genuinely independent plan
        // (forced to 1 partition) of the SAME logical dataset -- total row count AND an order-insensitive
        // content digest must match. This catches duplicate/dropped rows at partition boundaries that a
        // mere re-plan-and-compare-counts check would miss.
        var (multiRowCount, multiDigest) = await ReadRowsAndDigest(partitions);

        var singlePartitions = await source.PlanReadAsync(singlePartitionSpec, ReadHints.None, CancellationToken.None);
        var (singleRowCount, singleDigest) = await ReadRowsAndDigest(singlePartitions);

        Assert.Equal(singleRowCount, multiRowCount);
        Assert.Equal(singleDigest, multiDigest);
    }

    [SkippableFact]
    public async Task Cancellation_honored_within_5s()
    {
        Gate();
        if (LargeDataset is null)
        {
            Assert.True(true);
            return;
        }

        var connector = CreateSource();
        SkipIfNativeOnly(connector);
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var partitions = await source.PlanReadAsync(LargeDataset, ReadHints.None, CancellationToken.None);
        var partition = partitions[0];

        using var cts = new CancellationTokenSource();
        var batchesSeen = 0;
        var readTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var batch in partition.ReadAsync(BatchOptions.Default, cts.Token))
                {
                    Interlocked.Increment(ref batchesSeen);
                    batch.Dispose();
                    if (Volatile.Read(ref batchesSeen) == 1)
                    {
                        cts.Cancel();
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Clean cancellation is an acceptable termination for this test.
            }
        });

        var winner = await Task.WhenAny(readTask, Task.Delay(TimeSpan.FromSeconds(5)));

        Assert.Same(readTask, winner);
        await readTask; // Propagate any unexpected (non-OperationCanceledException) failure.

        // A dataset satisfying the >= 100k row / "cancellation observable" contract must, at the
        // default batch size, take several batches to read in full. Bounding batchesSeen (rather than
        // only checking that *some* task finished within 5s) is what actually distinguishes "the
        // connector stopped because it honored cancellation" from "the connector ignored cancellation
        // but happened to finish quickly anyway" -- the latter would otherwise satisfy Task.WhenAny
        // trivially and let a broken connector pass.
        Assert.True(batchesSeen <= CancellationBatchTolerance,
            $"expected the read to stop within ~{CancellationBatchTolerance} batch(es) of cancellation, but saw {batchesSeen} batches");
    }

    [SkippableFact]
    public async Task Transient_failures_carry_is_transient()
    {
        Gate();
        if (TransientFailureDataset is null)
        {
            Assert.True(true);
            return;
        }

        var connector = CreateSource();
        SkipIfNativeOnly(connector);
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var partitions = await source.PlanReadAsync(TransientFailureDataset, ReadHints.None, CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            foreach (var partition in partitions)
            {
                await foreach (var batch in partition.ReadAsync(
                    new BatchOptions(TargetBatchBytes: SmallBatchTargetBytes), CancellationToken.None))
                {
                    batch.Dispose();
                }
            }
        });

        Assert.True(ex.IsTransient);
    }

    /// <summary>The executable contract every BoundedWindow-capable connector must satisfy —
    /// given a seed producing cursor values 0..10, a spec with lower=3 (exclusive)/upper=7 (inclusive)
    /// returns exactly cursor values 4,5,6,7. (a) rows with cursor &lt;= lower or &gt; upper are not
    /// returned; (b) connectors that don't opt in via <see cref="BoundedWindowDataset"/> never exercise
    /// this fact at all (the kit skips, via the null-hook check below).</summary>
    [SkippableFact]
    public async Task BoundedWindow_filters_to_lower_exclusive_upper_inclusive_cursor_range()
    {
        Gate();
        if (BoundedWindowDataset is null)
        {
            Assert.True(true);
            return;
        }

        var connector = CreateSource();
        SkipIfNativeOnly(connector);
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var (_, column) = await ReadFirstColumn(source, BoundedWindowDataset);

        var cursors = column.Select(v => Convert.ToInt64(v, CultureInfo.InvariantCulture)).OrderBy(v => v).ToList();
        Assert.Equal([4L, 5L, 6L, 7L], cursors);
    }

    /// <summary>The executable contract every InclusiveWatermarkBound-capable connector must
    /// satisfy. Uses <see cref="SmallDataset"/> (every subclass supplies one, >= 100 rows) rather than a
    /// dedicated hook: reads it once unfiltered to discover its own minimum "id" cursor value (no
    /// hardcoded row content assumed), then re-reads it twice more with that exact value stamped as
    /// <see cref="DatasetSpec.WatermarkCursor"/>="id"/<see cref="DatasetSpec.WatermarkValue"/> --
    /// once with the engine default (<see cref="DatasetSpec.WatermarkLowerInclusive"/>=false, strict
    /// <c>&gt;</c>) and once with it set true (<c>&gt;=</c>). The boundary row must be present in the
    /// inclusive read and absent from the strict read -- proving the flag actually changes behavior, not
    /// just that the connector didn't crash. Connectors that don't declare
    /// <see cref="ConnectorCapabilities.InclusiveWatermarkBound"/> never exercise this fact at all (the
    /// capability-gated Skip below) -- currently only Postgres/SqlServer, whose SmallDataset ("orders")
    /// both expose "id" as the leading column, which is why that column name is safe to assume here.</summary>
    [SkippableFact]
    public virtual async Task Inclusive_watermark_bound_returns_boundary_row()
    {
        Gate();
        var connector = CreateSource();
        SkipIfNativeOnly(connector);
        Skip.IfNot(connector.Capabilities.HasFlag(ConnectorCapabilities.InclusiveWatermarkBound),
            "connector does not declare InclusiveWatermarkBound");

        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var (_, unfiltered) = await ReadFirstColumn(source, SmallDataset);
        var minCursor = unfiltered.Select(v => Convert.ToInt64(v, CultureInfo.InvariantCulture)).Min();
        var boundaryValue = minCursor.ToString(CultureInfo.InvariantCulture);

        var exclusiveSpec = SmallDataset with { WatermarkCursor = "id", WatermarkValue = boundaryValue };
        var (_, exclusiveColumn) = await ReadFirstColumn(source, exclusiveSpec);
        var exclusiveCursors = exclusiveColumn.Select(v => Convert.ToInt64(v, CultureInfo.InvariantCulture)).ToList();

        var inclusiveSpec = exclusiveSpec with { WatermarkLowerInclusive = true };
        var (_, inclusiveColumn) = await ReadFirstColumn(source, inclusiveSpec);
        var inclusiveCursors = inclusiveColumn.Select(v => Convert.ToInt64(v, CultureInfo.InvariantCulture)).ToList();

        Assert.DoesNotContain(minCursor, exclusiveCursors);
        Assert.Contains(minCursor, inclusiveCursors);
        Assert.Equal(exclusiveCursors.Count + 1, inclusiveCursors.Count);
    }

    /// <summary>Routing: a connector declaring <see cref="ConnectorCapabilities.GatedOperations"/>
    /// must implement <see cref="IOperationGateAware"/> on its <see cref="ISource"/> and route every
    /// remote read operation through the engine-supplied gate. Connectors that don't declare the
    /// capability never exercise this fact at all (the capability-gated Skip below).</summary>
    [SkippableFact]
    public async Task Gated_connector_routes_reads_through_gate()
    {
        Gate();
        var connector = CreateSource();
        SkipIfNativeOnly(connector);
        Skip.If(!connector.Capabilities.HasFlag(ConnectorCapabilities.GatedOperations),
            "connector does not declare GatedOperations");

        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        Assert.True(source is IOperationGateAware,
            "a connector declaring GatedOperations must implement IOperationGateAware on its ISource");
        var gate = new CountingOperationGate();
        ((IOperationGateAware)source).UseOperationGate(gate);

        var partitions = await source.PlanReadAsync(SmallDataset, ReadHints.None, CancellationToken.None);
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(
                new BatchOptions(TargetBatchBytes: SmallBatchTargetBytes), CancellationToken.None))
            {
                batch.Dispose();
            }
        }

        Assert.True(gate.Calls >= 1, "a gated read produced zero gate operations");
    }

    /// <summary>No untracked retries: a transient failure injected AT the gate boundary must
    /// surface unchanged after exactly one gate call -- proving the connector performs no retry of its
    /// own outside the gate (node-level retry stays the sole backstop). The injected failure may
    /// surface from <c>PlanReadAsync</c> itself or from the subsequent read: a connector whose
    /// planning does a genuine remote operation (e.g. listing files to resolve partitions) is just as
    /// compliant gating that as gating the read -- either way, exactly one gate call happens and the
    /// failure is never retried.</summary>
    [SkippableFact]
    public async Task Gated_connector_does_not_retry_outside_gate()
    {
        Gate();
        var connector = CreateSource();
        SkipIfNativeOnly(connector);
        Skip.If(!connector.Capabilities.HasFlag(ConnectorCapabilities.GatedOperations),
            "connector does not declare GatedOperations");

        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var gate = new CountingOperationGate();
        ((IOperationGateAware)source).UseOperationGate(gate);
        gate.FailNextWith(new PzConnectorException("testkit: injected transient failure", isTransient: true));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            var partitions = await source.PlanReadAsync(SmallDataset, ReadHints.None, CancellationToken.None);
            foreach (var partition in partitions)
            {
                await foreach (var batch in partition.ReadAsync(
                    new BatchOptions(TargetBatchBytes: SmallBatchTargetBytes), CancellationToken.None))
                {
                    batch.Dispose();
                }
            }
        });

        Assert.True(ex.IsTransient, "the injected transient failure must surface unchanged");
        Assert.Equal(1, gate.Calls); // exactly one gate call: no connector-side retry happened.
    }

    /// <summary>Label hygiene: opLabel is a STATIC, connector-authored identifier -- never a
    /// URL, parameter, or any value derived from config/payloads (secret/PII hygiene binding
    /// convention). A URL scheme separator, query-string marker, or embedded space would betray a
    /// dynamic label.</summary>
    [SkippableFact]
    public async Task Gated_op_labels_are_static_tokens()
    {
        Gate();
        var connector = CreateSource();
        SkipIfNativeOnly(connector);
        Skip.If(!connector.Capabilities.HasFlag(ConnectorCapabilities.GatedOperations),
            "connector does not declare GatedOperations");

        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var gate = new CountingOperationGate();
        ((IOperationGateAware)source).UseOperationGate(gate);

        var partitions = await source.PlanReadAsync(SmallDataset, ReadHints.None, CancellationToken.None);
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(
                new BatchOptions(TargetBatchBytes: SmallBatchTargetBytes), CancellationToken.None))
            {
                batch.Dispose();
            }
        }

        foreach (var label in gate.Labels)
        {
            Assert.DoesNotContain("://", label);
            Assert.DoesNotContain("?", label);
            Assert.DoesNotContain(" ", label);
        }
    }

    [SkippableFact]
    public async Task Stable_partition_ids_are_present_unique_and_stable()
    {
        Gate();
        var connector = CreateSource();
        SkipIfNativeOnly(connector);
        Skip.If(!connector.Capabilities.HasFlag(ConnectorCapabilities.StablePartitionIds),
            "connector does not declare StablePartitionIds");

        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var first = await PlanIdsAsync(source);
        var second = await PlanIdsAsync(source);
        Assert.NotEmpty(first);
        Assert.Equal(first.Count, first.Distinct(StringComparer.Ordinal).Count());
        Assert.Equal(first, second);

        await using var reopened = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        Assert.Equal(first, await PlanIdsAsync(reopened));

        async Task<List<string>> PlanIdsAsync(ISource s)
        {
            var partitions = await s.PlanReadAsync(SmallDataset, ReadHints.None, CancellationToken.None);
            return partitions.Select(p =>
            {
                Assert.True(p is IIdentifiedPartition,
                    "a connector declaring StablePartitionIds must plan IIdentifiedPartition partitions");
                var id = ((IIdentifiedPartition)p).PartitionId;
                Assert.False(string.IsNullOrEmpty(id), "partition ids must be non-empty");
                return id;
            }).ToList();
        }
    }

    [SkippableFact]
    public void Checkpointable_connector_also_declares_stable_ids()
    {
        Gate();
        var connector = CreateSource();
        Skip.If(!connector.Capabilities.HasFlag(ConnectorCapabilities.CheckpointableReads),
            "connector does not declare CheckpointableReads");
        Assert.True(connector.Capabilities.HasFlag(ConnectorCapabilities.StablePartitionIds),
            "CheckpointableReads requires StablePartitionIds (the planner refuses the combination, PZ0319)");
    }

    [SkippableFact]
    public async Task Checkpoint_resume_yields_strictly_after_the_token()
    {
        Gate();
        var connector = CreateSource();
        SkipIfNativeOnly(connector);
        Skip.If(!connector.Capabilities.HasFlag(ConnectorCapabilities.CheckpointableReads),
            "connector does not declare CheckpointableReads");
        Skip.If(CheckpointDataset is null, "no CheckpointDataset provided");

        var options = new BatchOptions(TargetBatchBytes: SmallBatchTargetBytes);

        // Full read (reference row set). The source backing this phase's partition is opened, read, and
        // disposed before the next phase opens its own -- ICheckpointingPartition may depend on its
        // source staying alive while it is read, so the source must never be disposed early, but it must
        // also not leak across phases.
        (List<string> Keys, string? Token) full;
        {
            var (source, partition) = await FirstCheckpointingPartitionAsync(connector);
            await using var _ = source;
            full = await ReadKeysAsync(partition, options, stopAtToken: false);
        }

        Assert.Null(full.Token);

        // Prefix read: stop at the first offered token.
        (List<string> Keys, string? Token) prefix;
        {
            var (source, partition) = await FirstCheckpointingPartitionAsync(connector);
            await using var _ = source;
            prefix = await ReadKeysAsync(partition, options, stopAtToken: true);
        }

        Skip.If(prefix.Token is null, "the CheckpointDataset read never offered a checkpoint");
        Assert.NotEmpty(prefix.Keys);
        Assert.True(prefix.Keys.Count < full.Keys.Count, "the checkpoint must appear mid-read, not at the end");

        // Resume read.
        (List<string> Keys, string? Token) resumed;
        {
            var (source, partition) = await FirstCheckpointingPartitionAsync(connector);
            await using var _ = source;
            Assert.True(partition.TryResumeFrom(prefix.Token!), "a token the partition just produced must be resumable");
            resumed = await ReadKeysAsync(partition, options, stopAtToken: false);
        }

        Assert.Empty(prefix.Keys.Intersect(resumed.Keys, StringComparer.Ordinal));
        Assert.Equal(full.Keys.Order(StringComparer.Ordinal),
            prefix.Keys.Concat(resumed.Keys).Order(StringComparer.Ordinal));

        // Garbage token: refused, never thrown.
        {
            var (source, partition) = await FirstCheckpointingPartitionAsync(connector);
            await using var _ = source;
            Assert.False(partition.TryResumeFrom("pz-testkit-garbage-token"));
        }

        async Task<(ISource Source, ICheckpointingPartition Partition)> FirstCheckpointingPartitionAsync(ISourceConnector c)
        {
            var source = await c.OpenAsync(ValidConfig, CancellationToken.None);
            var partitions = await source.PlanReadAsync(CheckpointDataset!, ReadHints.None, CancellationToken.None);
            var checkpointing = partitions.OfType<ICheckpointingPartition>().FirstOrDefault();
            if (checkpointing is null)
            {
                await source.DisposeAsync();
                Skip.If(true, "CheckpointDataset planned no ICheckpointingPartition");
            }

            return (source, checkpointing!);
        }

        async Task<(List<string> Keys, string? Token)> ReadKeysAsync(
            ICheckpointingPartition partition, BatchOptions batchOptions, bool stopAtToken)
        {
            var keys = new List<string>();
            string? token = null;
            await foreach (var batch in partition.ReadAsync(batchOptions, CancellationToken.None))
            {
                using (batch)
                {
                    for (var row = 0; row < batch.Length; row++)
                    {
                        keys.Add(KeyAt(batch, CheckpointKeyColumn, row));
                    }
                }

                if (stopAtToken && partition.TryGetCheckpoint(out token) && token is not null)
                {
                    break;
                }
            }

            return (keys, token);
        }
    }

    // ---- change-capture acceptance facts ----

    /// <summary>Fact 1, "snapshot handoff": a first read yields the full table as inserts; mutations
    /// applied AFTER that read (with the resulting sync-state token as <see
    /// cref="DatasetSpec.PriorSyncState"/>) appear exactly once in a second read — collapsed to net
    /// state per key so mssql's overlap-allowed-but-keyed semantics (the same key legitimately surfacing
    /// more than once inside one window) never fail this on raw row counts.</summary>
    [SkippableFact]
    public async Task ChangeCapture_snapshot_then_poll_lands_mutations_exactly_once_per_key()
    {
        Gate();
        Skip.If(ChangeCaptureFixture is null, "no ChangeCaptureFixture provided");
        var fixture = ChangeCaptureFixture!;
        var connector = CreateSource();
        SkipIfNativeOnly(connector);
        Skip.If(!connector.Capabilities.HasFlag(ConnectorCapabilities.ChangeCapture),
            "connector does not declare ChangeCapture");

        var spec = await fixture.CdcSpecAsync();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var snapshotPartition = Assert.Single(
            await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));
        var (snapshotRows, _) = await ReadChangeRowsAsync(snapshotPartition);
        Assert.NotEmpty(snapshotRows);
        Assert.All(snapshotRows, r => Assert.Equal("insert", r.Op));

        Assert.True(((ISyncStatePartition)snapshotPartition).TryGetSyncStateCandidate(out var token));
        Assert.NotNull(token);

        // Mutations land BETWEEN the snapshot/position setup and the second read.
        await fixture.MutateAsync(inserts: 1, updates: 1, deletes: 1);

        var pollPartition = Assert.Single(await source.PlanReadAsync(
            spec with { PriorSyncState = token }, ReadHints.None, CancellationToken.None));
        var (pollRows, keyColumns) = await ReadChangeRowsAsync(pollPartition);
        Assert.NotEmpty(keyColumns);

        var net = CollapseNetByKey(pollRows, keyColumns);
        Assert.Equal(3, net.Count); // exactly the 3 keys this mutate touched -- no gap, no overlap
        Assert.Contains(pollRows, r => r.Op == "insert");
        Assert.Contains(pollRows, r => r.Op == "update");
        Assert.Contains(pollRows, r => r.Op == "delete");
    }

    /// <summary>Fact 2, "delete propagation": a deleted row lands as <c>_pz_op='delete'</c> with its key
    /// column(s) non-null (a delete carrying a null key would be useless to the engine's merge).</summary>
    [SkippableFact]
    public async Task ChangeCapture_delete_lands_as_delete_op_with_nonnull_key()
    {
        Gate();
        Skip.If(ChangeCaptureFixture is null, "no ChangeCaptureFixture provided");
        var fixture = ChangeCaptureFixture!;
        var connector = CreateSource();
        SkipIfNativeOnly(connector);
        Skip.If(!connector.Capabilities.HasFlag(ConnectorCapabilities.ChangeCapture),
            "connector does not declare ChangeCapture");

        var spec = await fixture.CdcSpecAsync();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var snapshotPartition = Assert.Single(
            await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));
        await foreach (var batch in snapshotPartition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            batch.Dispose();
        }

        Assert.True(((ISyncStatePartition)snapshotPartition).TryGetSyncStateCandidate(out var token));

        await fixture.MutateAsync(inserts: 0, updates: 0, deletes: 1);

        var pollPartition = Assert.Single(await source.PlanReadAsync(
            spec with { PriorSyncState = token }, ReadHints.None, CancellationToken.None));
        var (rows, keyColumns) = await ReadChangeRowsAsync(pollPartition);
        Assert.NotEmpty(keyColumns);

        var delete = Assert.Single(rows, r => r.Op == "delete");
        foreach (var key in keyColumns)
        {
            Assert.True(delete.Columns.TryGetValue(key, out var value) && value is not null,
                $"a delete change row must carry a non-null key column '{key}'");
        }
    }

    /// <summary>Fact 3, "window replay idempotency": polling the SAME <see
    /// cref="DatasetSpec.PriorSyncState"/> twice yields byte-equal per-key net results — proving a retry
    /// of a not-yet-committed poll (or a genuine at-least-once replay) never diverges.</summary>
    [SkippableFact]
    public async Task ChangeCapture_replaying_the_same_prior_sync_state_yields_equal_net_state()
    {
        Gate();
        Skip.If(ChangeCaptureFixture is null, "no ChangeCaptureFixture provided");
        var fixture = ChangeCaptureFixture!;
        var connector = CreateSource();
        SkipIfNativeOnly(connector);
        Skip.If(!connector.Capabilities.HasFlag(ConnectorCapabilities.ChangeCapture),
            "connector does not declare ChangeCapture");

        var spec = await fixture.CdcSpecAsync();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var snapshotPartition = Assert.Single(
            await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));
        await foreach (var batch in snapshotPartition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            batch.Dispose();
        }

        Assert.True(((ISyncStatePartition)snapshotPartition).TryGetSyncStateCandidate(out var token));

        await fixture.MutateAsync(inserts: 1, updates: 1, deletes: 1);
        var pollSpec = spec with { PriorSyncState = token };

        var partition1 = Assert.Single(
            await source.PlanReadAsync(pollSpec, ReadHints.None, CancellationToken.None));
        var (rows1, keys1) = await ReadChangeRowsAsync(partition1);
        var net1 = CollapseNetByKey(rows1, keys1);

        var partition2 = Assert.Single(
            await source.PlanReadAsync(pollSpec, ReadHints.None, CancellationToken.None));
        var (rows2, keys2) = await ReadChangeRowsAsync(partition2);
        var net2 = CollapseNetByKey(rows2, keys2);

        Assert.Equal(
            net1.OrderBy(kv => kv.Key, StringComparer.Ordinal),
            net2.OrderBy(kv => kv.Key, StringComparer.Ordinal));
    }

    /// <summary>Fact 4, "contract shape": the three <c>_pz_</c> header columns come first (string,
    /// string, timestamp) followed by the dataset columns; <c>_pz_lsn</c> is fixed-width and
    /// ordinal-increasing within one read; both partition interfaces yield a candidate/keys.</summary>
    [SkippableFact]
    public async Task ChangeCapture_contract_shape_is_pz_header_then_columns_with_ordinal_lsn()
    {
        Gate();
        Skip.If(ChangeCaptureFixture is null, "no ChangeCaptureFixture provided");
        var fixture = ChangeCaptureFixture!;
        var connector = CreateSource();
        SkipIfNativeOnly(connector);
        Skip.If(!connector.Capabilities.HasFlag(ConnectorCapabilities.ChangeCapture),
            "connector does not declare ChangeCapture");

        var spec = await fixture.CdcSpecAsync();
        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var snapshotPartition = Assert.Single(
            await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));
        Schema? schema = null;
        await foreach (var batch in snapshotPartition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            schema ??= batch.Schema;
            batch.Dispose();
        }

        Assert.NotNull(schema);
        Assert.Equal("_pz_op", schema!.FieldsList[0].Name);
        Assert.Equal("_pz_lsn", schema.FieldsList[1].Name);
        Assert.Equal("_pz_changed_at", schema.FieldsList[2].Name);
        Assert.Equal(ArrowTypeId.String, schema.FieldsList[0].DataType.TypeId);
        Assert.Equal(ArrowTypeId.String, schema.FieldsList[1].DataType.TypeId);
        Assert.Equal(ArrowTypeId.Timestamp, schema.FieldsList[2].DataType.TypeId);

        Assert.True(((ISyncStatePartition)snapshotPartition).TryGetSyncStateCandidate(out var token));
        Assert.NotNull(token);

        await fixture.MutateAsync(inserts: 1, updates: 1, deletes: 1);

        var pollPartition = Assert.Single(await source.PlanReadAsync(
            spec with { PriorSyncState = token }, ReadHints.None, CancellationToken.None));
        var (rows, keyColumns) = await ReadChangeRowsAsync(pollPartition);

        Assert.NotEmpty(rows);
        var width = rows[0].Lsn.Length;
        Assert.All(rows, r => Assert.Equal(width, r.Lsn.Length)); // fixed-width
        for (var i = 1; i < rows.Count; i++)
        {
            Assert.True(string.CompareOrdinal(rows[i].Lsn, rows[i - 1].Lsn) > 0,
                "_pz_lsn must be ordinal-increasing within one read");
        }

        Assert.NotEmpty(keyColumns); // IChangeCapturePartition yields the keys
        Assert.True(((ISyncStatePartition)pollPartition).TryGetSyncStateCandidate(out var candidate));
        Assert.NotNull(candidate); // ISyncStatePartition yields a candidate
    }

    /// <summary>Fact 5, "retention/teardown honesty" (mssql-shaped default): a prior position before the
    /// server's retained minimum must make the read THROW, never silently skip the gap. Derives a
    /// synthetically stale token (all zeros, same length as <see
    /// cref="IChangeCaptureFixture.ServerPositionAsync"/>'s real one) — guaranteed below any real,
    /// already-primed retained minimum. Virtual so Postgres — whose poll resumes from the SLOT's own
    /// server-side position rather than the caller-supplied token, so a too-old token alone proves
    /// nothing — overrides this with slot-drop teardown semantics instead (see
    /// <c>PostgresSourceAcceptance</c>).</summary>
    [SkippableFact]
    public virtual async Task ChangeCapture_position_before_retained_minimum_throws()
    {
        Gate();
        Skip.If(ChangeCaptureFixture is null, "no ChangeCaptureFixture provided");
        var fixture = ChangeCaptureFixture!;
        var connector = CreateSource();
        SkipIfNativeOnly(connector);
        Skip.If(!connector.Capabilities.HasFlag(ConnectorCapabilities.ChangeCapture),
            "connector does not declare ChangeCapture");

        var spec = await fixture.CdcSpecAsync();
        var position = await fixture.ServerPositionAsync();
        Skip.If(position is null, "fixture reports no server position");

        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var stalePrior = new string('0', position.Length);
        var pollPartition = Assert.Single(await source.PlanReadAsync(
            spec with { PriorSyncState = stalePrior }, ReadHints.None, CancellationToken.None));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var batch in pollPartition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batch.Dispose();
            }
        });
        Assert.False(ex.IsTransient);
    }

    /// <summary>One change row: the op, the raw <c>_pz_lsn</c> string, and every dataset column keyed by
    /// name (the three <c>_pz_</c> header columns folded down to just <see cref="Op"/>/<see
    /// cref="Lsn"/> — <c>_pz_changed_at</c> is asserted separately by
    /// <c>ChangeCapture_contract_shape_is_pz_header_then_columns_with_ordinal_lsn</c>).</summary>
    private readonly record struct ChangeRow(string Op, string Lsn, IReadOnlyDictionary<string, object?> Columns);

    /// <summary>Drains every batch of a change-capture partition's read and, once fully drained (the
    /// contract both partition capabilities require), reports the key columns
    /// <see cref="IChangeCapturePartition"/> yields — empty if the partition doesn't implement it or
    /// reports none.</summary>
    private async Task<(List<ChangeRow> Rows, IReadOnlyList<string> KeyColumns)> ReadChangeRowsAsync(
        IDatasetPartition partition)
    {
        var rows = new List<ChangeRow>();
        Schema? schema = null;
        await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            using (batch)
            {
                schema ??= batch.Schema;
                var op = (StringArray)batch.Column(0);
                var lsn = (StringArray)batch.Column(1);
                for (var row = 0; row < batch.Length; row++)
                {
                    var columns = new Dictionary<string, object?>(StringComparer.Ordinal);
                    for (var col = 3; col < batch.ColumnCount; col++)
                    {
                        columns[schema.FieldsList[col].Name] = GetScalar(batch.Column(col), row);
                    }

                    rows.Add(new ChangeRow(op.GetString(row), lsn.GetString(row), columns));
                }
            }
        }

        IReadOnlyList<string> keyColumns = [];
        if (partition is IChangeCapturePartition changeCapture &&
            changeCapture.TryGetChangeKeyColumns(out var keys) && keys is not null)
        {
            keyColumns = keys;
        }

        return (rows, keyColumns);
    }

    /// <summary>Collapses change rows to their NET state per key (last write wins, in emission/commit
    /// order) -- a connector-agnostic comparison: mssql's change-window semantics
    /// can legitimately surface the same key more than once inside a window, so a raw row-by-row
    /// comparison would be flaky where a per-key net comparison is exact.</summary>
    private static Dictionary<string, string> CollapseNetByKey(
        IReadOnlyList<ChangeRow> rows, IReadOnlyList<string> keyColumns)
    {
        var net = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var row in rows)
        {
            var key = string.Join('\u001f', keyColumns.Select(k => RenderNetValue(row.Columns.GetValueOrDefault(k))));
            var value = row.Op + '\u001f' + string.Join('\u001f', row.Columns
                .OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .Select(kv => $"{kv.Key}={RenderNetValue(kv.Value)}"));
            net[key] = value; // last write wins -- rows are appended in read (commit) order
        }

        return net;
    }

    private static string RenderNetValue(object? value) => value switch
    {
        null => "\u0000NULL",
        DateTimeOffset dto => dto.ToString("O", CultureInfo.InvariantCulture),
        DateTime dt => dt.ToString("O", CultureInfo.InvariantCulture),
        IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
        _ => value.ToString() ?? "",
    };

    private async Task<long> CountRows(IReadOnlyList<IDatasetPartition> partitions)
    {
        long rows = 0;
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(
                new BatchOptions(TargetBatchBytes: SmallBatchTargetBytes), CancellationToken.None))
            {
                rows += batch.Length;
                batch.Dispose();
            }
        }

        return rows;
    }

    /// <summary>Reads every row of every partition and returns (a) the total row count and (b) an
    /// order-insensitive SHA-256 digest over the row contents: each row is rendered as a canonical
    /// delimited string across all columns (covering the v0 type matrix -- int32/int64/double/
    /// decimal128/utf8/bool/date32/timestamp), the resulting strings are sorted, and the sorted list is
    /// hashed. Order-insensitivity matters because a single-partition plan and a multi-partition plan of
    /// the same logical dataset need not yield rows in the same interleaving.</summary>
    private async Task<(long RowCount, string Digest)> ReadRowsAndDigest(IReadOnlyList<IDatasetPartition> partitions)
    {
        var rows = new List<string>();
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(
                new BatchOptions(TargetBatchBytes: SmallBatchTargetBytes), CancellationToken.None))
            {
                for (var row = 0; row < batch.Length; row++)
                {
                    var rowBuilder = new StringBuilder();
                    for (var col = 0; col < batch.ColumnCount; col++)
                    {
                        if (col > 0)
                        {
                            rowBuilder.Append('\u0001');
                        }

                        rowBuilder.Append(CanonicalScalarValue(batch.Column(col), row));
                    }

                    rows.Add(rowBuilder.ToString());
                }

                batch.Dispose();
            }
        }

        rows.Sort(StringComparer.Ordinal);
        var digestBytes = SHA256.HashData(Encoding.UTF8.GetBytes(string.Join('\n', rows)));
        return (rows.Count, Convert.ToHexString(digestBytes));
    }

    /// <summary>Renders one value as a culture-invariant canonical string, covering the fixed v0 type
    /// matrix (int32/int64/double/decimal128/utf8/bool/date32/timestamp) -- see
    /// <see cref="Abstractions.Batches.ArrowBatchBuilder"/>.</summary>
    private static string CanonicalScalarValue(IArrowArray array, int index)
    {
        if (array.IsNull(index))
        {
            return "\u0000NULL";
        }

        return array switch
        {
            Int32Array a => a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
            Int64Array a => a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
            DoubleArray a => a.GetValue(index)!.Value.ToString("R", CultureInfo.InvariantCulture),
            Decimal128Array a => a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
            BooleanArray a => a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture),
            Date32Array a => a.GetDateTime(index)!.Value.ToString("O", CultureInfo.InvariantCulture),
            TimestampArray a => a.GetTimestamp(index)!.Value.ToString("O", CultureInfo.InvariantCulture),
            StringArray a => a.GetString(index),
            _ => throw new NotSupportedException(
                $"unsupported array type {array.GetType()} in TestKit partition-union content digest"),
        };
    }

    private async Task<(long RowCount, List<object?> FirstColumn)> ReadFirstColumn(ISource source, DatasetSpec spec)
    {
        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        long rowCount = 0;
        var values = new List<object?>();
        foreach (var partition in partitions)
        {
            await foreach (var batch in partition.ReadAsync(
                new BatchOptions(TargetBatchBytes: SmallBatchTargetBytes), CancellationToken.None))
            {
                var column = batch.Column(0);
                for (var i = 0; i < column.Length; i++)
                {
                    values.Add(GetScalar(column, i));
                }

                rowCount += batch.Length;
                batch.Dispose();
            }
        }

        return (rowCount, values);
    }

    /// <summary>A connector declaring <see cref="ConnectorCapabilities.NativeScan"/> must actually offer
    /// one for its own dataset. The suite does NOT execute the fragment: running it would put DuckDB
    /// into every connector's test dependencies, and the TestKit deliberately depends on nothing but the
    /// ABI and xunit. What it can prove is everything the planner relies on before execution — that a
    /// scan is offered, that asking twice is free and gives the same answer, and that the mechanism it
    /// names carries no location or credential.</summary>
    [SkippableFact]
    public async Task NativeScan_is_offered_when_the_capability_is_declared()
    {
        Gate();
        var connector = CreateSource();
        Skip.IfNot(connector.Capabilities.HasFlag(ConnectorCapabilities.NativeScan),
            "connector does not declare NativeScan");

        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        Assert.True(
            source.TryGetNativeScan(NativeScanDataset, out var scan),
            "a connector declaring NativeScan must offer one for its own NativeScanDataset");
        Assert.False(string.IsNullOrWhiteSpace(scan!.SqlFragment));
        Assert.NotNull(scan.SetupStatements);
    }

    /// <summary>TryGetNativeScan is documented as cheap and offline, which makes it a pure function of
    /// the spec: the planner may call it more than once, and a fragment that differed between calls
    /// would make a plan disagree with the run it produced.</summary>
    [SkippableFact]
    public async Task NativeScan_is_repeatable_for_the_same_spec()
    {
        Gate();
        var connector = CreateSource();
        Skip.IfNot(connector.Capabilities.HasFlag(ConnectorCapabilities.NativeScan),
            "connector does not declare NativeScan");

        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        Skip.IfNot(source.TryGetNativeScan(NativeScanDataset, out var first), "no native scan offered");
        Assert.True(source.TryGetNativeScan(NativeScanDataset, out var second));

        Assert.Equal(first!.SqlFragment, second!.SqlFragment);
        Assert.Equal(first.SetupStatements, second.SetupStatements);
        Assert.Equal(first.Mechanism, second.Mechanism);
    }

    /// <summary>Secret hygiene at the one boundary where a connector hands pz a string bound for planner
    /// Reason strings and plan.json: <see cref="NativeScan.Mechanism"/> is a short, static description of
    /// the mechanism, so it must not carry a URL or a query string — the shapes a credential or a
    /// location travels in. Setup statements may legitimately contain secrets (CREATE SECRET is exactly
    /// what they are for) and are deliberately not asserted on; keeping those unlogged is the engine's
    /// job, not the connector's.</summary>
    [SkippableFact]
    public async Task NativeScan_mechanism_is_a_static_token()
    {
        Gate();
        var connector = CreateSource();
        Skip.IfNot(connector.Capabilities.HasFlag(ConnectorCapabilities.NativeScan),
            "connector does not declare NativeScan");

        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        Skip.IfNot(source.TryGetNativeScan(NativeScanDataset, out var scan), "no native scan offered");
        Skip.If(scan!.Mechanism is null, "connector declares no mechanism");

        Assert.DoesNotContain("://", scan.Mechanism!);
        Assert.DoesNotContain("?", scan.Mechanism!);
    }

    /// <summary>A native-only source still owes the engine a schema — the planner reads one before it
    /// ever executes a scan — so GetSchemaAsync must work on the path that has no PlanReadAsync.</summary>
    [SkippableFact]
    public async Task NativeScan_source_still_declares_a_schema()
    {
        Gate();
        var connector = CreateSource();
        Skip.IfNot(connector.Capabilities.HasFlag(ConnectorCapabilities.NativeScan),
            "connector does not declare NativeScan");

        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);
        var declared = await source.GetSchemaAsync(NativeScanDataset, CancellationToken.None);

        Assert.NotNull(declared.Schema);
        Assert.NotEmpty(declared.Schema.FieldsList);
    }

    /// <summary><see cref="INativeOnlySource"/> means PlanReadAsync always throws, and the refusal is
    /// part of the contract rather than an accident: the planner turns engine.force_universal into PZ0312
    /// for these connectors, and one that returned an empty partition list instead would produce a
    /// silently empty run.</summary>
    [SkippableFact]
    public async Task Native_only_source_refuses_the_universal_read_path()
    {
        Gate();
        var connector = CreateSource();
        Skip.If(connector is not INativeOnlySource, "connector is not INativeOnlySource");

        await using var source = await connector.OpenAsync(ValidConfig, CancellationToken.None);

        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            var partitions = await source.PlanReadAsync(NativeScanDataset, ReadHints.None, CancellationToken.None);
            foreach (var partition in partitions)
            {
                await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
                {
                    batch.Dispose();
                }
            }
        });

        // Permanent, not transient: retrying cannot make a universal read path exist.
        if (ex is PzConnectorException connectorException)
        {
            Assert.False(connectorException.IsTransient);
        }
    }

    private static void AssertSchemasMatch(Schema expected, Schema actual)
    {
        Assert.Equal(expected.FieldsList.Count, actual.FieldsList.Count);
        for (var i = 0; i < expected.FieldsList.Count; i++)
        {
            Assert.Equal(expected.FieldsList[i].Name, actual.FieldsList[i].Name);
            Assert.Equal(expected.FieldsList[i].DataType.TypeId, actual.FieldsList[i].DataType.TypeId);
        }
    }

    /// <summary>Extracts one value from an <see cref="IArrowArray"/> as a boxed scalar, covering the
    /// fixed type matrix <see cref="Abstractions.Batches.ArrowBatchBuilder"/> supports.</summary>
    private static object? GetScalar(IArrowArray array, int index)
    {
        if (array.IsNull(index))
        {
            return null;
        }

        return array switch
        {
            Int32Array a => a.GetValue(index),
            Int64Array a => a.GetValue(index),
            DoubleArray a => a.GetValue(index),
            BooleanArray a => a.GetValue(index),
            Date32Array a => a.GetDateTime(index),
            TimestampArray a => a.GetTimestamp(index),
            Decimal128Array a => a.GetValue(index),
            StringArray a => a.GetString(index),
            _ => throw new NotSupportedException($"unsupported array type {array.GetType()} in TestKit determinism check"),
        };
    }

    private static string KeyAt(RecordBatch batch, int column, int row) => batch.Column(column) switch
    {
        StringArray s => s.GetString(row),
        Int64Array i => i.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        Int32Array i => i.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture),
        var other => throw new NotSupportedException(
            $"CheckpointKeyColumn is a {other.GetType().Name}; use a string or integer key column"),
    };
}
