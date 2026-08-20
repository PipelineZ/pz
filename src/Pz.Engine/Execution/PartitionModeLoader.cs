using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.DuckDb;

namespace Pz.Engine.Execution;

/// <summary>Partition-scoped extraction: each partition appends into its own
/// part table via per-batch session calls; a short transaction moves a completed partition into
/// the main staging table together with its pz_meta done row, so any later attempt — same run or
/// pz retry — skips it. This type owns no retry: partition failures aggregate into ONE thrown
/// PzConnectorException (transient iff every recorded failure was) and
/// KindDispatchingExecutor's node attempt loop is the only retry driver.
///
/// Fault isolation: a partition's own read fault is isolated — recorded and the
/// partition's part table dropped, but siblings keep running. An ENGINE-side fault (append/ledger
/// statement failure) is a different regime entirely: it escapes <see cref="RunPartitionAsync"/> raw
/// and self-cancels the shared <see cref="CancellationTokenSource"/> from INSIDE the faulting task,
/// mirroring how the legacy channel path's <c>PumpPartitionAsync</c> self-cancels <c>pumpCts</c> in its
/// own catch — siblings tear down immediately rather than only once the orchestration's
/// <see cref="Task.WhenAll(Task[])"/> observes the fault.</summary>
internal static class PartitionModeLoader
{
    public static async Task<(PzError? Error, long Rows, PartitionStats Stats)> LoadAsync(
        DagNode node, RunContext ctx, IReadOnlyList<(IIdentifiedPartition Partition, string Id)> partitions,
        Schema schema, string mainTable, string? windowLower, string? windowUpper, long reusedCount,
        bool isSyncDataset, CancellationToken ct)
    {
        var duck = ctx.Duck;
        var nodeId = node.Id.Value;
        var nodeKey = PartitionLedger.NodeKey(node);

        await PartitionLedger.EnsureAsync(duck, ct).ConfigureAwait(false);
        await PartitionLedger.CleanupLeftoversAsync(duck, nodeId, nodeKey, ct).ConfigureAwait(false);
        if (windowUpper is not null)
        {
            await PartitionLedger.UpsertWindowAsync(duck, nodeId, windowLower!, windowUpper, ct).ConfigureAwait(false);
        }

        await EnsureMainTableAsync(duck, mainTable, schema, ct).ConfigureAwait(false);
        var done = await PartitionLedger.ReadDoneAsync(duck, nodeId, ct).ConfigureAwait(false);

        IReadOnlyList<(IIdentifiedPartition Partition, string Id)> pending;
        if (isSyncDataset)
        {
            // Sync done-skip exception: a sync dataset's single completed partition is
            // NEVER skip-reused within a run — the sync token candidate can only be captured from a
            // live read, and skipping would silently stall sync-state advancement. Reset it (main
            // rows + both ledger rows in one transaction; part/seg tables best-effort) and let it
            // re-read below exactly like any other pending partition. `done.Remove` also keeps the
            // stats formula below correct — a reset partition is no longer "already done".
            foreach (var p in partitions)
            {
                if (done.Remove(p.Id))
                {
                    await ResetSyncDonePartitionAsync(duck, nodeId, nodeKey, mainTable, p.Id, ct).ConfigureAwait(false);
                }
            }

            pending = partitions;
        }
        else
        {
            pending = partitions.Where(p => !done.ContainsKey(p.Id)).ToArray();
        }

        // Read once the whole set of prior checkpoint rows -- read AFTER the
        // sync-reset loop above so a reset partition's just-deleted checkpoint row can never appear
        // stale here. `resumedCount` feeds PartitionStats.Resumed; a local function (not a lambda
        // over a captured local) keeps the Interlocked increment obviously safe under concurrent
        // partition tasks.
        var checkpoints = await PartitionLedger.ReadCheckpointsAsync(duck, nodeId, ct).ConfigureAwait(false);
        var resumedCount = 0;
        void OnResumed() => Interlocked.Increment(ref resumedCount);

        var failures = new ConcurrentQueue<PzConnectorException>();

        // Bounded fan-out: without the shared bounded channel there is no backpressure, so the
        // same ProcessorCount bound the streaming path uses applies here.
        using var admission = new SemaphoreSlim(Environment.ProcessorCount);
        using var loadCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var tasks = pending.Select(p => RunAdmittedAsync(duck, p.Partition, p.Id, nodeId, nodeKey, mainTable,
            schema, ctx.EffectiveBatch, admission, loadCts, failures, checkpoints, OnResumed)).ToArray();

        try
        {
            await AwaitAllAsync(tasks, loadCts).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Pure run cancellation (the caller's own token, not a sibling's self-cancel) is not a
            // failure surface — no notice, propagate as-is.
            throw;
        }
        catch
        {
            // A sibling engine fault (or the self-cancel it triggered) is about to escape raw below —
            // without this, any failures already recorded by isolated siblings would vanish without a
            // trace — no silent failures.
            EmitRecordedFailuresNotice(ctx, failures);
            throw;
        }

        ThrowIfAnyFailed(failures, partitions.Count);

        var rows = await duck.ScalarAsync<long>($"select count(*) from {mainTable}", ct).ConfigureAwait(false);
        var stats = new PartitionStats(partitions.Count, done.Count + pending.Count, reusedCount, resumedCount);
        return (null, rows, stats);
    }

    /// <summary>Streaming sibling of <see cref="LoadAsync"/>: partitions arrive lazily from <paramref name="stream"/>, so identity (StablePartitionIds contract) is
    /// validated AT ADMISSION time — one partition at a time — instead of up front over a whole
    /// materialized list. Admission mirrors <c>SourceLoadExecutor.PumpStreamingPartitionsAsync</c>:
    /// enumerate under <c>loadCts.Token</c>, a <see cref="SemaphoreSlim"/> of
    /// <see cref="Environment.ProcessorCount"/> bounds in-flight reads, and the task list is pruned of
    /// already-succeeded entries after each admission so it never grows unbounded over a
    /// millions-of-partitions source. A sync dataset can never reach here — the planner already
    /// refuses a streaming + sync combination (<see cref="PzErrorCode.SyncPartitionedReadConflict"/>),
    /// so there is no done-skip exception to plumb through this path.</summary>
    public static async Task<(PzError? Error, long Rows, PartitionStats Stats)> LoadStreamingAsync(
        DagNode node, RunContext ctx, IAsyncEnumerable<IDatasetPartition> stream, Schema schema,
        string mainTable, string? windowLower, string? windowUpper, long reusedCount, SourceDatasetDef def,
        CancellationToken ct)
    {
        var duck = ctx.Duck;
        var nodeId = node.Id.Value;
        var nodeKey = PartitionLedger.NodeKey(node);

        await PartitionLedger.EnsureAsync(duck, ct).ConfigureAwait(false);
        await PartitionLedger.CleanupLeftoversAsync(duck, nodeId, nodeKey, ct).ConfigureAwait(false);
        if (windowUpper is not null)
        {
            await PartitionLedger.UpsertWindowAsync(duck, nodeId, windowLower!, windowUpper, ct).ConfigureAwait(false);
        }

        await EnsureMainTableAsync(duck, mainTable, schema, ct).ConfigureAwait(false);
        var done = await PartitionLedger.ReadDoneAsync(duck, nodeId, ct).ConfigureAwait(false);

        // A sync dataset can never reach this streaming path (the planner
        // already refuses PartitionedRead+Sync as a combination), so there is no reset-loop
        // staleness concern here the way there is in LoadAsync above.
        var checkpoints = await PartitionLedger.ReadCheckpointsAsync(duck, nodeId, ct).ConfigureAwait(false);
        var resumedCount = 0;
        void OnResumed() => Interlocked.Increment(ref resumedCount);

        var failures = new ConcurrentQueue<PzConnectorException>();
        using var admission = new SemaphoreSlim(Environment.ProcessorCount);
        using var loadCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var tasks = new List<Task>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var observed = 0;
        var admitted = 0;
        PzError? identityError = null;

        try
        {
            await foreach (var partition in stream.WithCancellation(loadCts.Token).ConfigureAwait(false))
            {
                observed++;
                if (partition is not IIdentifiedPartition identified)
                {
                    identityError = IdentityError(def, observed, "does not implement IIdentifiedPartition");
                    loadCts.Cancel();
                    break;
                }

                if (string.IsNullOrEmpty(identified.PartitionId))
                {
                    identityError = IdentityError(def, observed, "has an empty id");
                    loadCts.Cancel();
                    break;
                }

                var pid = identified.PartitionId;

                // An already-done id is the expected resume-skip case, not a duplicate
                // violation — a re-planned stream is expected to still emit ids it already finished.
                if (done.ContainsKey(pid))
                {
                    continue;
                }

                if (!seen.Add(pid))
                {
                    identityError = IdentityError(def, observed, "duplicates another partition's id");
                    loadCts.Cancel();
                    break;
                }

                await admission.WaitAsync(loadCts.Token).ConfigureAwait(false);
                tasks.RemoveAll(t => t.IsCompletedSuccessfully);
                tasks.Add(RunThenReleaseAsync(duck, identified, pid, nodeId, nodeKey, mainTable, schema,
                    ctx.EffectiveBatch, loadCts, failures, admission, checkpoints, OnResumed));
                admitted++;
            }
        }
        catch (OperationCanceledException) when (identityError is null && !ct.IsCancellationRequested)
        {
            // The enumeration itself observed loadCts's cancellation, but NOT because the caller's own
            // `ct` was cancelled — so this must be an already-admitted partition's engine-side fault
            // self-cancelling loadCts from inside its own task. Swallowed here only because
            // Task.WhenAll(tasks) below re-observes and surfaces that exact same fault from the task
            // that raised it.
        }
        catch (OperationCanceledException) when (identityError is null)
        {
            // Genuine external run cancellation observed directly by the enumerator — with zero or few
            // tasks admitted so far, Task.WhenAll(tasks) below might complete over an EMPTY list without
            // ever throwing (Task.WhenAll([]) does not throw), which would otherwise silently swallow a
            // real cancellation instead of propagating it (mirrors the list path's
            // Run_cancellation_propagates_as_cancellation contract). Drain whatever was admitted, then
            // rethrow explicitly.
            foreach (var task in tasks)
            {
                try { await task.ConfigureAwait(false); }
                catch { /* teardown cancellations/faults are expected once cancellation is underway */ }
            }

            throw;
        }

        if (identityError is not null)
        {
            ExceptionDispatchInfo? engineFault = null;
            foreach (var task in tasks)
            {
                try { await task.ConfigureAwait(false); }
                catch (OperationCanceledException) { /* teardown cancellation, expected once identity has already failed */ }
                catch (Exception ex)
                {
                    // A genuine engine-side fault must still escape raw — an
                    // identity error must never swallow one. First fault wins; keep draining the rest.
                    engineFault ??= ExceptionDispatchInfo.Capture(ex);
                }
            }

            if (engineFault is not null)
            {
                engineFault.Throw();
            }

            EmitRecordedFailuresNotice(ctx, failures);

            // Ordinals only — raw partition ids never surface in errors.
            return (identityError, 0, new PartitionStats(observed, done.Count + admitted, reusedCount, resumedCount));
        }

        try
        {
            await AwaitAllAsync(tasks, loadCts).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Pure run cancellation (the caller's own token, not a sibling's self-cancel) is not a
            // failure surface — no notice, propagate as-is.
            throw;
        }
        catch
        {
            // A sibling engine fault (or the self-cancel it triggered) is about to escape raw below —
            // without this, any failures already recorded by isolated siblings would vanish without a
            // trace — no silent failures.
            EmitRecordedFailuresNotice(ctx, failures);
            throw;
        }

        ThrowIfAnyFailed(failures, observed);

        var rows = await duck.ScalarAsync<long>($"select count(*) from {mainTable}", ct).ConfigureAwait(false);
        var stats = new PartitionStats(observed, done.Count + admitted, reusedCount, resumedCount);
        return (null, rows, stats);
    }

    /// <summary>Pre-populates this run's main table + pz_meta from a FAILED prior
    /// run's staging DB, so LoadAsync skips the already-done partitions. Returns the number of
    /// done partitions copied; 0 means "extract fresh" (silent when the prior simply has no
    /// accounting; noticed when a guard rejects it). List-planned partitions only — the caller
    /// guarantees plannedIds is the complete fresh plan.</summary>
    public static async Task<long> TryCopyPartialAsync(DagNode node, SourceDatasetDef def, RunContext ctx,
        IReadOnlySet<string> plannedIds, string mainTable, string? windowLower, string? windowUpper,
        PartialReuseEntry entry, CancellationToken ct)
    {
        var duck = ctx.Duck;
        var nodeId = node.Id.Value;
        var nodeKey = PartitionLedger.NodeKey(node);
        var datasetLabel = $"{def.Source.Name}.{def.Dataset.Name}";
        var alias = "pz_prior4_" + nodeKey;
        var quotedAlias = ArrowInterop.QuoteQualified(alias);
        var pathLiteral = entry.PriorStagingPath.Replace("'", "''");
        var attached = false;
        var copiedMain = false;
        var copiedLedger = false;
        var copiedParts = new List<string>();

        // Retry-safety guard: SourceLoadExecutor.ExecutePartitionModeAsync
        // calls this method on EVERY node attempt, because KindDispatchingExecutor's retry loop
        // re-runs the whole executor over the SAME ctx/staging DB. If this node's LOCAL ledger
        // (no catalog arg -- this run's own pz_meta, not the prior failed run's) already has any
        // done or checkpointed rows, that means either an earlier attempt of THIS run already
        // applied the copy below (and possibly extracted further partitions on top of it), or this
        // run made progress on its own without ever copying. Either way, the retry-safety contract
        // ("a retried attempt consults the ledger and re-runs only what is missing" -- see this
        // type's and KindDispatchingExecutor's doc comments) already owns that state; the
        // create-or-replace copy below is destructive (it wipes the main table and this node's
        // ledger rows before repopulating them from the prior run) and must never run over progress
        // a retry attempt is responsible for preserving. Bail out immediately and silently (no
        // notice) -- this is not a guard rejection, it is "there is nothing for this method to do."
        // Accepted side effect: on a retried node, PartitionStats.Reused reports 0 even when the
        // surviving done rows actually originated from a prior-run copy applied on an earlier
        // attempt -- Reused is informational and scoped to what THIS successful attempt copied.
        await PartitionLedger.EnsureAsync(duck, ct).ConfigureAwait(false);
        var localDone = await PartitionLedger.ReadDoneAsync(duck, nodeId, ct).ConfigureAwait(false);
        var localCheckpoints = await PartitionLedger.ReadCheckpointsAsync(duck, nodeId, ct).ConfigureAwait(false);
        if (localDone.Count > 0 || localCheckpoints.Count > 0)
        {
            return 0;
        }

        try
        {
            await duck.ExecuteAsync($"attach '{pathLiteral}' as {quotedAlias} (read_only)", ct).ConfigureAwait(false);
            attached = true;

            Dictionary<string, long> priorDone;
            try
            {
                priorDone = await PartitionLedger.ReadDoneAsync(duck, nodeId, ct, quotedAlias).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return 0; // no pz_meta in the prior staging: legacy failed node, silent skip
            }

            if (priorDone.Count == 0)
            {
                return 0;
            }

            var priorWindow = await ReadPriorWindowAsync(duck, quotedAlias, nodeId, ct).ConfigureAwait(false);
            var windowed = windowUpper is not null;
            if (windowed != priorWindow.HasValue ||
                (priorWindow is { } pw && (pw.Lower != windowLower || pw.Upper != windowUpper)))
            {
                ctx.Notice?.Invoke(
                    $"source '{datasetLabel}': prior partial progress was extracted under a different window; re-extracting");
                return 0;
            }

            if (!priorDone.Keys.All(plannedIds.Contains))
            {
                ctx.Notice?.Invoke(
                    $"source '{datasetLabel}': prior partitions no longer match this run's plan; re-extracting");
                return 0;
            }

            var priorMainCount = await duck.ScalarAsync<long>(
                $"select count(*) from {quotedAlias}.{mainTable}", ct).ConfigureAwait(false);
            if (priorMainCount != priorDone.Values.Sum())
            {
                ctx.Notice?.Invoke(
                    $"source '{datasetLabel}': prior staged rows do not match the partition ledger; re-extracting");
                return 0;
            }

            await PartitionLedger.EnsureAsync(duck, ct).ConfigureAwait(false);
            await duck.ExecuteAsync(
                $"create or replace table {mainTable} as select * from {quotedAlias}.{mainTable}", ct).ConfigureAwait(false);
            copiedMain = true;

            await duck.ExecuteAsync(
                $"delete from {PartitionLedger.DoneTable} where node_id = '{PartitionLedger.Escape(nodeId)}'", ct)
                .ConfigureAwait(false);
            await duck.ExecuteAsync(
                $"insert into {PartitionLedger.DoneTable} select * from {quotedAlias}.pz_meta.partitions_done " +
                $"where node_id = '{PartitionLedger.Escape(nodeId)}'", ct).ConfigureAwait(false);
            copiedLedger = true;

            // Checkpointed prefixes ride along when intact; a torn one just restarts fresh.
            var priorCheckpoints = await PartitionLedger.ReadCheckpointsAsync(duck, nodeId, ct, quotedAlias)
                .ConfigureAwait(false);
            await duck.ExecuteAsync(
                $"delete from {PartitionLedger.CheckpointTable} where node_id = '{PartitionLedger.Escape(nodeId)}'", ct)
                .ConfigureAwait(false);
            foreach (var (pid, (checkpoint, rows)) in priorCheckpoints)
            {
                if (!plannedIds.Contains(pid))
                {
                    continue;
                }

                var partTable = PartitionLedger.PartTable(nodeKey, pid);
                try
                {
                    await duck.ExecuteAsync(
                        $"create or replace table {partTable} as select * from {quotedAlias}.{partTable}", ct)
                        .ConfigureAwait(false);
                    var count = await duck.ScalarAsync<long>($"select count(*) from {partTable}", ct).ConfigureAwait(false);
                    if (count != rows)
                    {
                        await duck.ExecuteAsync($"drop table if exists {partTable}", ct).ConfigureAwait(false);
                        continue;
                    }

                    await duck.ExecuteAsync(
                        $"insert into {PartitionLedger.CheckpointTable} values ('{PartitionLedger.Escape(nodeId)}', " +
                        $"'{PartitionLedger.Escape(pid)}', '{PartitionLedger.Escape(checkpoint)}', {rows})", ct)
                        .ConfigureAwait(false);
                    copiedParts.Add(partTable);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    await DropBestEffortAsync(duck, partTable).ConfigureAwait(false);
                }
            }

            ctx.Notice?.Invoke(
                $"source '{datasetLabel}': reusing {priorDone.Count} completed partition(s) from the failed run");
            return priorDone.Count;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            if (copiedMain)
            {
                await DropBestEffortAsync(duck, mainTable).ConfigureAwait(false);
            }

            if (copiedLedger)
            {
                try
                {
                    await duck.ExecuteAsync(
                        $"delete from {PartitionLedger.DoneTable} where node_id = '{PartitionLedger.Escape(nodeId)}'",
                        CancellationToken.None).ConfigureAwait(false);
                    await duck.ExecuteAsync(
                        $"delete from {PartitionLedger.CheckpointTable} where node_id = '{PartitionLedger.Escape(nodeId)}'",
                        CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                    // Best-effort undo; the notice below is the user-visible outcome either way.
                }
            }

            foreach (var partTable in copiedParts)
            {
                await DropBestEffortAsync(duck, partTable).ConfigureAwait(false);
            }

            ctx.Notice?.Invoke(
                $"source '{datasetLabel}': partial reuse failed " +
                $"({NativeStatementRedactor.SanitizeEngineMessage(ex.Message).Split('\n', 2)[0].TrimEnd('\r')}); re-extracting");
            return 0;
        }
        finally
        {
            if (attached)
            {
                try
                {
                    await duck.ExecuteAsync($"detach {quotedAlias}", CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                    // Best-effort: a failed detach must never mask the outcome.
                }
            }
        }
    }

    private static async Task<(string Lower, string Upper)?> ReadPriorWindowAsync(
        IDuckSession duck, string quotedAlias, string nodeId, CancellationToken ct)
    {
        var count = await duck.ScalarAsync<long>(
            $"select count(*) from {quotedAlias}.pz_meta.node_window where node_id = '{PartitionLedger.Escape(nodeId)}'",
            ct).ConfigureAwait(false);
        if (count == 0)
        {
            return null;
        }

        var lower = await duck.ScalarAsync<string>(
            $"select lower from {quotedAlias}.pz_meta.node_window where node_id = '{PartitionLedger.Escape(nodeId)}'",
            ct).ConfigureAwait(false);
        var upper = await duck.ScalarAsync<string>(
            $"select upper from {quotedAlias}.pz_meta.node_window where node_id = '{PartitionLedger.Escape(nodeId)}'",
            ct).ConfigureAwait(false);
        return (lower, upper);
    }

    private static PzError IdentityError(SourceDatasetDef def, int ordinal, string problem) => new(
        PzErrorCode.PartitionIdentityInvalid,
        $"source '{def.Source.Name}' dataset '{def.Dataset.Name}': connector declares StablePartitionIds " +
        $"but its planned partitions violate the identity contract: partition {ordinal} {problem}",
        def.Source.FilePath, null,
        "fix the connector's partition planning (see https://pipelinez.dev/how-to/author-a-connector/)");

    /// <summary>Waits out every launched partition task exactly once: the happy path just awaits
    /// them; a fault (engine-side, already self-cancelled from inside its own task per the type doc,
    /// or a genuine external run cancellation) is caught here ONLY to drain the rest of the siblings
    /// before re-throwing the very first fault — <c>loadCts.Cancel()</c> here is a no-op in the
    /// self-cancel case and only does real work for the run-cancellation case.</summary>
    private static async Task AwaitAllAsync(IReadOnlyList<Task> tasks, CancellationTokenSource loadCts)
    {
        try
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        catch
        {
            loadCts.Cancel();
            foreach (var task in tasks)
            {
                try
                {
                    await task.ConfigureAwait(false);
                }
                catch
                {
                    // First fault wins; teardown cancellations/faults triggered by it are expected.
                }
            }

            throw;
        }
    }

    /// <summary>Aggregates every isolated connector-side read fault into ONE thrown
    /// transient-or-not <see cref="PzConnectorException"/>: transient only when every recorded
    /// failure is, RetryAfter is the max of any that carry one. Thrown (not returned) because only a
    /// thrown transient <see cref="PzConnectorException"/> re-enters <see cref="KindDispatchingExecutor"/>'s
    /// retry loop.</summary>
    private static void ThrowIfAnyFailed(ConcurrentQueue<PzConnectorException> failures, long total)
    {
        if (failures.IsEmpty)
        {
            return;
        }

        var all = failures.ToArray();
        var transient = all.All(f => f.IsTransient);
        TimeSpan? retryAfter = null;
        foreach (var failure in all)
        {
            if (failure.RetryAfter is { } ra && (retryAfter is null || ra > retryAfter))
            {
                retryAfter = ra;
            }
        }

        throw new PzConnectorException(
            $"{all.Length} of {total} partitions failed; first: {all[0].Message}", transient, retryAfter, all[0]);
    }

    /// <summary>A mixed-fault teardown (a sibling engine fault escaping raw,
    /// or an identity violation) must not let already-recorded partition failures vanish silently. One
    /// notice line, never partition ids — connector-authored messages are permitted here under the same
    /// policy as <see cref="ThrowIfAnyFailed"/>'s aggregate exception message. A no-op when
    /// <paramref name="failures"/> is empty or <see cref="RunContext.Notice"/> has no subscriber.</summary>
    private static void EmitRecordedFailuresNotice(RunContext ctx, ConcurrentQueue<PzConnectorException> failures)
    {
        if (!failures.TryPeek(out var first))
        {
            return;
        }

        ctx.Notice?.Invoke(
            $"{failures.Count} partition failure(s) were recorded before the node tore down; first: {first.Message}");
    }

    private static async Task RunAdmittedAsync(IDuckSession duck, IIdentifiedPartition partition, string pid,
        string nodeId, string nodeKey, string mainTable, Schema schema, BatchOptions batchOptions,
        SemaphoreSlim admission, CancellationTokenSource loadCts, ConcurrentQueue<PzConnectorException> failures,
        IReadOnlyDictionary<string, (string Checkpoint, long Rows)> checkpoints, Action onResumed)
    {
        await admission.WaitAsync(loadCts.Token).ConfigureAwait(false);
        try
        {
            await RunPartitionAsync(duck, partition, pid, nodeId, nodeKey, mainTable, schema, batchOptions,
                loadCts, failures, checkpoints, onResumed).ConfigureAwait(false);
        }
        finally
        {
            admission.Release();
        }
    }

    /// <summary>Streaming variant of <see cref="RunAdmittedAsync"/>: the admission wait already
    /// happened before this task was created (mirrors <c>PumpOneThenReleaseAsync</c>), so this just
    /// runs the partition and releases its slot when done, whatever the outcome.</summary>
    private static async Task RunThenReleaseAsync(IDuckSession duck, IIdentifiedPartition partition, string pid,
        string nodeId, string nodeKey, string mainTable, Schema schema, BatchOptions batchOptions,
        CancellationTokenSource loadCts, ConcurrentQueue<PzConnectorException> failures, SemaphoreSlim admission,
        IReadOnlyDictionary<string, (string Checkpoint, long Rows)> checkpoints, Action onResumed)
    {
        try
        {
            await RunPartitionAsync(duck, partition, pid, nodeId, nodeKey, mainTable, schema, batchOptions,
                loadCts, failures, checkpoints, onResumed).ConfigureAwait(false);
        }
        finally
        {
            admission.Release();
        }
    }

    private static async Task RunPartitionAsync(IDuckSession duck, IIdentifiedPartition partition,
        string pid, string nodeId, string nodeKey, string mainTable, Schema schema,
        BatchOptions batchOptions, CancellationTokenSource loadCts,
        ConcurrentQueue<PzConnectorException> failures,
        IReadOnlyDictionary<string, (string Checkpoint, long Rows)> checkpoints, Action onResumed)
    {
        try
        {
            if (partition is ICheckpointingPartition checkpointing)
            {
                await RunCheckpointingPartitionAsync(duck, checkpointing, pid, nodeId, nodeKey, mainTable,
                    schema, batchOptions, loadCts, failures, checkpoints, onResumed).ConfigureAwait(false);
                return;
            }

            var partTable = PartitionLedger.PartTable(nodeKey, pid);
            await duck.ExecuteAsync($"drop table if exists {partTable}", loadCts.Token).ConfigureAwait(false);
            await duck.ExecuteAsync(
                $"delete from {PartitionLedger.CheckpointTable} where node_id = '{PartitionLedger.Escape(nodeId)}' " +
                $"and partition_id = '{PartitionLedger.Escape(pid)}'", loadCts.Token).ConfigureAwait(false);
            await duck.CreateEmptyTableAsync(partTable, schema, loadCts.Token).ConfigureAwait(false);

            var rows = 0L;
            await using var reader = partition.ReadAsync(batchOptions, loadCts.Token)
                .GetAsyncEnumerator(loadCts.Token);
            while (true)
            {
                RecordBatch batch;
                try
                {
                    if (!await reader.MoveNextAsync().ConfigureAwait(false))
                    {
                        break;
                    }

                    batch = reader.Current;
                }
                catch (OperationCanceledException)
                {
                    // Run cancellation or another partition's engine-fault teardown — never a
                    // recordable partition failure.
                    throw;
                }
                catch (Exception ex)
                {
                    // Connector-side fault: isolate it — record, clean this partition's
                    // landing pad, let siblings finish undisturbed. loadCts is NOT touched here: a
                    // read fault is this partition's business alone. CancellationToken.None: cleanup
                    // must run even though this partition is done for.
                    failures.Enqueue(Classify(ex));
                    await DropBestEffortAsync(duck, partTable).ConfigureAwait(false);
                    return;
                }

                // Engine-side fault (append failure) escapes to the outer catch below, which
                // self-cancels loadCts before propagating — an append/ledger failure is the engine's
                // own fault, not isolable to this one partition.
                rows += await duck.AppendArrowBatchAsync(partTable, batch, loadCts.Token).ConfigureAwait(false);
            }

            await duck.ExecuteTransactionAsync(
                PartitionLedger.CompleteStatements(nodeId, pid, mainTable, partTable, rows), loadCts.Token)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Run cancellation, or this token was already cancelled by another partition's engine
            // fault (see below) — propagate as-is, never self-cancel twice or misclassify as a
            // connector failure.
            throw;
        }
        catch (Exception)
        {
            // Engine-side fault (a setup statement, AppendArrowBatchAsync, or the completion
            // transaction) escaped the isolated read-fault handling above. Self-cancel loadCts from
            // INSIDE this faulting task — mirroring the legacy channel path's PumpPartitionAsync
            // self-cancelling pumpCts in its own catch — so every in-flight sibling tears down
            // immediately instead of only once the orchestration's Task.WhenAll observes this fault.
            loadCts.Cancel();
            throw;
        }
    }

    /// <summary>The <see cref="ICheckpointingPartition"/> branch of
    /// <see cref="RunPartitionAsync"/>. Resume guard: a checkpoint row PLUS an intact part table
    /// (its live row count still matches what the ledger recorded) PLUS connector assent
    /// (<see cref="ICheckpointingPartition.TryResumeFrom"/>) — anything less degrades to a scratch
    /// restart, never a wrong-data resume. Extraction then stages into a SEPARATE segment table;
    /// each token the connector offers is committed in its own short transaction (segment → part
    /// table, checkpoint row upserted) so the checkpointed prefix is durable the instant it is
    /// reported, exactly like <see cref="RunPartitionAsync"/>'s completion transaction is durable the
    /// instant the whole partition finishes. Fault routing mirrors the plain path bit-for-bit: this
    /// method is called from INSIDE <see cref="RunPartitionAsync"/>'s try block, so an engine-side
    /// fault (an append or transaction statement throwing) simply escapes raw to that method's outer
    /// catch, which self-cancels <paramref name="loadCts"/> — nothing engine-side needs duplicating
    /// here. A connector read fault is isolated locally (record + drop the segment — NEVER the part
    /// table, which together with its ledger row is the resume prefix and must survive — then
    /// return normally, no self-cancel). <see cref="OperationCanceledException"/> is never touched,
    /// only rethrown.</summary>
    private static async Task RunCheckpointingPartitionAsync(IDuckSession duck,
        ICheckpointingPartition partition, string pid, string nodeId, string nodeKey, string mainTable,
        Schema schema, BatchOptions batchOptions, CancellationTokenSource loadCts,
        ConcurrentQueue<PzConnectorException> failures,
        IReadOnlyDictionary<string, (string Checkpoint, long Rows)> checkpoints, Action onResumed)
    {
        var partTable = PartitionLedger.PartTable(nodeKey, pid);
        var segTable = PartitionLedger.SegTable(nodeKey, pid);
        var token = loadCts.Token;

        // Resume decision: a checkpoint row + an intact part table + connector assent,
        // else scratch restart. Guard failures degrade to correct-but-slower, never wrong data. The
        // `&&` short-circuit matters: TryResumeFrom is consulted ONLY once the part table's row count
        // is proven intact — a torn table must never even reach the connector.
        var partRows = 0L;
        var resumed = false;
        if (checkpoints.TryGetValue(pid, out var prior))
        {
            var partCount = await TryCountAsync(duck, partTable, token).ConfigureAwait(false);
            if (partCount == prior.Rows && partition.TryResumeFrom(prior.Checkpoint))
            {
                partRows = prior.Rows;
                resumed = true;
                onResumed();
            }
        }

        if (!resumed)
        {
            await duck.ExecuteAsync($"drop table if exists {partTable}", token).ConfigureAwait(false);
            await duck.ExecuteAsync(
                $"delete from {PartitionLedger.CheckpointTable} where node_id = '{PartitionLedger.Escape(nodeId)}' " +
                $"and partition_id = '{PartitionLedger.Escape(pid)}'", token).ConfigureAwait(false);
            await duck.CreateEmptyTableAsync(partTable, schema, token).ConfigureAwait(false);
        }

        await duck.ExecuteAsync($"drop table if exists {segTable}", token).ConfigureAwait(false);
        await duck.CreateEmptyTableAsync(segTable, schema, token).ConfigureAwait(false);

        var segRows = 0L;
        await using var reader = partition.ReadAsync(batchOptions, token).GetAsyncEnumerator(token);
        while (true)
        {
            RecordBatch batch;
            try
            {
                if (!await reader.MoveNextAsync().ConfigureAwait(false))
                {
                    break;
                }

                batch = reader.Current;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                // Rows since the last checkpoint are discarded; the checkpointed prefix in the part
                // table (and its ledger row) is the whole point — keep both. NOT a
                // self-cancel: this is this partition's own business, exactly like the plain path's
                // isolated read fault.
                failures.Enqueue(Classify(ex));
                await DropBestEffortAsync(duck, segTable).ConfigureAwait(false);
                return;
            }

            // Engine-side faults from here on (append, or either transaction below) escape raw to
            // RunPartitionAsync's outer catch, which self-cancels loadCts — this method adds no catch
            // of its own for that regime.
            segRows += await duck.AppendArrowBatchAsync(segTable, batch, token).ConfigureAwait(false);

            if (partition.TryGetCheckpoint(out var checkpoint) && checkpoint is not null)
            {
                await duck.ExecuteTransactionAsync(
                    PartitionLedger.CheckpointStatements(nodeId, pid, segTable, partTable, checkpoint, partRows + segRows),
                    token).ConfigureAwait(false);
                partRows += segRows;
                segRows = 0;
                await duck.CreateEmptyTableAsync(segTable, schema, token).ConfigureAwait(false);
            }
        }

        // Clean end: residual segment + completion in ONE transaction — main gains the whole
        // partition atomically with its done row, and the checkpoint row clears.
        var statements = new List<string>
        {
            $"insert into {partTable} select * from {segTable}",
            $"drop table {segTable}",
        };
        statements.AddRange(PartitionLedger.CompleteStatements(nodeId, pid, mainTable, partTable, partRows + segRows));
        await duck.ExecuteTransactionAsync(statements, token).ConfigureAwait(false);
    }

    private static async Task<long?> TryCountAsync(IDuckSession duck, string table, CancellationToken ct)
    {
        var name = table.Split('.')[1];
        var exists = await duck.ScalarAsync<long>(
            "select count(*) from information_schema.tables where table_schema = 'staging' " +
            $"and table_name = '{PartitionLedger.Escape(name)}'", ct).ConfigureAwait(false);
        return exists == 0
            ? null
            : await duck.ScalarAsync<long>($"select count(*) from {table}", ct).ConfigureAwait(false);
    }

    private static PzConnectorException Classify(Exception ex) => ex as PzConnectorException
        ?? new PzConnectorException($"partition read failed: {ex.Message}", isTransient: false, innerException: ex);

    private static async Task DropBestEffortAsync(IDuckSession duck, string table)
    {
        try
        {
            await duck.ExecuteAsync($"drop table if exists {table}", CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
            // Best-effort: never mask the recorded partition failure with cleanup trouble.
        }
    }

    /// <summary>Sync done-skip exception: resets a sync dataset's already-done single
    /// partition so it can be re-read live this attempt — the sync token candidate can only ever come
    /// from a live read. One transaction (main rows + both ledger rows) keeps "rows are in main" and
    /// "ledger says done" from ever disagreeing mid-reset; the part/segment table drops are best-effort
    /// (nothing downstream depends on them surviving).</summary>
    private static async Task ResetSyncDonePartitionAsync(
        IDuckSession duck, string nodeId, string nodeKey, string mainTable, string pid, CancellationToken ct)
    {
        await duck.ExecuteTransactionAsync(
        [
            $"delete from {mainTable}",
            $"delete from {PartitionLedger.DoneTable} where node_id = '{PartitionLedger.Escape(nodeId)}' " +
            $"and partition_id = '{PartitionLedger.Escape(pid)}'",
            $"delete from {PartitionLedger.CheckpointTable} where node_id = '{PartitionLedger.Escape(nodeId)}' " +
            $"and partition_id = '{PartitionLedger.Escape(pid)}'",
        ], ct).ConfigureAwait(false);

        await DropBestEffortAsync(duck, PartitionLedger.PartTable(nodeKey, pid)).ConfigureAwait(false);
        await DropBestEffortAsync(duck, PartitionLedger.SegTable(nodeKey, pid)).ConfigureAwait(false);
    }

    private static async Task EnsureMainTableAsync(IDuckSession duck, string mainTable, Schema schema, CancellationToken ct)
    {
        var name = mainTable.Split('.')[1];
        var exists = await duck.ScalarAsync<long>(
            "select count(*) from information_schema.tables where table_schema = 'staging' " +
            $"and table_name = '{PartitionLedger.Escape(name)}'", ct).ConfigureAwait(false);
        if (exists == 0)
        {
            await duck.CreateEmptyTableAsync(mainTable, schema, ct).ConfigureAwait(false);
        }
    }
}
