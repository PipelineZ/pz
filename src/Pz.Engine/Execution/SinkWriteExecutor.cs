using System.Diagnostics;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Validation;
using Pz.Diagnostics.Otel;
using Pz.DuckDb;
using Pz.Engine.Planning;
using Pz.Engine.Resilience;

namespace Pz.Engine.Execution;

/// <summary>Writes the staging relation backing a sink output through the sink connector's write
/// session, guaranteeing exactly one of Commit/Abort per the ISink contract.</summary>
public sealed class SinkWriteExecutor : INodeExecutor
{
    public async Task<NodeResult> ExecuteAsync(DagNode node, RunContext ctx, CancellationToken ct)
    {
        var def = (SinkOutputDef)node.Definition;

        if (!ctx.Connectors.TryGetSink(def.Sink.Connector, out var connector))
        {
            var error = new PzError(PzErrorCode.ConnectorNotInstalled,
                $"connector '{def.Sink.Connector}' is not installed", def.Sink.FilePath, null,
                "run 'pz restore'");
            return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0, TimeSpan.Zero, error);
        }

        var relation = StagingNames.ForSinkInput(def.Output.Input);

        await using var sink = await connector.OpenAsync(
            new ConnectorConfig(def.Sink.Connection), ct).ConfigureAwait(false);

        // Hand a gate-aware sink its operation gate before any plan/write call. The gate exists even
        // with no pacing registry (op-level retry alone).
        OperationGate? gate = null;
        if (sink is IOperationGateAware gateAware)
        {
            gate = new OperationGate(RetryPolicyResolver.Resolve(node), ctx.RateLimiters?.For(node),
                Random.Shared, Task.Delay);
            gateAware.UseOperationGate(gate);
        }

        var spec = SpecBuilder.ForSinkOutput(def);

        if (ctx.Plan?.StrategyFor(node.Id) == EdgeStrategy.NativeCopy && sink.TryGetNativeCopy(spec, out var copy))
        {
            // No separate egress/write split for the native-copy tier: DuckDB's COPY statement reads
            // and writes atomically in one shot, so there is no seam to split on.
            using var nativeWriteActivity = PzActivitySource.Instance.StartActivity("write");

            var nativeRows = await ctx.Duck.ScalarAsync<long>($"select count(*) from {relation}", ct).ConfigureAwait(false);
            foreach (var statement in copy.SetupStatements)
            {
                await ctx.SetupLedger.ExecuteOnceAsync(ctx.Duck, statement, ct).ConfigureAwait(false);
            }

            // Planning (ExecutionPlanner's TryGetNativeCopy probe) must be side-effect-free, so the
            // connector's probe never creates the output directory itself — the engine creates
            // whatever temp/final parent directories this COPY needs right here, at execution time.
            foreach (var move in copy.Finalizations)
            {
                CreateParentDirectory(move.TempPath);
                CreateParentDirectory(move.FinalPath);
            }

            try
            {
                await ctx.Duck.ExecuteAsync(copy.CopySql.Replace("{{source}}", relation), ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Litter guarantee: never leave a `.pz-native-*` temp file behind when the COPY
                // itself fails, independent of whatever DuckDB does internally on its own failure path.
                foreach (var move in copy.Finalizations)
                {
                    TryDeleteFile(move.TempPath);
                }

                // The inner engine message MUST be sanitized (never the raw ex.Message): a DuckDB
                // parser/binder error's "LINE <n>: ..." context block would otherwise echo the
                // COPY statement (and anything embedded in it) verbatim into this NodeResult/log.
                var sanitized = NativeStatementRedactor.SanitizeEngineMessage(ex.Message);
                throw new PzConnectorException(
                    $"native copy statement failed: {sanitized}", isTransient: DuckTransientErrors.IsTransient(ex.Message),
                    innerException: ex);
            }

            foreach (var move in copy.Finalizations)
            {
                File.Move(move.TempPath, move.FinalPath, overwrite: true);
            }

            PzMeters.RowsMoved.Add(nativeRows, new KeyValuePair<string, object?>("pz.node.kind", "SinkWrite"));
            // Ops: universal tier only -- the native tier never routes through a .NET gate.
            return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Success, nativeRows, TimeSpan.Zero, null);
        }

        // Stamped only now, past the native-copy branch: a COPY has no session to carry a progress
        // marker, and TryGetNativeCopy above must see the same spec the planner probed with.
        spec = spec with
        {
            Attempt = new WriteAttempt(
                node.Id.Value, ctx.Paths.RunId, ctx.Attempts.TryGetValue(node.Id, out var a) ? a : 1),
        };

        var schema = await ctx.Duck.GetResultSchemaAsync($"select * from {relation}", ct).ConfigureAwait(false);

        if (connector.Capabilities.HasFlag(ConnectorCapabilities.TextLengthStats))
        {
            var stringColumns = schema.FieldsList
                .Where(f => f.DataType.TypeId == Apache.Arrow.Types.ArrowTypeId.String)
                .Select(f => f.Name).ToArray();
            if (stringColumns.Length > 0)
            {
                spec = spec with
                {
                    MaxTextLengths = await ComputeMaxTextLengthsAsync(ctx.Duck, relation, stringColumns, ct)
                        .ConfigureAwait(false),
                };
            }
        }

        // PZ0340 guard -- runs BEFORE BeginWriteAsync (no
        // session opened yet) so a doomed delete drain never delivers a single upsert row. Checks
        // both halves of "the merge keys flow unchanged from the cdc dataset": every declared key
        // column exists in the deletes relation, and none of them is null in any row there.
        if (def.Output.OnDelete is "delete" or "soft" && def.CdcDeleteOrigin is { } cdcOrigin)
        {
            var deletesRelation = StagingNames.ForSourceLoad(cdcOrigin.Source, cdcOrigin.Dataset) + "__deletes";
            var deletesSchema = await ctx.Duck.GetResultSchemaAsync($"select * from {deletesRelation}", ct).ConfigureAwait(false);
            var deletesColumns = deletesSchema.FieldsList.Select(f => f.Name).ToHashSet();
            var missingKey = def.Output.Keys.FirstOrDefault(k => !deletesColumns.Contains(k));
            if (missingKey is not null)
            {
                var missingKeyError = new PzError(PzErrorCode.CdcDeleteKeysUnavailable,
                    $"output '{def.Output.Name}': merge key '{missingKey}' is missing from the cdc deletes " +
                    $"relation '{deletesRelation}'",
                    def.Sink.FilePath, null,
                    "on_delete: delete|soft requires the merge keys to flow unchanged from the cdc dataset " +
                    "(rename them in the pipeline's upsert view only)");
                return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0, TimeSpan.Zero, missingKeyError);
            }

            var nullPredicate = string.Join(" or ", def.Output.Keys.Select(k => ArrowInterop.QuoteQualified(k) + " is null"));
            var nullKeyCount = await ctx.Duck.ScalarAsync<long>(
                $"select count(*) from {deletesRelation} where {nullPredicate}", ct).ConfigureAwait(false);
            if (nullKeyCount > 0)
            {
                var nullKeyError = new PzError(PzErrorCode.CdcDeleteKeysUnavailable,
                    $"output '{def.Output.Name}': merge key(s) [{string.Join(", ", def.Output.Keys)}] are null " +
                    $"in {nullKeyCount} row(s) of the cdc deletes relation '{deletesRelation}'",
                    def.Sink.FilePath, null,
                    "on_delete: delete|soft requires the merge keys to flow unchanged from the cdc dataset " +
                    "(rename them in the pipeline's upsert view only)");
                return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0, TimeSpan.Zero, nullKeyError);
            }
        }

        // A merge cannot match a NULL key: ON CONFLICT (pg) / MERGE ... ON (mssql) never join on NULL, so a
        // null-keyed row (a) collapses within the batch -- the key-dedup (DISTINCT ON / ROW_NUMBER over the
        // keys) treats all NULLs as one group and keeps a single arbitrary survivor -- and (b) is re-inserted
        // on every run, since it never matches an existing target row. That is silent data loss plus unbounded
        // duplication, violating the merge = effectively-once delivery guarantee. Fail loudly before opening a
        // session, mirroring the CDC-deletes PZ0340 guard above. Runtime-only: a nullable key column is legal
        // DDL; only the actual presence of a NULL value in the staged input is the defect.
        if (string.Equals(def.Output.Mode, "merge", StringComparison.Ordinal) && def.Output.Keys.Count > 0)
        {
            var nullKeyPredicate = string.Join(
                " or ", def.Output.Keys.Select(k => ArrowInterop.QuoteQualified(k) + " is null"));
            var nullKeyRows = await ctx.Duck.ScalarAsync<long>(
                $"select count(*) from {relation} where {nullKeyPredicate}", ct).ConfigureAwait(false);
            if (nullKeyRows > 0)
            {
                var nullMergeKeyError = new PzError(PzErrorCode.MergeKeyNull,
                    $"output '{def.Output.Name}': merge key(s) [{string.Join(", ", def.Output.Keys)}] are null " +
                    $"in {nullKeyRows} row(s) of the staged input -- a merge cannot match a null key, so those " +
                    "rows would silently collapse within the batch and re-insert (duplicate) on every run",
                    def.Sink.FilePath, null,
                    "coalesce or filter out the null merge keys in the pipeline (a null key can't identify a " +
                    "target row), or use write.strategy: append with duplicates: accept if duplicates are acceptable");
                return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0, TimeSpan.Zero, nullMergeKeyError);
            }

            // Duplicate merge keys within one staged batch collapse to a
            // single connector-determined survivor -- physical staging order (e.g. postgres's
            // `distinct on ... ctid desc`), NOT cursor order -- so a stale row can silently beat the newer
            // one while the watermark still advances past both, and `partitions:` makes the pick
            // nondeterministic. That collapse is the sink ABI's documented Absorb contract and legitimate
            // for event-log-shaped inputs, so unlike the NULL-key guard above this stays a warning, not a
            // failure: an event with counts only (never row values), so the author can dedup
            // deterministically in the pipeline when a specific row must win.
            var keyList = string.Join(", ", def.Output.Keys.Select(ArrowInterop.QuoteQualified));
            var extraRows = await ctx.Duck.ScalarAsync<long>(
                "select cast(coalesce(sum(cnt - 1), 0) as bigint) from " +
                $"(select count(*) as cnt from {relation} group by {keyList} having count(*) > 1) t",
                ct).ConfigureAwait(false);
            if (extraRows > 0)
            {
                var duplicateGroups = await ctx.Duck.ScalarAsync<long>(
                    $"select count(*) from (select 1 from {relation} group by {keyList} having count(*) > 1) t",
                    ct).ConfigureAwait(false);
                ctx.Events.SafeMergeKeyDuplicatesDetected(node, def.Output.Name, def.Output.Keys,
                    duplicateGroups, extraRows);
            }
        }

        var session = await sink.BeginWriteAsync(spec, schema, ct).ConfigureAwait(false);
        var stall = new StallAccumulator(ctx.EffectiveTime);

        // Checkpoint-mode setup. Everything below is skipped for a non-checkpointing session. The
        // fingerprint is computed once per attempt, BEFORE the drain begins (the drain holds the
        // serialized connection).
        var cps = session as ICheckpointingSinkSession;
        SinkDeliveryLedger.Fingerprint? fingerprint = null;
        long resumedRows = 0;
        long lastAcknowledged = 0;
        if (cps is not null)
        {
            await SinkDeliveryLedger.EnsureAsync(ctx.Duck, ct).ConfigureAwait(false);
            fingerprint = await SinkDeliveryLedger.FingerprintAsync(ctx.Duck, relation, ct).ConfigureAwait(false);

            // pz retry's cross-run seed — runs before the local read so
            // the seeded row is found by the very protocol below; TrySeedFromPriorAsync's own
            // first guard keeps a local row authoritative.
            if (ctx.Reuse is { } reuse && reuse.TryGetDeliveryResume(node.Id, out var deliveryResume))
            {
                await SinkDeliveryLedger.TrySeedFromPriorAsync(ctx.Duck, node.Id.Value,
                    PartitionLedger.NodeKey(node), deliveryResume.PriorStagingPath, fingerprint, ct)
                    .ConfigureAwait(false);
            }

            var prior = await SinkDeliveryLedger.ReadAsync(ctx.Duck, node.Id.Value, ct).ConfigureAwait(false);
            if (prior is { } priorRow)
            {
                if (priorRow.AcknowledgedRows > 0 &&
                    priorRow.RelationCount == fingerprint.Count &&
                    priorRow.RelationHash == fingerprint.Hash &&
                    cps.TryResumeFrom(priorRow.AcknowledgedRows))
                {
                    resumedRows = priorRow.AcknowledgedRows;
                    lastAcknowledged = priorRow.AcknowledgedRows;
                }
                else
                {
                    // Any guard failure => scratch: stale prefixes must never survive to a
                    // later attempt or a later pz retry.
                    await SinkDeliveryLedger.ClearAsync(ctx.Duck, node.Id.Value, ct).ConfigureAwait(false);
                }
            }
        }

        // "egress" (reading from staging via QueryArrowAsync below) and
        // "write" (WriteBatchAsync + the eventual Commit) are interleaved seams —
        // this single loop alternates between them per batch rather than running them as two concurrent
        // tasks (unlike SourceLoadExecutor's extract/ingest). Splitting into per-batch spans would be
        // needlessly expensive when a listener IS attached (thousands of spans for a large batch count)
        // for no material tracing benefit, so both stage spans simply cover the whole read+write+commit
        // seam as siblings under the node span (parented explicitly, same trick as SourceLoadExecutor).
        var nodeActivity = Activity.Current;
        try
        {
            // Abort-on-failure applies ONLY to the write phase (below), never to Commit (outside this
            // try/catch, further down): once CommitAsync has been attempted, its outcome — succeeded,
            // in-flight, or failed — is unknown to this caller, and per the Commit-xor-Abort ABI contract
            // Abort must never run after Commit has been attempted, since aborting could unwind a write
            // that actually went through. A write-phase failure, in contrast, has definitely not committed
            // anything yet, so Abort is still the correct and only cleanup.
            //
            // egress/write stage spans are scoped tightly to this write-phase block (disposed in the
            // finally right below, before Commit) rather than lingering through Commit/the metric
            // record below it — so Activity.Current is back to the node span by the time the
            // completion-only pz.rows_moved increment runs, matching SourceLoadExecutor's "ingest" span
            // being scoped tightly around just the IngestArrowAsync call.
            var egressActivity = PzActivitySource.Instance.StartActivity("egress");
            Activity.Current = nodeActivity;
            var writeActivity = PzActivitySource.Instance.StartActivity("write");

            // Mirrors SourceLoadExecutor.ReportProgress's every-10-batches cadence: bytes must be
            // measured before the batch is disposed below. Hoisted above the write-phase try so the
            // catch below can read rowsSoFar for DeliveryStats.
            var rowsSoFar = 0L;
            var bytesSoFar = 0L;
            var batchCount = 0L;
            var bytesAtLastReport = 0L;
            var batchesAtLastReport = 0L;
            // The in-flight batch's row count, captured just before each
            // WriteBatchAsync call and overwritten every batch. rowsSoFar alone excludes the
            // batch currently being written, which understates a non-checkpointing sink's
            // upper-bound RowsVisible on failure (see the catch block below).
            var inFlightRows = 0L;
            try
            {
                try
                {
                    // Manual enumerator over the egress stream (rather than
                    // `await foreach`) so the producer-side stall — MoveNextAsync waiting on DuckDB to
                    // produce the next batch, i.e. egress/staging is the bottleneck — can be timed
                    // separately from the consumer-side WriteBatchAsync stall below (sink is the
                    // bottleneck). Never per-row.
                    var drainSql = cps is null
                        ? $"select * from {relation}"
                        : resumedRows > 0
                            ? $"select * from {relation} order by all offset {resumedRows}"
                            : $"select * from {relation} order by all";
                    var enumerator = ctx.Duck
                        .QueryArrowAsync(drainSql, ctx.EffectiveBatch.TargetBatchBytes, ct)
                        .GetAsyncEnumerator(ct);
                    try
                    {
                        while (true)
                        {
                            var readStart = stall.Timestamp;
                            bool moved;
                            try
                            {
                                moved = await enumerator.MoveNextAsync().ConfigureAwait(false);
                            }
                            finally
                            {
                                stall.AddProducer(stall.Timestamp - readStart);
                            }

                            if (!moved)
                            {
                                break;
                            }

                            var batch = enumerator.Current;
                            try
                            {
                                var writeStart = stall.Timestamp;
                                inFlightRows = batch.Length;
                                try
                                {
                                    await session.WriteBatchAsync(batch, ct).ConfigureAwait(false);
                                }
                                finally
                                {
                                    stall.AddConsumer(stall.Timestamp - writeStart);
                                }

                                rowsSoFar += batch.Length;
                                bytesSoFar += batch.ApproximateSize();
                                batchCount++;
                                if (cps is not null &&
                                    cps.TryGetAcknowledgedRows(out var acknowledged) &&
                                    acknowledged > lastAcknowledged)
                                {
                                    lastAcknowledged = acknowledged;
                                }

                                if (batchCount % 10 == 0)
                                {
                                    ctx.Events.SafeNodeProgress(node, rowsSoFar, bytesSoFar, batchCount);

                                    // Delta since the last report, not the cumulative running total —
                                    // Counter.Add IS the accumulation.
                                    PzMeters.BytesMoved.Add(bytesSoFar - bytesAtLastReport);
                                    PzMeters.Batches.Add(batchCount - batchesAtLastReport);
                                    bytesAtLastReport = bytesSoFar;
                                    batchesAtLastReport = batchCount;
                                }
                            }
                            finally
                            {
                                batch.Dispose();
                            }
                        }
                    }
                    finally
                    {
                        await enumerator.DisposeAsync().ConfigureAwait(false);
                    }

                    // The delete drain runs AFTER every upsert
                    // batch has been written, still inside this try -- a delete-apply failure routes
                    // through the same AbortAsync path as an upsert-batch failure below (Commit-xor-
                    // Abort's write-phase contract covers both halves of the drain). `on_delete:
                    // ignore` never reaches here; the PZ0340 guard above already proved the merge keys
                    // are present/non-null, so this loop cannot discover that failure mode itself.
                    // Deletes never add to rowsSoFar/RowsMoved (that stays "rows in the output"; raw
                    // per-op counts already live in NodeResult.Cdc from the landing side).
                    if (def.Output.OnDelete is "delete" or "soft" && def.CdcDeleteOrigin is { } deleteOrigin)
                    {
                        // Defense-in-depth (no first-party sink is both): a checkpointing session
                        // composing with a delete drain is a connector defect, not a supported shape.
                        if (cps is not null)
                        {
                            throw new PzConnectorException(
                                $"output '{def.Output.Name}': on_delete cannot be combined with a " +
                                "checkpointing sink session (ICheckpointingSinkSession and " +
                                "IDeleteApplyingWriteSession do not compose)", isTransient: false);
                        }

                        if (session is not IDeleteApplyingWriteSession deleteSession)
                        {
                            throw new PzConnectorException(
                                $"output '{def.Output.Name}': on_delete requires the sink session to implement " +
                                "IDeleteApplyingWriteSession (connector declared ApplyDeletes but its session does not)",
                                isTransient: false);
                        }

                        var deletesRelation = StagingNames.ForSourceLoad(deleteOrigin.Source, deleteOrigin.Dataset) + "__deletes";
                        var keyList = string.Join(", ", def.Output.Keys.Select(ArrowInterop.QuoteQualified));
                        await foreach (var keyBatch in ctx.Duck.QueryArrowAsync(
                            $"select {keyList} from {deletesRelation}", ctx.EffectiveBatch.TargetBatchBytes, ct).ConfigureAwait(false))
                        {
                            using (keyBatch)
                            {
                                await deleteSession.ApplyDeleteKeysAsync(keyBatch, ct).ConfigureAwait(false);
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    try
                    {
                        // CancellationToken.None: abort must run to completion even when the failure (including
                        // the write loop observing `ct` itself) was caused by cancellation of `ct` — honoring an
                        // already-canceled token here would risk skipping the very cleanup Commit-xor-Abort exists
                        // to guarantee.
                        await session.AbortAsync(CancellationToken.None).ConfigureAwait(false);
                    }
                    catch
                    {
                        // Suppressed by design: never mask the original write failure with an abort failure.
                    }

                    // Checkpointing sessions confirm rows
                    // intra-batch (e.g. the HTTP sink's ~245 chunk requests per default engine
                    // batch), so a batch that failed mid-write may have already had a prefix of
                    // its own rows acknowledged since the last successful-WriteBatchAsync poll
                    // above. One final poll here -- before recording DeliveryFailures and before
                    // the teardown ledger upsert below -- captures that up-to-the-moment
                    // acknowledgment. The ABI guarantees TryGetAcknowledgedRows must not throw and
                    // reports only destination-confirmed rows, so taking it (when it exceeds what
                    // we already have) can never persist/report a row that was never delivered.
                    if (cps is not null && cps.TryGetAcknowledgedRows(out var ackAtFailure) && ackAtFailure > lastAcknowledged)
                    {
                        lastAcknowledged = ackAtFailure;
                    }

                    // Honesty side-band. Checkpointing sessions report the
                    // exact acknowledged count; non-checkpointing sinks report the handed-over
                    // upper bound, which must include the in-flight batch that was failing (rowsSoFar
                    // alone excludes it and would under-count). Guarded against
                    // OperationCanceledException: a canceled run never builds a Failed NodeResult, so
                    // recording here would be inert (and misleading if ever read).
                    if (ex is not OperationCanceledException && sink.AbortSemantics != AbortSemantics.DiscardsAll)
                    {
                        ctx.DeliveryFailures[node.Id] = new DeliveryStats(
                            SemanticsName(sink.AbortSemantics),
                            cps is not null ? lastAcknowledged : rowsSoFar + inFlightRows,
                            resumedRows);
                    }

                    // Teardown-time ledger persistence -- the enumerator's
                    // own finally has already disposed it (the serialized connection is free),
                    // and a persistence failure must never mask the original write failure. Runs
                    // even on cancellation (unlike the DeliveryFailures recording above): a
                    // Ctrl-C'd run is a prime pz-retry-resume case, and persisting the
                    // acknowledged prefix at teardown is exactly what makes that retry cheap.
                    if (cps is not null && lastAcknowledged > resumedRows && fingerprint is { } fp)
                    {
                        try
                        {
                            await ctx.Duck.ExecuteTransactionAsync(
                                SinkDeliveryLedger.UpsertStatements(node.Id.Value, lastAcknowledged, fp),
                                CancellationToken.None).ConfigureAwait(false);
                        }
                        catch
                        {
                            // Suppressed by design: losing one attempt's checkpoint only means
                            // re-delivery from an earlier prefix -- the safe direction.
                        }
                    }

                    throw;
                }
            }
            finally
            {
                egressActivity?.Dispose();
                writeActivity?.Dispose();
            }

            // Deliberately outside the write-phase try/catch above and uncaught here: a CommitAsync
            // failure surfaces directly as a Failed NodeResult (KindDispatchingExecutor wraps it as
            // PZ0501) with no Abort call — see the comment above for why.
            var result = await session.CommitAsync(ct).ConfigureAwait(false);
            PzMeters.RowsMoved.Add(result.RowsWritten, new KeyValuePair<string, object?>("pz.node.kind", "SinkWrite"));

            // One Snapshot() call feeds both the pz.ops.* meters and NodeResult.Ops —
            // never snapshot twice, since OpStats is a point-in-time read of mutable Interlocked counters.
            var opStats = gate?.Snapshot();
            if (opStats is { } stats)
            {
                var instanceTag = new KeyValuePair<string, object?>("pz.instance", InstanceKey.For(node));
                PzMeters.OpsExecuted.Add(stats.Executed, instanceTag);
                PzMeters.OpsRetried.Add(stats.Retried, instanceTag);
                PzMeters.OpsThrottleWait.Add(stats.ThrottleWaitMs, instanceTag);
            }

            // A committed sink needs no resume state, and a stale row must
            // never survive into a later pz retry. CancellationToken.None + suppress, mirroring
            // the teardown upsert's pattern: CommitAsync has already succeeded by this point, so
            // a failure or cancellation of this best-effort cleanup must never turn a committed
            // sink into a Failed node. A surviving stale row is harmless -- committed nodes are
            // never resume candidates, and cross-run seeding (TrySeedFromPriorAsync) is
            // fingerprint-guarded against exactly this kind of stale leftover.
            if (cps is not null)
            {
                try
                {
                    await SinkDeliveryLedger.ClearAsync(ctx.Duck, node.Id.Value, CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                    // Suppressed by design: see comment above.
                }
            }

            ctx.DeliveryFailures.TryRemove(node.Id, out _);
            return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Success, result.RowsWritten, TimeSpan.Zero,
                null, stall.ToTimings(), Ops: opStats,
                Delivery: resumedRows > 0
                    ? new DeliveryStats(SemanticsName(sink.AbortSemantics), result.RowsWritten, resumedRows)
                    : null);
        }
        finally
        {
            await session.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static string SemanticsName(AbortSemantics semantics) => semantics switch
    {
        AbortSemantics.DiscardsAll => "discards_all",
        AbortSemantics.BestEffort => "best_effort",
        AbortSemantics.None => "none",
        _ => throw new ArgumentOutOfRangeException(nameof(semantics), semantics, "unknown abort semantics"),
    };

    private static void CreateParentDirectory(string path)
    {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
        {
            Directory.CreateDirectory(dir);
        }
    }

    private static void TryDeleteFile(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch
        {
            // Suppressed by design: best-effort litter cleanup must never mask the original
            // COPY failure this runs alongside.
        }
    }

    /// <summary>One vectorized scan for every string column's
    /// max(length()). NULL aggregate (no rows / all-null column) => key omitted. Runtime-only:
    /// never enters the NodeId content hash.</summary>
    private static async Task<IReadOnlyDictionary<string, long>> ComputeMaxTextLengthsAsync(
        IDuckSession duck, string relation, string[] columns, CancellationToken ct)
    {
        var selects = string.Join(", ", columns.Select(
            (c, i) => $"max(length({ArrowInterop.QuoteQualified(c)}))::bigint as c{i}"));
        var result = new Dictionary<string, long>(StringComparer.Ordinal);
        await foreach (var batch in duck.QueryArrowAsync($"select {selects} from {relation}", 1 << 20, ct)
            .ConfigureAwait(false))
        {
            using (batch)
            {
                for (var i = 0; i < columns.Length; i++)
                {
                    if (batch.Column(i) is Apache.Arrow.Int64Array arr && arr.Length == 1 && arr.IsValid(0))
                    {
                        result[columns[i]] = arr.GetValue(0)!.Value;
                    }
                }
            }
        }

        return result;
    }
}

/// <summary>Maps a sink output's <c>input:</c> reference to the staging relation that backs it.
/// <c>input:</c> is always a bare pipeline name, so this is always the pipeline's staging
/// table/view as-is.</summary>
public static class StagingNames
{
    public static string ForSinkInput(string input) => $"staging.{input}";

    /// <summary>The staging table a SourceLoad node lands into — shared by both its native-scan and
    /// universal (Arrow ingest) branches so the table name is computed exactly once.</summary>
    public static string ForSourceLoad(string sourceName, string datasetName) =>
        $"staging.{StagingName.ForSourceLoad(sourceName, datasetName)}";
}
