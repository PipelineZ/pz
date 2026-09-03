using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Core.Dag;
using Pz.Core.Incremental;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Diagnostics.Otel;
using Pz.DuckDb;
using Pz.Engine.Planning;
using Pz.Engine.Resilience;
using Pz.Engine.State;

namespace Pz.Engine.Execution;

/// <summary>Loads one source dataset into its staging table by streaming Arrow batches from every
/// partition the connector plans, through a bounded channel, into <see cref="Pz.DuckDb.IDuckSession.IngestArrowAsync"/>.</summary>
public sealed class SourceLoadExecutor : INodeExecutor
{
    public async Task<NodeResult> ExecuteAsync(DagNode node, RunContext ctx, CancellationToken ct)
    {
        var def = (SourceDatasetDef)node.Definition;

        // A manifested node tries the copy path FIRST — before the connector is even resolved, because
        // "the flaky source is never contacted" is the point. A null return means a guard failed (table
        // missing, count mismatch, attach error); fall through to the normal extraction below.
        if (ctx.Reuse is { } reuseManifest && reuseManifest.TryGet(node.Id, out var reuseEntry))
        {
            var reused = await TryReuseAsync(node, def, reuseEntry, ctx, ct).ConfigureAwait(false);
            if (reused is not null)
            {
                return reused;
            }
        }

        if (!ctx.Connectors.TryGetSource(def.Source.Connector, out var connector))
        {
            var error = new PzError(PzErrorCode.ConnectorNotInstalled,
                $"connector '{def.Source.Connector}' is not installed", def.Source.FilePath, null,
                "run 'pz restore'");
            return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0, TimeSpan.Zero, error);
        }

        await using var source = await connector.OpenAsync(
            new ConnectorConfig(def.Source.Connection), ct).ConfigureAwait(false);

        // Hand a gate-aware source its operation gate before any plan/read call. The gate exists even
        // with no pacing registry (op-level retry alone).
        OperationGate? gate = null;
        if (source is IOperationGateAware gateAware)
        {
            gate = new OperationGate(RetryPolicyResolver.Resolve(node), ctx.RateLimiters?.For(node),
                Random.Shared, Task.Delay);
            gateAware.UseOperationGate(gate);
        }

        // Resolve the dataset's read shape once, up front -- every sync-state site below (prior-token
        // replay, the PZ0316 runtime guard, candidate capture, the partial-copy/done-skip exclusion)
        // keys on it.
        // Probed with the plain (watermark-free) spec: GetNaturalReadShape only inspects the dataset's
        // static options (mirrors ExecutionPlanner's own probe), never the watermark/prior-token state
        // this method is still in the middle of assembling below.
        var shape = ReadShapeResolver.Resolve(def.Dataset, source, SpecBuilder.ForSourceLoad(def));

        // The watermark rides a SpecBuilder overload used only here, at execution time (the planner keeps
        // probing with the watermark-free one) -- and only when this is an incremental dataset, not a
        // full-refresh run: --full-refresh skips only this read side; capture + advancement below still
        // run either way.
        Watermark? stored = null;
        if (def.Dataset.SyncMode is { Mode: SyncMode.Incremental } && !ctx.FullRefresh)
        {
            stored = ctx.Watermarks?.Get(WatermarkStore.Key(def.Source.Name, def.Dataset.Name), ctx.Notice);
        }

        // A Feed-shaped dataset replays its stored opaque token (unless --full-refresh) so the
        // connector can resume the change feed. Mirrors the watermark read-side gate: --full-refresh
        // skips only this read; capture + advancement still run.
        SyncState? priorSync = null;
        if (shape is ResolvedReadShape.Feed or ResolvedReadShape.Cdc && !ctx.FullRefresh)
        {
            priorSync = ctx.SyncState?.Get(SyncStateStore.Key(def.Source.Name, def.Dataset.Name), ctx.Notice);
        }

        // A windowed dataset (incremental.MaxWindow present -- DagCompiler
        // guarantees Initial is present and canonical, and Until canonical when present, for every
        // compiled windowed dataset) extracts an explicit [lower, upper] slice every run instead of an
        // unbounded "cursor > watermark" read. lowerWm carries the effective lower bound (the stored
        // watermark, or the declared Initial on a dataset's first run); windowUpper/caughtUp are threaded
        // into CaptureWatermarkAsync below so the empty-slice/candidate-capping rules only apply here.
        Watermark? lowerWm = stored;
        string? windowUpper = null;
        string? declaredType = null;
        var caughtUp = false;
        string? windowLower = null;
        // YAML max_window's ceiling is always inclusive, so `true` preserves that path byte-for-byte; a
        // SQL-declared `c < e` sets it false and the staging trim below cuts at >= instead of >.
        var upperInclusive = true;
        if (def.Dataset.SyncMode?.Incremental is { MaxWindow: not null } incremental)
        {
            // Contract mode types the cursor in columns:; raw-envelope datasets (http) type it via
            // the cursor/cursor_type options. PZ0213 guarantees one of the two resolved at compile.
            declaredType = CursorContract.ResolveDeclaredType(def.Dataset)!;

            // A windowed dataset's stored watermark may predate a `columns:` contract change (e.g. an
            // earlier backfill stored a `date`-typed cursor that the dataset now declares as
            // `bigint`/`timestamp`). Guarded here, BEFORE
            // WindowMath.AddWindow/Min below ever run on it -- those parse lowerWm.Value with the
            // DECLARED type's format, so a real type mismatch would otherwise throw a raw FormatException
            // (caught only by the dispatcher's generic safety net -> a non-actionable PZ0501). Checked
            // against `stored` specifically (not `lowerWm`, which may already hold a synthetic
            // Initial-backed watermark with no independent type of its own to disagree with).
            if (stored is not null && !string.Equals(stored.TypeName, declaredType, StringComparison.Ordinal))
            {
                var mismatchError = new PzError(PzErrorCode.UnsupportedCursorType,
                    $"source '{def.Source.Name}.{def.Dataset.Name}': stored watermark cursor type " +
                    $"'{stored.TypeName}' does not match the declared cursor type '{declaredType}'",
                    def.Source.FilePath, null,
                    "run with --full-refresh to restart the windowed backfill from initial, or align " +
                    "columns: with the stored watermark's type");
                return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0, TimeSpan.Zero, mismatchError);
            }

            lowerWm ??= new Watermark(incremental.Cursor, declaredType, incremental.Initial!, "initial");

            var upper = WindowMath.AddWindow(declaredType, lowerWm.Value, incremental.MaxWindow);
            if (incremental.Until is not null)
            {
                upper = WindowMath.Min(declaredType, upper, incremental.Until);
            }

            if (WindowMath.Compare(declaredType, upper, lowerWm.Value) <= 0)
            {
                caughtUp = true;
                ctx.Notice?.Invoke(
                    $"source '{def.Source.Name}.{def.Dataset.Name}' is caught up (watermark {lowerWm.Value} has reached until {incremental.Until})");
            }

            windowUpper = upper;
            windowLower = lowerWm.Value;
        }

        DatasetSpec spec;
        if (windowUpper is not null)
        {
            spec = SpecBuilder.ForSourceLoad(def, lowerWm, windowUpper);
        }
        else if (def.Dataset.SyncMode?.Incremental is { DeclaredInSql: true } sqlIncremental)
        {
            // SQL-declared bounds are evaluated (in DuckDB) to one floor and one ceiling; a
            // YAML-windowed dataset can never reach here (PZ0225 forbids SQL declaration on windowed).
            var bounds = await EvaluateSqlBoundsAsync(ctx, def, sqlIncremental, stored, connector, ct)
                .ConfigureAwait(false);

            // A ceiling is load-bearing for ADVANCEMENT, not merely for extraction volume, so it
            // drives the SAME locals YAML max_window drives -- which is what buys the (lower, upper]
            // staging trim, the advancement cap in CaptureWatermarkAsync, and the caught-up notice. Note
            // SpecBuilder only stamps an upper bound alongside a lower one, so a
            // ceiling whose floor evaluated to NULL (first run, bare watermark()) does not reach the
            // connector at all -- the trim below is what still makes staging, and therefore advancement,
            // exact in that case.
            if (bounds.Upper is not null)
            {
                declaredType = bounds.CursorType;
                windowUpper = bounds.Upper;
                windowLower = bounds.Lower?.Value;
                upperInclusive = bounds.UpperInclusive;
                if (windowLower is not null
                    && WindowMath.Compare(declaredType!, bounds.Upper, windowLower) <= 0)
                {
                    caughtUp = true;
                    ctx.Notice?.Invoke(
                        $"source '{def.Source.Name}.{def.Dataset.Name}' is caught up " +
                        $"(watermark {windowLower} has reached the SQL-declared ceiling {bounds.Upper})");
                }
            }

            spec = SpecBuilder.ForSourceLoad(def, bounds.Lower, bounds.Upper, bounds.LowerInclusive);
        }
        else
        {
            spec = SpecBuilder.ForSourceLoad(def, stored);
        }

        if (priorSync is not null)
        {
            spec = spec with { PriorSyncState = priorSync.Token };
        }

        if (ctx.Plan?.StrategyFor(node.Id) == EdgeStrategy.NativeScan && source.TryGetNativeScan(spec, out var scan))
        {
            // No separate extract/ingest split for the native-scan tier: DuckDB reads and loads in one
            // statement, so there is no seam to split on.
            using var ingestActivity = PzActivitySource.Instance.StartActivity("ingest");

            foreach (var statement in scan.SetupStatements)
            {
                await ctx.SetupLedger.ExecuteOnceAsync(statement, ct).ConfigureAwait(false);
            }

            var nativeTable = StagingNames.ForSourceLoad(def.Source.Name, def.Dataset.Name);
            try
            {
                await ctx.Duck.ExecuteAsync($"create or replace table {nativeTable} as select * from {scan.SqlFragment}", ct)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // The inner engine message MUST be sanitized (never the raw ex.Message): a DuckDB
                // parser/binder error's "LINE <n>: ..." context block would otherwise echo the scan
                // fragment (and anything embedded in it) verbatim into this NodeResult/log.
                var sanitized = NativeStatementRedactor.SanitizeEngineMessage(ex.Message);
                throw new PzConnectorException(
                    $"native scan statement failed: {sanitized}", isTransient: DuckTransientErrors.IsTransient(ex.Message),
                    innerException: ex);
            }

            var nativeRows = await ctx.Duck.ScalarAsync<long>($"select count(*) from {nativeTable}", ct).ConfigureAwait(false);

            var (nativeWatermarkError, nativeWatermarkCandidate) =
                await CaptureWatermarkAsync(ctx, def, nativeTable, windowUpper, windowLower, upperInclusive, declaredType, caughtUp, ct).ConfigureAwait(false);
            if (nativeWatermarkError is not null)
            {
                return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0, TimeSpan.Zero, nativeWatermarkError);
            }

            PzMeters.RowsMoved.Add(nativeRows, new KeyValuePair<string, object?>("pz.node.kind", "SourceLoad"));
            // Ops: universal tier only -- the native tier never routes through a .NET gate.
            var nativeSuccess = new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Success, nativeRows, TimeSpan.Zero, null,
                WatermarkCandidate: nativeWatermarkCandidate, SyncStateCandidate: null);
            // Lint only where the connector declared the schema was DuckDB-inferred (contract-less
            // csv/json auto_detect) — a database source's DOUBLE was already a double at the source, so
            // linting it would be a false positive.
            if (scan.SchemaInferred)
            {
                await IntegerInferenceLint.ApplyAsync(node, def, nativeTable, ctx, ct).ConfigureAwait(false);
                if (scan.SniffFragment is { } sniffFragment)
                {
                    await AmbiguousDateLint.ApplyAsync(node, def, nativeTable, sniffFragment, ctx, ct)
                        .ConfigureAwait(false);
                }
            }

            // The drift gate covers both data-plane tiers uniformly --
            // native scan never surfaces Arrow batches to .NET, but it always produces a staging
            // table, and that materialized table is the gate's only input. HintsFor (not the
            // universal branch's ProjectToHints-reconciled `hints`, which requires a queried logical
            // schema this tier never resolves) is the native tier's own stable read-shape hash input.
            return await SchemaDriftGate.ApplyAsync(nativeSuccess, node, def, HintsFor(def, connector.Capabilities),
                nativeTable, ctx, ct).ConfigureAwait(false);
        }

        var schema = (await source.GetSchemaAsync(spec, ct).ConfigureAwait(false)).Schema;
        // Column pruning has to reshape the STAGING schema too. GetSchemaAsync takes no hints, so it
        // reports the dataset's full shape, while a pruning connector sends batches carrying only the
        // requested columns -- and ingest binds by position. Left unreconciled, the staged table takes
        // the full column list and each pruned batch lands one column to the left of where it belongs:
        // an int64 id silently reading a float64 amount's bytes. Projecting here keeps the two in step.
        ReadHints hints;
        (hints, schema) = ProjectToHints(HintsFor(def, connector.Capabilities), schema);

        // A source that BOTH implements IStreamingSource AND advertises
        // ConnectorCapabilities.StreamingPartitions is drained lazily below (PumpStreamingPartitionsAsync)
        // instead of materializing the whole partition list here. When either half is absent the source
        // takes the list path (PlanReadAsync) instead.
        var streamingSource = source is IStreamingSource candidate
            && connector.Capabilities.HasFlag(ConnectorCapabilities.StreamingPartitions)
                ? candidate
                : null;
        var partitions = streamingSource is null
            ? await source.PlanReadAsync(spec, hints, ct).ConfigureAwait(false)
            : null;

        // Runtime guard backing ExecutionPlanner's PZ0316 for a connector that returns >1 partition
        // without declaring PartitionedRead: one opaque sync token / cdc log position cannot span
        // partitions.
        if (shape is ResolvedReadShape.Feed or ResolvedReadShape.Cdc && partitions is { Count: > 1 })
        {
            var span = shape == ResolvedReadShape.Cdc
                ? "one log position cannot span partitions"
                : "one opaque token cannot span partitions";
            return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0, TimeSpan.Zero,
                new PzError(PzErrorCode.SyncPartitionedReadConflict,
                    $"source '{def.Source.Name}' dataset '{def.Dataset.Name}' is sync-state but its connector " +
                    $"returned {partitions.Count} partitions -- {span}",
                    def.Source.FilePath, null,
                    "a sync dataset's connector must return a single partition"));
        }

        // Partition-scoped mode. Engages only under StablePartitionIds — native scans and non-declaring
        // connectors are untouched. Streaming sources join partition mode too (see
        // ExecutePartitionModeAsync's streaming branch, backed by PartitionModeLoader.LoadStreamingAsync).
        // Defensive cdc exclusion: cdc partitions come from a ChangeCapture connector that plans exactly
        // one partition (guarded above), and the collapse below runs only on the plain channel path. A
        // connector that declared BOTH ChangeCapture and StablePartitionIds must NOT route a cdc read
        // into partition mode (which knows nothing about __changes/collapse) -- keep it on the channel path.
        if (connector.Capabilities.HasFlag(ConnectorCapabilities.StablePartitionIds) && shape != ResolvedReadShape.Cdc)
        {
            return await ExecutePartitionModeAsync(node, def, ctx, source, partitions, streamingSource,
                spec, hints, schema, gate, windowLower, windowUpper, upperInclusive, declaredType, caughtUp, shape,
                ct).ConfigureAwait(false);
        }

        var channel = Channel.CreateBounded<RecordBatch>(4);

        // Timestamp-delta stall attribution around the two batch-level awaits this channel already has —
        // producer = PumpPartitionAsync's writer.WriteAsync, consumer = ReportProgress's manual-enumerator
        // MoveNextAsync below. Never per-row. Deltas come from StallAccumulator.Timestamp (i.e.
        // ctx.EffectiveTime), not a raw Stopwatch.
        var stall = new StallAccumulator(ctx.EffectiveTime);

        // Linked (not the same object as `ct`) so it can be cancelled either from inside the pump (a
        // partition's own fault, per PumpPartitionAsync) or from the consumer's failure path below —
        // whichever happens first tears down everything else. Producers (PumpPartitionAsync) use
        // pumpCts.Token for both the partition's ReadAsync enumeration and the channel WriteAsync; the
        // consumer below keeps using the original `ct`.
        using var pumpCts = CancellationTokenSource.CreateLinkedTokenSource(ct);

        // "extract" (this pump task, reading every partition) and "ingest" (the IngestArrowAsync call
        // below) are two separate, genuinely concurrent units of work — sibling stage spans under the
        // node span. Started here (both parented to
        // whatever is currently ambient, i.e. the node span) and Activity.Current explicitly restored
        // right after so "ingest" parents to the node span too, not to "extract".
        var nodeActivity = Activity.Current;
        using var extractActivity = PzActivitySource.Instance.StartActivity("extract");

        // Streaming sources yield partitions lazily (the point is a source over millions of small files):
        // the list path below starts ALL partition tasks at once, backpressured only by the bounded
        // channel(4) -- for a streaming source that would materialize millions of pending tasks up front,
        // defeating laziness. So the streaming variant gates in-flight partition reads with a SemaphoreSlim
        // sized to Environment.ProcessorCount -- a bounded I/O fan-out default, no YAML knob. Determinism
        // is unaffected: partitions already funnel through the one bounded channel into the one serialized
        // DuckDB ingest, so cross-partition row order was never guaranteed either way.
        var pump = streamingSource is null
            ? PumpPartitionsAsync(partitions!, channel.Writer, pumpCts, stall, ctx.EffectiveBatch)
            : PumpStreamingPartitionsAsync(
                streamingSource.PlanReadStreamingAsync(spec, hints, pumpCts.Token),
                channel.Writer, pumpCts, stall, ctx.EffectiveBatch, Environment.ProcessorCount);
        Activity.Current = nodeActivity;

        try
        {
            var tableName = StagingNames.ForSourceLoad(def.Source.Name, def.Dataset.Name);
            // cdc lands the RAW change rows into <staging>__changes; the collapse below builds the
            // canonical <staging> (upserts) and <staging>__deletes from it. Every other shape lands
            // straight into the canonical table.
            var landingTable = shape == ResolvedReadShape.Cdc ? tableName + "__changes" : tableName;
            long rows;
            using (PzActivitySource.Instance.StartActivity("ingest"))
            {
                rows = await ctx.Duck.IngestArrowAsync(
                    landingTable, schema, ReportProgress(channel.Reader, node, ctx, stall, ct), ct).ConfigureAwait(false);
            }

            // Success: no teardown needed, just join the producer.
            await pump.ConfigureAwait(false);

            // After a clean read, collect the connector's opaque sync-state candidate (if the
            // partition emits one). Feed-shaped datasets are single-partition (guarded above), so there is
            // exactly one partition to poll.
            SyncState? syncCandidate = null;
            if (shape is ResolvedReadShape.Feed or ResolvedReadShape.Cdc && partitions is { Count: 1 } &&
                partitions[0] is ISyncStatePartition syncPartition &&
                syncPartition.TryGetSyncStateCandidate(out var syncToken) && syncToken is not null)
            {
                syncCandidate = new SyncState(syncToken, ctx.Paths.RunId);
            }

            // Cdc collapse: with the raw window landed in <staging>__changes and the candidate
            // captured above, poll the partition's change keys, then collapse to last-event-per-key
            // upserts (canonical <staging>) + net-delete keys (<staging>__deletes), and count raw ops.
            // If landing failed nothing above ran, so this is only reached on a clean pump.
            CdcStats? cdcStats = null;
            if (shape == ResolvedReadShape.Cdc)
            {
                var cdcPartition = partitions is { Count: 1 } && partitions[0] is IChangeCapturePartition cp ? cp : null;
                var (cdcError, cdcCounts) = await CollapseCdcAsync(ctx, def, tableName, landingTable, cdcPartition, rows, ct)
                    .ConfigureAwait(false);
                if (cdcError is not null)
                {
                    return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0, TimeSpan.Zero, cdcError);
                }

                cdcStats = cdcCounts;

                // RowsMoved must report what is actually visible downstream post-collapse, same
                // precedent as the windowed recount below --
                // for cdc that's the canonical table's row count, not `rows` (the raw __changes ingest
                // count CollapseCdcAsync was handed). TryReuseAsync copies the canonical table and
                // count-verifies against the PRIOR run's RowsMoved; comparing a canonical copy against a
                // raw count fails on any window containing an update, a delete, or a repeated key.
                rows = await ctx.Duck.ScalarAsync<long>($"select count(*) from {tableName}", ct).ConfigureAwait(false);
            }

            // Universal-path engine backstop. The candidate-cap (WindowMath.Min in CaptureWatermarkAsync)
            // and window-scoped MAX (the `where` clause CaptureWatermarkAsync adds when windowUpper is
            // set) keep the WATERMARK value safe against an over-delivering connector, but neither one
            // removes the out-of-window rows from staging CONTENT itself -- ISource.PlanReadAsync honoring
            // DatasetSpec.WatermarkLowerBound/WatermarkUpperBound is MAY, not MUST (mirrors BoundedWindow's
            // native-tier MAY), and a force_universal tier flip can land the whole backlog regardless.
            // This DELETE is what actually guarantees staging is `(windowLower, windowUpper]` on THIS path,
            // for the sinks/pipelines that read it downstream -- the native branch above is untouched
            // (bounded by capable connectors; the PZ0313 gate guarantees NativeScan capability) and a
            // non-windowed dataset (windowUpper null) runs none of this. The candidate-cap and scoped MAX
            // remain as belt-and-braces for the watermark specifically.
            if (windowUpper is not null)
            {
                rows = await TrimToWindowAsync(ctx, def, tableName, windowLower, windowUpper, upperInclusive, ct)
                    .ConfigureAwait(false);
            }

            var (watermarkError, watermarkCandidate) =
                await CaptureWatermarkAsync(ctx, def, tableName, windowUpper, windowLower, upperInclusive, declaredType, caughtUp, ct).ConfigureAwait(false);
            if (watermarkError is not null)
            {
                return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0, TimeSpan.Zero, watermarkError);
            }

            PzMeters.RowsMoved.Add(rows, new KeyValuePair<string, object?>("pz.node.kind", "SourceLoad"));

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

            var legacySuccess = new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Success, rows, TimeSpan.Zero, null,
                stall.ToTimings(), watermarkCandidate, SyncStateCandidate: syncCandidate, Ops: opStats, Cdc: cdcStats);
            return await SchemaDriftGate.ApplyAsync(legacySuccess, node, def, hints, tableName, ctx, ct)
                .ConfigureAwait(false);
        }
        catch
        {
            // The consumer (IngestArrowAsync) failed for a reason that may be unrelated to any partition
            // (e.g. a DuckDB-side error) with producers potentially still blocked on the bounded(4) channel
            // (nobody left to drain it) or mid-partition-read. Without cancelling pumpCts here, those
            // blocked WriteAsync/ReadAsync calls would never unblock and the `await pump` below would hang
            // forever — this is the deadlock this teardown exists to prevent. Cancelling first lets every
            // producer observe OperationCanceledException promptly and unwind.
            pumpCts.Cancel();
            try
            {
                await pump.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected: this is exactly the teardown cancellation above unwinding the pump, not a new
                // failure — the original consumer exception (about to be rethrown) must not be masked by it.
            }

            throw;
        }
    }

    /// <summary>Collapses the raw change window landed in
    /// <paramref name="changesTable"/> (<c>&lt;staging&gt;__changes</c>) into last-event-per-key upserts
    /// (canonical <paramref name="canonicalTable"/>) plus a <c>&lt;staging&gt;__deletes</c> side table of
    /// net-deleted keys, and returns the raw per-op counts. Guards, in order: (1) the `_pz_op`/`_pz_lsn`
    /// change-row contract must be present in the landed schema; (2) the connector must report change key
    /// columns (<see cref="IChangeCapturePartition"/>) — an empty window with none is legitimate (empty
    /// collapse), a non-empty window with none is a connector defect. Ordering matters: candidate + keys
    /// are polled after the pump joins, THEN the two collapse statements, THEN
    /// counts. Identifier quoting reuses <see cref="ArrowInterop.QuoteQualified"/> (the same helper
    /// <see cref="CaptureWatermarkAsync"/> uses), never raw concatenation of key names.</summary>
    /// <summary>The compiler decided what the single reading pipeline
    /// makes pushable; this decides whether THIS connector gets it. The two halves are gated
    /// independently because a connector may honour one and not the other. An absent capability means
    /// push nothing for that half, silently — this is an optimisation, and the pipeline's own SQL still
    /// runs in DuckDB over whatever landed, so a connector that receives nothing produces exactly the
    /// same rows, just after moving more of them.</summary>
    internal static ReadHints HintsFor(SourceDatasetDef def, ConnectorCapabilities caps)
    {
        if (def.Hints is not { } plan)
        {
            return ReadHints.None;
        }

        return new ReadHints(
            caps.HasFlag(ConnectorCapabilities.ColumnPruning) ? plan.Columns : null,
            caps.HasFlag(ConnectorCapabilities.PredicatePushdown) ? plan.PredicateSql : null);
    }

    /// <summary>Reconciles a column-pruning hint with the schema the connector reported, narrowing
    /// <paramref name="schema"/> to the requested columns and restating the hint in that same schema
    /// order. Ordering matters: ingest binds batch fields by position, so the staged table and the
    /// connector's SELECT list must agree, and taking the source's own order (rather than the compiler's
    /// sorted set) also keeps a staged table's columns laid out the way the source lays them out.
    /// A hint naming a column the source has not got is dropped whole — extraction then proceeds
    /// unpruned and the pipeline's own SQL reports the unknown column with its usual message, instead of
    /// the connector failing on a SELECT list pz built.</summary>
    internal static (ReadHints Hints, Schema Schema) ProjectToHints(ReadHints hints, Schema schema)
    {
        if (hints.Columns is not { Count: > 0 } wanted)
        {
            return (hints, schema);
        }

        var names = new HashSet<string>(wanted, StringComparer.OrdinalIgnoreCase);
        var fields = schema.FieldsList.Where(f => names.Contains(f.Name)).ToList();
        if (fields.Count != names.Count)
        {
            return (hints with { Columns = null }, schema);
        }

        return (hints with { Columns = [.. fields.Select(f => f.Name)] }, new Schema(fields, schema.Metadata));
    }

    private static async Task<(PzError? Error, CdcStats? Stats)> CollapseCdcAsync(
        RunContext ctx, SourceDatasetDef def, string canonicalTable, string changesTable,
        IChangeCapturePartition? partition, long changeRows, CancellationToken ct)
    {
        // (1) change-row contract: the collapse's ORDER BY / WHERE reference _pz_op and _pz_lsn, so both
        // must exist in the landed __changes schema. Probed via DESCRIBE (same pattern as the watermark
        // cursor probe) so a bad connector fails the node cleanly instead of throwing a raw binder error.
        var contractCols = await ctx.Duck.ScalarAsync<long>(
            $"select count(*) from (describe {changesTable}) where column_name in ('_pz_op', '_pz_lsn')", ct)
            .ConfigureAwait(false);
        if (contractCols != 2)
        {
            return (new PzError(PzErrorCode.ChangeCaptureUnsupported,
                $"source '{def.Source.Name}.{def.Dataset.Name}': change-capture rows must carry the " +
                "_pz_op and _pz_lsn change-row contract columns, but the connector landed rows without them",
                def.Source.FilePath, null,
                "fix the connector to emit the _pz_op / _pz_lsn / _pz_changed_at change-row envelope"), null);
        }

        IReadOnlyList<string>? keys = null;
        partition?.TryGetChangeKeyColumns(out keys);
        if (keys is null || keys.Count == 0)
        {
            // No key metadata: an empty window is fine (build empty collapsed + deletes tables, no ops);
            // a non-empty window cannot be collapsed per key -> connector defect.
            if (changeRows == 0)
            {
                await ctx.Duck.ExecuteAsync(
                    $"create or replace table {canonicalTable} as " +
                    $"select * exclude (_pz_op, _pz_lsn, _pz_changed_at) from {changesTable} limit 0", ct)
                    .ConfigureAwait(false);
                await ctx.Duck.ExecuteAsync(
                    $"create or replace table {canonicalTable}__deletes as " +
                    $"select * exclude (_pz_op, _pz_lsn, _pz_changed_at) from {changesTable} limit 0", ct)
                    .ConfigureAwait(false);
                return (null, new CdcStats(0, 0, 0));
            }

            return (new PzError(PzErrorCode.ChangeCaptureUnsupported,
                $"source '{def.Source.Name}.{def.Dataset.Name}': the connector landed {changeRows} change row(s) " +
                "but did not report the change key columns needed to collapse them per key",
                def.Source.FilePath, null,
                "fix the connector to report its replica-identity / primary-key columns via IChangeCapturePartition"), null);
        }

        var keyList = string.Join(", ", keys.Select(ArrowInterop.QuoteQualified));
        await ctx.Duck.ExecuteAsync(
            $"create or replace table {canonicalTable} as " +
            $"select * exclude (_pz_op, _pz_lsn, _pz_changed_at, _pz_rn) from (" +
            $"  select *, row_number() over (partition by {keyList} order by _pz_lsn desc) as _pz_rn " +
            $"  from {changesTable}) where _pz_rn = 1 and _pz_op <> 'delete'", ct).ConfigureAwait(false);
        await ctx.Duck.ExecuteAsync(
            $"create or replace table {canonicalTable}__deletes as " +
            $"select * exclude (_pz_op, _pz_lsn, _pz_changed_at, _pz_rn) from (" +
            $"  select *, row_number() over (partition by {keyList} order by _pz_lsn desc) as _pz_rn " +
            $"  from {changesTable}) where _pz_rn = 1 and _pz_op = 'delete'", ct).ConfigureAwait(false);

        // Raw per-op counts of the window (not net): three cheap scalar counts over the just-written
        // __changes table (simpler than reading a grouped result set through the scalar-only seam).
        var inserts = await ctx.Duck.ScalarAsync<long>($"select count(*) from {changesTable} where _pz_op = 'insert'", ct).ConfigureAwait(false);
        var updates = await ctx.Duck.ScalarAsync<long>($"select count(*) from {changesTable} where _pz_op = 'update'", ct).ConfigureAwait(false);
        var deletes = await ctx.Duck.ScalarAsync<long>($"select count(*) from {changesTable} where _pz_op = 'delete'", ct).ConfigureAwait(false);
        return (null, new CdcStats(inserts, updates, deletes));
    }

    /// <summary>Partition-scoped mode entry point. Validates every planned partition's identity
    /// (StablePartitionIds contract) before any extraction starts, then hands the identified list to
    /// <see cref="PartitionModeLoader"/> for per-partition part-table staging + ledger-gated completion.
    /// The epilogue below deliberately mirrors <see cref="ExecuteAsync"/>'s legacy-branch
    /// trim/watermark/sync/meters statements — a parallel copy that keeps the legacy path byte-identical
    /// rather than threading a shared helper through both.</summary>
    private static async Task<NodeResult> ExecutePartitionModeAsync(DagNode node, SourceDatasetDef def,
        RunContext ctx, ISource source, IReadOnlyList<IDatasetPartition>? partitions,
        IStreamingSource? streamingSource, DatasetSpec spec, ReadHints hints, Schema schema, OperationGate? gate,
        string? windowLower, string? windowUpper, bool upperInclusive, string? declaredType, bool caughtUp,
        ResolvedReadShape shape,
        CancellationToken ct)
    {
        var tableName = StagingNames.ForSourceLoad(def.Source.Name, def.Dataset.Name);

        PzError? loadError;
        long rows;
        PartitionStats stats;
        if (streamingSource is not null)
        {
            // A streaming source's identity contract is validated AT ADMISSION, one
            // partition at a time, inside LoadStreamingAsync itself — there is no whole list to
            // pre-validate up front the way the list branch below does.
            (loadError, rows, stats) = await PartitionModeLoader.LoadStreamingAsync(node, ctx,
                streamingSource.PlanReadStreamingAsync(spec, hints, ct), schema, tableName,
                windowLower, windowUpper, reusedCount: 0, def, ct).ConfigureAwait(false);
        }
        else
        {
            var identified = new List<(IIdentifiedPartition Partition, string Id)>(partitions!.Count);
            var seen = new HashSet<string>(StringComparer.Ordinal);
            var problems = new List<string>();
            for (var i = 0; i < partitions.Count; i++)
            {
                if (partitions[i] is not IIdentifiedPartition identifiedPartition)
                {
                    problems.Add($"partition {i + 1} of {partitions.Count} does not implement IIdentifiedPartition");
                }
                else if (string.IsNullOrEmpty(identifiedPartition.PartitionId))
                {
                    problems.Add($"partition {i + 1} of {partitions.Count} has an empty id");
                }
                else if (!seen.Add(identifiedPartition.PartitionId))
                {
                    problems.Add($"partition {i + 1} of {partitions.Count} duplicates another partition's id");
                }
                else
                {
                    identified.Add((identifiedPartition, identifiedPartition.PartitionId));
                }
            }

            if (problems.Count > 0)
            {
                // Ordinals only — raw partition ids never surface in errors.
                return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0, TimeSpan.Zero,
                    new PzError(PzErrorCode.PartitionIdentityInvalid,
                        $"source '{def.Source.Name}' dataset '{def.Dataset.Name}': connector declares " +
                        $"StablePartitionIds but its planned partitions violate the identity contract: " +
                        string.Join("; ", problems),
                        def.Source.FilePath, null,
                        "fix the connector's partition planning (see https://pipelinez.dev/how-to/author-a-connector/)"));
            }

            // A FAILED prior SourceLoad's partition-level progress is copied in
            // BEFORE LoadAsync runs, so its ledger already shows those partitions done and LoadAsync
            // skips them like any other resumed run. List path only -- streaming plans lazily and has
            // no up-front id set to guard partial copy against. A Feed-shaped dataset's sole partition is
            // EXCLUDED here (the done-skip exception): LoadAsync unconditionally resets and
            // re-reads a feed dataset's single partition live even if it were copied in "done", so
            // attempting the copy would only pay an ATTACH/copy round trip for a result LoadAsync
            // immediately discards, and would over-report PartitionStats.Reused for a partition that
            // was in fact fully re-extracted, not reused.
            long reused = 0;
            if (ctx.Reuse is { } reuseManifest && shape != ResolvedReadShape.Feed &&
                reuseManifest.TryGetPartial(node.Id, out var partialEntry))
            {
                var plannedIds = identified.Select(p => p.Id).ToHashSet(StringComparer.Ordinal);
                reused = await PartitionModeLoader.TryCopyPartialAsync(node, def, ctx, plannedIds,
                    tableName, windowLower, windowUpper, partialEntry, ct).ConfigureAwait(false);
            }

            // Sync done-skip exception: a Feed-shaped dataset's single partition is never
            // skip-reused within a run — plumbed through so PartitionModeLoader can reset-and-reread
            // it live instead of silently stalling sync-state advancement.
            (loadError, rows, stats) = await PartitionModeLoader.LoadAsync(node, ctx, identified, schema,
                tableName, windowLower, windowUpper, reused, shape == ResolvedReadShape.Feed, ct)
                .ConfigureAwait(false);
        }

        if (loadError is not null)
        {
            return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0, TimeSpan.Zero, loadError);
        }

        // Epilogue — same statements as the legacy branch (windowed trim + recount, watermark
        // capture, sync poll, meters); see the legacy branch above and mirror it exactly, with
        // rows recomputed from the trim's count when windowed. Timings deliberately null and
        // Partitions carrying the stats.
        SyncState? syncCandidate = null;
        if (shape == ResolvedReadShape.Feed && partitions is { Count: 1 } &&
            partitions[0] is ISyncStatePartition syncPartition &&
            syncPartition.TryGetSyncStateCandidate(out var syncToken) && syncToken is not null)
        {
            syncCandidate = new SyncState(syncToken, ctx.Paths.RunId);
        }

        if (windowUpper is not null)
        {
            rows = await TrimToWindowAsync(ctx, def, tableName, windowLower, windowUpper, upperInclusive, ct)
                .ConfigureAwait(false);
        }

        var (watermarkError, watermarkCandidate) = await CaptureWatermarkAsync(
            ctx, def, tableName, windowUpper, windowLower, upperInclusive, declaredType, caughtUp, ct).ConfigureAwait(false);
        if (watermarkError is not null)
        {
            return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Failed, 0, TimeSpan.Zero, watermarkError);
        }

        PzMeters.RowsMoved.Add(rows, new KeyValuePair<string, object?>("pz.node.kind", "SourceLoad"));
        var opStats = gate?.Snapshot();
        if (opStats is { } snapshot)
        {
            var instanceTag = new KeyValuePair<string, object?>("pz.instance", InstanceKey.For(node));
            PzMeters.OpsExecuted.Add(snapshot.Executed, instanceTag);
            PzMeters.OpsRetried.Add(snapshot.Retried, instanceTag);
            PzMeters.OpsThrottleWait.Add(snapshot.ThrottleWaitMs, instanceTag);
        }

        var partitionSuccess = new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Success, rows, TimeSpan.Zero, null,
            Timings: null, watermarkCandidate, SyncStateCandidate: syncCandidate, Ops: opStats,
            Partitions: stats);
        return await SchemaDriftGate.ApplyAsync(partitionSuccess, node, def, hints, tableName, ctx, ct)
            .ConfigureAwait(false);
    }

    /// <summary>Universal-path engine backstop, shared by the
    /// legacy and streaming branches. The candidate-cap (WindowMath.Min in CaptureWatermarkAsync) and
    /// window-scoped MAX keep the WATERMARK value safe against an over-delivering connector, but neither
    /// removes the out-of-window rows from staging CONTENT -- honoring DatasetSpec.WatermarkLowerBound/
    /// WatermarkUpperBound is MAY, not MUST, and a force_universal tier flip can land the whole backlog
    /// regardless. This DELETE is what actually guarantees staging is the declared window for the sinks and
    /// pipelines that read it downstream. Returns the post-trim row count: ingest's own count is PRE-trim,
    /// and NodeCompleted's RowsMoved must report what is visible downstream.
    ///
    /// Quoting discipline matches CaptureWatermarkAsync's scoped MAX: ArrowInterop.QuoteQualified for the
    /// identifier, single-quote-doubling for the literal. Both bounds are already canonical strings in the
    /// declared type's format (verified against the landed type by CaptureWatermarkAsync's mismatch guard),
    /// so a plain quoted literal is enough -- DuckDB implicitly casts it to the column's actual type.</summary>
    private static async Task<long> TrimToWindowAsync(RunContext ctx, SourceDatasetDef def, string tableName,
        string? windowLower, string windowUpper, bool upperInclusive, CancellationToken ct)
    {
        var quoted = ArrowInterop.QuoteQualified(def.Dataset.SyncMode!.Incremental!.Cursor);
        // An exclusive SQL ceiling (`c < e`) is pushed to the connector as if inclusive --
        // DatasetSpec.WatermarkUpperBound is <=-shaped in every connector -- so the boundary rows land and
        // this trim is what removes them. Over-extraction is safe; the trim is what makes advancement
        // exact. windowLower is null only for a SQL ceiling whose floor evaluated to NULL (first run, bare
        // watermark()); then there is no lower half to cut.
        var upperOp = upperInclusive ? ">" : ">=";
        var lowerHalf = windowLower is null ? "" : $"{quoted} <= '{windowLower.Replace("'", "''")}' or ";
        await ctx.Duck.ExecuteAsync(
            $"delete from {tableName} where {lowerHalf}{quoted} {upperOp} '{windowUpper.Replace("'", "''")}'",
            ct).ConfigureAwait(false);

        return await ctx.Duck.ScalarAsync<long>($"select count(*) from {tableName}", ct).ConfigureAwait(false);
    }

    /// <summary>The reduced result of a dataset's SQL-declared bounds. Lower is the LOOSEST floor (safe
    /// over-extraction — the pipeline filter cuts); Upper is the TIGHTEST ceiling, because advancement
    /// reads the staged table rather than the pipeline's output, so an over-high ceiling would advance the
    /// watermark past rows the pipeline never processed. Any member may be null, meaning "no bound of that
    /// kind" — a bare watermark() on a first run evaluates to NULL and is not an error.</summary>
    private readonly record struct SqlBounds(
        Watermark? Lower, bool LowerInclusive, string? Upper, bool UpperInclusive, string? CursorType);

    /// <summary>Evaluates a SQL-declared dataset's recorded bounds in
    /// DuckDB and reduces them to one floor and one ceiling. The floor takes the LOOSEST of its candidates
    /// (safe over-extraction — the pipeline filter cuts); the ceiling takes the TIGHTEST, because
    /// advancement is MAX(cursor) over the STAGED table, which the pipeline's WHERE never filters, so a
    /// ceiling that let extra rows land would advance the watermark past rows nothing processed.
    /// Evaluated on the first run too (stored is null → the sentinel substitutes as the literal NULL, so a
    /// coalesce resolves to its `initial` and a bare watermark() resolves to NULL = no bound).
    /// --full-refresh pushes nothing.</summary>
    private static async Task<SqlBounds> EvaluateSqlBoundsAsync(
        RunContext ctx, SourceDatasetDef def, IncrementalDef incremental, Watermark? stored,
        ISourceConnector connector, CancellationToken ct)
    {
        if (ctx.FullRefresh) { return default; }

        // Three type sources, in order. A columns: contract wins where declared; otherwise the stored
        // watermark carries its own type (exactly how PipelineExecutor types its own literal); on a FIRST
        // run with no contract there is neither, so DuckDB is asked what the bound expression evaluates
        // to. That third source is what keeps columns: optional for the whole trio.
        var cursorType = def.Dataset.Columns is { } contract && contract.TryGetValue(incremental.Cursor, out var t)
            ? t
            : stored?.TypeName;
        cursorType ??= await ProbeCursorTypeAsync(ctx, incremental, ct).ConfigureAwait(false);
        if (cursorType is null) { return default; }

        string? lower = null, upper = null;
        bool lowerInclusive = false, upperInclusive = false;
        foreach (var bound in incremental.SqlBounds!)
        {
            var canonical = await EvaluateOneAsync(ctx, def, bound, cursorType, stored, ct).ConfigureAwait(false);
            if (canonical is null) { continue; } // NULL bound (bare watermark(), first run) = no bound

            // The ceiling's tie-break is `&=` where the floor's is `|=`: equal-valued bounds take the
            // STRICTER inclusivity for a ceiling and the LOOSER for a floor -- the same "never exclude a
            // row the pipeline keeps / never include one past the window" asymmetry.
            if (bound.IsUpper)
            {
                var cmp = upper is null ? -1 : WindowMath.Compare(cursorType, canonical, upper);
                if (cmp < 0) { upper = canonical; upperInclusive = bound.Inclusive; }
                else if (cmp == 0) { upperInclusive &= bound.Inclusive; }
            }
            else
            {
                var cmp = lower is null ? -1 : WindowMath.Compare(cursorType, canonical, lower);
                if (cmp < 0) { lower = canonical; lowerInclusive = bound.Inclusive; }
                else if (cmp == 0) { lowerInclusive |= bound.Inclusive; }
            }
        }

        if (lowerInclusive && !connector.Capabilities.HasFlag(ConnectorCapabilities.InclusiveWatermarkBound))
        {
            ctx.Notice?.Invoke(
                $"source '{def.Source.Name}.{def.Dataset.Name}': connector '{def.Source.Connector}' cannot honor " +
                "an inclusive watermark bound; extracting unbounded — the pipeline filter applies the cut");
            lower = null;
            lowerInclusive = false;
        }

        var lowerWm = lower is null ? null : new Watermark(incremental.Cursor, cursorType, lower, "sql-bound");
        return new SqlBounds(lowerWm, lowerInclusive, upper, upperInclusive, cursorType);
    }

    /// <summary>Evaluates one bound expression in DuckDB and canonicalizes it. Returns null when the
    /// expression evaluates to NULL — a bare watermark() on a first run, which means "no bound" rather
    /// than an error. (DuckSession.ScalarAsync yields "" for a SQL NULL, the same ADO.NET-ism
    /// CaptureWatermarkAsync's empty-slice check relies on.)</summary>
    private static async Task<string?> EvaluateOneAsync(RunContext ctx, SourceDatasetDef def,
        SqlWatermarkBound bound, string cursorType, Watermark? stored, CancellationToken ct)
    {
        var exprSql = SubstituteSentinel(bound, cursorType, stored);
        var probe = cursorType switch
        {
            "timestamp" => $"select strftime(cast(({exprSql}) as timestamp), '%Y-%m-%dT%H:%M:%S.%f')",
            "date" => $"select strftime(cast(({exprSql}) as date), '%Y-%m-%d')",
            _ => $"select cast(({exprSql}) as varchar)",
        };

        string? raw;
        try { raw = await ctx.Duck.ScalarAsync<string>(probe, ct).ConfigureAwait(false); }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var sanitized = NativeStatementRedactor.SanitizeEngineMessage(ex.Message);
            throw new PzConnectorException($"watermark bound evaluation failed: {sanitized}",
                isTransient: false, innerException: ex);
        }

        if (string.IsNullOrEmpty(raw)) { return null; }
        if (!WindowMath.TryCanonicalize(cursorType, raw, out var canonical))
        {
            throw new PzConnectorException(
                $"source '{def.Source.Name}.{def.Dataset.Name}': watermark bound expression evaluated to " +
                $"'{raw}', which is not a valid {cursorType} cursor value", isTransient: false);
        }

        return canonical;
    }

    /// <summary>The sentinel becomes the stored value as a typed literal, or the bare keyword NULL on a
    /// first run — which is exactly what makes <c>coalesce(watermark(), &lt;initial&gt;)</c> resolve to
    /// the initial.</summary>
    private static string SubstituteSentinel(SqlWatermarkBound bound, string cursorType, Watermark? stored) =>
        bound.ValueExprSql.Replace($"'{bound.Sentinel}'",
            stored is null ? "NULL" : CursorLiterals.Typed(cursorType, stored.Value), StringComparison.Ordinal);

    /// <summary>Last-resort cursor typing: asks DuckDB what a bound expression evaluates to, with the
    /// sentinel as NULL. Reached only on a FIRST run of a dataset with no columns: contract, where neither
    /// a declared type nor a stored watermark exists. Returns null when no bound yields one of the allowed
    /// cursor types — a bare watermark() probes as DuckDB's "NULL" type, which maps to nothing — and the
    /// caller then pushes no bound at all, which is always safe.</summary>
    private static async Task<string?> ProbeCursorTypeAsync(
        RunContext ctx, IncrementalDef incremental, CancellationToken ct)
    {
        foreach (var bound in incremental.SqlBounds!)
        {
            // cursorType is only used to type the STORED value's literal, and stored is null here, so the
            // placeholder passed in is never read.
            var expr = SubstituteSentinel(bound, "timestamp", null);
            string? duckType;
            try { duckType = await ctx.Duck.ScalarAsync<string>($"select typeof(({expr}))", ct).ConfigureAwait(false); }
            catch (Exception ex) when (ex is not OperationCanceledException) { continue; }

            var mapped = (duckType ?? "").Split('(')[0].Trim().ToUpperInvariant() switch
            {
                "TIMESTAMP" or "TIMESTAMP WITH TIME ZONE" or "TIMESTAMP_NS" or "TIMESTAMP_MS" => "timestamp",
                "DATE" => "date",
                "BIGINT" or "HUGEINT" or "UBIGINT" => "bigint",
                "INTEGER" or "SMALLINT" or "TINYINT" or "UINTEGER" => "int",
                "DECIMAL" or "NUMERIC" => "decimal",
                _ => null,
            };
            if (mapped is not null) { return mapped; }
        }

        return null;
    }

    /// <summary>Copies the failed run's staged table into this run's staging:
    /// ATTACH (read_only) → CREATE OR REPLACE TABLE ... AS SELECT → count-verify → DETACH. Every guard
    /// failure returns null (fallback to normal extraction) with a ctx.Notice naming the reason — reuse
    /// is an optimization with a correctness bonus, never a new failure mode. OperationCanceledException
    /// always propagates.</summary>
    private static async Task<NodeResult?> TryReuseAsync(DagNode node, SourceDatasetDef def, ReuseEntry entry,
        RunContext ctx, CancellationToken ct)
    {
        var table = StagingNames.ForSourceLoad(def.Source.Name, def.Dataset.Name);
        var datasetLabel = $"{def.Source.Name}.{def.Dataset.Name}";
        var pathLiteral = entry.PriorStagingPath.Replace("'", "''");

        // The dispatcher runs nodes concurrently and DuckSession serializes per-STATEMENT, not
        // per-sequence (the run's one DuckDB connection is gated by a semaphore around a single statement's
        // execution, not this whole attach/copy/detach sequence) -- two reused SourceLoads interleaving on
        // a SHARED "pz_prior" alias would have one ATTACH observe the other's alias already bound and fail
        // with a duplicate-ATTACH error, sending the loser through the exact fallback-to-connector path
        // reuse exists to avoid. A per-node alias (derived from the node's own content-addressed ID)
        // removes the shared mutable name entirely, so concurrent reuses never collide.
        var alias = "pz_prior_" + new string(node.Id.Value.Where(char.IsLetterOrDigit).ToArray());
        var quotedAlias = ArrowInterop.QuoteQualified(alias);
        var attached = false;
        try
        {
            await ctx.Duck.ExecuteAsync($"attach '{pathLiteral}' as {quotedAlias} (read_only)", ct).ConfigureAwait(false);
            attached = true;

            await ctx.Duck.ExecuteAsync($"create or replace table {table} as select * from {quotedAlias}.{table}", ct)
                .ConfigureAwait(false);

            var rows = await ctx.Duck.ScalarAsync<long>($"select count(*) from {table}", ct).ConfigureAwait(false);
            if (rows != entry.Rows)
            {
                await ctx.Duck.ExecuteAsync($"drop table if exists {table}", ct).ConfigureAwait(false);
                ctx.Notice?.Invoke(
                    $"source '{datasetLabel}': prior run's staged table has {rows} row(s) but {entry.Rows} " +
                    "were recorded; re-extracting");
                return null;
            }

            // The failed run's cdc collapse also produced a <staging>__deletes side table,
            // which downstream delete-applying sinks read. Copy it too when the prior staging has it (the
            // raw <staging>__changes is NOT copied -- nothing downstream reads it, and the collapse already
            // ran). Probed via the attached catalog's information_schema so a non-cdc dataset (no deletes
            // table) simply copies nothing. table = "staging.<name>"; the deletes table's unqualified name
            // is "<name>__deletes" (alias is [A-Za-z0-9_] only, so a plain literal is injection-safe).
            var deletesTable = table + "__deletes";
            var deletesName = (table.Split('.', 2)[1] + "__deletes").Replace("'", "''");
            var hasDeletes = await ctx.Duck.ScalarAsync<long>(
                $"select count(*) from information_schema.tables where table_catalog = '{alias}' and table_name = '{deletesName}'", ct)
                .ConfigureAwait(false);
            if (hasDeletes > 0)
            {
                await ctx.Duck.ExecuteAsync(
                    $"create or replace table {deletesTable} as select * from {quotedAlias}.{deletesTable}", ct)
                    .ConfigureAwait(false);
            }

            var watermark = entry.Watermark is { } wm
                ? new Watermark(wm.Cursor, wm.Type, wm.Value, ctx.Paths.RunId)
                : null;

            PzMeters.RowsMoved.Add(rows, new KeyValuePair<string, object?>("pz.node.kind", "SourceLoad"));
            // Deliberately NOT routed through SchemaDriftGate: this table was landed by the FAILED
            // run, whose own gate pass already accepted/seeded its schema (or the run wouldn't have
            // produced a reusable ReuseEntry) — re-DESCRIBEing a byte-identical copy would only
            // re-run a check that already ran.
            return new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Success, rows, TimeSpan.Zero, null,
                WatermarkCandidate: watermark, Provenance: NodeProvenance.Reused);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Best-effort: CREATE OR REPLACE TABLE may have already succeeded before a later statement
            // (the count query, in practice) threw -- leaving a fully-populated copied table behind would
            // make the fallback extraction below hit "table already exists" (the universal ingest path
            // uses a plain CREATE TABLE, not CREATE OR REPLACE), a new failure mode reuse must never
            // introduce. Guarded with its own try/catch and CancellationToken.None so a failed drop can
            // never mask the real reuse-failure notice/fallback decision below.
            try
            {
                await ctx.Duck.ExecuteAsync($"drop table if exists {table}", CancellationToken.None).ConfigureAwait(false);
            }
            catch
            {
                // Best-effort: see above.
            }

            ctx.Notice?.Invoke(
                $"source '{datasetLabel}': staged data from the prior run could not be reused " +
                $"({NativeStatementRedactor.SanitizeEngineMessage(ex.Message).Split('\n', 2)[0].TrimEnd('\r')}); re-extracting");
            return null;
        }
        finally
        {
            if (attached)
            {
                try
                {
                    await ctx.Duck.ExecuteAsync($"detach {quotedAlias}", CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                    // Best-effort: a failed detach must never mask the reuse result/fallback decision.
                }
            }
        }
    }

    /// <summary>Pumps every partition concurrently into <paramref name="writer"/>. The first partition to
    /// fault cancels <paramref name="pumpCts"/> itself (see <see cref="PumpPartitionAsync"/>) so siblings
    /// stop immediately rather than only once <see cref="Task.WhenAll(Task[])"/> below observes the fault;
    /// this method then waits out whatever that cancellation produced in the rest and completes the channel
    /// with the fault. A clean run completes the channel with no error.</summary>
    private static async Task PumpPartitionsAsync(
        IReadOnlyList<IDatasetPartition> partitions, ChannelWriter<RecordBatch> writer, CancellationTokenSource pumpCts,
        StallAccumulator stall, BatchOptions batchOptions)
    {
        var tasks = partitions.Select(partition => PumpPartitionAsync(partition, writer, pumpCts, stall, batchOptions)).ToArray();

        try
        {
            await Task.WhenAll(tasks).ConfigureAwait(false);
            writer.Complete();
        }
        catch (Exception ex)
        {
            await Task.WhenAll(tasks.Select(SwallowAsync)).ConfigureAwait(false);
            writer.Complete(ex);
        }
    }

    /// <summary>Streaming sibling of <see cref="PumpPartitionsAsync"/>: launches <see cref="PumpPartitionAsync"/>
    /// for each partition as it arrives from the async enumerable, bounded by a <see cref="SemaphoreSlim"/> of
    /// <paramref name="maxConcurrency"/>, so the full partition set is never materialized (a streaming source
    /// may enumerate millions of partitions). Same fail-fast and channel-completion contract as the list
    /// variant: a partition fault cancels <paramref name="pumpCts"/> itself (see <see cref="PumpPartitionAsync"/>),
    /// and this method's own <c>catch</c> ALSO cancels it (see below) so an enumerator-level fault
    /// (the async enumerable itself throwing mid-enumeration, e.g. a listing-page failure -- distinct from a
    /// single partition's <c>ReadAsync</c> faulting) tears down every already-launched sibling immediately too.
    /// Either way the channel is completed with the GENUINE fault -- <see cref="DrainForFaultAsync"/> prefers a
    /// real partition failure over this method's own teardown <see cref="OperationCanceledException"/> or the
    /// caught enumerator fault, so the consumer sees exactly the exception the list path's
    /// <see cref="Task.WhenAll(Task[])"/> would surface (never a cancellation the dispatcher would mistake for an
    /// external cancel). A clean run completes the channel with no error.</summary>
    private static async Task PumpStreamingPartitionsAsync(
        IAsyncEnumerable<IDatasetPartition> partitions, ChannelWriter<RecordBatch> writer, CancellationTokenSource pumpCts,
        StallAccumulator stall, BatchOptions batchOptions, int maxConcurrency)
    {
        using var gate = new SemaphoreSlim(maxConcurrency);
        var tasks = new List<Task>();

        try
        {
            // The enumerator itself is driven under pumpCts.Token, so a sibling fault (which cancels the
            // token) also tears down the await foreach, not just the in-flight partition reads.
            await foreach (var partition in partitions.WithCancellation(pumpCts.Token).ConfigureAwait(false))
            {
                await gate.WaitAsync(pumpCts.Token).ConfigureAwait(false);

                // Bound `tasks`' memory over a millions-of-partitions run by pruning
                // already-finished successful tasks right after each newly-admitted partition. Safe because a
                // successfully-completed task has nothing left to observe (no fault, no cancel) -- dropping it
                // before the final Task.WhenAll below changes nothing it would have surfaced. Faulted/cancelled
                // tasks are NOT IsCompletedSuccessfully, so they stay in the list and are still observed by
                // Task.WhenAll / DrainForFaultAsync below -- fault surfacing is unchanged. During teardown after
                // a fault, retained cancelled siblings are themselves bounded by the gate (~maxConcurrency), so
                // memory stays bounded there too.
                tasks.RemoveAll(t => t.IsCompletedSuccessfully);
                tasks.Add(PumpOneThenReleaseAsync(partition, writer, pumpCts, stall, batchOptions, gate));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
            writer.Complete();
        }
        catch (Exception ex)
        {
            // PumpPartitionAsync's catch is the only place that normally cancels
            // pumpCts -- but if the fault reaching HERE came from the enumerator itself (rather than a
            // partition's ReadAsync), or from this method's own await foreach/gate.WaitAsync unwinding via a
            // sibling's cancellation, pumpCts may not be cancelled yet. Cancel it FIRST, before draining, so
            // every already-launched sibling tears down immediately instead of running to completion while
            // DrainForFaultAsync (below) awaits them. Idempotent and safe if a partition fault already
            // cancelled it -- Cancel() on an already-cancelled CTS is a no-op.
            pumpCts.Cancel();
            writer.Complete(await DrainForFaultAsync(tasks, ex).ConfigureAwait(false));
        }
    }

    /// <summary>Awaits <see cref="PumpPartitionAsync"/> for one streaming partition, then releases its gate
    /// slot -- so the <c>await foreach</c> can pull the next partition -- whether the read succeeded, faulted,
    /// or was cancelled by a sibling's fault.</summary>
    private static async Task PumpOneThenReleaseAsync(
        IDatasetPartition partition, ChannelWriter<RecordBatch> writer, CancellationTokenSource pumpCts,
        StallAccumulator stall, BatchOptions batchOptions, SemaphoreSlim gate)
    {
        try
        {
            await PumpPartitionAsync(partition, writer, pumpCts, stall, batchOptions).ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>Waits out every launched streaming-partition task (so none outlives the pump) and returns the
    /// exception the channel should be completed with: the first GENUINE partition fault takes precedence over
    /// <paramref name="fallback"/>, which is only kept when no partition faulted for a real reason (e.g. the
    /// fallback is this pump's own teardown <see cref="OperationCanceledException"/>, or a fault raised by the
    /// enumerable itself). This mirrors how <see cref="PumpPartitionsAsync"/>'s <see cref="Task.WhenAll(Task[])"/>
    /// surfaces the first faulted partition task rather than a sibling's cancellation.</summary>
    private static async Task<Exception> DrainForFaultAsync(IReadOnlyList<Task> tasks, Exception fallback)
    {
        Exception? genuine = null;
        foreach (var task in tasks)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // A sibling cancellation triggered by the first real fault -- not separately surfaced,
                // exactly like SwallowAsync in the list path.
            }
            catch (Exception ex)
            {
                genuine ??= ex;
            }
        }

        return genuine ?? fallback;
    }

    private static async Task PumpPartitionAsync(
        IDatasetPartition partition, ChannelWriter<RecordBatch> writer, CancellationTokenSource pumpCts,
        StallAccumulator stall, BatchOptions batchOptions)
    {
        try
        {
            await foreach (var batch in partition.ReadAsync(batchOptions, pumpCts.Token).ConfigureAwait(false))
            {
                // Producer stall: the channel is bounded(4), so this blocks exactly when
                // it's full — i.e. when the ingest/consumer side isn't draining fast enough.
                var start = stall.Timestamp;
                try
                {
                    await writer.WriteAsync(batch, pumpCts.Token).ConfigureAwait(false);
                }
                finally
                {
                    stall.AddProducer(stall.Timestamp - start);
                }
            }
        }
        catch
        {
            // Fail fast for real: cancel every sibling the moment THIS partition faults (a genuine read
            // failure, or an OperationCanceledException from a sibling's own fault already cancelling us),
            // instead of waiting for Task.WhenAll above to observe it — that would be too late by
            // definition, since by then every sibling had already been free to keep running to completion.
            pumpCts.Cancel();
            throw;
        }
    }

    private static async Task SwallowAsync(Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch
        {
            // Suppressed by design: only the first partition failure (already captured by the caller)
            // is reported; sibling cancellations/faults triggered by that failure are not separately
            // surfaced.
        }
    }

    /// <summary>Pass-through wrapper that counts rows/bytes/batches and reports progress every 10
    /// batches — the ingest side never buffers, so progress must be derived from the same stream it
    /// consumes. Drives the channel reader through a manual <see cref="IAsyncEnumerator{T}"/> (rather
    /// than <c>await foreach</c> over <see cref="ChannelReader{T}.ReadAllAsync"/>) so the consumer-side
    /// stall — waiting on <c>MoveNextAsync</c> for the next batch, i.e. the channel is empty because the
    /// source is the bottleneck — can be timed around that one await.</summary>
    private static async IAsyncEnumerable<RecordBatch> ReportProgress(
        ChannelReader<RecordBatch> reader, DagNode node, RunContext ctx, StallAccumulator stall,
        [EnumeratorCancellation] CancellationToken ct)
    {
        var rowsSoFar = 0L;
        var bytesSoFar = 0L;
        var batchCount = 0L;
        var bytesAtLastReport = 0L;
        var batchesAtLastReport = 0L;

        var enumerator = reader.ReadAllAsync(ct).GetAsyncEnumerator(ct);
        try
        {
            while (true)
            {
                var start = stall.Timestamp;
                bool moved;
                try
                {
                    moved = await enumerator.MoveNextAsync().ConfigureAwait(false);
                }
                finally
                {
                    stall.AddConsumer(stall.Timestamp - start);
                }

                if (!moved)
                {
                    yield break;
                }

                var batch = enumerator.Current;
                rowsSoFar += batch.Length;
                bytesSoFar += batch.ApproximateSize();
                batchCount++;
                if (batchCount % 10 == 0)
                {
                    ctx.Events.SafeNodeProgress(node, rowsSoFar, bytesSoFar, batchCount);

                    // Delta since the last report, not the cumulative running total — Counter.Add IS
                    // the accumulation.
                    PzMeters.BytesMoved.Add(bytesSoFar - bytesAtLastReport);
                    PzMeters.Batches.Add(batchCount - batchesAtLastReport);
                    bytesAtLastReport = bytesSoFar;
                    batchesAtLastReport = batchCount;
                }

                yield return batch;
            }
        }
        finally
        {
            await enumerator.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Mirrors <c>DagCompiler</c>'s compile-time contract check for a <c>columns:</c>-declared
    /// cursor. That check only fires when a contract is declared; this runtime check is what catches an
    /// unsupported type for a dataset with no declared contract, whose actual type is only known once
    /// the staging table exists.</summary>
    private static readonly string[] AllowedCursorTypes = ["int", "bigint", "decimal", "date", "timestamp"];

    /// <summary>After the staging table for an incremental dataset exists (either tier — native CTAS or
    /// universal Arrow ingest), probes the cursor column's actual DuckDB type and, unless the extract was
    /// empty (NULL max — no candidate, previous watermark stands), computes the new candidate watermark
    /// value. A non-incremental dataset (no <c>Incremental</c> config) always returns (null, null) — a
    /// no-op. An unsupported discovered type returns a PzError-shaped clean failure (never an exception)
    /// naming the column, the discovered type, and the allowed set, mirroring the direct-return failure
    /// convention this executor already uses above for a missing connector.
    ///
    /// Windowed rules, layered on top of that behavior: <paramref
    /// name="windowUpper"/> is non-null only for a windowed dataset (the run's computed window upper
    /// bound) and <paramref name="caughtUp"/> reflects the window bounds computed BEFORE extraction —
    /// <c>upper &lt;= lower</c>, a zero-width window — NOT whether the extract itself happened to come
    /// back empty at the source (a connector can still land rows inside a caught-up window if it
    /// ignores/misapplies <c>DatasetSpec.WatermarkUpperBound</c>).
    /// A caught-up window unconditionally returns NO candidate,
    /// regardless of what actually landed — never advance, never regress. This is checked up front,
    /// before the MAX probe even runs, so it overrides every rule that follows. Empty slice on a
    /// windowed, non-caught-up dataset still advances the watermark to <paramref name="windowUpper"/>
    /// (the window was legitimately exhausted, not merely "nothing new yet" like the unwindowed case). A
    /// non-empty slice on a windowed, non-caught-up dataset caps the candidate at <paramref
    /// name="windowUpper"/> so an over-extracting connector can never advance the cursor past the window
    /// (the ABI promise on <c>DatasetSpec.WatermarkUpperBound</c>).
    ///
    /// <paramref name="declaredType"/> (non-null only for a windowed dataset) is the cursor type the
    /// window bounds above were computed with — <c>def.Dataset.Columns[cursor]</c>. If the type actually
    /// probed here (<c>typeName</c>) disagrees with it (e.g. declared
    /// <c>timestamp</c>, landed <c>DATE</c>), <see cref="WindowMath"/>'s Min/AddWindow on the next run
    /// would otherwise throw a raw <see cref="FormatException"/> — this is caught here instead and turned
    /// into the same PzError-shaped clean failure as the outright-unsupported-type case above.
    ///
    /// <paramref name="windowLower"/> (non-null exactly when <paramref
    /// name="windowUpper"/> is) is the effective lower bound the run's window was computed from --
    /// threaded through so the MAX probe below can be scoped with <c>cursor &gt; lower and cursor &lt;=
    /// upper</c> instead of taking MAX over the whole staging table. A connector's <c>WatermarkUpperBound</c>
    /// application is MAY, not MUST (<see cref="Pz.Connectors.Abstractions.ConnectorCapabilities.BoundedWindow"/>'s
    /// doc), and a <c>force_universal</c> tier switch can land the whole remaining backlog regardless --
    /// either way, rows below <paramref name="windowLower"/> can end up in the staging table alongside (or
    /// instead of) the intended slice. An UNSCOPED <c>MAX(cursor)</c> would then report the table's overall
    /// max even when every row that landed is actually stale (all below the stored watermark), and
    /// <see cref="WatermarkAdvancement"/>'s unconditional <c>Set</c> would regress the cursor. Scoping the
    /// probe to the window means an all-stale landing can never produce a candidate at all -- it falls
    /// through to the existing empty-slice rule below (advance to <paramref name="windowUpper"/>), and the
    /// candidate-capping <c>WindowMath.Min</c> further down becomes belt-and-braces rather than the only
    /// guard against over-extraction past the upper bound.</summary>
    /// <param name="upperInclusive">Whether <paramref name="windowUpper"/> is itself inside the window.
    /// Always true for a YAML max_window; a SQL-declared `c &lt; e` ceiling sets it false,
    /// which both narrows the scoped MAX below and forbids the empty-slice advance to the ceiling.</param>
    private static async Task<(PzError? Error, Watermark? Candidate)> CaptureWatermarkAsync(
        RunContext ctx, SourceDatasetDef def, string tableName, string? windowUpper, string? windowLower,
        bool upperInclusive, string? declaredType, bool caughtUp, CancellationToken ct)
    {
        if (def.Dataset.SyncMode?.Incremental is not { } incremental)
        {
            return (null, null);
        }

        var cursor = incremental.Cursor;
        string duckType;
        try
        {
            duckType = await ctx.Duck.ScalarAsync<string>(
                $"select column_type from (describe {tableName}) where column_name = '{cursor.Replace("'", "''")}'", ct)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Same sanitize-before-PzConnectorException convention this executor already applies to the
            // native CTAS statement above -- a DuckDB parser/binder error's "LINE <n>: ..." context block
            // must never echo the probe SQL verbatim into a NodeResult/log.
            var sanitized = NativeStatementRedactor.SanitizeEngineMessage(ex.Message);
            throw new PzConnectorException(
                $"watermark cursor probe failed: {sanitized}", isTransient: false, innerException: ex);
        }

        if (!TryMapDuckToCursorType(duckType, out var typeName))
        {
            var allowed = string.Join(", ", AllowedCursorTypes);
            var error = new PzError(PzErrorCode.UnsupportedCursorType,
                $"source '{def.Source.Name}.{def.Dataset.Name}': incremental cursor column '{cursor}' landed " +
                $"as DuckDB type '{duckType}', which is not a supported watermark cursor type -- allowed: {allowed}",
                def.Source.FilePath, null, null);
            return (error, null);
        }

        // For a windowed dataset, the bounds above (lowerWm/windowUpper) were computed with the DECLARED
        // column type -- if what actually landed probes to a different cursor type, WindowMath.Min's
        // candidate-capping call below would throw a raw FormatException in THIS SAME run (e.g. trying to
        // parse a DATE-shaped value with the timestamp format). Caught here as a clean, PZ-coded failure
        // instead of ever reaching that throw.
        if (declaredType is not null && !string.Equals(typeName, declaredType, StringComparison.Ordinal))
        {
            var error = new PzError(PzErrorCode.UnsupportedCursorType,
                $"source '{def.Source.Name}.{def.Dataset.Name}': incremental cursor column '{cursor}' is declared " +
                $"as cursor type '{declaredType}' but landed as DuckDB type '{duckType}' (cursor type '{typeName}') " +
                "-- the declared and landed cursor types must match",
                def.Source.FilePath, null, "align the columns: contract with the source's actual type");
            return (error, null);
        }

        // Caught up means NO candidate, unconditionally -- checked here, before
        // the MAX probe runs at all, so an over-extracting connector that lands rows anyway can never
        // produce a candidate that would regress the stored watermark on advancement.
        if (caughtUp)
        {
            return (null, null);
        }

        var q = ArrowInterop.QuoteQualified(cursor);
        var maxSql = typeName switch
        {
            "timestamp" => $"select strftime(max({q}), '%Y-%m-%dT%H:%M:%S.%f') from {tableName}",
            "date" => $"select strftime(max({q}), '%Y-%m-%d') from {tableName}",
            _ => $"select cast(max({q}) as varchar) from {tableName}", // int/bigint/decimal: plain digits
        };

        // Scope the MAX probe to the window's own bounds -- same quoting discipline as the
        // DESCRIBE probe above (single-quote-escape a value substituted into SQL text). windowLower/
        // windowUpper are already canonical strings in `typeName`'s format (verified equal to
        // declaredType by the mismatch guard above), so a plain quoted literal is enough; DuckDB
        // implicitly casts a VARCHAR literal to the column's actual type (date/timestamp/numeric) for
        // the comparison.
        if (windowUpper is not null)
        {
            var upperLiteral = windowUpper.Replace("'", "''");
            var upperOp = upperInclusive ? "<=" : "<";
            var lowerHalf = windowLower is null ? "" : $"{q} > '{windowLower.Replace("'", "''")}' and ";
            maxSql += $" where {lowerHalf}{q} {upperOp} '{upperLiteral}'";
        }

        string? value;
        try
        {
            value = await ctx.Duck.ScalarAsync<string?>(maxSql, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var sanitized = NativeStatementRedactor.SanitizeEngineMessage(ex.Message);
            throw new PzConnectorException(
                $"watermark max probe failed: {sanitized}", isTransient: false, innerException: ex);
        }

        // Empty slice -> no candidate for an unwindowed dataset: the previous watermark stands.
        // A windowed dataset instead advances to windowUpper -- the window was extracted in full and came
        // back empty, which is meaningfully different from "nothing new yet". (The
        // caught-up case never reaches here at all -- it already returned above.) DuckSession.ScalarAsync's
        // Convert.ChangeType(DBNull.Value, typeof(string)) yields "" rather than a true null reference for
        // a SQL NULL scalar (IConvertible.ToString() on DBNull.Value) -- IsNullOrEmpty covers both that
        // ADO.NET-ism and a genuine null uniformly, and no supported cursor type (int/bigint/decimal
        // digits, or the strftime-formatted date/timestamp forms above) ever legitimately produces "".
        // Only an INCLUSIVE ceiling may be advanced to. `c < e` never processes the rows at
        // exactly `e`, so advancing the watermark to `e` and resuming at `c > e` would skip them silently.
        // An exclusive ceiling that comes back empty therefore leaves the watermark where it was — the run
        // repeats the same empty window next time, which is visible and recoverable, unlike lost rows.
        if (string.IsNullOrEmpty(value))
        {
            return windowUpper is not null && upperInclusive
                ? (null, new Watermark(cursor, typeName, windowUpper, ctx.Paths.RunId))
                : (null, null);
        }

        // Non-empty slice on a windowed dataset: cap the candidate at windowUpper so an over-extracting
        // connector (one that ignores/misapplies DatasetSpec.WatermarkUpperBound) can never advance the
        // cursor past the window the engine computed.
        var candidateValue = windowUpper is not null ? WindowMath.Min(typeName, value, windowUpper) : value;
        return (null, new Watermark(cursor, typeName, candidateValue, ctx.Paths.RunId));
    }

    /// <summary>DuckDB type name -> v0 cursor type name. Any <c>DECIMAL(...)</c>
    /// maps to "decimal" regardless of precision/scale; any <c>TIMESTAMP*</c> variant maps to "timestamp".</summary>
    private static bool TryMapDuckToCursorType(string duckType, out string typeName)
    {
        if (string.Equals(duckType, "INTEGER", StringComparison.Ordinal))
        {
            typeName = "int";
            return true;
        }

        if (string.Equals(duckType, "BIGINT", StringComparison.Ordinal))
        {
            typeName = "bigint";
            return true;
        }

        if (duckType.StartsWith("DECIMAL", StringComparison.Ordinal))
        {
            typeName = "decimal";
            return true;
        }

        if (string.Equals(duckType, "DATE", StringComparison.Ordinal))
        {
            typeName = "date";
            return true;
        }

        if (duckType.StartsWith("TIMESTAMP", StringComparison.Ordinal))
        {
            typeName = "timestamp";
            return true;
        }

        typeName = "";
        return false;
    }
}
