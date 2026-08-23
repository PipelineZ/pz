using System.Globalization;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Paths;
using Pz.Core.Dag;
using Pz.Core.Model;
using Pz.Core.Validation;
using Pz.Engine.Execution;

namespace Pz.Engine.Planning;

/// <summary>Stamps every node with an <see cref="EdgeStrategy"/> by probing connectors'
/// TryGetNativeScan/TryGetNativeCopy (ABI contract: cheap and offline). Opens each connector once
/// per node and disposes it before returning; the probe never touches the network or DuckDB.</summary>
public sealed class ExecutionPlanner(ConnectorRegistry connectors)
{
    // Mirrors PostgresSource's MinPartitions/MaxPartitions bounds so a config that will actually fail
    // at run time (partitions outside
    // [1,16]) never shows a 0 or 17 in plan.json -- this is a display-clamp only; the executor's real
    // PlanReadAsync call is still what enforces the bound and throws PzConnectorException at run time.
    private const int MinDeclaredPartitions = 1;
    private const int MaxDeclaredPartitions = 16;

    public async Task<ExecutionPlan> PlanAsync(
        CompiledDag dag, bool forceUniversal, CancellationToken ct, EngineConfig? engineConfig = null)
    {
        var planned = new List<PlannedNode>(dag.Nodes.Count);
        var errors = new List<PzError>();

        // rate_limit is INSTANCE-level (ConnectionDef/ConnectionDef), but planning runs
        // per dataset/output node -- one HashSet per PlanAsync invocation, threaded into both loop
        // arms below, de-dupes the PZ0317 gate to one error per instance regardless of how many
        // dataset/output nodes share it.
        var pacingRefused = new HashSet<string>();

        // CheckpointableReads without StablePartitionIds is a connector defect
        // — checkpoints are keyed by stable partition identity. Static capability read, aggregated,
        // de-duped per instance like the PZ0317 gates above.
        var identityRefused = new HashSet<string>();

        // Mode-vs-capability refusal, deduped per (sink instance,
        // mode) -- one error per instance+mode however many outputs share it.
        var modeRefused = new HashSet<string>();

        // ChangeCapture capability refusal (PZ0338), deduped per
        // source instance -- mirrors pacingRefused above so several cdc datasets sharing one
        // incapable connector collapse to one error, not one per dataset.
        var changeCaptureRefused = new HashSet<string>();

        // Every SourceLoad node's resolved read shape,
        // populated by PlanSourceLoadAsync -- feeds the delivery-guarantee pass below (this planner's
        // half of PZ0214, split from DagCompiler's compile-time half).
        var shapeByNode = new Dictionary<NodeId, ResolvedReadShape>();

        foreach (var node in dag.TopologicalOrder())
        {
            planned.Add(node.Kind switch
            {
                NodeKind.SourceLoad => WithPushdown(
                    await PlanSourceLoadAsync(node, forceUniversal, errors, pacingRefused, identityRefused, changeCaptureRefused, shapeByNode, ct).ConfigureAwait(false),
                    node),
                NodeKind.SinkWrite => await PlanSinkWriteAsync(node, forceUniversal, errors, pacingRefused, modeRefused, ct).ConfigureAwait(false),
                NodeKind.Pipeline => new PlannedNode(node.Id, node.Kind, node.Name, EdgeStrategy.DuckSql, 1,
                    "duckdb sql: executes in-engine"),
                NodeKind.Check => new PlannedNode(node.Id, node.Kind, node.Name, EdgeStrategy.DuckSql, 1,
                    "check: not executed in this version"),
                _ => throw new ArgumentOutOfRangeException(nameof(node), node.Kind, "unknown node kind"),
            });
        }

        // Feed-side delivery-guarantee pass -- the planner's half of the (read x write) matrix, the
        // counterpart of DagCompiler's incremental-side, compile-time half.
        // Only reachable for a dataset DagCompiler could not classify: an implicit OR explicit `mode: auto`
        // dataset (SyncMode null, or SyncMode.Auto) that THIS connector resolves to Feed. DagCompiler
        // refuses only the unambiguous Incremental-mode cells (it has no connector-capability access, so an
        // `auto` block compiles there and the planner resolves its real shape here); an `auto` dataset that
        // resolves to Full needs no consent and never reaches these adds. Feed x append without consent is
        // PZ0214 (same code/message as the compile-time half); feed x replace is PZ0335 (a feed read is not guaranteed a
        // complete snapshot). One delivery-guarantee contract, split only by which phase can see which case.
        var byId = dag.Nodes.ToDictionary(n => n.Id);
        foreach (var sinkNode in dag.Nodes)
        {
            if (sinkNode.Definition is not SinkOutputDef sinkDef)
            {
                continue;
            }

            var checkAppendConsent = sinkDef.Output.Mode == "append" && !sinkDef.Output.AcceptDuplicates;
            var checkReplace = sinkDef.Output.Mode == "replace";
            if (!checkAppendConsent && !checkReplace)
            {
                continue;
            }

            var seen = new HashSet<NodeId>();
            var queue = new Queue<NodeId>(sinkNode.DependsOn);
            while (queue.Count > 0)
            {
                var id = queue.Dequeue();
                if (!seen.Add(id) || !byId.TryGetValue(id, out var ancestor))
                {
                    continue;
                }

                if (ancestor.Definition is SourceDatasetDef srcDef &&
                    shapeByNode.TryGetValue(id, out var shape) && shape == ResolvedReadShape.Feed)
                {
                    if (checkReplace)
                    {
                        errors.Add(new PzError(PzErrorCode.IncompatiblePair,
                            $"output '{sinkDef.Sink.Name}.{sinkDef.Output.Name}': write.strategy 'replace' fed by feed dataset " +
                            $"'{srcDef.Source.Name}.{srcDef.Dataset.Name}' -- a connector-managed feed's read is not " +
                            "guaranteed to be a complete snapshot, so replace would discard previously delivered rows",
                            sinkDef.Sink.FilePath, null,
                            "use write.strategy: merge, or append with duplicates: accept"));
                    }
                    else
                    {
                        errors.Add(new PzError(PzErrorCode.IncrementalAppendUnacknowledged,
                            $"sink '{sinkDef.Sink.Name}.{sinkDef.Output.Name}' has write.strategy: append and is fed by " +
                            $"incremental/sync dataset '{srcDef.Source.Name}.{srcDef.Dataset.Name}' -- delivery is " +
                            "at-least-once, so a retried or replayed slice can duplicate rows",
                            sinkDef.Sink.FilePath, null,
                            "use write.strategy: merge (with keys:) or write.strategy: replace, or set\n" +
                            "write:\n  strategy: append\n  duplicates: accept\n" +
                            "on the output to accept at-least-once delivery"));
                    }
                }

                foreach (var dep in ancestor.DependsOn)
                {
                    queue.Enqueue(dep);
                }
            }
        }

        if (errors.Count > 0)
        {
            throw new PzValidationException(errors);
        }

        var budget = MemoryBudget.Compute(engineConfig ?? new EngineConfig());
        return new ExecutionPlan(planned, budget);
    }

    /// <summary>Stamps what this SourceLoad will actually ask its
    /// connector for, so `pz plan` can tell a pushed read from an unpushed one. Reuses the executor's own
    /// capability gate rather than restating it, so the two can never drift. Only the universal tier is
    /// reported: the native path builds its own SQL and never calls PlanReadAsync, so claiming pushdown
    /// there would be a lie. Static Capabilities read — no OpenAsync, same never-connects promise as
    /// DeclaredPartitionCount.</summary>
    private PlannedNode WithPushdown(PlannedNode planned, DagNode node)
    {
        if (planned.Strategy != EdgeStrategy.ArrowStream
            || node.Definition is not SourceDatasetDef { Hints: not null } def
            || !connectors.TryGetSource(def.Source.Connector, out var connector))
        {
            return planned;
        }

        var hints = SourceLoadExecutor.HintsFor(def, connector.Capabilities);
        return hints is { Columns: null, PredicateSql: null }
            ? planned
            : planned with { Pushdown = new PushdownInfo(hints.Columns?.Count, hints.PredicateSql is not null) };
    }

    private async Task<PlannedNode> PlanSourceLoadAsync(DagNode node, bool forceUniversal, List<PzError> errors,
        ISet<string> pacingRefused, ISet<string> identityRefused, ISet<string> changeCaptureRefused,
        IDictionary<NodeId, ResolvedReadShape> shapeByNode, CancellationToken ct)
    {
        var def = (SourceDatasetDef)node.Definition;

        // Capabilities are a property of the connector registration itself (no OpenAsync/network
        // involved) — reading them to compute the declared partition count keeps the planner's "never
        // connects" promise for this static number; the executor alone calls the real PlanReadAsync.
        var connectorFound = connectors.TryGetSource(def.Source.Connector, out var connector);
        var declaredPartitions = connectorFound ? DeclaredPartitionCount(connector!.Capabilities, def.Dataset) : 1;

        // Bounded-window capability gate. Static read (no OpenAsync), same never-connects promise as
        // DeclaredPartitionCount. An uninstalled connector stays a run-time PZ0305, not a planning error.
        // A bounded window may be declared in YAML (max_window) or in pipeline SQL (a watermark()
        // ceiling). Both constrain watermark ADVANCEMENT, not merely extraction volume, so both
        // refuse an incapable connector rather than degrading: rows past the window landing in staging would
        // advance MAX(cursor) past rows the pipeline never processed.
        var incrementalDef = def.Dataset.SyncMode?.Incremental;
        var yamlWindow = incrementalDef?.MaxWindow is not null;
        var sqlCeiling = incrementalDef?.SqlBounds?.Any(b => b.IsUpper) == true;
        if ((yamlWindow || sqlCeiling) && connectorFound &&
            !connector!.Capabilities.HasFlag(ConnectorCapabilities.BoundedWindow))
        {
            var what = yamlWindow ? "declares max_window" : "declares a watermark() ceiling in pipeline SQL";
            var fix = yamlWindow ? "drop max_window" : "drop the ceiling from the pipeline's WHERE";
            errors.Add(new PzError(PzErrorCode.WindowCapabilityMissing,
                $"source '{def.Source.Name}' dataset '{def.Dataset.Name}' {what}, but connector " +
                $"'{def.Source.Connector}' does not support bounded windows",
                def.Source.FilePath, null,
                $"use a BoundedWindow-capable connector for this dataset, or {fix}"));
        }

        // ChangeCapture capability gate, same never-connects
        // shape as the BoundedWindow gate just above. Deduped per source instance (changeCaptureRefused)
        // so several cdc datasets sharing one incapable connector collapse to one error.
        if (def.Dataset.SyncMode?.Mode == SyncMode.Cdc && connectorFound &&
            !connector!.Capabilities.HasFlag(ConnectorCapabilities.ChangeCapture) &&
            changeCaptureRefused.Add("source:" + def.Source.Name))
        {
            errors.Add(new PzError(PzErrorCode.ChangeCaptureUnsupported,
                $"source '{def.Source.Name}' dataset '{def.Dataset.Name}' declares sync mode cdc, but connector " +
                $"'{def.Source.Connector}' does not support change capture",
                def.Source.FilePath, null,
                "use a ChangeCapture-capable connector (postgres, sqlserver) or a different sync mode"));
        }

        // Pacing capability gate. Static read (no OpenAsync). Instance-level
        // config, per-dataset planning pass -- pacingRefused de-dupes to one error per instance.
        if (def.Source.RateLimit is not null && connectorFound &&
            !connector!.Capabilities.HasFlag(ConnectorCapabilities.GatedOperations) &&
            pacingRefused.Add("source:" + def.Source.Name))
        {
            errors.Add(new PzError(PzErrorCode.PacingUnsupported,
                $"source '{def.Source.Name}': rate_limit is configured but connector " +
                $"'{def.Source.Connector}' does not support gated operations (pacing would be silently ignored)",
                def.Source.FilePath, null,
                "remove rate_limit, or use a connector that declares the GatedOperations capability"));
        }

        // GatedOperations is a connector-wide capability flag,
        // but connectors can adopt it sink-only (e.g. azureblob: AzureConnector declares
        // GatedOperations for its sink's open/copy/delete operations, yet AzureSource is
        // INativeOnlySource -- its read path never opens as IOperationGateAware). The check above
        // alone therefore misses exactly this case: rate_limit on such a source passes the
        // HasFlag(GatedOperations) gate yet is statically inert -- the silent-degrade PZ0317 exists
        // to prevent. INativeOnlySource is the same static, no-OpenAsync-needed marker the
        // files_per_partition/force_universal gates above already use for "this connector's read
        // path has no universal tier"; reuse it here rather than opening the source to probe
        // IOperationGateAware.
        if (def.Source.RateLimit is not null && connectorFound &&
            connector is INativeOnlySource &&
            pacingRefused.Add("source:" + def.Source.Name))
        {
            errors.Add(new PzError(PzErrorCode.PacingUnsupported,
                $"source '{def.Source.Name}': rate_limit is configured but connector " +
                $"'{def.Source.Connector}' reads natively (its source path performs no gateable " +
                "operations); remove rate_limit from the source",
                def.Source.FilePath, null,
                "remove rate_limit from the source (native reads are never paced)"));
        }

        // CheckpointableReads without StablePartitionIds is a connector defect
        // — checkpoints are keyed by stable partition identity. Static capability read, aggregated,
        // de-duped per instance like the PZ0317 gates above.
        if (connectorFound &&
            connector!.Capabilities.HasFlag(ConnectorCapabilities.CheckpointableReads) &&
            !connector.Capabilities.HasFlag(ConnectorCapabilities.StablePartitionIds) &&
            identityRefused.Add("source:" + def.Source.Name))
        {
            errors.Add(new PzError(PzErrorCode.PartitionIdentityInvalid,
                $"source '{def.Source.Name}': connector '{def.Source.Connector}' declares " +
                "CheckpointableReads without StablePartitionIds (checkpoints require stable partition identity)",
                def.Source.FilePath, null,
                "fix the connector to declare both capabilities, or neither"));
        }

        // Resolve the dataset's read shape up front -- every
        // remaining gate/return below (PZ0316, the force_universal/native-scan returns) needs it, and
        // resolution requires the OPENED ISource (INaturalReadShapeSource is additive on ISource, not the
        // unopened connector), so the connector opens here rather than only on the native-scan-probe
        // path further down. OpenAsync is the same "cheap and offline" call TryGetNativeScan
        // relies on below (class doc above), so opening it this early adds no network behavior --
        // including under force_universal.
        await using var source = connectorFound
            ? await connector!.OpenAsync(new ConnectorConfig(def.Source.Connection), ct).ConfigureAwait(false)
            : null;
        var spec = SpecBuilder.ForSourceLoad(def);
        ResolvedReadShape? shape = source is null ? null : ReadShapeResolver.Resolve(def.Dataset, source, spec);
        if (shape is { } resolvedShape)
        {
            shapeByNode[node.Id] = resolvedShape;
        }

        var readToken = shape switch
        {
            ResolvedReadShape.Incremental => $"read=incremental cursor={def.Dataset.SyncMode!.Incremental!.Cursor}",
            ResolvedReadShape.Feed => "read=feed",
            ResolvedReadShape.Cdc => "read=cdc",
            ResolvedReadShape.Full => "read=full",
            null => null,
            _ => throw new ArgumentOutOfRangeException(nameof(shape), shape, "unknown resolved read shape"),
        };

        // Incremental-declared-but-feed-natural conflict. A
        // dataset declaring cursor-incremental (YAML `sync: {mode: incremental}` or a SQL `watermark()`,
        // both resolving to Incremental here) on a connector that manages its OWN change feed for this
        // dataset (INaturalReadShapeSource resolving Feed, e.g. http with a `delta_pointer` option) pits two
        // resume mechanisms against each other -- the ordered cursor AND the connector's opaque token both
        // claim to resume the read. Only the planner holds the opened source to see this (DagCompiler and
        // WatermarkInference refuse the YAML-block-vs-YAML-block form, but not block-vs-connector-config).
        if (shape == ResolvedReadShape.Incremental && source is INaturalReadShapeSource nat &&
            nat.GetNaturalReadShape(spec) == NaturalReadShape.Feed)
        {
            errors.Add(new PzError(PzErrorCode.SyncStateConflict,
                $"source '{def.Source.Name}' dataset '{def.Dataset.Name}' declares cursor-incremental " +
                "(`sync: {mode: incremental}` or a SQL watermark()), but connector " +
                $"'{def.Source.Connector}' manages its own change feed for this dataset -- the two resume " +
                "mechanisms conflict",
                def.Source.FilePath, null,
                "remove the incremental declaration (the feed resumes itself), or remove the connector's " +
                "feed config (e.g. http `delta_pointer`)"));
        }

        // Sync-state capability gate (mirrors the BoundedWindow check above): a single opaque sync token
        // cannot reconcile across independent partition reads, so a Feed-shaped dataset on a
        // PartitionedRead connector is refused here, keyed on the RESOLVED shape. Also refuses StreamingPartitions: a connector
        // declaring StreamingPartitions | SyncState streams N partitions with no materialized list, so the
        // runtime guard in SourceLoadExecutor (which inspects the materialized partition list) is null on
        // that path and would never catch a many-partition feed dataset -- this plan-time gate is the only
        // refusal for that combination.
        if (shape == ResolvedReadShape.Feed && connectorFound &&
            (connector!.Capabilities.HasFlag(ConnectorCapabilities.PartitionedRead) ||
             connector.Capabilities.HasFlag(ConnectorCapabilities.StreamingPartitions)))
        {
            errors.Add(new PzError(PzErrorCode.SyncPartitionedReadConflict,
                $"source '{def.Source.Name}' dataset '{def.Dataset.Name}' is sync-state (opaque token), but " +
                $"connector '{def.Source.Connector}' declares partitioned or streaming-partitioned reads -- " +
                "one opaque token cannot span multiple partitions",
                def.Source.FilePath, null,
                "use a single-partition connector for a sync dataset, or an ordered-cursor " +
                "`sync: {mode: incremental}` dataset for partitioned/streaming reads"));
        }

        // Path-templating capability gate (mirrors the BoundedWindow check just
        // above): DagCompiler's PZ0217/0218/0221 already validated the templating *syntax*
        // connector-agnostically, but only the planner has the connector instance + its declared
        // Capabilities. A connector that ignores the tokens would silently write/read a literal
        // "{yyyy}" folder instead of routing per the calendar -- refuse it here instead.
        if (def.Dataset.Options.TryGetValue("path", out var pathOpt) && pathOpt?.ToString() is { } pathValue &&
            PathTemplate.HasDateTokens(pathValue) && connectorFound &&
            !connector!.Capabilities.HasFlag(ConnectorCapabilities.PathTemplating))
        {
            errors.Add(new PzError(PzErrorCode.TemplatingCapabilityMissing,
                $"source '{def.Source.Name}' dataset '{def.Dataset.Name}' has a date-templated path, but " +
                $"connector '{def.Source.Connector}' does not support path templating",
                def.Source.FilePath, null,
                "use a PathTemplating-capable connector, or remove the date tokens / partition_by"));
        }

        // files_per_partition only has meaning on a
        // universal partitioned read. On a native-only source the dataset plans onto the
        // native tier (where DuckDB gets the file list in one scan and needs no coalescing), so the
        // option would be silently ignored — refuse it loudly instead. Same planner-reads-options
        // precedent as the PathTemplating gate above; static marker check, never connects.
        if (def.Dataset.Options.ContainsKey("files_per_partition") && connectorFound &&
            connector is INativeOnlySource)
        {
            errors.Add(new PzError(PzErrorCode.NativePathRequired,
                $"source '{def.Source.Name}' dataset '{def.Dataset.Name}' sets files_per_partition, but " +
                $"connector '{def.Source.Connector}' supports only the native path",
                def.Source.FilePath, null,
                "remove files_per_partition (the native path hands DuckDB the file list in one scan and needs no coalescing)"));
        }

        if (forceUniversal)
        {
            // Mirrors the sink-side force_universal x INativeOnlySink collision check.
            if (connectorFound && connector is INativeOnlySource)
            {
                errors.Add(new PzError(PzErrorCode.NativePathRequired,
                    $"source '{def.Source.Name}' dataset '{def.Dataset.Name}' uses connector " +
                    $"'{def.Source.Connector}', which supports only the native path, but engine.force_universal is set",
                    def.Source.FilePath, null,
                    "remove engine.force_universal or use a connector with a universal read path"));
            }

            return Universal(node, "arrow stream: engine.force_universal = true", declaredPartitions, readToken);
        }

        // An uninstalled connector is NOT a planning error — the executor already produces PZ0305
        // with run semantics (failed node, skipped descendants); the plan just reports universal. No
        // opened source (and so no resolved shape/readToken) exists on this path.
        if (!connectorFound)
        {
            return Universal(node, $"arrow stream: connector '{def.Source.Connector}' has no native path");
        }

        NativeScan? scan;
        try
        {
            if (!source!.TryGetNativeScan(spec, out scan))
            {
                return Universal(node, $"arrow stream: connector '{def.Source.Connector}' has no native path", declaredPartitions, readToken);
            }
        }
        catch (PzConnectorException ex)
        {
            // A connector may refuse a native scan because the dataset config is inconsistent with the file
            // it would read (e.g. CsvSource's C1 positional-binding guard). That is a config error, not an
            // engine bug -- surface it aggregated and exit-2-coded rather than letting it escape as PZ0500.
            errors.Add(new PzError(PzErrorCode.NativeScanContractMismatch, ex.Message, def.Source.FilePath, null, null));
            return Universal(node, $"arrow stream: connector '{def.Source.Connector}' native path refused", declaredPartitions, readToken);
        }

        var nativePath = def.Dataset.Options.TryGetValue("path", out var p) ? p?.ToString() ?? def.Dataset.Name : def.Dataset.Name;
        return new PlannedNode(node.Id, node.Kind, node.Name, EdgeStrategy.NativeScan, 1,
            $"native scan: connector '{def.Source.Connector}' provides {scan!.Mechanism} over {nativePath} ({readToken})");
    }

    // Static (no connection) mirror of the postgres-style contract: a PartitionedRead-capable connector
    // only actually partitions when the dataset names a partition_column; "partitions" alone (without a
    // partition_column) or the capability alone is not enough — matches PostgresSource.PlanReadAsync's
    // own gate. The real, possibly-smaller-after-degenerate-collapse count comes from the executor's
    // actual PlanReadAsync call; this is a best-effort planning-time hint only.
    private static int DeclaredPartitionCount(ConnectorCapabilities capabilities, DatasetDef dataset)
    {
        if (!capabilities.HasFlag(ConnectorCapabilities.PartitionedRead))
        {
            return 1;
        }

        if (!dataset.Options.TryGetValue("partition_column", out var pc) || pc is null)
        {
            return 1;
        }

        if (!dataset.Options.TryGetValue("partitions", out var raw) || raw is null)
        {
            return 1;
        }

        var n = Convert.ToInt32(raw, CultureInfo.InvariantCulture);
        return Math.Clamp(n, MinDeclaredPartitions, MaxDeclaredPartitions);
    }

    private async Task<PlannedNode> PlanSinkWriteAsync(DagNode node, bool forceUniversal, List<PzError> errors,
        ISet<string> pacingRefused, ISet<string> modeRefused, CancellationToken ct)
    {
        var def = (SinkOutputDef)node.Definition;
        if (!connectors.TryGetSink(def.Sink.Connector, out var connector))
        {
            return Universal(node, $"arrow stream: connector '{def.Sink.Connector}' has no native path");
        }

        // Partitioned-write capability gate: the sink-side counterpart of the source-side check in
        // PlanSourceLoadAsync above. Static Capabilities read (no OpenAsync, same never-connects
        // promise). WHICH capability is required follows from `path:`, because that is what says who
        // owns the layout: calendar tokens mean pz's own rule (one timestamp column rendered into a
        // folder -- PathTemplating), no tokens mean the destination records its own partitioning
        // (ColumnPartitionedWrites). A connector declaring neither would silently write every row into
        // one unpartitioned place, or into a literal "{yyyy}" folder.
        var partitionColumns = PartitionColumns.Read(def.Output.Options);
        if (partitionColumns.Count > 0)
        {
            var path = def.Output.Options.TryGetValue("path", out var pathOpt) ? pathOpt?.ToString() : null;
            var rendersFromPath = path is not null && Pz.Connectors.Abstractions.Paths.PathTemplate.HasDateTokens(path);
            var required = rendersFromPath
                ? ConnectorCapabilities.PathTemplating
                : ConnectorCapabilities.ColumnPartitionedWrites;

            if (!connector.Capabilities.HasFlag(required))
            {
                errors.Add(new PzError(PzErrorCode.TemplatingCapabilityMissing,
                    $"output '{def.Output.Name}' on sink '{def.Sink.Name}' declares partition_by, but connector " +
                    $"'{def.Sink.Connector}' does not declare {required}",
                    def.Sink.FilePath, null,
                    rendersFromPath
                        ? "use a PathTemplating-capable connector, or remove the date tokens / partition_by"
                        : "use a connector that partitions by column value, or remove partition_by"));
            }
        }

        // Pacing capability gate, sink-side mirror of the source check in
        // PlanSourceLoadAsync above. Static Capabilities read (no OpenAsync); connector is already
        // known non-null here (the TryGetSink guard above returned early otherwise). Instance-level
        // config, per-output planning pass -- pacingRefused de-dupes to one error per instance.
        if (def.Sink.RateLimit is not null &&
            !connector.Capabilities.HasFlag(ConnectorCapabilities.GatedOperations) &&
            pacingRefused.Add("sink:" + def.Sink.Name))
        {
            errors.Add(new PzError(PzErrorCode.PacingUnsupported,
                $"sink '{def.Sink.Name}': rate_limit is configured but connector " +
                $"'{def.Sink.Connector}' does not support gated operations (pacing would be silently ignored)",
                def.Sink.FilePath, null,
                "remove rate_limit, or use a connector that declares the GatedOperations capability"));
        }

        // Write-mode capability gate: static Capabilities read (no OpenAsync). Append is the
        // universal floor -- every sink must accept it, so only merge/replace are gated.
        if (def.Output.Mode == "merge" &&
            !connector.Capabilities.HasFlag(ConnectorCapabilities.Merge) &&
            modeRefused.Add($"sink:{def.Sink.Name}:merge"))
        {
            errors.Add(new PzError(PzErrorCode.WriteModeUnsupported,
                $"output '{def.Output.Name}' on sink '{def.Sink.Name}' has write.strategy: merge, but connector " +
                $"'{def.Sink.Connector}' does not declare the Merge capability",
                def.Sink.FilePath, null,
                "use a merge-capable connector, or change the output's mode:"));
        }

        if (def.Output.Mode == "replace" &&
            !connector.Capabilities.HasFlag(ConnectorCapabilities.ReplaceWrites) &&
            modeRefused.Add($"sink:{def.Sink.Name}:replace"))
        {
            errors.Add(new PzError(PzErrorCode.WriteModeUnsupported,
                $"output '{def.Output.Name}' on sink '{def.Sink.Name}' has write.strategy: replace, but connector " +
                $"'{def.Sink.Connector}' does not declare the ReplaceWrites capability",
                def.Sink.FilePath, null,
                "use a replace-capable connector, or change the output's mode:"));
        }

        // ApplyDeletes capability gate (PZ0339), sink-side
        // counterpart of PlanSourceLoadAsync's ChangeCapture gate. Static Capabilities read (no
        // OpenAsync). `on_delete: ignore` needs no capability. Deduped per (sink instance, "on_delete")
        // via the same modeRefused set the merge/replace gates above use.
        if (def.Output.OnDelete is "delete" or "soft" &&
            !connector.Capabilities.HasFlag(ConnectorCapabilities.ApplyDeletes) &&
            modeRefused.Add("sink:" + def.Sink.Name + ":on_delete"))
        {
            errors.Add(new PzError(PzErrorCode.DeleteApplyUnsupported,
                $"sink '{def.Sink.Name}' output '{def.Output.Name}' declares on_delete: {def.Output.OnDelete}, but " +
                $"connector '{def.Sink.Connector}' cannot apply deletes",
                def.Sink.FilePath, null,
                "use on_delete: ignore, or an ApplyDeletes-capable sink (postgres, sqlserver)"));
        }

        await using var sink = await connector.OpenAsync(new ConnectorConfig(def.Sink.Connection), ct).ConfigureAwait(false);
        var spec = SpecBuilder.ForSinkOutput(def);
        if (forceUniversal)
        {
            if (connector is INativeOnlySink)
            {
                errors.Add(new PzError(PzErrorCode.NativePathRequired,
                    $"sink '{def.Sink.Name}' uses connector '{def.Sink.Connector}', which supports only the native path, " +
                    "but engine.force_universal is set", def.Sink.FilePath, null,
                    "remove engine.force_universal or use a connector with a universal write path"));
            }

            return Universal(node, "arrow stream: engine.force_universal = true");
        }

        if (sink.TryGetNativeCopy(spec, out var copy))
        {
            return new PlannedNode(node.Id, node.Kind, node.Name, EdgeStrategy.NativeCopy, 1,
                $"native copy: connector '{def.Sink.Connector}' provides {copy.Mechanism}");
        }

        return Universal(node, $"arrow stream: connector '{def.Sink.Connector}' has no native path");
    }

    private static PlannedNode Universal(DagNode node, string reason, int partitions = 1, string? readToken = null)
    {
        var withPartitions = partitions > 1 ? $"{reason} ({partitions} partitions)" : reason;
        var fullReason = readToken is null ? withPartitions : $"{withPartitions} ({readToken})";
        return new PlannedNode(node.Id, node.Kind, node.Name, EdgeStrategy.ArrowStream, partitions, fullReason);
    }
}
