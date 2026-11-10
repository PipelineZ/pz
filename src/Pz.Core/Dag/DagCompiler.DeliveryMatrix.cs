using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Dag;

/// <summary>The (read x write) pairing matrices over the finished node list: incremental and cdc datasets
/// against the write strategy of every sink they reach.</summary>
public static partial class DagCompiler
{
    /// <summary>Throws every pairing violation found; stamps <see cref="SinkOutputDef.CdcDeleteOrigin"/>
    /// onto the cdc-fed outputs whose delete keys can be routed.</summary>
    private static void CheckDeliveryMatrices(List<DagNode> nodes)
    {
        // Incremental matrix: the (read x write) pairing for an EXPLICITLY declared incremental
        //      dataset -- append without consent (PZ0214) and replace (PzErrorCode.IncompatiblePair)
        //      both discard/duplicate rows an incremental read relies on staying put across runs. Runs
        //      on the REAL DAG (never the selected subset) because the risk is a property of the
        //      project, not of one run -- so it must see every node/edge, not a --select-filtered slice.
        //      Violations aggregate like every other stage.
        //      Deliberately `Incremental` only, not `Incremental or Auto` -- Pz.Core has no
        //      connector-capability access, so an explicitly declared `sync: {mode: auto}` block might
        //      resolve to a full read (needing no consent at all) as easily as a feed. That ambiguity is
        //      ExecutionPlanner's job alone (its feed-side PZ0214 guard, which DOES hold the opened
        //      connector and the ReadShapeResolver's actual resolved shape); Core only ever refuses the
        //      unambiguous case, an explicit `sync: {mode: incremental}` block.
        var deliveryErrors = new List<PzError>();
        var deliveryById = nodes.ToDictionary(n => n.Id);
        foreach (var sinkNode in nodes)
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
                if (!seen.Add(id) || !deliveryById.TryGetValue(id, out var ancestor))
                {
                    continue;
                }

                if (ancestor.Definition is SourceDatasetDef src &&
                    src.Dataset.SyncMode is { Mode: SyncMode.Incremental })
                {
                    if (checkReplace)
                    {
                        deliveryErrors.Add(new PzError(PzErrorCode.IncompatiblePair,
                            $"output '{sinkDef.Sink.Name}.{sinkDef.Output.Name}': write.strategy 'replace' fed by incremental dataset " +
                            $"'{src.Source.Name}.{src.Dataset.Name}' would discard previously loaded rows each run.",
                            sinkDef.Sink.FilePath, null,
                            "use write.strategy: merge (effectively-once), or remove the dataset's sync block for a full re-read"));
                    }
                    else
                    {
                        deliveryErrors.Add(new PzError(PzErrorCode.IncrementalAppendUnacknowledged,
                            $"sink '{sinkDef.Sink.Name}.{sinkDef.Output.Name}' has write.strategy: append and is fed by " +
                            $"incremental dataset '{src.Source.Name}.{src.Dataset.Name}' -- delivery is " +
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

        // CDC matrix: the cdc column of the same published (read x write) matrix as the incremental
        //       pass above. cdc is always EXPLICITLY declared
        //       (never auto-resolved), so -- unlike the feed column, which needs ExecutionPlanner's
        //       held-open connector -- this validates entirely at compile time. An output is
        //       "cdc-fed" iff its SinkWrite node's transitive DependsOn closure (UpstreamSourceLoads
        //       below) contains >=1 SourceLoad whose dataset declares `sync: {mode: cdc}`; delete-key
        //       routing is valid iff that closure contains EXACTLY one SourceLoad in total and it is
        //       the cdc one -- a join/multi-source pipeline cannot map delete keys to a single
        //       upstream row identity. `CdcDeleteOrigin` is stamped only in that valid case, via
        //       `with` on the already NodeId-computed node (see DagNode.cs) so it never feeds a hash.
        var cdcErrors = new List<PzError>();
        for (var i = 0; i < nodes.Count; i++)
        {
            if (nodes[i].Definition is not SinkOutputDef sinkDef)
            {
                continue;
            }

            var upstream = UpstreamSourceLoads(nodes[i], deliveryById);
            var cdcSources = upstream.Where(s => s.Dataset.SyncMode?.Mode == SyncMode.Cdc)
                .OrderBy(s => s.Source.Name, StringComparer.Ordinal).ThenBy(s => s.Dataset.Name, StringComparer.Ordinal)
                .ToList();
            var outputRef = $"{sinkDef.Sink.Name}.{sinkDef.Output.Name}";

            if (cdcSources.Count > 0 && sinkDef.Output.Mode is "replace" or "append")
            {
                var datasetNames = string.Join(", ", cdcSources.Select(s => $"{s.Source.Name}.{s.Dataset.Name}"));
                var reason = sinkDef.Output.Mode == "replace"
                    ? "would discard previously applied change events each run (cdc x replace)"
                    : "cdc delivery is change events, not rows -- append would land raw events as duplicate " +
                      "rows (cdc x append)";
                cdcErrors.Add(new PzError(PzErrorCode.IncompatiblePair,
                    $"output '{outputRef}': write.strategy '{sinkDef.Output.Mode}' fed by cdc dataset(s) " +
                    $"'{datasetNames}' -- {reason}.",
                    sinkDef.Sink.FilePath, null,
                    "use write.strategy: merge with on_delete"));
            }

            if (cdcSources.Count > 0 && sinkDef.Output.Mode == "merge" && sinkDef.Output.OnDelete is null)
            {
                var datasetNames = string.Join(", ", cdcSources.Select(s => $"{s.Source.Name}.{s.Dataset.Name}"));
                cdcErrors.Add(new PzError(PzErrorCode.CdcConsentMissing,
                    $"output '{outputRef}': write.strategy merge fed by cdc dataset(s) '{datasetNames}' has not " +
                    "declared write.on_delete -- deletes on the source table need an explicit routing choice.",
                    sinkDef.Sink.FilePath, null,
                    "declare write.on_delete: delete, soft, or ignore"));
            }

            if (sinkDef.Output.OnDelete is not null && cdcSources.Count == 0)
            {
                cdcErrors.Add(new PzError(PzErrorCode.CdcDeleteRouteInvalid,
                    $"output '{outputRef}': write.on_delete '{sinkDef.Output.OnDelete}' is declared but no " +
                    "upstream dataset declares sync: {mode: cdc} -- on_delete requires a cdc-fed input.",
                    sinkDef.Sink.FilePath, null,
                    "remove write.on_delete, or feed this output from a sync: {mode: cdc} dataset"));
            }
            else if (sinkDef.Output.OnDelete is "delete" or "soft" && cdcSources.Count > 0 && upstream.Count > 1)
            {
                var datasetNames = string.Join(", ", upstream.Select(s => $"{s.Source.Name}.{s.Dataset.Name}"));
                cdcErrors.Add(new PzError(PzErrorCode.CdcDeleteRouteInvalid,
                    $"output '{outputRef}': write.on_delete '{sinkDef.Output.OnDelete}' fed by a multi-source " +
                    $"pipeline ('{datasetNames}') -- delete keys cannot be routed through a multi-source pipeline.",
                    sinkDef.Sink.FilePath, null,
                    "route on_delete: delete/soft through a single-source pipeline reading only the cdc " +
                    "dataset, or use on_delete: ignore"));
            }
            else if (sinkDef.Output.OnDelete is "delete" or "soft" && cdcSources.Count == 1 && upstream.Count == 1)
            {
                var origin = cdcSources[0];
                nodes[i] = nodes[i] with
                {
                    Definition = sinkDef with { CdcDeleteOrigin = new CdcOrigin(origin.Source.Name, origin.Dataset.Name) },
                };
            }
        }

        // Both pairing matrices read the finished node list and nothing of each other's, so an
        // incremental violation on one output must not hide a cdc violation on another.
        if (deliveryErrors.Count > 0 || cdcErrors.Count > 0)
        {
            throw new PzValidationException(OrderErrors(deliveryErrors.Concat(cdcErrors)));
        }
    }

    /// <summary>The cdc pairing-matrix pass's closure walk -- every
    /// <see cref="SourceDatasetDef"/> reachable from a SinkWrite node's transitive DependsOn ancestry.
    /// Mirrors the incremental pass's (and ExecutionPlanner's feed pass) HashSet-seen + Queue-over-DependsOn
    /// shape; a dataset reachable via two paths in a diamond DAG is collected once because `seen`
    /// gates on NodeId (one SourceLoad node per (source, dataset) pair), not on dataset identity.</summary>
    private static List<SourceDatasetDef> UpstreamSourceLoads(
        DagNode sinkNode, IReadOnlyDictionary<NodeId, DagNode> byId)
    {
        var result = new List<SourceDatasetDef>();
        var seen = new HashSet<NodeId>();
        var queue = new Queue<NodeId>(sinkNode.DependsOn);
        while (queue.Count > 0)
        {
            var id = queue.Dequeue();
            if (!seen.Add(id) || !byId.TryGetValue(id, out var ancestor))
            {
                continue;
            }

            if (ancestor.Definition is SourceDatasetDef src)
            {
                result.Add(src);
            }

            foreach (var dep in ancestor.DependsOn)
            {
                queue.Enqueue(dep);
            }
        }

        return result;
    }
}
