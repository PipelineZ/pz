using Pz.Core.Dag;
using Pz.Engine.Artifacts;

namespace Pz.Engine.Execution;

/// <summary>One reusable SourceLoad from the failed run: where its
/// staging.duckdb lives, how many rows its staging table recorded, and the watermark candidate it
/// produced. Carries only paths/counts/canonical watermark strings — nothing connector-config-derived
/// (secret-hygiene rule).</summary>
public sealed record ReuseEntry(string PriorStagingPath, long Rows, PriorWatermark? Watermark);

/// <summary>A failed prior SourceLoad whose staging DB may hold partition-level partial
/// progress. Deliberately path-only: whether reuse is actually possible is discovered
/// from the prior staging DB's pz_meta at ATTACH time — a failed node without accounting is the
/// common legacy case and must stay silent.</summary>
public sealed record PartialReuseEntry(string PriorStagingPath);

/// <summary>A failed prior SinkWrite whose staging DB may hold a delivery-checkpoint row.
/// Deliberately path-only, like <see cref="PartialReuseEntry"/>: whether
/// resume is actually possible is discovered from the prior pz_meta at ATTACH time, under the
/// executor's guards — and only a checkpointing session ever consults it.</summary>
public sealed record DeliveryResumeEntry(string PriorStagingPath);

/// <summary>NodeId → <see cref="ReuseEntry"/> for every SourceLoad `pz retry` may satisfy by copying
/// the failed run's staged table instead of re-extracting, plus NodeId → <see cref="PartialReuseEntry"/>
/// for every FAILED prior SourceLoad that may still yield partition-level partial progress,
/// plus NodeId → <see cref="DeliveryResumeEntry"/> for every FAILED prior SinkWrite that may
/// still yield a delivery-checkpoint resume. Consulted by
/// <see cref="SourceLoadExecutor"/>/<see cref="SinkWriteExecutor"/> via <see cref="RunContext.Reuse"/>;
/// empty (or a miss) means "extract/deliver normally". A SourceLoad node id is never in both the
/// full-reuse and partial dictionaries -- a prior node's recorded status is either "success"
/// (full-reuse candidate) or "failed" (partial candidate).</summary>
public sealed class ReuseManifest(
    IReadOnlyDictionary<NodeId, ReuseEntry> entries,
    IReadOnlyDictionary<NodeId, PartialReuseEntry>? partial = null,
    IReadOnlyDictionary<NodeId, DeliveryResumeEntry>? deliveryResume = null)
{
    public static readonly ReuseManifest Empty = new(new Dictionary<NodeId, ReuseEntry>());

    private readonly IReadOnlyDictionary<NodeId, PartialReuseEntry> _partial =
        partial ?? new Dictionary<NodeId, PartialReuseEntry>();

    private readonly IReadOnlyDictionary<NodeId, DeliveryResumeEntry> _deliveryResume =
        deliveryResume ?? new Dictionary<NodeId, DeliveryResumeEntry>();

    public int Count => entries.Count;

    public bool TryGet(NodeId id, out ReuseEntry entry) => entries.TryGetValue(id, out entry!);

    public bool TryGetPartial(NodeId id, out PartialReuseEntry entry) => _partial.TryGetValue(id, out entry!);

    public bool TryGetDeliveryResume(NodeId id, out DeliveryResumeEntry entry) =>
        _deliveryResume.TryGetValue(id, out entry!);
}

/// <summary>Turns a failed run's <see cref="PriorRun"/> artifact into (a) the reuse manifest and (b)
/// the carried-forward sink results. Pure over its inputs except one
/// <see cref="File.Exists(string)"/> probe on the prior staging path — a missing/deleted staging DB
/// degrades to empty (re-extract), never an error.</summary>
public static class RetryReusePlanner
{
    public static (ReuseManifest Manifest, IReadOnlyList<NodeResult> CarriedForward) Plan(
        CompiledDag dag, PriorRun prior, IReadOnlySet<NodeId> selection, string projectDir, bool fullRefresh)
    {
        if (fullRefresh)
        {
            return (ReuseManifest.Empty, []);
        }

        var priorStagingPath = new RunPaths(projectDir, prior.RunId).StagingDbPath;
        if (!File.Exists(priorStagingPath))
        {
            return (ReuseManifest.Empty, []);
        }

        // Duplicate-tolerant (first wins): a corrupt prior run_results.json with two nodes under the same
        // id must degrade gracefully, matching the reader's convention, not throw ArgumentException.
        var priorById = new Dictionary<string, PriorNode>(StringComparer.Ordinal);
        foreach (var priorNode in prior.Nodes)
        {
            if (priorNode.Status == "success")
            {
                priorById.TryAdd(priorNode.Id, priorNode);
            }
        }

        var byId = dag.Nodes.ToDictionary(n => n.Id);

        // Mirror of RunOrchestrator.ComputeEffectiveSet's ancestor expansion: the retry run's
        // effective set is selection + every transitive ancestor. Sinks OUTSIDE this set are the
        // carry-forward candidates; SourceLoads INSIDE it are the reuse candidates that will
        // actually run this retry.
        var effective = new HashSet<NodeId>(selection);
        var queue = new Queue<NodeId>(selection);
        while (queue.Count > 0)
        {
            var id = queue.Dequeue();
            if (!byId.TryGetValue(id, out var node))
            {
                continue;
            }

            foreach (var dep in node.DependsOn)
            {
                if (effective.Add(dep))
                {
                    queue.Enqueue(dep);
                }
            }
        }

        // Only SourceLoads that will actually run this retry (in the effective set) belong in the
        // manifest -- a prior-success source in a fully-succeeded independent branch never runs, so
        // including it would overcount `pz retry`'s "reusing N source load(s)" note.
        var manifest = new Dictionary<NodeId, ReuseEntry>();
        foreach (var node in dag.Nodes)
        {
            if (node.Kind == NodeKind.SourceLoad && effective.Contains(node.Id) &&
                priorById.TryGetValue(node.Id.Value, out var priorNode))
            {
                manifest[node.Id] = new ReuseEntry(priorStagingPath, priorNode.Rows, priorNode.Watermark);
            }
        }

        // Carried-forward soundness: a prior-success SinkWrite outside the effective set,
        // whose EVERY ancestor recorded a prior success under an unchanged id, and whose every
        // SourceLoad ancestor will actually be reused this retry (in manifest AND effective) — so the
        // slice this run's other sinks receive is byte-identical to what this sink already committed.
        var carried = new List<NodeResult>();
        foreach (var node in dag.Nodes)
        {
            if (node.Kind != NodeKind.SinkWrite || effective.Contains(node.Id) ||
                !priorById.TryGetValue(node.Id.Value, out var priorSink))
            {
                continue;
            }

            var sound = true;
            var sawReusedSource = false;
            var seen = new HashSet<NodeId>();
            var ancestors = new Queue<NodeId>(node.DependsOn);
            while (sound && ancestors.Count > 0)
            {
                var id = ancestors.Dequeue();
                if (!seen.Add(id) || !byId.TryGetValue(id, out var ancestor))
                {
                    continue;
                }

                if (!priorById.ContainsKey(ancestor.Id.Value))
                {
                    sound = false; // edited/new ancestor: prior commit may not match this run's data
                    break;
                }

                if (ancestor.Kind == NodeKind.SourceLoad)
                {
                    if (!manifest.ContainsKey(ancestor.Id) || !effective.Contains(ancestor.Id))
                    {
                        sound = false; // this retry won't reproduce that slice byte-identically
                        break;
                    }

                    sawReusedSource = true;
                }

                foreach (var dep in ancestor.DependsOn)
                {
                    ancestors.Enqueue(dep);
                }
            }

            if (sound && sawReusedSource)
            {
                carried.Add(new NodeResult(node.Id, node.Kind, node.Name, NodeStatus.Success,
                    priorSink.Rows, TimeSpan.Zero, null, Provenance: NodeProvenance.CarriedForward));
            }
        }

        // Failed effective SourceLoads are partial-reuse candidates — status
        // is the only artifact signal; the executor's ATTACH-time guards decide the rest.
        var priorFailedById = new Dictionary<string, PriorNode>(StringComparer.Ordinal);
        foreach (var priorNode in prior.Nodes)
        {
            if (priorNode.Status == "failed")
            {
                priorFailedById.TryAdd(priorNode.Id, priorNode);
            }
        }

        var partial = new Dictionary<NodeId, PartialReuseEntry>();
        foreach (var node in dag.Nodes)
        {
            if (node.Kind == NodeKind.SourceLoad && effective.Contains(node.Id) &&
                priorFailedById.ContainsKey(node.Id.Value))
            {
                partial[node.Id] = new PartialReuseEntry(priorStagingPath);
            }
        }

        // Failed effective SinkWrites are delivery-resume candidates —
        // status is the only artifact signal; the executor's ATTACH-time guards (and the
        // is-it-even-checkpointing gate) decide the rest at execution time.
        var deliveryResume = new Dictionary<NodeId, DeliveryResumeEntry>();
        foreach (var node in dag.Nodes)
        {
            if (node.Kind == NodeKind.SinkWrite && effective.Contains(node.Id) &&
                priorFailedById.ContainsKey(node.Id.Value))
            {
                deliveryResume[node.Id] = new DeliveryResumeEntry(priorStagingPath);
            }
        }

        return (new ReuseManifest(manifest, partial, deliveryResume), carried);
    }
}
