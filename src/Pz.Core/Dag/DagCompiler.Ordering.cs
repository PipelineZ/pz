using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Dag;

/// <summary>Deterministic topological ordering of the node list and cycle reporting.</summary>
public static partial class DagCompiler
{
    private static int KindRank(NodeKind kind) => kind switch
    {
        NodeKind.SourceLoad => 0,
        NodeKind.Pipeline => 1,
        NodeKind.Check => 2,
        NodeKind.SinkWrite => 3,
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    /// <summary>
    /// Kahn's algorithm; among ready nodes at each step, pick the lowest
    /// (kindRank, Name ordinal). If nodes remain once no more are ready, they form at least
    /// one cycle — walk their dependency edges (restricted to the remainder) to name one.
    /// </summary>
    private static List<DagNode> TopologicalSortOrThrow(List<DagNode> nodes)
    {
        var byId = nodes.ToDictionary(n => n.Id);
        var inDegree = nodes.ToDictionary(n => n.Id, n => n.DependsOn.Count);
        var children = new Dictionary<NodeId, List<NodeId>>();
        foreach (var node in nodes)
        {
            foreach (var dep in node.DependsOn)
            {
                if (!children.TryGetValue(dep, out var list))
                {
                    list = [];
                    children[dep] = list;
                }

                list.Add(node.Id);
            }
        }

        var comparer = Comparer<NodeId>.Create((a, b) =>
        {
            var na = byId[a];
            var nb = byId[b];
            var cmp = KindRank(na.Kind).CompareTo(KindRank(nb.Kind));
            if (cmp != 0)
            {
                return cmp;
            }

            cmp = string.CompareOrdinal(na.Name, nb.Name);
            return cmp != 0 ? cmp : string.CompareOrdinal(a.Value, b.Value);
        });

        var ready = new SortedSet<NodeId>(comparer);
        foreach (var node in nodes)
        {
            if (inDegree[node.Id] == 0)
            {
                ready.Add(node.Id);
            }
        }

        var result = new List<DagNode>(nodes.Count);
        while (ready.Count > 0)
        {
            var id = ready.Min;
            ready.Remove(id);
            result.Add(byId[id]);

            if (!children.TryGetValue(id, out var kids))
            {
                continue;
            }

            foreach (var kid in kids)
            {
                if (--inDegree[kid] == 0)
                {
                    ready.Add(kid);
                }
            }
        }

        if (result.Count == nodes.Count)
        {
            return result;
        }

        var remainder = nodes.Where(n => inDegree[n.Id] > 0).ToList();
        throw new PzValidationException([BuildCycleError(remainder, byId)]);
    }

    private static PzError BuildCycleError(List<DagNode> remainder, Dictionary<NodeId, DagNode> byId)
    {
        var remainderIds = remainder.Select(n => n.Id).ToHashSet();
        var start = remainder
            .OrderBy(n => KindRank(n.Kind))
            .ThenBy(n => n.Name, StringComparer.Ordinal)
            .First();

        var path = new List<NodeId>();
        var seen = new HashSet<NodeId>();
        var current = start.Id;
        while (seen.Add(current))
        {
            path.Add(current);
            var next = byId[current].DependsOn
                .Where(remainderIds.Contains)
                .OrderBy(id => byId[id].Name, StringComparer.Ordinal)
                .First();
            current = next;
        }

        var cycleStart = path.IndexOf(current);
        var cycleNames = path.Skip(cycleStart).Select(id => byId[id].Name).Append(byId[current].Name);
        var message = $"dependency cycle: {string.Join(" -> ", cycleNames)}";
        // `start` is always a Pipeline node: SourceLoad nodes depend on nothing (never stuck in
        // `remainder`), and a Check/SinkWrite's single dependency is a pipeline, so a genuine
        // cycle -- the loop `next` walks above -- can only run through ref() edges between pipelines.
        var file = start.Definition is PipelineDef pipeline ? pipeline.FilePath : null;
        return new PzError(PzErrorCode.Cycle, message, file, null,
            "remove or redirect one ref() in the chain above so the dependency graph has no cycle");
    }
}
