namespace Pz.Core.Dag;

/// <summary>One "independent flow": a connected component of the compiled DAG, treating DependsOn
/// edges as undirected. <see cref="TerminalNames"/> labels the
/// flow for display — its SinkWrite node names, or (when nothing drains the component) its leaf
/// non-Check node names — ordinal-sorted. Labels are display-only; CLI resolution is always by
/// node name.</summary>
public sealed record FlowComponent(IReadOnlySet<NodeId> Nodes, IReadOnlyList<string> TerminalNames)
{
    public string Label => string.Join(" + ", TerminalNames);
}

public static class FlowComponents
{
    /// <summary>Undirected BFS over DependsOn edges. Components are ordered by the position of
    /// their first member in <paramref name="dag"/>.Nodes (deterministic — DagCompiler emits Kahn
    /// topological order), so any rendering of the result is byte-stable for a given DAG.</summary>
    public static IReadOnlyList<FlowComponent> Compute(CompiledDag dag)
    {
        ArgumentNullException.ThrowIfNull(dag);

        var neighbors = dag.Nodes.ToDictionary(n => n.Id, _ => new List<NodeId>());
        foreach (var node in dag.Nodes)
        {
            foreach (var parent in node.DependsOn)
            {
                neighbors[node.Id].Add(parent);
                neighbors[parent].Add(node.Id);
            }
        }

        var byId = dag.Nodes.ToDictionary(n => n.Id);
        var visited = new HashSet<NodeId>();
        var components = new List<FlowComponent>();
        foreach (var seed in dag.Nodes)
        {
            if (!visited.Add(seed.Id))
            {
                continue;
            }

            var members = new HashSet<NodeId> { seed.Id };
            var queue = new Queue<NodeId>();
            queue.Enqueue(seed.Id);
            while (queue.Count > 0)
            {
                foreach (var next in neighbors[queue.Dequeue()])
                {
                    if (visited.Add(next))
                    {
                        members.Add(next);
                        queue.Enqueue(next);
                    }
                }
            }

            components.Add(new FlowComponent(members, TerminalNames(byId, members)));
        }

        return components;
    }

    /// <summary>SinkWrite names when the component has any; otherwise its non-Check
    /// leaves (nodes no non-Check node depends on) — e.g. an unsunk pipeline queried manually.
    /// Check nodes never label a flow: they are structural leaves but not what a user thinks of
    /// as the flow's terminal.</summary>
    private static IReadOnlyList<string> TerminalNames(
        IReadOnlyDictionary<NodeId, DagNode> byId, IReadOnlySet<NodeId> members)
    {
        var nodes = members.Select(id => byId[id]).ToList();
        var sinks = nodes.Where(n => n.Kind == NodeKind.SinkWrite).Select(n => n.Name)
            .OrderBy(n => n, StringComparer.Ordinal).ToList();
        if (sinks.Count > 0)
        {
            return sinks;
        }

        var hasNonCheckChild = new HashSet<NodeId>();
        foreach (var node in nodes.Where(n => n.Kind != NodeKind.Check))
        {
            foreach (var parent in node.DependsOn)
            {
                hasNonCheckChild.Add(parent);
            }
        }

        return [.. nodes.Where(n => n.Kind != NodeKind.Check && !hasNonCheckChild.Contains(n.Id))
            .Select(n => n.Name).OrderBy(n => n, StringComparer.Ordinal)];
    }
}
