using Pz.Core.Validation;

using Pz.Core.Model;

namespace Pz.Core.Dag;

/// <summary>
/// <c>_byId</c>/<c>_children</c> are derived state built from <see cref="Nodes"/>. They cannot
/// be plain field-initializer values: record <c>with</c>-cloning copies all instance fields via
/// the compiler-generated copy constructor and then applies only the property changes named in
/// the <c>with</c> expression, so a clone with a different <see cref="Nodes"/> would otherwise
/// carry over its source's stale lookups. Instead, the derived state is rebuilt lazily whenever
/// it no longer matches the current <see cref="Nodes"/> reference (checked via
/// <see cref="EnsureBuilt"/>), so every accessor stays correct after <c>with</c>-cloning without
/// needing a custom copy constructor.
/// </summary>
public sealed record CompiledDag(IReadOnlyList<DagNode> Nodes)
{
    /// <summary>Non-blocking validation findings produced alongside a successful compile (e.g.
    /// dead-leaf pipeline detection). Distinct from the informational compile-notices channel and from
    /// <see cref="Pz.Core.Validation.PzValidationException"/>'s blocking errors: warnings never fail
    /// compilation and never change a caller's exit code.</summary>
    public IReadOnlyList<PzWarning> Warnings { get; init; } = [];

    /// <summary>The EFFECTIVE connections: the project's, plus every entity a source()/sink() call site
    /// declared. Two surfaces mean the loaded project is not the whole story — a validator reading
    /// `PzProject.Connections` would silently skip a call-site entity. Callers that validate AFTER
    /// compiling should read this instead. Empty only if nothing was compiled.</summary>
    public IReadOnlyList<ConnectionDef> Connections { get; init; } = [];

    private readonly Lock _lock = new();
    private IReadOnlyList<DagNode>? _builtFrom;
    private Dictionary<NodeId, DagNode> _byId = [];
    private Dictionary<NodeId, List<NodeId>> _children = [];

    /// <summary>
    /// <see cref="Nodes"/> is already produced by <c>DagCompiler.Compile</c> in deterministic
    /// (Kahn) topological order, so this simply exposes that order.
    /// </summary>
    public IEnumerable<DagNode> TopologicalOrder() => Nodes;

    /// <summary>BFS over the (lazily built) child adjacency, i.e. every node reachable by following DependsOn edges downstream from <paramref name="id"/>.</summary>
    public IReadOnlyList<DagNode> Descendants(NodeId id)
    {
        var (byId, children) = EnsureBuilt();

        var visited = new HashSet<NodeId>();
        var result = new List<DagNode>();
        var queue = new Queue<NodeId>();

        if (children.TryGetValue(id, out var direct))
        {
            foreach (var child in direct)
            {
                queue.Enqueue(child);
            }
        }

        while (queue.Count > 0)
        {
            var current = queue.Dequeue();
            if (!visited.Add(current))
            {
                continue;
            }

            result.Add(byId[current]);

            if (children.TryGetValue(current, out var next))
            {
                foreach (var child in next)
                {
                    queue.Enqueue(child);
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Returns lookups guaranteed to be derived from the *current* <see cref="Nodes"/>,
    /// rebuilding them if <see cref="Nodes"/> was swapped by <c>with</c>-cloning. Returned as a
    /// pair (rather than read back from the fields by the caller) so callers always work with a
    /// consistent snapshot even if another thread triggers a rebuild concurrently. Not a hot
    /// path, so a plain lock is fine.
    /// </summary>
    private (Dictionary<NodeId, DagNode> ById, Dictionary<NodeId, List<NodeId>> Children) EnsureBuilt()
    {
        lock (_lock)
        {
            if (!ReferenceEquals(_builtFrom, Nodes))
            {
                _byId = Nodes.ToDictionary(n => n.Id);
                _children = BuildChildAdjacency(Nodes);
                _builtFrom = Nodes;
            }

            return (_byId, _children);
        }
    }

    private static Dictionary<NodeId, List<NodeId>> BuildChildAdjacency(IReadOnlyList<DagNode> nodes)
    {
        var map = new Dictionary<NodeId, List<NodeId>>();
        foreach (var node in nodes)
        {
            foreach (var dependency in node.DependsOn)
            {
                if (!map.TryGetValue(dependency, out var children))
                {
                    children = [];
                    map[dependency] = children;
                }

                children.Add(node.Id);
            }
        }

        return map;
    }
}
