using Pz.Core.Dag;

namespace Pz.Engine.Planning;

public sealed record ExecutionPlan(IReadOnlyList<PlannedNode> Nodes, MemoryBudget MemoryBudget)
{
    private readonly Dictionary<NodeId, PlannedNode> _byId = Nodes.ToDictionary(n => n.Id);

    /// <summary>The planned strategy, or the kind-appropriate default when the node wasn't planned
    /// (universal for loads/writes, in-engine for the rest) — absent planning never blocks execution.</summary>
    public EdgeStrategy StrategyFor(NodeId id) =>
        _byId.TryGetValue(id, out var node) ? node.Strategy : EdgeStrategy.ArrowStream;

    public PlannedNode? Find(NodeId id) => _byId.TryGetValue(id, out var node) ? node : null;
}
