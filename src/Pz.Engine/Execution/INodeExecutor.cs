using Pz.Core.Dag;

namespace Pz.Engine.Execution;

public interface INodeExecutor
{
    Task<NodeResult> ExecuteAsync(DagNode node, RunContext ctx, CancellationToken ct);
}
