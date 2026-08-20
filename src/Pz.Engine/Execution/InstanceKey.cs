using Pz.Core.Dag;

namespace Pz.Engine.Execution;

/// <summary>Identifies the connector INSTANCE a node belongs to, for per-instance gating
/// (<c>max_concurrency</c> in <see cref="Pz.Engine.Dispatch.RunOrchestrator"/>, and the circuit
/// breaker). Pipeline/Check nodes have no owning connector
/// instance and are never gated -- <see cref="For"/> returns <c>null</c> for them. Deliberately just the
/// instance name (not dataset/output) because <c>max_concurrency</c> is instance-only (no per-dataset
/// override, unlike retry): every dataset/output of the same instance shares one key, and thus one
/// permit pool.
///
/// The instance is the CONNECTION, not the direction. A database pz both reads and writes is one place
/// under one budget -- keying by direction would let <c>max_concurrency: 4</c> admit eight concurrent
/// nodes against it.</summary>
public static class InstanceKey
{
    public static string? For(DagNode node) => node.Definition switch
    {
        SourceDatasetDef def => "conn:" + def.Source.Name,
        SinkOutputDef def => "conn:" + def.Sink.Name,
        _ => null,
    };
}
