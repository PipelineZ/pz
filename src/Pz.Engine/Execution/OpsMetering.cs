using Pz.Core.Dag;
using Pz.Diagnostics.Otel;
using Pz.Engine.Resilience;

namespace Pz.Engine.Execution;

internal static class OpsMetering
{
    /// <summary>Snapshots <paramref name="gate"/> once and feeds the <c>pz.ops.*</c> meters from it. The
    /// returned snapshot is the one <see cref="NodeResult.Ops"/> must carry: <see cref="OpStats"/> is a
    /// point-in-time read of mutable Interlocked counters, so snapshotting twice would let the meters and
    /// the result disagree. Null when the node has no gate.</summary>
    public static OpStats? RecordAndSnapshot(OperationGate? gate, DagNode node)
    {
        var opStats = gate?.Snapshot();
        if (opStats is { } stats)
        {
            var instanceTag = new KeyValuePair<string, object?>("pz.instance", InstanceKey.For(node));
            PzMeters.OpsExecuted.Add(stats.Executed, instanceTag);
            PzMeters.OpsRetried.Add(stats.Retried, instanceTag);
            PzMeters.OpsThrottleWait.Add(stats.ThrottleWaitMs, instanceTag);
        }

        return opStats;
    }
}
