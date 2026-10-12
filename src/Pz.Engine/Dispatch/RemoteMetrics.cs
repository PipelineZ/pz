using System.Diagnostics;
using Pz.Core.Dag;
using Pz.Diagnostics.Otel;
using Pz.Engine.Execution;

namespace Pz.Engine.Dispatch;

/// <summary>Records a finished node's <see cref="NodeFacts.Remote"/> on the <c>pz.remote.*</c> instruments,
/// once per node. Tagged with the instance and connector only: never an entity, SQL or connection config.</summary>
internal static class RemoteMetrics
{
    public static void Record(DagNode node, NodeResult result)
    {
        if (result.Facts is not { Remote: { } remote } facts) return;

        var tags = new TagList
        {
            { "pz.instance", InstanceKey.For(node) },
            { "pz.connector", facts.Connector },
        };

        if (remote.Requests > 0) PzMeters.RemoteRequests.Add(remote.Requests, tags);
        if (remote.WireBytes > 0) PzMeters.RemoteWireBytes.Add(remote.WireBytes, tags);
        if (remote.Throttled > 0) PzMeters.RemoteThrottled.Add(remote.Throttled, tags);
        if (remote.RemoteMs > 0) PzMeters.RemoteDuration.Record(remote.RemoteMs, tags);
        if (remote.CostUnit is { } unit)
        {
            var costTags = tags;
            costTags.Add("pz.remote.cost_unit", unit);
            PzMeters.RemoteCost.Add(remote.CostUnits ?? 0, costTags);
        }
    }
}
