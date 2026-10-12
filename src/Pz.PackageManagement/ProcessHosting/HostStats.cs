using Pz.Connectors.Abstractions;
using PbStats = Pz.Connectors.Protocol.V1.ConnectorStats;

namespace Pz.PackageManagement.ProcessHosting;

/// <summary>Folds the stats a hosted connector reported over PCP into the ambient
/// <see cref="ConnectorStats.Current"/> of the attempt the host call runs inside. The wire message is
/// the connector's own tally for one operation, so merging is addition: the engine's accumulator spans
/// attempts, and each attempt's report lands in it once.</summary>
internal static class HostStats
{
    /// <summary>No-op for an absent message. Never throws: telemetry cannot fail a read or write.</summary>
    public static void Merge(PbStats? proto)
    {
        if (proto is null)
        {
            return;
        }

        var stats = FromProto(proto);
        var drops = (StatsDrop)proto.Drops;
        if (stats.IsEmpty && drops == StatsDrop.None)
        {
            return;
        }

        var current = ConnectorStats.Current;
        if (current is ConnectorStatsAccumulator accumulator)
        {
            accumulator.Add(stats);
            accumulator.AddDrops(drops);
            return;
        }

        // A foreign sink only has the six reporting methods; dropped spans and drop flags have no home there.
        current.AddRequests(stats.Requests);
        current.AddWireBytes(stats.WireBytes);
        current.AddRemoteMs(stats.RemoteMs);
        current.AddThrottled(stats.Throttled);
        if (stats.CostUnit is { } unit)
        {
            current.AddCost(stats.CostUnits ?? 0, unit);
        }

        foreach (var extra in stats.Extras)
        {
            current.AddExtra(extra.Key, extra.Value);
        }
    }

    /// <summary>An empty <c>cost_unit</c> is the wire's "no cost", so it maps to both a null unit and
    /// null units: the units number alone means nothing without its unit.</summary>
    internal static RemoteStats FromProto(PbStats proto)
    {
        var hasCost = proto.CostUnit.Length > 0;
        return new RemoteStats(
            proto.Requests,
            proto.WireBytes,
            proto.RemoteMs,
            proto.Throttled,
            hasCost ? proto.CostUnits : null,
            hasCost ? proto.CostUnit : null,
            [.. proto.Extras.OrderBy(e => e.Key, StringComparer.Ordinal)],
            proto.SpansDropped);
    }
}
