using Pz.Connectors.Abstractions;
using PbStats = Pz.Connectors.Protocol.V1.ConnectorStats;

namespace Pz.Connectors.Sdk;

/// <summary>Abstractions <see cref="RemoteStats"/> to and from the wire message. An empty
/// <c>cost_unit</c> is the wire's "no cost", so it maps to both a null unit and null units: the
/// units number alone means nothing without its unit.</summary>
internal static class ConnectorStatsMapping
{
    /// <summary>Null for an empty snapshot, so an unreported stream leaves the field unset.</summary>
    public static PbStats? ToProto(RemoteStats stats)
    {
        if (stats.IsEmpty)
        {
            return null;
        }

        var proto = new PbStats
        {
            Requests = stats.Requests,
            WireBytes = stats.WireBytes,
            RemoteMs = stats.RemoteMs,
            Throttled = stats.Throttled,
            CostUnits = stats.CostUnit is null ? 0 : stats.CostUnits ?? 0,
            CostUnit = stats.CostUnit ?? "",
            SpansDropped = stats.SpansDropped,
        };
        foreach (var extra in stats.Extras)
        {
            proto.Extras[extra.Key] = extra.Value;
        }

        return proto;
    }

    public static RemoteStats FromProto(PbStats proto)
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
