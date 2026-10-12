using System.Text.Json;
using Pz.Connectors.Abstractions;

namespace Pz.Engine.Artifacts;

/// <summary>The one JSON shape for a node's <see cref="RemoteStats"/> (<c>nodes[].remote</c> in
/// <c>run_results.json</c>). <c>RunEventFields</c> in Pz.Diagnostics cannot reference this assembly, so it
/// repeats the same key order for the event stream; a test pins both to identical bytes.</summary>
public static class RemoteStatsJson
{
    /// <summary>Writes a <c>remote</c> property into the open node object. Count keys are written when
    /// positive, <c>cost</c> when a unit was reported, <c>extras</c> when non-empty; nothing is written when
    /// no key would remain (a snapshot holding only dropped spans).</summary>
    public static void Write(Utf8JsonWriter writer, RemoteStats? remote)
    {
        if (remote is null) return;
        if (remote.Requests <= 0 && remote.WireBytes <= 0 && remote.RemoteMs <= 0 && remote.Throttled <= 0
            && remote.CostUnit is null && remote.Extras.Count == 0)
            return;

        writer.WriteStartObject("remote");
        if (remote.Requests > 0) writer.WriteNumber("requests", remote.Requests);
        if (remote.WireBytes > 0) writer.WriteNumber("wireBytes", remote.WireBytes);
        if (remote.RemoteMs > 0) writer.WriteNumber("remoteMs", remote.RemoteMs);
        if (remote.Throttled > 0) writer.WriteNumber("throttled", remote.Throttled);
        if (remote.CostUnit is { } unit)
        {
            writer.WriteStartObject("cost");
            writer.WriteNumber("units", remote.CostUnits ?? 0);
            writer.WriteString("unit", unit);
            writer.WriteEndObject();
        }

        if (remote.Extras.Count > 0)
        {
            writer.WriteStartObject("extras");
            foreach (var extra in remote.Extras) writer.WriteNumber(extra.Key, extra.Value);
            writer.WriteEndObject();
        }

        writer.WriteEndObject();
    }
}
