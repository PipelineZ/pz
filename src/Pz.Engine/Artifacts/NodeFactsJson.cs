using System.Text.Json;
using Pz.Engine.Execution;

namespace Pz.Engine.Artifacts;

/// <summary>The one JSON shape for <see cref="NodeFacts"/>, shared by <see cref="RunResultsWriter"/> and the
/// SQL Server artifact store so both persist identical keys. Public because the SQL Server backend is a
/// separate assembly.</summary>
public static class NodeFactsJson
{
    /// <summary>Writes the facts' properties into the already-open node object. Additive-optional: absent
    /// facts, and absent parts of them, write nothing.</summary>
    public static void Write(Utf8JsonWriter writer, NodeFacts? facts)
    {
        if (facts is null) return;
        if (facts.Connector is { } connector) writer.WriteString("connector", connector);
        if (facts.Connection is { } connection) writer.WriteString("connection", connection);
        if (facts.Strategy is { } strategy) writer.WriteString("strategy", strategy);
        if (facts.Stages is { } stages)
        {
            writer.WriteStartObject("stages");
            WriteMs(writer, "extractMs", stages.Extract);
            WriteMs(writer, "ingestMs", stages.Ingest);
            WriteMs(writer, "egressMs", stages.Egress);
            WriteMs(writer, "writeMs", stages.Write);
            WriteMs(writer, "extractStartMs", stages.ExtractStart);
            WriteMs(writer, "ingestStartMs", stages.IngestStart);
            WriteMs(writer, "egressStartMs", stages.EgressStart);
            WriteMs(writer, "writeStartMs", stages.WriteStart);
            writer.WriteEndObject();
        }

        if (facts.Attempts > 0) writer.WriteNumber("attempts", facts.Attempts);
        RemoteStatsJson.Write(writer, facts.Remote);
    }

    private static void WriteMs(Utf8JsonWriter writer, string name, TimeSpan? value)
    {
        if (value is { } v) writer.WriteNumber(name, (long)v.TotalMilliseconds);
    }
}
