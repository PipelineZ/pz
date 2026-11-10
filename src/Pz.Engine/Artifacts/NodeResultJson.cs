using System.Text.Json;
using Pz.Engine.Execution;

namespace Pz.Engine.Artifacts;

/// <summary>The one wire spelling of a <see cref="NodeResult"/>'s status, provenance and additive-optional
/// stats blocks, shared by <see cref="RunResultsWriter"/>, the NDJSON event publisher and the SQL Server
/// artifact store so every persisted or streamed form agrees. Public because the SQL Server backend is a
/// separate assembly.</summary>
public static class NodeResultJson
{
    public static string StatusName(NodeStatus status) => status switch
    {
        NodeStatus.Success => "success",
        NodeStatus.Failed => "failed",
        NodeStatus.Skipped => "skipped",
        _ => throw new ArgumentOutOfRangeException(nameof(status), status, "unknown node status"),
    };

    /// <summary>Null for a normally-executed node, which writes no <c>provenance</c> at all.</summary>
    public static string? ProvenanceName(NodeProvenance? provenance) => provenance switch
    {
        null => null,
        NodeProvenance.Reused => "reused",
        NodeProvenance.CarriedForward => "carried_forward",
        _ => throw new ArgumentOutOfRangeException(nameof(provenance), provenance, "unknown provenance"),
    };

    /// <summary>Writes the <c>timings</c>, <c>ops</c>, <c>partitions</c>, <c>delivery</c>, <c>cdc</c> and
    /// <c>observed_schema</c> blocks into the already-open node object, in that order. Each is
    /// additive-optional: omitted entirely when its source is null, so a node untouched by the feature
    /// never carries a <c>"key":null</c>.</summary>
    public static void WriteStatsBlocks(Utf8JsonWriter writer, NodeResult node)
    {
        if (node.Timings is { } timings)
        {
            writer.WriteStartObject("timings");
            writer.WriteNumber("producerStallMs", (long)timings.ProducerStall.TotalMilliseconds);
            writer.WriteNumber("consumerStallMs", (long)timings.ConsumerStall.TotalMilliseconds);
            writer.WriteEndObject();
        }

        if (node.Ops is { } ops)
        {
            writer.WriteStartObject("ops");
            writer.WriteNumber("executed", ops.Executed);
            writer.WriteNumber("retried", ops.Retried);
            writer.WriteNumber("throttle_wait_ms", ops.ThrottleWaitMs);
            writer.WriteEndObject();
        }

        // Counts only; never partition ids.
        if (node.Partitions is { } partitions)
        {
            writer.WriteStartObject("partitions");
            writer.WriteNumber("total", partitions.Total);
            writer.WriteNumber("completed", partitions.Completed);
            writer.WriteNumber("reused", partitions.Reused);
            writer.WriteNumber("resumed", partitions.Resumed);
            writer.WriteEndObject();
        }

        if (node.Delivery is { } delivery)
        {
            writer.WriteStartObject("delivery");
            writer.WriteString("abort_semantics", delivery.AbortSemantics);
            writer.WriteNumber("rows_visible", delivery.RowsVisible);
            writer.WriteNumber("resumed_rows", delivery.ResumedRows);
            writer.WriteEndObject();
        }

        // Raw per-op counts plus `position`, the cdc-shaped SourceLoad's SyncStateCandidate token
        // (explicit null when the connector emitted none).
        if (node.Cdc is { } cdc)
        {
            writer.WriteStartObject("cdc");
            writer.WriteNumber("inserts", cdc.Inserts);
            writer.WriteNumber("updates", cdc.Updates);
            writer.WriteNumber("deletes", cdc.Deletes);
            if (node.SyncStateCandidate is { } position)
            {
                writer.WriteString("position", position.Token);
            }
            else
            {
                writer.WriteNull("position");
            }

            writer.WriteEndObject();
        }

        // The DESCRIBE'd staging schema and read-hints hash the drift gate captured for a contract-less
        // SourceLoad under on_source_drift: warn|fail; absent under the default ignore policy.
        if (node.Observed is { } observed)
        {
            writer.WriteStartObject("observed_schema");
            writer.WriteStartArray("columns");
            foreach (var col in observed.Columns)
            {
                writer.WriteStartObject();
                writer.WriteString("name", col.Name);
                writer.WriteString("type", col.Type);
                writer.WriteEndObject();
            }

            writer.WriteEndArray();
            writer.WriteString("hintsHash", observed.HintsHash);
            writer.WriteEndObject();
        }
    }
}
