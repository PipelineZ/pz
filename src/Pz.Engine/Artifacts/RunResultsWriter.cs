using System.Buffers;
using System.Text.Json;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Engine.Artifacts;

/// <summary>
/// Writes <c>.pz/runs/&lt;runId&gt;/run_results.json</c> crash-safely: every call
/// serializes to a unique <c>&lt;path&gt;.&lt;guid&gt;.tmp</c> (same directory, so the following
/// <see cref="File.Move(string, string, bool)"/> stays an atomic same-volume rename) then moves it
/// into place, so a reader never observes a partially-written file and a crash mid-write leaves only
/// the previous (complete) snapshot or a stray <c>.tmp</c> — never a corrupt <c>run_results.json</c>.
/// Call <see cref="WriteSnapshot"/> after every node completion with status <c>"running"</c> and the
/// accumulated results so far, then once more with the terminal status
/// (<c>"success" | "completed_with_failures" | "fatal"</c>) once the run winds down.
///
/// Concurrency: with <c>engine.threads &gt; 1</c>, two
/// <c>NodeCompleted</c> callbacks can call <see cref="WriteSnapshot"/> near-simultaneously. Each call
/// gets its own tmp file (no shared-path race), and the write-then-move is additionally serialized by
/// a private <see cref="_publishLock"/> so applications never interleave — whichever call is last to
/// hold the lock is the one whose snapshot survives on disk, never a partial/corrupt mix of two
/// writers. This lock is internal to this class and intentionally not shared with any caller-side
/// lock (e.g. <c>ConsoleRunEvents._gate</c>) — callers must not couple their own synchronization to
/// this writer's.
/// </summary>
public sealed class RunResultsWriter(RunPaths paths, string startedAtIso)
{
    private readonly Lock _publishLock = new();

    public void WriteSnapshot(IReadOnlyList<NodeResult> completed, string status)
    {
        Directory.CreateDirectory(paths.RunDir);

        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteNumber("version", 1);
            writer.WriteString("runId", paths.RunId);
            writer.WriteString("status", status);
            writer.WriteString("startedAt", startedAtIso);
            writer.WriteStartArray("nodes");
            foreach (var node in completed)
            {
                WriteNode(writer, node);
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        var tmpPath = $"{paths.RunResultsPath}.{Guid.NewGuid():N}.tmp";
        var moved = false;
        try
        {
            lock (_publishLock)
            {
                using (var stream = new FileStream(tmpPath, FileMode.Create, FileAccess.Write))
                {
                    stream.Write(buffer.WrittenSpan);
                }

                File.Move(tmpPath, paths.RunResultsPath, overwrite: true);
                moved = true;
            }
        }
        finally
        {
            if (!moved)
            {
                try { File.Delete(tmpPath); } catch { /* best-effort cleanup — never mask the real exception */ }
            }
        }
    }

    private static void WriteNode(Utf8JsonWriter writer, NodeResult node)
    {
        writer.WriteStartObject();
        writer.WriteString("id", node.Id.Value);
        writer.WriteString("kind", node.Kind.ToString());
        writer.WriteString("name", node.Name);
        writer.WriteString("status", StatusName(node.Status));

        // Additive-optional: omitted entirely when null so a normally-executed node writes no
        // `provenance` key at all — same discipline as `timings`.
        if (node.Provenance is { } provenance)
        {
            writer.WriteString("provenance", provenance switch
            {
                NodeProvenance.Reused => "reused",
                NodeProvenance.CarriedForward => "carried_forward",
                _ => throw new ArgumentOutOfRangeException(nameof(node), provenance, "unknown provenance"),
            });
        }

        writer.WriteNumber("rows", node.RowsMoved);
        writer.WriteNumber("durationMs", (long)node.Duration.TotalMilliseconds);

        if (node.Error is null)
        {
            writer.WriteNull("error");
        }
        else
        {
            writer.WriteStartObject("error");
            writer.WriteString("code", node.Error.Code);
            writer.WriteString("message", node.Error.Message);
            writer.WriteEndObject();
        }

        // Additive-optional: omitted entirely when null, so a run without stalls (or one
        // that never reached the channel-instrumented path — Pipeline/Check nodes, native tiers) writes
        // no `timings` key at all — never a `"timings":null` key.
        if (node.Timings is { } timings)
        {
            writer.WriteStartObject("timings");
            writer.WriteNumber("producerStallMs", (long)timings.ProducerStall.TotalMilliseconds);
            writer.WriteNumber("consumerStallMs", (long)timings.ConsumerStall.TotalMilliseconds);
            writer.WriteEndObject();
        }

        // Additive-optional: omitted entirely when null, same discipline as
        // `timings` immediately above — a node with no operation gate (not gate-aware, native tier, or
        // failed) writes no `ops` key at all.
        if (node.Ops is { } ops)
        {
            writer.WriteStartObject("ops");
            writer.WriteNumber("executed", ops.Executed);
            writer.WriteNumber("retried", ops.Retried);
            writer.WriteNumber("throttle_wait_ms", ops.ThrottleWaitMs);
            writer.WriteEndObject();
        }

        // Additive-optional: success-side partition-mode stats. Omitted when
        // null. Counts only; never partition ids.
        if (node.Partitions is { } partitionStats)
        {
            writer.WriteStartObject("partitions");
            writer.WriteNumber("total", partitionStats.Total);
            writer.WriteNumber("completed", partitionStats.Completed);
            writer.WriteNumber("reused", partitionStats.Reused);
            writer.WriteNumber("resumed", partitionStats.Resumed);
            writer.WriteEndObject();
        }

        // Additive-optional: honest-abort / delivery-resume stats. Omitted when null.
        if (node.Delivery is { } deliveryStats)
        {
            writer.WriteStartObject("delivery");
            writer.WriteString("abort_semantics", deliveryStats.AbortSemantics);
            writer.WriteNumber("rows_visible", deliveryStats.RowsVisible);
            writer.WriteNumber("resumed_rows", deliveryStats.ResumedRows);
            writer.WriteEndObject();
        }

        // Raw per-op counts from the last-event-per-key collapse, plus
        // `position` (the cdc-shaped SourceLoad's SyncStateCandidate token — cdc reuses the sync-state
        // seam — explicit null when the connector emitted none). Omitted entirely when null,
        // same discipline as `ops`/`partitions`/`delivery` above.
        if (node.Cdc is { } cdc)
        {
            writer.WriteStartObject("cdc");
            writer.WriteNumber("inserts", cdc.Inserts);
            writer.WriteNumber("updates", cdc.Updates);
            writer.WriteNumber("deletes", cdc.Deletes);
            if (node.SyncStateCandidate is { } cdcPosition)
            {
                writer.WriteString("position", cdcPosition.Token);
            }
            else
            {
                writer.WriteNull("position");
            }

            writer.WriteEndObject();
        }

        // Additive-optional: the DESCRIBE'd staging schema +
        // read-hints hash the drift gate captured for a contract-less SourceLoad under
        // on_source_drift: warn|fail. Omitted when null — same discipline as ops/partitions/
        // delivery/cdc above — so an ignore-policy run (the default) or a contract dataset writes
        // no `observed_schema` key at all.
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

        // The slice identity `pz retry` inherits: candidate MAX(cursor) [or windowed upper bound]
        // this SourceLoad produced. `runId` deliberately not persisted — a reuse re-materializes the
        // Watermark with the REUSING run's id (the run that will advance the store).
        if (node.WatermarkCandidate is { } wm)
        {
            writer.WriteStartObject("watermark");
            writer.WriteString("cursor", wm.Cursor);
            writer.WriteString("type", wm.TypeName);
            writer.WriteString("value", wm.Value);
            writer.WriteEndObject();
        }

        writer.WriteEndObject();
    }

    private static string StatusName(NodeStatus status) => status switch
    {
        NodeStatus.Success => "success",
        NodeStatus.Failed => "failed",
        NodeStatus.Skipped => "skipped",
        _ => throw new ArgumentOutOfRangeException(nameof(status), status, "unknown node status"),
    };
}
