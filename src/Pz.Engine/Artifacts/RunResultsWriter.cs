using System.Buffers;
using System.Globalization;
using System.Text.Json;
using Pz.Core.Artifacts;
using Pz.Diagnostics.Events;
using Pz.DuckDb;
using Pz.Engine.Execution;

namespace Pz.Engine.Artifacts;

/// <summary>
/// Writes <c>.pz/runs/&lt;runId&gt;/run_results.json</c> crash-safely, via <see cref="AtomicFile"/>
/// (write-aside-and-rename): a reader never observes a partially-written file, and a crash mid-write
/// leaves only the previous (complete) snapshot or a stray <c>.tmp</c> — never a corrupt
/// <c>run_results.json</c>. Call <see cref="WriteSnapshot"/> after every node completion with status
/// <c>"running"</c> and the accumulated results so far, then once more with the terminal status
/// (<c>"success" | "completed_with_failures" | "fatal"</c>) once the run winds down.
///
/// Concurrency: with <c>engine.threads &gt; 1</c>, two
/// <c>NodeCompleted</c> callbacks can call <see cref="WriteSnapshot"/> near-simultaneously.
/// <see cref="AtomicFile.Write"/> already gives each call its own tmp file (no shared-path race), and
/// the write-then-move is additionally serialized by a private <see cref="_publishLock"/> so
/// applications never interleave — whichever call is last to hold the lock is the one whose snapshot
/// survives on disk, never a partial/corrupt mix of two writers. This lock is internal to this class
/// and intentionally not shared with any caller-side lock (e.g. <c>ConsoleRunEvents._gate</c>) —
/// callers must not couple their own synchronization to this writer's.
/// </summary>
public sealed class RunResultsWriter(RunPaths paths, string startedAtIso, TimeProvider? time = null)
{
    private readonly Lock _publishLock = new();

    public void WriteSnapshot(IReadOnlyList<NodeResult> completed, string status, RunPhasesPayload? phases = null,
        IReadOnlyList<LoadedExtension>? extensions = null)
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

            // Additive-optional, mirroring SqlRunArtifactStore's finished_at column: only the terminal
            // write (status != "running") stamps it, so `pz runs` can tell "still running" (or a crashed
            // run's last snapshot) apart from a completed one without needing anything beyond this file.
            if (status != "running")
            {
                writer.WriteString("finishedAt",
                    (time ?? TimeProvider.System).GetUtcNow().UtcDateTime
                        .ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture));
                WritePhases(writer, phases);
                WriteExtensions(writer, extensions);
            }

            writer.WriteStartArray("nodes");
            foreach (var node in completed)
            {
                WriteNode(writer, node);
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        lock (_publishLock)
        {
            AtomicFile.Write(paths.RunResultsPath, stream => stream.Write(buffer.WrittenSpan));
        }
    }

    // Terminal snapshot only; keys omitted when null, in the order the event stream uses.
    private static void WritePhases(Utf8JsonWriter writer, RunPhasesPayload? phases)
    {
        if (phases is null) return;
        writer.WriteStartObject("phases");
        WriteOptional(writer, "loadMs", phases.LoadMs);
        WriteOptional(writer, "compileMs", phases.CompileMs);
        WriteOptional(writer, "validateMs", phases.ValidateMs);
        WriteOptional(writer, "restoreCheckMs", phases.RestoreCheckMs);
        WriteOptional(writer, "planMs", phases.PlanMs);
        WriteOptional(writer, "executeMs", phases.ExecuteMs);
        WriteOptional(writer, "finalizeMs", phases.FinalizeMs);
        writer.WriteEndObject();
    }

    // Terminal snapshot only; omitted when the project lists no extensions. The version is the build
    // DuckDB reports, so a behaviour change can be traced to a new extension build.
    private static void WriteExtensions(Utf8JsonWriter writer, IReadOnlyList<LoadedExtension>? extensions)
    {
        if (extensions is not { Count: > 0 }) return;
        writer.WriteStartArray("extensions");
        foreach (var extension in extensions)
        {
            writer.WriteStartObject();
            writer.WriteString("name", extension.Name);
            writer.WriteString("version", extension.Version);
            writer.WriteEndObject();
        }

        writer.WriteEndArray();
    }

    private static void WriteOptional(Utf8JsonWriter writer, string name, long? value)
    {
        if (value is { } v) writer.WriteNumber(name, v);
    }

    private static void WriteNode(Utf8JsonWriter writer, NodeResult node)
    {
        writer.WriteStartObject();
        writer.WriteString("id", node.Id.Value);
        writer.WriteString("kind", node.Kind.ToString());
        writer.WriteString("name", node.Name);
        writer.WriteString("status", NodeResultJson.StatusName(node.Status));

        // Omitted entirely when null, so a normally-executed node writes no `provenance` key.
        if (NodeResultJson.ProvenanceName(node.Provenance) is { } provenance)
        {
            writer.WriteString("provenance", provenance);
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

        // Additive-optional blocks (timings, ops, partitions, delivery, cdc, observed_schema): each is
        // omitted entirely when null.
        NodeResultJson.WriteStatsBlocks(writer, node);

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

        // Whether a windowed source with a stopping point has caught up. Absent for every other node.
        if (node.CaughtUp is { } caughtUp)
        {
            writer.WriteBoolean("caughtUp", caughtUp);
        }

        // Appended after every older key so documents without facts stay byte-identical.
        NodeFactsJson.Write(writer, node.Facts);

        writer.WriteEndObject();
    }
}
