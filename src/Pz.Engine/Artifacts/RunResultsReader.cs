using System.Text.Json;
using Pz.Engine.Execution;
using Pz.Engine.State;

namespace Pz.Engine.Artifacts;

/// <summary>The persisted slice identity of a prior run's SourceLoad: the JSON <c>watermark</c>
/// object's cursor/type/value. No runId — the reusing run stamps its own when re-materializing a
/// <see cref="Pz.Engine.State.Watermark"/> from this.</summary>
public sealed record PriorWatermark(string Cursor, string Type, string Value);

/// <summary>The persisted <c>error</c> object of a prior run's failed node, read back so the MCP result
/// envelope can say WHY a node failed instead of a bare "failed" status.</summary>
public sealed record PriorError(string Code, string Message);

/// <summary>One node from a prior run's <c>run_results.json</c>, trimmed to exactly what
/// <c>pz retry</c>'s selection needs: <see cref="Id"/> is the content-hash node id
/// (matched against the freshly recompiled dag to detect a changed node), <see cref="Name"/> is used
/// only for the human-readable "changed since the failed run" notice. <see cref="Observed"/> is
/// additive (null when the prior node carried no <c>observed_schema</c>) — round-tripped for
/// completeness/future consumers; `pz retry`'s own selection logic does not read it.
/// <see cref="Error"/> is additive (null for a non-failed node), read by the MCP execution tools'
/// result envelope.</summary>
public sealed record PriorNode(string Id, string Name, string Status,
    string Kind = "", long Rows = 0, PriorWatermark? Watermark = null, ObservedSchema? Observed = null,
    PriorError? Error = null);

/// <summary>The prior run <see cref="RunResultsReader.ReadLatest"/> found: <see cref="Status"/> is
/// exactly as last written by <see cref="RunResultsWriter"/>. Usually a terminal status
/// ("success" | "completed_with_failures" | "fatal"), but can also be the intermediate "running" value
/// <see cref="RunResultsWriter.WriteSnapshot"/> writes after every node completion — the value a crashed
/// run's LAST snapshot is left holding, since nothing ever wrote a terminal status over it. `pz retry`
/// treats "running" as non-retryable: the recorded nodes may all show "success"
/// even though the run never finished, so selecting on node status alone would misreport "nothing to
/// retry".</summary>
public sealed record PriorRun(string RunId, string Status, IReadOnlyList<PriorNode> Nodes);

/// <summary>Reads the most recent parseable <c>run_results.json</c> under <c>.pz/runs/</c> for
/// <c>pz retry</c>. Run ids sort lexicographically by design (<see cref="Pz.Cli"/>'s run id
/// format is a zero-padded UTC timestamp), so "most recent" is simply "greatest by ordinal string
/// comparison" — no need to parse <c>startedAt</c>. A run dir whose file is missing, mid-write (a stray
/// <c>.tmp</c> only, no committed file — <see cref="RunResultsWriter"/>'s atomic-rename discipline means
/// this is the only way a real file could be absent), or unparseable is skipped in favor of the next
/// older one, rather than failing <c>pz retry</c> outright.</summary>
public static class RunResultsReader
{
    public static PriorRun? ReadLatest(string projectDir) => ReadAllNewestFirst(projectDir).FirstOrDefault();

    /// <summary>Every parseable run, newest first, evaluated lazily — `pz state`'s rollback menu needs
    /// the whole list while <see cref="ReadLatest"/> must still parse exactly one file.
    /// A run dir whose file is missing, mid-write, or unparseable is skipped in favor of the next older
    /// one, matching `pz retry`'s behavior.</summary>
    public static IEnumerable<PriorRun> ReadAllNewestFirst(string projectDir)
    {
        var runsDir = Path.Combine(projectDir, ".pz", "runs");
        if (!Directory.Exists(runsDir))
        {
            yield break;
        }

        var runIds = Directory.EnumerateDirectories(runsDir)
            .Select(Path.GetFileName)
            .Where(name => !string.IsNullOrEmpty(name))
            .OrderByDescending(name => name, StringComparer.Ordinal);

        foreach (var runId in runIds)
        {
            var path = Path.Combine(runsDir, runId!, "run_results.json");
            if (!File.Exists(path))
            {
                continue;
            }

            if (TryParse(runId!, path, out var parsed))
            {
                yield return parsed!;
            }
        }
    }

    private static bool TryParse(string runId, string path, out PriorRun? result)
    {
        try
        {
            using var document = JsonDocument.Parse(File.ReadAllBytes(path));
            var root = document.RootElement;
            var status = root.GetProperty("status").GetString()!;

            var nodes = new List<PriorNode>();
            foreach (var node in root.GetProperty("nodes").EnumerateArray())
            {
                PriorWatermark? watermark = null;
                if (node.TryGetProperty("watermark", out var wmElement) && wmElement.ValueKind == JsonValueKind.Object)
                {
                    var cursor = wmElement.GetProperty("cursor").GetString();
                    var type = wmElement.GetProperty("type").GetString();
                    var value = wmElement.GetProperty("value").GetString();
                    if (cursor is not null && type is not null && value is not null)
                    {
                        watermark = new PriorWatermark(cursor, type, value);
                    }
                }

                ObservedSchema? observed = null;
                if (node.TryGetProperty("observed_schema", out var obsElement) &&
                    obsElement.ValueKind == JsonValueKind.Object)
                {
                    var hintsHash = obsElement.TryGetProperty("hintsHash", out var hh) ? hh.GetString() : null;
                    if (hintsHash is not null && obsElement.TryGetProperty("columns", out var colsElement) &&
                        colsElement.ValueKind == JsonValueKind.Array)
                    {
                        var columns = new List<SchemaColumn>();
                        foreach (var col in colsElement.EnumerateArray())
                        {
                            var name = col.TryGetProperty("name", out var n) ? n.GetString() : null;
                            var type = col.TryGetProperty("type", out var t) ? t.GetString() : null;
                            if (name is not null && type is not null)
                            {
                                columns.Add(new SchemaColumn(name, type));
                            }
                        }

                        observed = new ObservedSchema(columns, hintsHash);
                    }
                }

                PriorError? error = null;
                if (node.TryGetProperty("error", out var errElement) && errElement.ValueKind == JsonValueKind.Object)
                {
                    var code = errElement.TryGetProperty("code", out var c) ? c.GetString() : null;
                    var message = errElement.TryGetProperty("message", out var m) ? m.GetString() : null;
                    if (code is not null && message is not null)
                    {
                        error = new PriorError(code, message);
                    }
                }

                nodes.Add(new PriorNode(
                    node.GetProperty("id").GetString()!,
                    node.GetProperty("name").GetString()!,
                    node.GetProperty("status").GetString()!,
                    node.TryGetProperty("kind", out var kind) ? kind.GetString() ?? "" : "",
                    node.TryGetProperty("rows", out var rows) && rows.TryGetInt64(out var rowCount) ? rowCount : 0,
                    watermark, observed, error));
            }

            result = new PriorRun(runId, status, nodes);
            return true;
        }
        catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException or IOException)
        {
            // Unparseable/incomplete snapshot -- try the next older run rather than failing `pz retry`.
            result = null;
            return false;
        }
    }
}
