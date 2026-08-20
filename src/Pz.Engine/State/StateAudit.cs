using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace Pz.Engine.State;

/// <summary>One manual state mutation, as `pz state` requests it. No timestamp:
/// <see cref="StateAudit.Append"/> stamps it from the injected clock, so a caller cannot backdate the
/// ledger. Null fields are omitted from the rendered line entirely — the omit-when-absent discipline
/// <see cref="Pz.Engine.Artifacts.RunResultsWriter"/> uses for `provenance`/`timings`.</summary>
public sealed record StateAuditEntry(
    string Action,
    string Key,
    string? Cursor,
    string? Type,
    string? From,
    string? FromRunId,
    string? To,
    string? Target,
    string? Reason);

/// <summary>A ledger line as read back: the stored timestamp plus the entry it recorded.</summary>
public sealed record StateAuditLine(string Ts, StateAuditEntry Entry);

/// <summary>The append-only record of every manual watermark change. One NDJSON line per completed
/// write, fixed field order, LF endings.
///
/// It lives in `.pz/state/` for one reason: `pz clean` is structurally unable to touch that directory
/// (no --state, no --everything, no escape hatch), so the ledger outlives every
/// run directory it refers to. Nothing in the engine's run path ever opens it; it is write-only from the
/// engine's perspective and read only by `pz state show`.
///
/// It is an append-only LEDGER, not a deterministic ARTIFACT: it embeds real timestamps by design, so the
/// byte-stability contract governing `.pz/target` does not apply. Field order and LF endings are still
/// fixed, which is what keeps it greppable and diffable, and `ts` comes from an injected
/// <see cref="TimeProvider"/> so tests assert exact bytes.</summary>
public sealed class StateAudit(string stateDir, TimeProvider time)
{
    public const string FileName = "audit.jsonl";

    private const string TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fffZ";

    private string Path => System.IO.Path.Combine(stateDir, FileName);

    /// <summary>The exact ledger line for an entry, without its trailing newline. Public because
    /// <c>StateCommand</c> prints it verbatim in a warning when the append fails after a completed state
    /// write — an operator handed the line can append it by hand.</summary>
    public string Render(StateAuditEntry entry)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteString("ts", time.GetUtcNow().UtcDateTime.ToString(TimestampFormat, CultureInfo.InvariantCulture));
            writer.WriteString("action", entry.Action);
            writer.WriteString("key", entry.Key);
            WriteIfPresent(writer, "cursor", entry.Cursor);
            WriteIfPresent(writer, "type", entry.Type);
            WriteIfPresent(writer, "from", entry.From);
            WriteIfPresent(writer, "fromRunId", entry.FromRunId);
            WriteIfPresent(writer, "to", entry.To);
            WriteIfPresent(writer, "target", entry.Target);
            WriteIfPresent(writer, "reason", entry.Reason);
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    /// <summary>Appends one line. Creates `.pz/state/` and the file if absent. Never rewrites what is
    /// already there — an audit trail a later write can revise is not one.</summary>
    public void Append(StateAuditEntry entry)
    {
        Directory.CreateDirectory(stateDir);
        File.AppendAllText(Path, Render(entry) + "\n");
    }

    /// <summary>Every recorded change to one key, newest first. An unparseable line is skipped rather than
    /// fatal: the file is appended to by a process that can be killed mid-write, and a torn final line
    /// must not break `pz state show` — the verb an operator reaches for when things are already wrong.</summary>
    public IReadOnlyList<StateAuditLine> Read(string key)
    {
        if (!File.Exists(Path))
        {
            return [];
        }

        var lines = new List<StateAuditLine>();
        string[] raw;
        try
        {
            raw = File.ReadAllLines(Path);
        }
        catch (IOException)
        {
            return [];
        }

        foreach (var line in raw)
        {
            if (TryParse(line, key, out var parsed))
            {
                lines.Add(parsed!);
            }
        }

        lines.Reverse();
        return lines;
    }

    private static bool TryParse(string line, string key, out StateAuditLine? parsed)
    {
        parsed = null;
        if (string.IsNullOrWhiteSpace(line))
        {
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(line);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object ||
                !string.Equals(Text(root, "key"), key, StringComparison.Ordinal))
            {
                return false;
            }

            parsed = new StateAuditLine(
                Text(root, "ts") ?? "",
                new StateAuditEntry(
                    Text(root, "action") ?? "",
                    key,
                    Text(root, "cursor"),
                    Text(root, "type"),
                    Text(root, "from"),
                    Text(root, "fromRunId"),
                    Text(root, "to"),
                    Text(root, "target"),
                    Text(root, "reason")));
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static string? Text(JsonElement element, string name) =>
        element.TryGetProperty(name, out var value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;

    private static void WriteIfPresent(Utf8JsonWriter writer, string name, string? value)
    {
        if (value is not null)
        {
            writer.WriteString(name, value);
        }
    }
}
