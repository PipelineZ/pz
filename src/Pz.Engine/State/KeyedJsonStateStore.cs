using System.Text.Json;

namespace Pz.Engine.State;

/// <summary>The local implementation of <see cref="IKeyedStateStore{T}"/>, and the single
/// implementation behind <see cref="WatermarkStore"/> and <see cref="SyncStateStore"/>. One keyed JSON file under
/// .pz/state/: byte-stable writes (Utf8JsonWriter, 2-space indent, LF newlines, trailing newline
/// byte, entries ordinal-sorted by key, version 1 header) through a same-directory temp file +
/// atomic File.Move, so a reader never observes a partial file. Missing file: <see cref="Get"/>
/// returns null silently. Present-but-unparseable (garbage bytes, wrong shape, a null field): null
/// plus the notice callback -- never throws. <paramref name="readEntry"/> returns null to mark an
/// entry (and therefore the file) malformed; <paramref name="writeEntry"/> writes exactly the
/// entry's fields in their fixed order.</summary>
public sealed class KeyedJsonStateStore<T>(
    string stateDir,
    string fileName,
    string sectionName,
    string corruptNoticeSubject,
    Func<JsonElement, T?> readEntry,
    Action<Utf8JsonWriter, T> writeEntry) : IKeyedStateStore<T> where T : class
{
    public T? Get(string key, Action<string>? notice = null)
    {
        var path = Path.Combine(stateDir, fileName);
        if (!File.Exists(path))
        {
            return null;
        }

        if (!TryReadAll(path, out var entries))
        {
            notice?.Invoke($"{corruptNoticeSubject} '{path}' is corrupt or has an unexpected shape -- a full extract will occur.");
            return null;
        }

        return entries.TryGetValue(key, out var value) ? value : null;
    }

    /// <summary>Every entry, ordinal-ascending by key — the enumeration `pz state show` needs.
    /// Returns an EMPTY list when the file is absent (normal on first run, no notice, matching
    /// <see cref="Get"/>'s silence) and NULL when it exists but cannot be parsed (plus the notice). That
    /// distinction is load-bearing: `pz state show` exits 1 on a corrupt file and 0 on an empty one, and
    /// a single "no entries" answer for both would make the exit code wrong.
    ///
    /// The sort lives here rather than relying on the file already being sorted: <see cref="WriteAll"/>
    /// does sort, but <see cref="TryReadAll"/> lands entries in a <see cref="Dictionary{TKey,TValue}"/>
    /// whose enumeration order is not contractual.</summary>
    public IReadOnlyList<KeyValuePair<string, T>>? ListAll(Action<string>? notice = null)
    {
        var path = Path.Combine(stateDir, fileName);
        if (!File.Exists(path))
        {
            return [];
        }

        if (!TryReadAll(path, out var entries))
        {
            notice?.Invoke($"{corruptNoticeSubject} '{path}' is corrupt or has an unexpected shape -- a full extract will occur.");
            return null;
        }

        return entries.OrderBy(kv => kv.Key, StringComparer.Ordinal).ToList();
    }

    public void Set(string key, T value)
    {
        var path = Path.Combine(stateDir, fileName);
        // If the file is corrupt (exists but fails to parse), treat it as empty. This is safe:
        // corrupt state means no entries (never an error); the engine always reads Get()
        // before writing Set(), and the read-side surfaces a notice so the operator knows a full
        // extract will occur; thus overwriting here re-establishes valid state rather than losing
        // recoverable data.
        var entries = File.Exists(path) && TryReadAll(path, out var existing)
            ? new Dictionary<string, T>(existing, StringComparer.Ordinal)
            : new Dictionary<string, T>(StringComparer.Ordinal);

        entries[key] = value;
        WriteAll(entries, path);
    }

    /// <summary>Removes one entry (`pz cdc drop`) so the next run treats the dataset as never-synced.
    /// Mirrors <see cref="Set"/>'s read-modify-write + byte-stable rewrite;
    /// a missing file or a missing/already-absent key is a no-op (idempotent, matches <see cref="Get"/>'s
    /// missing-file silence -- a caller removing a key that was never there, or already gone, should
    /// never see an error). A corrupt file is likewise treated as empty (same rationale as <see
    /// cref="Set"/>'s corrupt-file handling): there is nothing to remove, so no rewrite happens at all.</summary>
    public void Remove(string key)
    {
        var path = Path.Combine(stateDir, fileName);
        if (!File.Exists(path) || !TryReadAll(path, out var entries) || !entries.Remove(key))
        {
            return;
        }

        WriteAll(entries, path);
    }

    private bool TryReadAll(string path, out Dictionary<string, T> entries)
    {
        entries = new Dictionary<string, T>(StringComparer.Ordinal);
        try
        {
            using var document = JsonDocument.Parse(File.ReadAllBytes(path));
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object ||
                !root.TryGetProperty(sectionName, out var section) ||
                section.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            foreach (var entry in section.EnumerateObject())
            {
                if (readEntry(entry.Value) is not { } value)
                {
                    return false;
                }

                entries[entry.Name] = value;
            }

            return true;
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException or KeyNotFoundException or IOException)
        {
            entries = new Dictionary<string, T>(StringComparer.Ordinal);
            return false;
        }
    }

    private void WriteAll(IReadOnlyDictionary<string, T> entries, string path)
    {
        var dir = Path.GetDirectoryName(path)!;
        Directory.CreateDirectory(dir);

        var tmpPath = $"{path}.{Guid.NewGuid():N}.tmp";
        var moved = false;
        try
        {
            using (var stream = File.Create(tmpPath))
            {
                using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true, IndentSize = 2, NewLine = "\n" }))
                {
                    writer.WriteStartObject();
                    writer.WriteNumber("version", 1);
                    writer.WriteStartObject(sectionName);
                    foreach (var (key, value) in entries.OrderBy(kv => kv.Key, StringComparer.Ordinal))
                    {
                        writer.WriteStartObject(key);
                        writeEntry(writer, value);
                        writer.WriteEndObject();
                    }

                    writer.WriteEndObject();
                    writer.WriteEndObject();
                }

                stream.WriteByte((byte)'\n');
            }

            File.Move(tmpPath, path, overwrite: true);
            moved = true;
        }
        finally
        {
            if (!moved)
            {
                try { File.Delete(tmpPath); } catch { /* best-effort cleanup -- never mask the real exception */ }
            }
        }
    }
}
