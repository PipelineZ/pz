using System.Buffers;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Pz.Core.Validation;

namespace Pz.Engine.State;

/// <summary>The local implementation of <see cref="IKeyedStateStore{T}"/>, and the single
/// implementation behind <see cref="WatermarkStore"/> and <see cref="SyncStateStore"/>. One keyed JSON file under
/// .pz/state/: byte-stable writes (Utf8JsonWriter, 2-space indent, LF newlines, trailing newline
/// byte, entries ordinal-sorted by key, version 1 header) through a same-directory temp file +
/// atomic File.Move, so a reader never observes a partial file. Missing file: <see cref="Get"/>
/// returns null silently. Present-but-unparseable (garbage bytes, wrong shape, a null field): null
/// plus the notice callback -- never throws. <paramref name="readEntry"/> returns null to mark an
/// entry (and therefore the file) malformed; <paramref name="writeEntry"/> writes exactly the
/// entry's fields in their fixed order.
///
/// **Overlapping runs share this file**, and a write is a read-modify-write of all of it. Every write
/// therefore happens under an OS-held lock on a sibling <c>.lock</c> file, so two runs advancing
/// different keys never drop each other's entry. For the SAME key the store keeps the contract the
/// remote backends keep with a version column: this instance remembers what each <see cref="Get"/>
/// saw, and a <see cref="Set"/> that finds something else there now lost a race with another run —
/// PZ0520, never a silent overwrite, because the later finisher may carry the older MAX(cursor) and
/// would regress the watermark. The token stays off <see cref="IKeyedStateStore{T}"/> for the same
/// reason it does there: it matches the run's access pattern (Get at plan time, Set once at
/// advancement, one instance per run). A key this instance never read was never observed, so it
/// cannot have gone stale: the state-editing verbs write that way and simply overwrite.</summary>
public sealed class KeyedJsonStateStore<T>(
    string stateDir,
    string fileName,
    string sectionName,
    string corruptNoticeSubject,
    Func<JsonElement, T?> readEntry,
    Action<Utf8JsonWriter, T> writeEntry) : IKeyedStateStore<T> where T : class
{
    /// <summary>How long a write waits for another process's write to finish. Holders keep the lock
    /// for one small file rewrite, so reaching this means a wedged process, not contention.</summary>
    private static readonly TimeSpan LockTimeout = TimeSpan.FromSeconds(30);

    /// <summary>What "no entry" looks like in <see cref="_seen"/>; a serialized entry is never empty.</summary>
    private const string Absent = "";

    /// <summary>Each key's entry as THIS instance last read or wrote it, serialized. Concurrent because
    /// <see cref="Get"/> is called per node from executors the dispatcher runs in parallel.</summary>
    private readonly ConcurrentDictionary<string, string> _seen = new(StringComparer.Ordinal);

    public T? Get(string key, Action<string>? notice = null)
    {
        var path = Path.Combine(stateDir, fileName);
        T? value = null;
        if (File.Exists(path))
        {
            if (TryReadAll(path, out var entries))
            {
                entries.TryGetValue(key, out value);
            }
            else
            {
                // Set treats a corrupt file as empty, so that is what this read observed.
                notice?.Invoke($"{corruptNoticeSubject} '{path}' is corrupt or has an unexpected shape -- a full extract will occur.");
            }
        }

        _seen[key] = value is null ? Absent : Serialize(value);
        return value;
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
        using var fileLock = AcquireLock(path);
        // If the file is corrupt (exists but fails to parse), treat it as empty. This is safe:
        // corrupt state means no entries (never an error); the engine always reads Get()
        // before writing Set(), and the read-side surfaces a notice so the operator knows a full
        // extract will occur; thus overwriting here re-establishes valid state rather than losing
        // recoverable data.
        var entries = File.Exists(path) && TryReadAll(path, out var existing)
            ? new Dictionary<string, T>(existing, StringComparer.Ordinal)
            : new Dictionary<string, T>(StringComparer.Ordinal);

        if (_seen.TryGetValue(key, out var seen))
        {
            var current = entries.TryGetValue(key, out var stored) ? Serialize(stored) : Absent;
            if (!string.Equals(current, seen, StringComparison.Ordinal))
            {
                throw new PzConfigException(new PzError(PzErrorCode.StateConcurrencyConflict,
                    $"state key '{key}' in '{path}' was advanced by another run while this run was executing.",
                    "project.yml", null,
                    "re-run; if concurrent runs over the same datasets are intended, split them by dataset"));
            }
        }

        entries[key] = value;
        WriteAll(entries, path);
        _seen[key] = Serialize(value);
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
        if (!File.Exists(path))
        {
            return;
        }

        using var fileLock = AcquireLock(path);
        _seen.TryRemove(key, out _); // an explicit removal is not a stale read of what it removed
        if (!TryReadAll(path, out var entries) || !entries.Remove(key))
        {
            return;
        }

        WriteAll(entries, path);
    }

    private string Serialize(T value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writeEntry(writer, value);
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    /// <summary>Exclusive across processes AND across store instances in this one: .NET holds
    /// <see cref="FileShare.None"/> as an advisory flock on Unix and a native share mode on Windows, and
    /// the OS drops either when the holder dies, so a killed run never wedges the next. The lock is a
    /// sibling file, not the state file itself, because the state file is replaced by rename on every
    /// write — a lock on it would be a lock on an inode about to be unlinked. It is never deleted:
    /// removing it would let a waiter lock the old inode while a newcomer creates and locks a new one.</summary>
    private static FileStream AcquireLock(string path)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var lockPath = path + ".lock";
        var deadline = Environment.TickCount64 + (long)LockTimeout.TotalMilliseconds;
        while (true)
        {
            try
            {
                return new FileStream(lockPath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);
            }
            catch (IOException) when (Environment.TickCount64 < deadline)
            {
                Thread.Sleep(15);
            }
            catch (IOException ex)
            {
                throw new PzConfigException(new PzError(PzErrorCode.StateStoreUnavailable,
                    $"state file '{path}' stayed locked by another pz process for {LockTimeout.TotalSeconds:0}s: {ex.Message}",
                    "project.yml", null,
                    "check for a hung pz process in this project; the lock frees itself when that process exits"));
            }
        }
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
