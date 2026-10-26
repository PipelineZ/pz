using System.Text.Json;

namespace Pz.Engine.State;

/// <summary>Reads and writes <c>&lt;project&gt;/.pz/state/watermarks.json</c> -- the per-dataset
/// incremental-extraction cursor store. Byte-stable per <see cref="Pz.Engine.Artifacts.SchemaCacheWriter"/>'s
/// discipline: <see cref="System.Text.Json.Utf8JsonWriter"/> with 2-space indentation, LF newlines, a trailing newline
/// byte, and entries sorted ordinal by key. Writes go through a same-directory temp file plus
/// <see cref="File.Move(string, string, bool)"/> (mirroring <see cref="Pz.Engine.Artifacts.RunResultsWriter"/>)
/// so a reader never observes a partially-written file.
///
/// A missing file is normal on first run -- <see cref="Get"/> returns null silently. A present-but-
/// unparseable file (garbage bytes, or JSON that doesn't match the expected shape, e.g. literal
/// <c>null</c>) also yields a null result but additionally invokes the <c>notice</c> callback so the
/// caller can tell the operator a full extract will occur -- it never throws.</summary>
public sealed class WatermarkStore(IKeyedStateStore<Watermark> store)
{
    private readonly IKeyedStateStore<Watermark> _store = store;

    /// <summary>The local backend, wired to <c>&lt;project&gt;/.pz/state/watermarks.json</c>.</summary>
    public static WatermarkStore Local(string stateDir) => new(new KeyedJsonStateStore<Watermark>(
        stateDir, "watermarks.json", "watermarks", "Watermark state file", ReadEntry, WriteEntry));

    /// <summary>The read/write pair for this façade's entry shape, shared with the SQL backend
    /// (<c>Pz.State.SqlServer.SqlKeyedStateStore{Watermark}</c>) so both backends serialize a
    /// <see cref="Watermark"/> identically.</summary>
    public static Watermark? ReadEntry(JsonElement entry)
    {
        var cursor = entry.GetProperty("cursor").GetString();
        var typeName = entry.GetProperty("type").GetString();
        var value = entry.GetProperty("value").GetString();
        var runId = entry.GetProperty("runId").GetString();
        return cursor is null || typeName is null || value is null || runId is null
            ? null
            : new Watermark(cursor, typeName, value, runId);
    }

    public static void WriteEntry(Utf8JsonWriter writer, Watermark wm)
    {
        writer.WriteString("cursor", wm.Cursor);
        writer.WriteString("type", wm.TypeName);
        writer.WriteString("value", wm.Value);
        writer.WriteString("runId", wm.RunId);
    }

    public Watermark? Get(string sourceDataset, Action<string>? notice = null) => _store.Get(sourceDataset, notice);

    public void Set(string sourceDataset, Watermark wm) => _store.Set(sourceDataset, wm);

    /// <summary>Every stored watermark, ordinal by key. Null when the file is corrupt, empty when
    /// absent — see <see cref="KeyedJsonStateStore{T}.ListAll"/>.</summary>
    public IReadOnlyList<KeyValuePair<string, Watermark>>? ListAll(Action<string>? notice = null) =>
        _store.ListAll(notice);

    /// <summary>`pz state clear` drops an entry so the next run extracts in full. The one remedy for an
    /// entry whose cursor type pz has no arithmetic for, which
    /// <see cref="Pz.Core.Incremental.WindowMath"/> can neither canonicalize nor compare. See
    /// <see cref="KeyedJsonStateStore{T}.Remove"/> for the no-op-on-missing semantics.</summary>
    public void Remove(string sourceDataset) => _store.Remove(sourceDataset);

    public static string Key(string source, string dataset) => $"{source}.{dataset}";
}
