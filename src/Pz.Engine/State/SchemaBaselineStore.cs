using System.Text.Json;

namespace Pz.Engine.State;

public sealed record SchemaColumn(string Name, string Type);

/// <summary>What one accepted schema looks like; HintsHash guards config-change reseeds.</summary>
public sealed record SchemaBaseline(IReadOnlyList<SchemaColumn> Columns, string HintsHash, string RunId);

/// <summary>Reads and writes <c>&lt;project&gt;/.pz/state/schemas.json</c> -- the per-dataset accepted
/// schema store (schema-drift design). Byte-stable per <see cref="KeyedJsonStateStore{T}"/>'s discipline:
/// 2-space indentation, LF newlines, a trailing newline byte, and entries sorted ordinal by key. Mirrors
/// <see cref="WatermarkStore"/> exactly -- same lenient-corrupt-read, same local/SQL/HTTP composition
/// shape.</summary>
public sealed class SchemaBaselineStore(IKeyedStateStore<SchemaBaseline> store)
{
    /// <summary>The local backend, wired to <c>&lt;project&gt;/.pz/state/schemas.json</c>.</summary>
    public static SchemaBaselineStore Local(string stateDir) => new(new KeyedJsonStateStore<SchemaBaseline>(
        stateDir, "schemas.json", "schemas", "Schema baseline file", ReadEntry, WriteEntry));

    /// <summary>The read/write pair for this façade's entry shape, shared with the SQL and HTTP backends
    /// so every backend serializes a <see cref="SchemaBaseline"/> identically. Lenient like
    /// <see cref="WatermarkStore.ReadEntry"/> -- any missing/mistyped part yields null rather than
    /// throwing.</summary>
    public static SchemaBaseline? ReadEntry(JsonElement entry)
    {
        var hintsHash = entry.TryGetProperty("hintsHash", out var h) ? h.GetString() : null;
        var runId = entry.TryGetProperty("runId", out var r) ? r.GetString() : null;
        if (hintsHash is null || runId is null ||
            !entry.TryGetProperty("columns", out var cols) || cols.ValueKind != JsonValueKind.Array)
        {
            return null;
        }

        var list = new List<SchemaColumn>();
        foreach (var col in cols.EnumerateArray())
        {
            var name = col.TryGetProperty("name", out var n) ? n.GetString() : null;
            var type = col.TryGetProperty("type", out var t) ? t.GetString() : null;
            if (name is null || type is null) { return null; }
            list.Add(new SchemaColumn(name, type));
        }

        return new SchemaBaseline(list, hintsHash, runId);
    }

    /// <summary>Fixed field order (columns, hintsHash, runId) for byte-stability.</summary>
    public static void WriteEntry(Utf8JsonWriter writer, SchemaBaseline b)
    {
        writer.WriteStartArray("columns");
        foreach (var col in b.Columns)
        {
            writer.WriteStartObject();
            writer.WriteString("name", col.Name);
            writer.WriteString("type", col.Type);
            writer.WriteEndObject();
        }
        writer.WriteEndArray();
        writer.WriteString("hintsHash", b.HintsHash);
        writer.WriteString("runId", b.RunId);
    }

    public SchemaBaseline? Get(string key, Action<string>? notice = null) => store.Get(key, notice);

    public void Set(string key, SchemaBaseline baseline) => store.Set(key, baseline);

    public IReadOnlyList<KeyValuePair<string, SchemaBaseline>>? ListAll(Action<string>? notice = null) =>
        store.ListAll(notice);

    public static string Key(string connection, string entity) => $"{connection}.{entity}";
}
