using System.Text.Json;

namespace Pz.Engine.State;

/// <summary>Reads and writes <c>&lt;project&gt;/.pz/state/sync-state.json</c> -- the per-dataset opaque
/// sync-state store. Byte-stable exactly like <see cref="WatermarkStore"/>: 2-space indent,
/// LF newlines, a trailing newline byte, entries ordinal-sorted by key, temp-file + atomic
/// <see cref="File.Move(string, string, bool)"/>. Missing file -> <see cref="Get"/> returns null
/// silently; corrupt/unexpected-shape -> null plus the <c>notice</c> callback (a full extract will
/// occur); never throws.</summary>
public sealed class SyncStateStore(IKeyedStateStore<SyncState> store)
{
    private readonly IKeyedStateStore<SyncState> _store = store;

    /// <summary>The local backend, wired to <c>&lt;project&gt;/.pz/state/sync-state.json</c>.</summary>
    public static SyncStateStore Local(string stateDir) => new(new KeyedJsonStateStore<SyncState>(
        stateDir, "sync-state.json", "syncState", "Sync-state file", ReadEntry, WriteEntry));

    /// <summary>The read/write pair for this façade's entry shape, shared with the SQL backend
    /// (<c>Pz.State.SqlServer.SqlKeyedStateStore{SyncState}</c>) so both backends serialize a
    /// <see cref="SyncState"/> identically.</summary>
    public static SyncState? ReadEntry(JsonElement entry)
    {
        var token = entry.GetProperty("token").GetString();
        var runId = entry.GetProperty("runId").GetString();
        return token is null || runId is null
            ? null
            : new SyncState(token, runId);
    }

    public static void WriteEntry(Utf8JsonWriter writer, SyncState state)
    {
        writer.WriteString("token", state.Token);
        writer.WriteString("runId", state.RunId);
    }

    public SyncState? Get(string sourceDataset, Action<string>? notice = null) => _store.Get(sourceDataset, notice);

    public void Set(string sourceDataset, SyncState state) => _store.Set(sourceDataset, state);

    /// <summary>`pz state show` reports sync state read-only — no write
    /// subcommand accepts a sync-state key, because an opaque connector token has no previous value to roll
    /// back to. `pz cdc drop` remains the way to clear one.</summary>
    public IReadOnlyList<KeyValuePair<string, SyncState>>? ListAll(Action<string>? notice = null) =>
        _store.ListAll(notice);

    /// <summary>`pz cdc drop` calls this so the next run re-snapshots the dataset instead of resuming
    /// from a now-dropped position. See <see
    /// cref="KeyedJsonStateStore{T}.Remove"/> for the no-op-on-missing/corrupt semantics.</summary>
    public void Remove(string sourceDataset) => _store.Remove(sourceDataset);

    public static string Key(string source, string dataset) => $"{source}.{dataset}";
}
