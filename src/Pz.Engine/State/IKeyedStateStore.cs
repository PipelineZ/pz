namespace Pz.Engine.State;

/// <summary>The keyed-state seam behind every pluggable state backend, mirroring
/// <see cref="KeyedJsonStateStore{T}"/>'s surface. One instance serves one SCOPE
/// ("watermarks" / "sync-state"), supplied at construction exactly as the file name is on the local
/// implementation.
///
/// It stays generic deliberately. A non-generic interface would have to pass entries as pre-serialized
/// JSON, and re-emitting a raw payload through an indented Utf8JsonWriter does not reproduce the
/// existing formatting — which would put the byte-stability contract and its golden files at risk for
/// no gain. With T, the local implementation is unchanged and each façade keeps its own
/// readEntry/writeEntry pair, so no store ever learns what a Watermark is.
///
/// Contract every implementation owes (pinned once by
/// <c>Pz.TestSupport.State.KeyedStateStoreContract</c>): a missing entry returns null SILENTLY; a
/// present-but-unreadable one returns null AND invokes <c>notice</c>, and never throws;
/// <see cref="ListAll"/> returns EMPTY when there is nothing stored and NULL when what is stored cannot
/// be read — a distinction `pz state show`'s exit code depends on; <see cref="Remove"/> is idempotent.</summary>
public interface IKeyedStateStore<T> where T : class
{
    T? Get(string key, Action<string>? notice = null);

    IReadOnlyList<KeyValuePair<string, T>>? ListAll(Action<string>? notice = null);

    void Set(string key, T value);

    void Remove(string key);
}
