using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json;
using Pz.Core.Validation;
using Pz.Engine.State;

namespace Pz.State.Http;

/// <summary>The HTTP implementation of
/// <see cref="IKeyedStateStore{T}"/>, backed by the server's run-scoped state endpoints
/// (<c>/api/agents/runs/{id}/state/{scope}[/{key}]</c>). Structurally a twin of
/// <c>SqlKeyedStateStore</c> -- same scope-per-instance shape, same remembered-version CAS, same
/// PZ0520 on a lost race -- with HTTP status codes where SQL had row counts.
///
/// **What each status means here**, and why 404 is not "absent": only <c>204</c> is absence. A
/// <c>404</c> means the run id or scope in <c>PZ_STATE_URL</c> does not resolve, and swallowing that
/// as "no watermark stored" would silently re-extract from the beginning -- duplicates in append
/// mode, wasted work in merge mode. It throws PZ0518 instead. The contract's never-throw rule covers
/// state that is present but unreadable (a payload that will not deserialize -> null + notice), not a
/// transport that did not answer -- exactly the line the SQL backend draws at PZ0518.
///
/// **Versions come off the wire, never computed.** The server's store is append-only and a
/// <c>Remove</c> writes a tombstone, so re-creating a removed key lands at the tombstone's version
/// plus one, not at 1. The <c>ETag</c> on every 2xx is authoritative; a local <c>expected + 1</c>
/// would be wrong the first time anyone clears a watermark and sets it again.</summary>
public sealed class HttpKeyedStateStore<T>(
    HttpStateEndpoint endpoint,
    string scope,
    Func<JsonElement, T?> readEntry,
    Action<Utf8JsonWriter, T> writeEntry) : IKeyedStateStore<T> where T : class
{
    /// <summary>The version each key was last seen at by THIS instance -- the compare-and-swap token
    /// deliberately kept off <see cref="IKeyedStateStore{T}"/>. Concurrent for the same
    /// reason as the SQL backend's: one store instance serves a whole run and <see cref="Get"/> is
    /// called per node while SourceLoads run in parallel under the topological dispatcher.</summary>
    private readonly ConcurrentDictionary<string, int> _versions = new(StringComparer.Ordinal);

    public T? Get(string key, Action<string>? notice = null)
    {
        var response = endpoint.Send(HttpMethod.Get, scope, key);

        if (response.Status == HttpStatusCode.NoContent)
        {
            return null; // Absent (or tombstoned) -- silent, per the contract.
        }

        if (response.Status != HttpStatusCode.OK)
        {
            throw endpoint.Unexpected(response.Status, scope, key);
        }

        // Remembered even when the payload below turns out to be corrupt: a Set that follows a corrupt
        // Get must still be able to overwrite the entry (mirrors both other backends -- corrupt state
        // is never an error).
        Remember(key, response.Version);

        var value = ParsePayload(response.Body);
        if (value is null)
        {
            notice?.Invoke(
                $"state entry '{key}' (scope '{scope}') is corrupt or has an unexpected shape -- a full extract will occur.");
        }

        return value;
    }

    public IReadOnlyList<KeyValuePair<string, T>>? ListAll(Action<string>? notice = null)
    {
        var response = endpoint.Send(HttpMethod.Get, scope, key: null);
        if (response.Status != HttpStatusCode.OK)
        {
            throw endpoint.Unexpected(response.Status, scope, key: null);
        }

        List<(string Key, string Payload, int? Version)> rows;
        try
        {
            rows = ReadEntries(response.Body);
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException)
        {
            // The envelope itself did not parse: state IS stored (the server answered 200) but this
            // run cannot read it -- null, not empty, which is what `pz state show` exits 1 on.
            notice?.Invoke(
                $"state (scope '{scope}') came back in an unexpected shape -- a full extract will occur.");
            return null;
        }

        var results = new List<KeyValuePair<string, T>>();
        foreach (var (key, payload, version) in rows)
        {
            Remember(key, version);

            var value = ParsePayload(payload);
            if (value is null)
            {
                notice?.Invoke(
                    $"state (scope '{scope}') contains a corrupt or unexpected entry for key '{key}' -- a full extract will occur.");
                return null;
            }

            results.Add(new(key, value));
        }

        // Sorted here rather than trusting the server: the ordinal ordering is this contract's, and the
        // server's own ordering is not part of the wire contract.
        return results.OrderBy(kv => kv.Key, StringComparer.Ordinal).ToList();
    }

    public void Set(string key, T value)
    {
        var payload = SerializePayload(value);
        var expected = _versions.TryGetValue(key, out var known) ? known : (int?)null;
        var response = endpoint.Send(HttpMethod.Put, scope, key, payload, expected);

        switch (response.Status)
        {
            case HttpStatusCode.Created:
            case HttpStatusCode.NoContent:
                Remember(key, response.Version);
                return;

            case HttpStatusCode.Conflict:
                // The 409's ETag is the server's true current version, but it may belong to a
                // tombstone -- so it is deliberately NOT remembered here. A retry after a conflict must
                // re-Get and find out whether the key is present at all.
                _versions.TryRemove(key, out _);
                throw Conflict(key);

            default:
                throw endpoint.Unexpected(response.Status, scope, key);
        }
    }

    public void Remove(string key)
    {
        var response = endpoint.Send(HttpMethod.Delete, scope, key);
        _versions.TryRemove(key, out _);

        if (response.Status == HttpStatusCode.NoContent)
        {
            return; // Idempotent: absent, already tombstoned, or this call wrote the tombstone.
        }

        throw response.Status == HttpStatusCode.Conflict
            ? Conflict(key)
            : endpoint.Unexpected(response.Status, scope, key);
    }

    private void Remember(string key, int? version)
    {
        if (version is { } v)
        {
            _versions[key] = v;
        }
        else
        {
            // No usable ETag: forget rather than guess, so the next Set is an honest insert-if-absent.
            _versions.TryRemove(key, out _);
        }
    }

    /// <summary><c>{"entries":[{"key","payload","version"}]}</c>. <c>payload</c> is an opaque string,
    /// never embedded JSON -- re-emitting it through a writer would reformat it and break pz's
    /// byte-stability contract.</summary>
    private static List<(string Key, string Payload, int? Version)> ReadEntries(string body)
    {
        using var document = JsonDocument.Parse(body);
        var entries = document.RootElement.GetProperty("entries");
        var rows = new List<(string, string, int?)>();

        foreach (var entry in entries.EnumerateArray())
        {
            var key = entry.GetProperty("key").GetString()
                ?? throw new JsonException("state entry has a null key");
            var payload = entry.GetProperty("payload").GetString()
                ?? throw new JsonException("state entry has a null payload");
            var version = entry.TryGetProperty("version", out var v) && v.TryGetInt32(out var parsed)
                ? parsed
                : (int?)null;
            rows.Add((key, payload, version));
        }

        return rows;
    }

    private PzConfigException Conflict(string key) =>
        new(new PzError(PzErrorCode.StateConcurrencyConflict,
            $"state key '{key}' (scope '{scope}') was advanced by another run while this run was executing.",
            "project.yml", null,
            "re-run; if concurrent runs over the same datasets are intended, split them by dataset"));

    private T? ParsePayload(string payload)
    {
        try
        {
            using var document = JsonDocument.Parse(payload);
            return readEntry(document.RootElement);
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException or KeyNotFoundException)
        {
            return null;
        }
    }

    private string SerializePayload(T value)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            writeEntry(writer, value);
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }
}
