using System.Globalization;
using System.Text.Json;
using Pz.Connectors.TestKit;

namespace Pz.State.Http.Tests;

using TestEntry = Pz.TestSupport.State.KeyedStateStoreContract.TestEntry;

/// <summary>The server half of the wire contract, in-proc: the four run-scoped state
/// endpoints, scripted on the <see cref="StubHttpServer"/> the connector suites already use (so this
/// suite needs neither docker nor a new dependency).
///
/// **Storage here is append-only, because the contract says a server's may be.** A key's version
/// counter never resets: <see cref="Remove"/> writes a tombstone at version+1, so re-creating a
/// removed key lands at version+2, not at 1. A fake that restarted versions at 1 would let pz's store
/// get away with computing the next version locally instead of reading the ETag the server
/// returned — so the harder legal behavior is the one modeled.</summary>
internal sealed class FakeStateServer : IAsyncDisposable
{
    private static readonly string[] Scopes = ["watermarks", "sync-state"];
    private const int MaxKeyLength = 512;

    /// <summary>ASP.NET Core's own defaults, so the list envelope is camelCase on the wire the way an
    /// ASP.NET server serves it -- the property names are part of the contract pz reads.</summary>
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private readonly StubHttpServer _server = new();
    private readonly Dictionary<(string Scope, string Key), Entry> _state = [];
    private readonly List<HttpStateEndpoint> _endpoints = [];
    private readonly Guid _runId = Guid.NewGuid();

    public FakeStateServer() => _server.MapPrefix("/api/agents/runs/", Handle);

    /// <summary>A store pointed at this server, disposed with it. Two calls give two independent
    /// stores over the same storage — which is how a CAS conflict is staged deterministically, without
    /// racing threads.</summary>
    public HttpKeyedStateStore<TestEntry> Connect(string? token = null, string? url = null)
    {
        var endpoint = new HttpStateEndpoint(url ?? Url, token);
        _endpoints.Add(endpoint);
        return new HttpKeyedStateStore<TestEntry>(endpoint, "watermarks",
            readEntry: static entry =>
            {
                var value = entry.GetProperty("value").GetString();
                var runId = entry.GetProperty("runId").GetString();
                return value is null || runId is null ? null : new TestEntry(value, runId);
            },
            writeEntry: static (writer, e) =>
            {
                writer.WriteString("value", e.Value);
                writer.WriteString("runId", e.RunId);
            });
    }

    /// <summary>The run-scoped base URL an agent would hand pz as PZ_STATE_URL.</summary>
    public string Url => $"{_server.BaseUrl}api/agents/runs/{_runId}/state";

    /// <summary>Same shape, a run this server has never heard of — every endpoint answers 404.</summary>
    public string UnknownRunUrl => $"{_server.BaseUrl}api/agents/runs/{Guid.NewGuid()}/state";

    public IReadOnlyList<StubRequest> Requests => _server.Requests;

    /// <summary>Present but unreadable: every live payload becomes something that will not
    /// deserialize, with versions untouched (so a following write still has a real CAS token).</summary>
    public void CorruptEveryPayload()
    {
        lock (_state)
        {
            foreach (var key in _state.Keys.ToList())
            {
                _state[key] = _state[key] with { Payload = "{ not json at all" };
            }
        }
    }

    private StubResponse Handle(StubRequest request)
    {
        var segments = request.Url.AbsolutePath.Split('/', StringSplitOptions.RemoveEmptyEntries);

        // /api/agents/runs/{id}/state[/{scope}[/{key}]]
        if (segments.Length < 6 || !string.Equals(segments[4], "state", StringComparison.Ordinal))
        {
            return new StubResponse(404, "");
        }

        var scope = segments[5];
        if (!Scopes.Contains(scope, StringComparer.Ordinal)) return new StubResponse(404, "");
        if (!Guid.TryParse(segments[3], out var runId) || runId != _runId) return new StubResponse(404, "");

        if (segments.Length == 6)
        {
            return request.Method == "GET" ? List(scope) : new StubResponse(405, "");
        }

        // AbsolutePath keeps reserved characters escaped, so this is the caller's original key back.
        var key = Uri.UnescapeDataString(segments[6]);
        if (!IsValidKey(key)) return new StubResponse(400, "");

        return request.Method switch
        {
            "GET" => Get(scope, key),
            "PUT" => Put(request, scope, key),
            "DELETE" => Delete(scope, key),
            _ => new StubResponse(405, ""),
        };
    }

    private StubResponse List(string scope)
    {
        lock (_state)
        {
            var entries = _state
                .Where(kv => string.Equals(kv.Key.Scope, scope, StringComparison.Ordinal) && !kv.Value.Deleted)
                .OrderBy(kv => kv.Key.Key, StringComparer.Ordinal)
                .Select(kv => new EntryDto(kv.Key.Key, kv.Value.Payload, kv.Value.Version))
                .ToList();

            return new StubResponse(200, JsonSerializer.Serialize(new ListDto(entries), Web));
        }
    }

    private StubResponse Get(string scope, string key)
    {
        lock (_state)
        {
            return _state.TryGetValue((scope, key), out var entry) && !entry.Deleted
                ? new StubResponse(200, entry.Payload, Tag(entry.Version))
                : new StubResponse(204, "");
        }
    }

    private StubResponse Put(StubRequest request, string scope, string key)
    {
        if (string.IsNullOrWhiteSpace(request.Body)) return new StubResponse(400, "");
        if (!TryReadIfMatch(request, out var expected)) return new StubResponse(400, "");

        lock (_state)
        {
            _state.TryGetValue((scope, key), out var entry);
            var live = entry is { Deleted: false };

            if (expected is null)
            {
                if (live) return new StubResponse(409, "", Tag(entry!.Version));
            }
            else if (!live || entry!.Version != expected.Value)
            {
                return new StubResponse(409, "", Tag(entry?.Version ?? 0));
            }

            var next = (entry?.Version ?? 0) + 1;
            _state[(scope, key)] = new Entry(request.Body, next, Deleted: false);
            return new StubResponse(expected is null ? 201 : 204, "", Tag(next));
        }
    }

    private StubResponse Delete(string scope, string key)
    {
        lock (_state)
        {
            if (_state.TryGetValue((scope, key), out var entry) && !entry.Deleted)
            {
                _state[(scope, key)] = entry with { Version = entry.Version + 1, Deleted = true };
            }

            return new StubResponse(204, "");
        }
    }

    /// <summary>Absent = insert-if-absent. `"0"` and `*` are 400 — versions are 1-based and the
    /// wildcard is not supported.</summary>
    private static bool TryReadIfMatch(StubRequest request, out int? expected)
    {
        expected = null;
        if (!request.Headers.TryGetValue("If-Match", out var raw) || raw.Trim().Length == 0)
        {
            return true;
        }

        raw = raw.Trim();
        if (raw.Length < 3 || raw[0] != '"' || raw[^1] != '"') return false;
        if (!int.TryParse(raw[1..^1], NumberStyles.None, CultureInfo.InvariantCulture, out var version)
            || version <= 0)
        {
            return false;
        }

        expected = version;
        return true;
    }

    private static bool IsValidKey(string key) =>
        !string.IsNullOrWhiteSpace(key) && key.Length <= MaxKeyLength && !key.Any(char.IsControl);

    private static Dictionary<string, string> Tag(int version) =>
        new(StringComparer.Ordinal) { ["ETag"] = $"\"{version.ToString(CultureInfo.InvariantCulture)}\"" };

    public async ValueTask DisposeAsync()
    {
        foreach (var endpoint in _endpoints)
        {
            endpoint.Dispose();
        }

        await _server.DisposeAsync();
    }

    private sealed record Entry(string Payload, int Version, bool Deleted);

    private sealed record EntryDto(string Key, string Payload, int Version);

    private sealed record ListDto(IReadOnlyList<EntryDto> Entries);
}
