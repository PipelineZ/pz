using System.Net;
using Pz.Core.Validation;

namespace Pz.State.Http.Tests;

/// <summary>What the shared <c>KeyedStateStoreContract</c> cannot reach: the failure modes HTTP has and
/// SQL does not. Every test drives a real socket against <see cref="FakeStateServer"/>, so the request that
/// went out is the request the server would see.</summary>
public sealed class HttpKeyedStateStoreTests
{
    [Fact]
    public async Task A_stale_writer_loses_the_CAS_race_with_PZ0520()
    {
        // Two stores = two run processes over one server, driven explicitly rather than raced.
        await using var server = new FakeStateServer();
        var first = server.Connect();
        var second = server.Connect();

        first.Set("orders", new("seed", "run-0"));
        Assert.NotNull(first.Get("orders"));
        Assert.NotNull(second.Get("orders")); // both now hold version 1

        first.Set("orders", new("1", "run-1")); // version 2

        var ex = Assert.Throws<PzConfigException>(() => second.Set("orders", new("2", "run-2")));

        Assert.Equal(PzErrorCode.StateConcurrencyConflict, ex.Error.Code);
        Assert.Equal("1", first.Get("orders")!.Value); // nothing clobbered
    }

    [Fact]
    public async Task An_insert_over_a_live_key_is_PZ0520()
    {
        await using var server = new FakeStateServer();
        var first = server.Connect();
        var second = server.Connect(); // never read the key, so its Set is insert-if-absent

        first.Set("orders", new("1", "run-1"));

        var ex = Assert.Throws<PzConfigException>(() => second.Set("orders", new("2", "run-2")));

        Assert.Equal(PzErrorCode.StateConcurrencyConflict, ex.Error.Code);
    }

    [Fact]
    public async Task A_204_is_absence_and_stays_silent()
    {
        await using var server = new FakeStateServer();
        var store = server.Connect();
        var notices = new List<string>();

        Assert.Null(store.Get("never-written", notices.Add));
        Assert.Empty(notices);
        Assert.Equal([], store.ListAll());
    }

    [Fact]
    public async Task A_404_is_PZ0518_not_absence()
    {
        // The trap this guards: swallowing "wrong run id in PZ_STATE_URL" as "no watermark stored"
        // would silently re-extract from the beginning of every source.
        await using var server = new FakeStateServer();
        var store = server.Connect(url: server.UnknownRunUrl);

        var get = Assert.Throws<PzConfigException>(() => store.Get("orders"));
        var list = Assert.Throws<PzConfigException>(() => store.ListAll());

        Assert.Equal(PzErrorCode.StateStoreUnavailable, get.Error.Code);
        Assert.Equal(PzErrorCode.StateStoreUnavailable, list.Error.Code);
        Assert.Contains("PZ_STATE_URL", get.Error.Hint, StringComparison.Ordinal);
    }

    [Fact]
    public async Task An_unreachable_server_is_PZ0518()
    {
        // Nothing listening on this port: a transport failure, not a corrupt-state notice.
        using var endpoint = new HttpStateEndpoint($"http://127.0.0.1:{FreePort()}/api/agents/runs/x/state", null);
        var store = new HttpKeyedStateStore<Entry>(endpoint, "watermarks",
            static e => new Entry(e.GetProperty("value").GetString()!),
            static (w, e) => w.WriteString("value", e.Value));

        var ex = Assert.Throws<PzConfigException>(() => store.Get("orders"));

        Assert.Equal(PzErrorCode.StateStoreUnavailable, ex.Error.Code);
        await Task.CompletedTask;
    }

    [Fact]
    public async Task A_key_needing_escaping_round_trips_as_one_path_segment()
    {
        await using var server = new FakeStateServer();
        var store = server.Connect();
        const string key = "ops.orders +awkward#chars%";

        store.Set(key, new("1", "run-1"));

        Assert.Equal("1", store.Get(key)!.Value);
        Assert.Equal([key], store.ListAll()!.Select(kv => kv.Key));

        // The escaping is real, not the server being lenient: '#' would otherwise start a fragment.
        var put = server.Requests.Single(r => r.Method == "PUT");
        Assert.Contains("%23", put.Url.AbsolutePath, StringComparison.Ordinal);
        Assert.DoesNotContain("#", put.Url.AbsolutePath, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_removed_key_is_recreated_at_the_servers_next_version_not_at_one()
    {
        // The server's store is append-only: Remove tombstones at version+1, so a re-create lands at
        // version+2. A store that computed versions locally would send If-Match "1" here and get a 409.
        await using var server = new FakeStateServer();
        var store = server.Connect();

        store.Set("orders", new("1", "run-1"));
        store.Remove("orders");
        store.Set("orders", new("2", "run-2"));
        store.Set("orders", new("3", "run-3")); // needs the version the server actually assigned

        Assert.Equal("3", store.Get("orders")!.Value);
    }

    [Fact]
    public async Task A_configured_token_is_sent_as_a_bearer_header_and_an_absent_one_sends_nothing()
    {
        await using var withToken = new FakeStateServer();
        withToken.Connect(token: "s3cret").Set("orders", new("1", "run-1"));

        await using var without = new FakeStateServer();
        without.Connect().Set("orders", new("1", "run-1"));

        Assert.Equal("Bearer s3cret", withToken.Requests[0].Headers["Authorization"]);
        Assert.DoesNotContain("Authorization", without.Requests[0].Headers.Keys, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Set_sends_If_Match_only_once_it_knows_a_version()
    {
        await using var server = new FakeStateServer();
        var store = server.Connect();

        store.Set("orders", new("1", "run-1")); // insert-if-absent: no If-Match
        store.Set("orders", new("2", "run-2")); // knows version 1 now

        var puts = server.Requests.Where(r => r.Method == "PUT").ToList();
        Assert.False(puts[0].Headers.ContainsKey("If-Match"));
        Assert.Equal("\"1\"", puts[1].Headers["If-Match"]);
    }

    private static int FreePort()
    {
        var probe = new System.Net.Sockets.TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
    }

    private sealed record Entry(string Value);
}
