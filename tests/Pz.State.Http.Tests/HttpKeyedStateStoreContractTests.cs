using Pz.Engine.State;
using Pz.TestSupport.State;

namespace Pz.State.Http.Tests;

/// <summary>The one contract, run against the HTTP backend — missing-vs-corrupt, empty-vs-null, and
/// idempotent remove, pinned identically for all four implementations. No docker: each store gets its
/// own in-proc <see cref="FakeStateServer"/>.</summary>
public sealed class HttpKeyedStateStoreContractTests : KeyedStateStoreContract, IAsyncLifetime
{
    private readonly List<FakeStateServer> _servers = [];
    private readonly Dictionary<IKeyedStateStore<TestEntry>, FakeStateServer> _serverByStore = [];

    protected override IKeyedStateStore<TestEntry> NewStore()
    {
        var server = new FakeStateServer();
        _servers.Add(server);

        var store = server.Connect();
        _serverByStore[store] = server;
        return store;
    }

    /// <summary>Over HTTP, "present but unreadable" is the endpoint serving a payload that will not
    /// deserialize — the server's storage is untouched otherwise, versions included.</summary>
    protected override void CorruptStoredState(IKeyedStateStore<TestEntry> store) =>
        _serverByStore[store].CorruptEveryPayload();

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync()
    {
        foreach (var server in _servers)
        {
            await server.DisposeAsync();
        }
    }
}
