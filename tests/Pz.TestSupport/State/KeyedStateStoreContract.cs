using Pz.Engine.State;

namespace Pz.TestSupport.State;

/// <summary>One contract, run against every
/// <see cref="IKeyedStateStore{T}"/> implementation, mirroring how the connector TestKit enforces one
/// contract across connectors. This is the only place the missing-vs-corrupt, empty-vs-null, and
/// idempotent-remove semantics are pinned — so the backends cannot drift.</summary>
public abstract class KeyedStateStoreContract
{
    /// <summary>A fresh, empty store. Each call must be independent of the last.</summary>
    protected abstract IKeyedStateStore<TestEntry> NewStore();

    /// <summary>Force this store's already-written state to be present but unreadable — what "corrupt"
    /// means differs per backend (garbage bytes over the JSON file locally, an unparseable payload
    /// column in a SQL store), so only the implementation can stage it.</summary>
    protected abstract void CorruptStoredState(IKeyedStateStore<TestEntry> store);

    public sealed record TestEntry(string Value, string RunId);

    [SkippableFact]
    public void Get_on_a_missing_key_is_null_and_silent()
    {
        var store = NewStore();
        var notices = new List<string>();

        Assert.Null(store.Get("absent", notices.Add));
        Assert.Empty(notices);
    }

    [SkippableFact]
    public void Get_on_unreadable_state_returns_null_and_notices()
    {
        var store = NewStore();
        store.Set("a", new TestEntry("1", "run-1"));
        CorruptStoredState(store);
        var notices = new List<string>();

        // Never throws: an exception out of Get fails this test rather than reaching the engine.
        Assert.Null(store.Get("a", notices.Add));
        Assert.Single(notices);
    }

    [SkippableFact]
    public void Set_then_get_roundtrips()
    {
        var store = NewStore();
        store.Set("a", new TestEntry("1", "run-1"));

        var got = store.Get("a");

        Assert.NotNull(got);
        Assert.Equal("1", got!.Value);
        Assert.Equal("run-1", got.RunId);
    }

    [SkippableFact]
    public void Set_preserves_other_entries()
    {
        var store = NewStore();
        store.Set("a", new TestEntry("1", "run-1"));
        store.Set("b", new TestEntry("2", "run-1"));
        store.Set("a", new TestEntry("3", "run-2"));

        Assert.Equal("3", store.Get("a")!.Value);
        Assert.Equal("2", store.Get("b")!.Value);
    }

    [SkippableFact]
    public void ListAll_on_an_empty_store_is_empty_not_null()
    {
        // Load-bearing: `pz state show` exits 0 on empty and 1 on corrupt, so these must differ.
        Assert.Equal([], NewStore().ListAll());
    }

    [SkippableFact]
    public void ListAll_on_unreadable_state_returns_null_and_notices()
    {
        // NULL, not empty — the other half of the load-bearing distinction above: `pz state show`
        // exits 1 on a corrupt store and 0 on an empty one, so collapsing the two breaks the exit code.
        var store = NewStore();
        store.Set("a", new TestEntry("1", "run-1"));
        CorruptStoredState(store);
        var notices = new List<string>();

        Assert.Null(store.ListAll(notices.Add));
        Assert.Single(notices);
    }

    [SkippableFact]
    public void ListAll_is_ordinal_by_key()
    {
        var store = NewStore();
        store.Set("b", new TestEntry("2", "r"));
        store.Set("A", new TestEntry("1", "r"));
        store.Set("a", new TestEntry("3", "r"));

        Assert.Equal(["A", "a", "b"], store.ListAll()!.Select(kv => kv.Key).ToArray());
    }

    [SkippableFact]
    public void Remove_drops_one_entry_and_is_idempotent()
    {
        var store = NewStore();
        store.Set("a", new TestEntry("1", "r"));

        store.Remove("a");
        store.Remove("a");
        store.Remove("never-existed");

        Assert.Null(store.Get("a"));
    }
}
