using Pz.Engine.State;

namespace Pz.Engine.Tests.State;

/// <summary>The enumeration primitive `pz state show` needs. The
/// null-vs-empty distinction is the whole point — `show` exits 1 on a corrupt file and 0 on an empty
/// one, so a single "no entries" answer for both would make the exit code wrong.</summary>
public sealed class StateStoreListAllTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void Absent_file_lists_empty_and_gives_no_notice()
    {
        var notices = new List<string>();

        var entries = WatermarkStore.Local(_dir).ListAll(notices.Add);

        Assert.NotNull(entries);
        Assert.Empty(entries);
        Assert.Empty(notices);
    }

    [Fact]
    public void Entries_come_back_ordinal_sorted_by_key()
    {
        var store = WatermarkStore.Local(_dir);
        store.Set("crm.orders", new Watermark("updated_at", "timestamp", "2026-07-04T10:00:00.000000", "run-1"));
        store.Set("acme.accounts", new Watermark("id", "bigint", "42", "run-1"));
        store.Set("zeta.items", new Watermark("id", "int", "7", "run-2"));

        var entries = store.ListAll();

        Assert.NotNull(entries);
        Assert.Equal(["acme.accounts", "crm.orders", "zeta.items"], entries.Select(e => e.Key));
        Assert.Equal("42", entries[0].Value.Value);
    }

    [Fact]
    public void Corrupt_file_lists_null_and_gives_a_notice()
    {
        Directory.CreateDirectory(_dir);
        File.WriteAllText(Path.Combine(_dir, "watermarks.json"), "{ not json at all");
        var notices = new List<string>();

        var entries = WatermarkStore.Local(_dir).ListAll(notices.Add);

        Assert.Null(entries);
        Assert.Single(notices);
        Assert.Contains("corrupt", notices[0]);
    }

    [Fact]
    public void Sync_state_lists_through_the_same_primitive()
    {
        var store = SyncStateStore.Local(_dir);
        store.Set("erp.shipments", new SyncState("0/1A2B3C4D", "run-1"));

        var entries = store.ListAll();

        Assert.NotNull(entries);
        Assert.Equal("erp.shipments", Assert.Single(entries).Key);
        Assert.Equal("0/1A2B3C4D", entries[0].Value.Token);
    }

    [Fact]
    public void Watermark_remove_drops_one_entry_and_leaves_the_rest()
    {
        var store = WatermarkStore.Local(_dir);
        store.Set("crm.orders", new Watermark("updated_at", "timestamp", "2026-07-04T10:00:00.000000", "run-1"));
        store.Set("acme.accounts", new Watermark("id", "bigint", "42", "run-1"));

        store.Remove("crm.orders");

        var entries = store.ListAll();
        Assert.NotNull(entries);
        Assert.Equal("acme.accounts", Assert.Single(entries).Key);
    }

    [Fact]
    public void Watermark_remove_of_a_missing_key_is_a_no_op()
    {
        var store = WatermarkStore.Local(_dir);
        store.Set("acme.accounts", new Watermark("id", "bigint", "42", "run-1"));

        store.Remove("nope.nothing");

        Assert.Single(WatermarkStore.Local(_dir).ListAll()!);
    }
}
