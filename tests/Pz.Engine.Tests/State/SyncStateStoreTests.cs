using System.Text;
using Pz.Engine.State;
using Xunit;

namespace Pz.Engine.Tests.State;

public sealed class SyncStateStoreTests
{
    private static string TempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-syncstate-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public void Missing_file_returns_null_without_notice()
    {
        var store = SyncStateStore.Local(TempDir());
        var notices = new List<string>();
        Assert.Null(store.Get("s.d", notices.Add));
        Assert.Empty(notices);
    }

    [Fact]
    public void Set_then_get_roundtrips()
    {
        var dir = TempDir();
        SyncStateStore.Local(dir).Set("s.d", new SyncState("tok-1", "run-9"));
        var got = SyncStateStore.Local(dir).Get("s.d");
        Assert.NotNull(got);
        Assert.Equal("tok-1", got!.Token);
        Assert.Equal("run-9", got.RunId);
    }

    [Fact]
    public void File_is_byte_stable_sorted_with_trailing_newline()
    {
        var dir = TempDir();
        var store = SyncStateStore.Local(dir);
        store.Set("s.b", new SyncState("t-b", "r"));
        store.Set("s.a", new SyncState("t-a", "r"));
        var bytes = File.ReadAllBytes(Path.Combine(dir, "sync-state.json"));
        var text = Encoding.UTF8.GetString(bytes);
        Assert.EndsWith("\n", text);
        Assert.DoesNotContain("\r", text);
        Assert.True(text.IndexOf("s.a", StringComparison.Ordinal) < text.IndexOf("s.b", StringComparison.Ordinal));
    }

    [Fact]
    public void Corrupt_file_returns_null_with_notice()
    {
        var dir = TempDir();
        File.WriteAllText(Path.Combine(dir, "sync-state.json"), "{ not valid");
        var notices = new List<string>();
        Assert.Null(SyncStateStore.Local(dir).Get("s.d", notices.Add));
        Assert.Single(notices);
    }

    [Fact]
    public void Key_joins_source_and_dataset()
    {
        Assert.Equal("src.ds", SyncStateStore.Key("src", "ds"));
    }

    [Fact]
    public void Remove_existing_key_deletes_it_with_a_byte_stable_rewrite()
    {
        var dir = TempDir();
        var store = SyncStateStore.Local(dir);
        store.Set("s.a", new SyncState("t-a", "r"));
        store.Set("s.b", new SyncState("t-b", "r"));

        store.Remove("s.a");

        Assert.Null(store.Get("s.a"));
        Assert.NotNull(store.Get("s.b"));

        var bytes = File.ReadAllBytes(Path.Combine(dir, "sync-state.json"));
        var text = Encoding.UTF8.GetString(bytes);
        Assert.EndsWith("\n", text);
        Assert.DoesNotContain("\r", text);
        Assert.DoesNotContain("s.a", text);
        Assert.Contains("s.b", text);
    }

    [Fact]
    public void Remove_missing_key_is_a_no_op()
    {
        var dir = TempDir();
        var store = SyncStateStore.Local(dir);
        store.Set("s.b", new SyncState("t-b", "r"));
        var path = Path.Combine(dir, "sync-state.json");
        var before = File.ReadAllBytes(path);

        store.Remove("s.does-not-exist");

        Assert.Equal(before, File.ReadAllBytes(path));
    }

    [Fact]
    public void Remove_on_missing_file_is_a_no_op()
    {
        var dir = TempDir();
        var store = SyncStateStore.Local(dir);

        store.Remove("s.a"); // must not throw, must not create a file

        Assert.False(File.Exists(Path.Combine(dir, "sync-state.json")));
    }
}
