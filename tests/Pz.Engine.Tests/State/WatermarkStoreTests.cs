using Pz.Engine.State;

namespace Pz.Engine.Tests.State;

public sealed class WatermarkStoreTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    public static IEnumerable<object[]> SupportedTypes()
    {
        // Every supported cursor type paired with its canonical string form.
        yield return new object[] { "int", "42" };
        yield return new object[] { "bigint", "9223372036854775807" };
        yield return new object[] { "decimal", "12345.678900000" };
        yield return new object[] { "date", "2026-07-04" };
        yield return new object[] { "timestamp", "2026-07-04T10:00:00.000000" };
    }

    [Theory]
    [MemberData(nameof(SupportedTypes))]
    public void Set_then_get_roundtrips_all_supported_types(string typeName, string value)
    {
        var store = WatermarkStore.Local(_dir);
        var dataset = WatermarkStore.Key("crm", "orders");
        var wm = new Watermark("updated_at", typeName, value, "run-1");

        store.Set(dataset, wm);
        var result = store.Get(dataset);

        Assert.Equal(wm, result);
    }

    [Fact]
    public void Write_is_byte_stable_and_sorted()
    {
        var dirA = Path.Combine(_dir, "a");
        var dirB = Path.Combine(_dir, "b");
        var storeA = WatermarkStore.Local(dirA);
        var storeB = WatermarkStore.Local(dirB);

        // Insert in reverse-sorted order in one store and forward-sorted in the other, to prove
        // the on-disk order is always ordinal-by-key, never insertion order.
        storeA.Set("crm.orders", new Watermark("updated_at", "timestamp", "2026-07-04T10:00:00.000000", "run-1"));
        storeA.Set("acme.accounts", new Watermark("id", "bigint", "42", "run-1"));

        storeB.Set("acme.accounts", new Watermark("id", "bigint", "42", "run-1"));
        storeB.Set("crm.orders", new Watermark("updated_at", "timestamp", "2026-07-04T10:00:00.000000", "run-1"));

        var bytesA = File.ReadAllBytes(Path.Combine(dirA, "watermarks.json"));
        var bytesB = File.ReadAllBytes(Path.Combine(dirB, "watermarks.json"));
        Assert.Equal(bytesA, bytesB);

        var expected =
            "{\n" +
            "  \"version\": 1,\n" +
            "  \"watermarks\": {\n" +
            "    \"acme.accounts\": {\n" +
            "      \"cursor\": \"id\",\n" +
            "      \"type\": \"bigint\",\n" +
            "      \"value\": \"42\",\n" +
            "      \"runId\": \"run-1\"\n" +
            "    },\n" +
            "    \"crm.orders\": {\n" +
            "      \"cursor\": \"updated_at\",\n" +
            "      \"type\": \"timestamp\",\n" +
            "      \"value\": \"2026-07-04T10:00:00.000000\",\n" +
            "      \"runId\": \"run-1\"\n" +
            "    }\n" +
            "  }\n" +
            "}\n";
        Assert.Equal(expected, File.ReadAllText(Path.Combine(dirA, "watermarks.json")));
    }

    [Theory]
    [InlineData("{ this is not json at all")]
    [InlineData("null")]
    public void Corrupt_file_returns_null_with_notice(string content)
    {
        Directory.CreateDirectory(_dir);
        File.WriteAllText(Path.Combine(_dir, "watermarks.json"), content);
        var store = WatermarkStore.Local(_dir);

        string? notice = null;
        var result = store.Get("crm.orders", n => notice = n);

        Assert.Null(result);
        Assert.NotNull(notice);
        Assert.Contains("watermarks.json", notice);
    }

    [Fact]
    public void Missing_file_returns_null_without_notice()
    {
        var store = WatermarkStore.Local(_dir);
        var noticeFired = false;

        var result = store.Get("crm.orders", _ => noticeFired = true);

        Assert.Null(result);
        Assert.False(noticeFired);
    }

    [Fact]
    public void Set_preserves_other_entries()
    {
        var store = WatermarkStore.Local(_dir);
        var ordersWm = new Watermark("updated_at", "timestamp", "2026-07-04T10:00:00.000000", "run-1");
        var accountsWm = new Watermark("id", "bigint", "42", "run-2");
        store.Set("crm.orders", ordersWm);

        store.Set(WatermarkStore.Key("acme", "accounts"), accountsWm);

        Assert.Equal(ordersWm, store.Get("crm.orders"));
        Assert.Equal(accountsWm, store.Get(WatermarkStore.Key("acme", "accounts")));
    }

    [Fact]
    public void Set_over_corrupt_file_reestablishes_valid_state()
    {
        Directory.CreateDirectory(_dir);
        var filePath = Path.Combine(_dir, "watermarks.json");

        // Write garbage bytes to simulate corruption
        File.WriteAllBytes(filePath, new byte[] { 0xFF, 0xFE, 0x00, 0x00 });

        var store = WatermarkStore.Local(_dir);
        var dataset = WatermarkStore.Key("crm", "orders");
        var wm = new Watermark("updated_at", "timestamp", "2026-07-04T10:00:00.000000", "run-1");

        // Should not throw when writing over corrupt file
        store.Set(dataset, wm);

        // File now parses and Get returns the entry
        var result = store.Get(dataset);
        Assert.Equal(wm, result);

        // Verify file is valid (no notice on Get)
        string? notice = null;
        var result2 = store.Get(dataset, n => notice = n);
        Assert.Equal(wm, result2);
        Assert.Null(notice);

        // Verify byte stability: identical Set produces identical bytes
        var bytes1 = File.ReadAllBytes(filePath);
        store.Set(dataset, wm);
        var bytes2 = File.ReadAllBytes(filePath);
        Assert.Equal(bytes1, bytes2);
    }
}
