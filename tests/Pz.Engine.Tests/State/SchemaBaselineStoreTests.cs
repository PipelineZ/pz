using Pz.Engine.State;

namespace Pz.Engine.Tests.State;

public sealed class SchemaBaselineStoreTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public void Set_then_get_roundtrips_and_preserves_column_order()
    {
        var store = SchemaBaselineStore.Local(_dir);
        var key = SchemaBaselineStore.Key("pg", "orders");
        var baseline = new SchemaBaseline(
            [new SchemaColumn("id", "bigint"), new SchemaColumn("updated_at", "timestamp")],
            "hash-1",
            "run-1");

        store.Set(key, baseline);
        var result = store.Get(key);

        Assert.NotNull(result);
        Assert.Equal(baseline.Columns, result!.Columns);
        Assert.Equal(baseline.HintsHash, result.HintsHash);
        Assert.Equal(baseline.RunId, result.RunId);
        Assert.Equal(["id", "updated_at"], result.Columns.Select(c => c.Name).ToArray());
    }

    [Fact]
    public void Corrupt_entry_missing_hintsHash_returns_null_with_notice()
    {
        Directory.CreateDirectory(_dir);
        var content =
            "{\n" +
            "  \"version\": 1,\n" +
            "  \"schemas\": {\n" +
            "    \"pg.orders\": {\n" +
            "      \"columns\": [],\n" +
            "      \"runId\": \"run-1\"\n" +
            "    }\n" +
            "  }\n" +
            "}\n";
        File.WriteAllText(Path.Combine(_dir, "schemas.json"), content);
        var store = SchemaBaselineStore.Local(_dir);

        string? notice = null;
        var result = store.Get("pg.orders", n => notice = n);

        Assert.Null(result);
        Assert.NotNull(notice);
        Assert.Contains("schemas.json", notice);
    }

    [Fact]
    public void Key_joins_connection_and_entity_with_dot()
    {
        Assert.Equal("pg.orders", SchemaBaselineStore.Key("pg", "orders"));
    }

    [Fact]
    public void Write_is_byte_stable_and_sorted()
    {
        var dirA = Path.Combine(_dir, "a");
        var dirB = Path.Combine(_dir, "b");
        var storeA = SchemaBaselineStore.Local(dirA);
        var storeB = SchemaBaselineStore.Local(dirB);

        var orders = new SchemaBaseline(
            [new SchemaColumn("id", "bigint"), new SchemaColumn("updated_at", "timestamp")],
            "hash-orders",
            "run-1");
        var accounts = new SchemaBaseline(
            [new SchemaColumn("id", "bigint")],
            "hash-accounts",
            "run-1");

        storeA.Set("pg.orders", orders);
        storeA.Set("acme.accounts", accounts);

        storeB.Set("acme.accounts", accounts);
        storeB.Set("pg.orders", orders);

        var bytesA = File.ReadAllBytes(Path.Combine(dirA, "schemas.json"));
        var bytesB = File.ReadAllBytes(Path.Combine(dirB, "schemas.json"));
        Assert.Equal(bytesA, bytesB);

        var expected =
            "{\n" +
            "  \"version\": 1,\n" +
            "  \"schemas\": {\n" +
            "    \"acme.accounts\": {\n" +
            "      \"columns\": [\n" +
            "        {\n" +
            "          \"name\": \"id\",\n" +
            "          \"type\": \"bigint\"\n" +
            "        }\n" +
            "      ],\n" +
            "      \"hintsHash\": \"hash-accounts\",\n" +
            "      \"runId\": \"run-1\"\n" +
            "    },\n" +
            "    \"pg.orders\": {\n" +
            "      \"columns\": [\n" +
            "        {\n" +
            "          \"name\": \"id\",\n" +
            "          \"type\": \"bigint\"\n" +
            "        },\n" +
            "        {\n" +
            "          \"name\": \"updated_at\",\n" +
            "          \"type\": \"timestamp\"\n" +
            "        }\n" +
            "      ],\n" +
            "      \"hintsHash\": \"hash-orders\",\n" +
            "      \"runId\": \"run-1\"\n" +
            "    }\n" +
            "  }\n" +
            "}\n";
        Assert.Equal(expected, File.ReadAllText(Path.Combine(dirA, "schemas.json")));
    }
}
