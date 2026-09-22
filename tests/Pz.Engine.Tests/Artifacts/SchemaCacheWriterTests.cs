using Pz.Engine.Artifacts;

namespace Pz.Engine.Tests.Artifacts;

public sealed class SchemaCacheWriterTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private static Dictionary<string, string> Schemas() => new(StringComparer.Ordinal)
    {
        ["crm.orders"] = "amount: Double, id: Int64",
        ["crm.customers"] = "id: Int64, name: Utf8",
    };

    [Fact]
    public void Schemas_json_is_byte_stable_across_writes()
    {
        var schemas = Schemas();
        var dirA = Path.Combine(_dir, "a");
        var dirB = Path.Combine(_dir, "b");

        SchemaCacheWriter.Write(schemas, dirA);
        SchemaCacheWriter.Write(schemas, dirB);

        var bytesA = File.ReadAllBytes(Path.Combine(dirA, "schemas.json"));
        var bytesB = File.ReadAllBytes(Path.Combine(dirB, "schemas.json"));
        Assert.Equal(bytesA, bytesB);
    }

    [Fact]
    public void Schemas_json_keys_sorted_field_order_and_final_newline()
    {
        SchemaCacheWriter.Write(Schemas(), _dir);

        var text = File.ReadAllText(Path.Combine(_dir, "schemas.json"));

        var expected =
            "{\n" +
            "  \"version\": 1,\n" +
            "  \"schemas\": {\n" +
            "    \"crm.customers\": \"id: Int64, name: Utf8\",\n" +
            "    \"crm.orders\": \"amount: Double, id: Int64\"\n" +
            "  }\n" +
            "}\n";

        Assert.Equal(expected, text);
    }

    [Fact]
    public void Empty_schemas_still_writes_a_valid_file()
    {
        SchemaCacheWriter.Write(new Dictionary<string, string>(), _dir);

        var text = File.ReadAllText(Path.Combine(_dir, "schemas.json"));

        Assert.Equal("{\n  \"version\": 1,\n  \"schemas\": {}\n}\n", text);
    }

    /// <summary>Mirrors <c>PlanWriterTests</c>' sibling test: <c>SchemaCacheWriter</c> writes aside and
    /// renames into place (via <c>AtomicFile</c>) rather than opening <c>schemas.json</c> directly, so
    /// overlapping `pz validate --connect` runs in one project never collide on the same open handle
    /// and a reader never observes a partially-written file.</summary>
    [Fact]
    public void Overlapping_writers_all_succeed_and_leave_one_complete_file()
    {
        Parallel.For(0, 8, _ =>
        {
            for (var i = 0; i < 50; i++)
            {
                SchemaCacheWriter.Write(Schemas(), _dir);
            }
        });

        var text = File.ReadAllText(Path.Combine(_dir, "schemas.json"));
        Assert.Contains("\"crm.orders\": \"amount: Double, id: Int64\"", text, StringComparison.Ordinal);
        Assert.Equal(["schemas.json"], Directory.EnumerateFiles(_dir).Select(Path.GetFileName));
    }
}
