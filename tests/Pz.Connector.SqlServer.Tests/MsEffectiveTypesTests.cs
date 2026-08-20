using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.Connector.SqlServer;

namespace Pz.Connector.SqlServer.Tests;

public sealed class MsEffectiveTypesTests
{
    private static Schema StringSchema(params string[] names) =>
        new(names.Select(n => new Field(n, StringType.Default, nullable: true)).ToList(), null);

    private static OutputSpec Spec(
        IReadOnlyDictionary<string, object?>? columns = null,
        IReadOnlyDictionary<string, long>? stats = null) =>
        new("s", "dbo.t", "append", "fail_on_change",
            columns is null
                ? new Dictionary<string, object?>()
                : new Dictionary<string, object?> { ["columns"] = columns })
        { MaxTextLengths = stats };

    [Fact]
    public void Declared_beats_derived_beats_fallback()
    {
        var resolved = MsEffectiveTypes.Resolve(
            Spec(columns: new Dictionary<string, object?> { ["a"] = "nvarchar(20)" },
                 stats: new Dictionary<string, long> { ["a"] = 3000, ["b"] = 10 }),
            StringSchema("a", "b", "c"));
        Assert.Equal("nvarchar(20)", resolved.Types["a"]);   // declared wins over stats
        Assert.Equal("nvarchar(32)", resolved.Types["b"]);   // 10*2=20 -> bucket 32
        Assert.Equal("nvarchar(4000)", resolved.Types["c"]); // no observation -> fallback
        Assert.Equal(new HashSet<string> { "a" }, resolved.Declared);
    }

    [Theory]
    [InlineData(0, "nvarchar(16)")]
    [InlineData(8, "nvarchar(16)")]
    [InlineData(9, "nvarchar(32)")]
    [InlineData(500, "nvarchar(1000)")]
    [InlineData(1500, "nvarchar(4000)")]  // 3000 -> next bucket is 4000
    [InlineData(2000, "nvarchar(4000)")]
    [InlineData(3000, "nvarchar(4000)")]  // 2x exceeds 4000, observed <= 4000 -> pinned to 4000
    [InlineData(4000, "nvarchar(4000)")]
    [InlineData(4001, "nvarchar(max)")]   // real data larger than 4000 is never truncated
    public void Buckets_apply_2x_headroom(long observed, string expected)
    {
        var resolved = MsEffectiveTypes.Resolve(
            Spec(stats: new Dictionary<string, long> { ["a"] = observed }), StringSchema("a"));
        Assert.Equal(expected, resolved.Types["a"]);
    }

    [Fact]
    public void Null_stats_map_falls_back_for_every_string_column()
    {
        var resolved = MsEffectiveTypes.Resolve(Spec(), StringSchema("a"));
        Assert.Equal("nvarchar(4000)", resolved.Types["a"]);
    }

    [Fact]
    public void Non_string_columns_keep_the_ddltype_default()
    {
        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, nullable: true),
            new Field("amount", new Decimal128Type(38, 9), nullable: true),
        ], null);
        var resolved = MsEffectiveTypes.Resolve(Spec(), schema);
        Assert.Equal("bigint", resolved.Types["id"]);
        Assert.Equal("decimal(38,9)", resolved.Types["amount"]);
    }

    [Fact]
    public void All_columns_errors_are_reported_in_one_exception()
    {
        var ex = Assert.Throws<PzConnectorException>(() => MsEffectiveTypes.Resolve(
            Spec(columns: new Dictionary<string, object?>
            {
                ["nope"] = "nvarchar(20)",     // unknown column
                ["a"] = "text",                // bad grammar
            }),
            StringSchema("a")));
        Assert.Contains("nope", ex.Message);
        Assert.Contains("text", ex.Message);
        Assert.Contains("dbo.t", ex.Message); // names the output
        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Non_map_columns_value_is_an_error()
    {
        var spec = new OutputSpec("s", "dbo.t", "append", "fail_on_change",
            new Dictionary<string, object?> { ["columns"] = "nvarchar(20)" });
        var ex = Assert.Throws<PzConnectorException>(() => MsEffectiveTypes.Resolve(spec, StringSchema("a")));
        Assert.Contains("columns", ex.Message);
    }
}
