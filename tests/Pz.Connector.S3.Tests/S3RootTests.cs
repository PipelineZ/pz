using Pz.Connectors.Abstractions;

namespace Pz.Connector.S3.Tests;

/// <summary>The connection's <c>root:</c> says which lake —
/// <c>&lt;bucket&gt;</c> or <c>&lt;bucket&gt;/&lt;prefix&gt;</c> — and the entity says which dataset in
/// it. An output naming its own bucket/path still works: that is composition at a different level, not
/// a second declaration of the same thing. Pure SQL generation, so no container is needed.</summary>
public class S3RootTests
{
    private static readonly Dictionary<string, object?> Credentials = new()
    {
        ["access_key"] = "AKIA_TEST",
        ["secret_key"] = "S3CRET_VALUE",
    };

    private static string CopySql(string? root, string output = "curated",
        IReadOnlyDictionary<string, object?>? options = null)
    {
        var values = new Dictionary<string, object?>(Credentials);
        if (root is not null)
        {
            values["root"] = root;
        }

        var sink = new S3Sink(new ConnectorConfig(values));
        var spec = new OutputSpec("lake", output, "replace", "fail_on_change",
            options ?? new Dictionary<string, object?> { ["format"] = "parquet" });

        Assert.True(sink.TryGetNativeCopy(spec, out var copy));
        return copy!.CopySql;
    }

    [Fact]
    public void Root_supplies_the_bucket_and_the_entity_names_the_object() =>
        Assert.Contains("'s3://my-bucket/curated.parquet'", CopySql("my-bucket"), StringComparison.Ordinal);

    [Fact]
    public void Root_may_carry_a_key_prefix() =>
        Assert.Contains("'s3://my-bucket/warehouse/curated.parquet'",
            CopySql("my-bucket/warehouse"), StringComparison.Ordinal);

    [Fact]
    public void Surrounding_slashes_in_root_are_ignored() =>
        Assert.Contains("'s3://my-bucket/warehouse/curated.parquet'",
            CopySql("/my-bucket/warehouse/"), StringComparison.Ordinal);

    [Fact]
    public void An_output_path_composes_under_the_root_prefix() =>
        Assert.Contains("'s3://my-bucket/warehouse/daily/curated.parquet'",
            CopySql("my-bucket/warehouse",
                options: new Dictionary<string, object?> { ["format"] = "parquet", ["path"] = "daily" }),
            StringComparison.Ordinal);

    // No root at all: the output names everything.
    [Fact]
    public void An_explicit_bucket_still_works_without_root() =>
        Assert.Contains("'s3://other/out/curated.parquet'",
            CopySql(root: null,
                options: new Dictionary<string, object?>
                {
                    ["format"] = "parquet",
                    ["bucket"] = "other",
                    ["path"] = "out",
                }),
            StringComparison.Ordinal);

    // An output naming a DIFFERENT bucket must not inherit the root's prefix -- that prefix belongs to
    // the root's bucket, and silently pasting it onto another one would write to a path nobody asked for.
    [Fact]
    public void An_output_bucket_overriding_root_does_not_inherit_its_prefix() =>
        Assert.Contains("'s3://other/curated.parquet'",
            CopySql("my-bucket/warehouse",
                options: new Dictionary<string, object?> { ["format"] = "parquet", ["bucket"] = "other" }),
            StringComparison.Ordinal);

    [Fact]
    public void Neither_root_nor_bucket_is_a_named_error()
    {
        var sink = new S3Sink(new ConnectorConfig(Credentials));
        var spec = new OutputSpec("lake", "curated", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["format"] = "parquet" });

        var ex = Assert.Throws<PzConnectorException>(() => sink.TryGetNativeCopy(spec, out _));

        Assert.False(ex.IsTransient);
        Assert.Contains("root", ex.Message, StringComparison.Ordinal);
        Assert.Contains("bucket", ex.Message, StringComparison.Ordinal);
    }
}
