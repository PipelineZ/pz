using Pz.Connectors.Abstractions;

namespace Pz.Connector.Gcs.Tests;

/// <summary>Offline proof of the gcs sink's NATIVE tier: under hmac every non-partitioned output is
/// a DuckDB COPY over httpfs with the scoped sink secret (the s3 shapes over <c>gs://</c>), while a
/// non-hmac connection or a partitioned output declines native so the planner routes the universal
/// write-session tier instead.</summary>
public sealed class GcsSinkNativeTests
{
    private static ConnectorConfig Hmac(string? root = "my-bucket/out") =>
        new(new Dictionary<string, object?>
        {
            ["auth"] = "hmac",
            ["key_id"] = "GOOGHMAC_TEST",
            ["secret"] = "HMAC_SECRET_VALUE",
            ["root"] = root,
        });

    private static ConnectorConfig Adc(string? root = "my-bucket/out") =>
        new(new Dictionary<string, object?> { ["auth"] = "adc", ["root"] = root });

    private static OutputSpec Out(
        string output = "daily", string mode = "replace", string? format = "parquet", string? path = null,
        string? bucket = null, IReadOnlyList<string>? partitionBy = null)
    {
        var options = new Dictionary<string, object?>();
        if (format is not null) options["format"] = format;
        if (path is not null) options["path"] = path;
        if (bucket is not null) options["bucket"] = bucket;
        if (partitionBy is not null) options["partition_by"] = partitionBy;
        return new OutputSpec("lake", output, mode, "strict", options);
    }

    [Fact]
    public void Replace_parquet_is_a_stable_named_copy()
    {
        Assert.True(new GcsSink(Hmac()).TryGetNativeCopy(Out(), out var copy));
        Assert.Equal(
            "copy (select * from {{source}}) to 'gs://my-bucket/out/daily.parquet' (format parquet)",
            copy!.CopySql);
        Assert.Equal("COPY TO gcs parquet", copy.Mechanism);
        Assert.Equal(3, copy.SetupStatements.Count);
        Assert.StartsWith("create or replace secret pz_gcs_snk_lake_", copy.SetupStatements[2], StringComparison.Ordinal);
    }

    [Fact]
    public void Append_mode_lands_a_guid_suffixed_object()
    {
        Assert.True(new GcsSink(Hmac()).TryGetNativeCopy(Out(mode: "append", format: "csv"), out var copy));
        Assert.Contains("to 'gs://my-bucket/out/daily-", copy!.CopySql, StringComparison.Ordinal);
        Assert.EndsWith(".csv' (format csv, header)", copy.CopySql, StringComparison.Ordinal);
    }

    [Fact]
    public void Json_uses_the_ndjson_copy_shape()
    {
        Assert.True(new GcsSink(Hmac()).TryGetNativeCopy(Out(format: "json"), out var copy));
        Assert.EndsWith("daily.json' (format json)", copy!.CopySql, StringComparison.Ordinal);
    }

    [Fact]
    public void Output_path_rides_under_the_root_prefix()
    {
        Assert.True(new GcsSink(Hmac()).TryGetNativeCopy(Out(path: "curated/daily"), out var copy));
        Assert.Contains("to 'gs://my-bucket/out/curated/daily/daily.parquet'", copy!.CopySql, StringComparison.Ordinal);
    }

    [Fact]
    public void Missing_format_and_bad_format_are_named_errors()
    {
        var sink = new GcsSink(Hmac());
        var missing = Assert.Throws<PzConnectorException>(() => sink.TryGetNativeCopy(Out(format: null), out _));
        Assert.Contains("'format'", missing.Message, StringComparison.Ordinal);
        Assert.StartsWith("PZ0361: output '", missing.Message, StringComparison.Ordinal);
        Assert.Contains("(supported: csv, json, parquet, tsv)", missing.Message, StringComparison.Ordinal);

        var bad = Assert.Throws<PzConnectorException>(() => sink.TryGetNativeCopy(Out(format: "avro"), out _));
        Assert.Contains("'avro'", bad.Message, StringComparison.Ordinal);
        Assert.StartsWith("PZ0361: output '", bad.Message, StringComparison.Ordinal);
        Assert.Contains("(supported: csv, json, parquet, tsv)", bad.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void No_root_and_no_bucket_is_the_named_error()
    {
        var ex = Assert.Throws<PzConnectorException>(() =>
            new GcsSink(Hmac(root: null)).TryGetNativeCopy(Out(), out _));
        Assert.Contains("'daily'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("root", ex.Message, StringComparison.Ordinal);
        Assert.Contains("bucket", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Non_hmac_connection_declines_native_copy()
    {
        // The OAuth methods have no DuckDB secret; their writes ride the universal SDK tier.
        Assert.False(new GcsSink(Adc()).TryGetNativeCopy(Out(), out var copy));
        Assert.Null(copy);
    }

    [Fact]
    public void Partitioned_output_declines_native_copy_under_oauth_auth()
    {
        // A single COPY ... TO cannot express a per-row-value fan-out (the azure rule); under the
        // OAuth methods the SDK fan-out session carries it. Under hmac the same shape is a plan-time
        // refusal instead -- see GcsReviewFixTests.Hmac_with_partition_by_fails_at_plan_time.
        Assert.False(new GcsSink(Adc()).TryGetNativeCopy(
            Out(path: "d={yyyy}-{MM}-{dd}", partitionBy: ["ts"]), out _));
    }

    [Fact]
    public async Task Universal_write_under_hmac_is_a_named_permanent_refusal()
    {
        // hmac has no SDK client, so an engine forced onto the universal tier gets the clear refusal.
        var schema = new Apache.Arrow.Schema([new Apache.Arrow.Field("id", Apache.Arrow.Types.Int32Type.Default, true)], null);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await new GcsSink(Hmac()).BeginWriteAsync(Out(), schema, CancellationToken.None));
        Assert.False(ex.IsTransient);
        Assert.Contains("native COPY", ex.Message, StringComparison.Ordinal);
    }
}
