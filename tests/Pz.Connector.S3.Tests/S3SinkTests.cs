using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.S3.Tests;

/// <summary>Unit tests for <see cref="S3Sink"/>'s statement generation -- the SQL/secret-shaping heart
/// of the object-store sink. No docker/network required (TryGetNativeCopy is a pure, offline probe).</summary>
public sealed class S3SinkTests
{
    private static ConnectorConfig Config(
        string accessKey = "AKIA_TEST", string secretKey = "S3CRET_VALUE", string? endpoint = null,
        string? region = null, string? urlStyle = null, bool? useSsl = null)
    {
        var values = new Dictionary<string, object?>
        {
            ["access_key"] = accessKey,
            ["secret_key"] = secretKey,
        };
        if (endpoint is not null) values["endpoint"] = endpoint;
        if (region is not null) values["region"] = region;
        if (urlStyle is not null) values["url_style"] = urlStyle;
        if (useSsl is not null) values["use_ssl"] = useSsl.Value;
        return new ConnectorConfig(values);
    }

    private static OutputSpec Spec(
        string sink = "lake", string output = "data", string mode = "replace",
        string bucket = "my-bucket", string? path = "raw/orders", string format = "parquet")
    {
        var options = new Dictionary<string, object?> { ["bucket"] = bucket, ["format"] = format };
        if (path is not null)
        {
            options["path"] = path;
        }

        return new OutputSpec(sink, output, mode, "fail_on_change", options);
    }

    [Fact]
    public void Setup_statements_shape_secret_correctly()
    {
        var sink = new S3Sink(Config());

        var ok = sink.TryGetNativeCopy(Spec(), out var copy);

        Assert.True(ok);
        Assert.Equal("install httpfs", copy!.SetupStatements[0]);
        Assert.Equal("load httpfs", copy.SetupStatements[1]);
        Assert.StartsWith("create or replace secret pz_s3_", copy.SetupStatements[2], StringComparison.Ordinal);
        // Deliberately allowed here: this is the ONE legitimate carrier of the secret literal.
        Assert.Contains("AKIA_TEST", copy.SetupStatements[2], StringComparison.Ordinal);
        Assert.Contains("S3CRET_VALUE", copy.SetupStatements[2], StringComparison.Ordinal);
    }

    [Fact]
    public void Copy_sql_targets_bucket_and_prefix()
    {
        var sink = new S3Sink(Config());

        sink.TryGetNativeCopy(Spec(), out var copy);

        Assert.Equal(
            "copy (select * from {{source}}) to 's3://my-bucket/raw/orders/data.parquet' (format parquet)",
            copy!.CopySql);
    }

    [Fact]
    public void Append_mode_uses_unique_object_key()
    {
        var sink = new S3Sink(Config());

        sink.TryGetNativeCopy(Spec(mode: "append"), out var first);
        sink.TryGetNativeCopy(Spec(mode: "append"), out var second);

        Assert.NotEqual(first!.CopySql, second!.CopySql);
    }

    [Fact]
    public void Universal_write_session_throws_permanent()
    {
        var sink = new S3Sink(Config());
        var emptySchema = new Schema([], null);

        var ex = Assert.Throws<PzConnectorException>(() =>
        {
            _ = sink.BeginWriteAsync(Spec(), emptySchema, CancellationToken.None);
        });

        Assert.False(ex.IsTransient);
    }

    [Fact]
    public void Unknown_format_is_named_error()
    {
        var sink = new S3Sink(Config());

        var ex = Assert.Throws<PzConnectorException>(() =>
        {
            _ = sink.TryGetNativeCopy(Spec(format: "prquet"), out _);
        });

        Assert.False(ex.IsTransient);
        Assert.StartsWith("PZ0361: output '", ex.Message, StringComparison.Ordinal);
        Assert.Contains("prquet", ex.Message, StringComparison.Ordinal);
        Assert.Contains("(supported: csv, json, parquet)", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Sink_secret_name_is_direction_prefixed_and_hash_suffixed()
    {
        // The hash suffix is what keeps secret names distinct: `create or replace secret` is
        // last-wins, so two connections whose names sanitize identically ("prod-db", "prod_db")
        // would otherwise silently share one secret (wrong-credentials I/O).
        var sink = new S3Sink(Config());

        sink.TryGetNativeCopy(Spec(sink: "wh"), out var copy);

        Assert.StartsWith("create or replace secret pz_s3_snk_wh_509bcf06 ", copy!.SetupStatements[2],
            StringComparison.Ordinal);
        Assert.NotEqual(S3Sql.SinkSecretName("prod-db"), S3Sql.SinkSecretName("prod_db"));
        Assert.Equal(S3Sql.SinkSecretName("prod-db"), S3Sql.SinkSecretName("prod-db"));
        Assert.StartsWith("pz_s3_src_prod_db_", S3Sql.SourceSecretName("prod-db"), StringComparison.Ordinal);
    }

    [Fact]
    public void Json_format_copies_ndjson_with_a_json_object_key()
    {
        // json (NDJSON) alongside csv/parquet on the s3 sink, mirroring localfiles' native COPY
        // shape (`format json` = DuckDB's newline-delimited json writer).
        var sink = new S3Sink(Config());

        sink.TryGetNativeCopy(Spec(format: "json"), out var copy);

        Assert.Equal(
            "copy (select * from {{source}}) to 's3://my-bucket/raw/orders/data.json' (format json)",
            copy!.CopySql);
        Assert.Equal("COPY TO s3 json", copy.Mechanism);
    }

    [Fact]
    public void Unknown_format_error_names_json_too()
    {
        var sink = new S3Sink(Config());

        var ex = Assert.Throws<PzConnectorException>(() =>
        {
            _ = sink.TryGetNativeCopy(Spec(format: "jsn"), out _);
        });

        Assert.StartsWith("PZ0361: output '", ex.Message, StringComparison.Ordinal);
        Assert.Contains("(supported: csv, json, parquet)", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Validate_missing_access_key_fails()
    {
        var connector = new S3Connector();
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["secret_key"] = "S3CRET_VALUE" });

        var result = await connector.ValidateAsync(config, CancellationToken.None);

        Assert.False(result.IsValid);
        Assert.Contains(result.Errors, e => e.Contains("access_key", StringComparison.Ordinal));
    }

    [Fact]
    public void Single_quotes_escaped_in_values()
    {
        var sink = new S3Sink(Config(secretKey: "a'b"));

        sink.TryGetNativeCopy(Spec(), out var copy);

        Assert.Contains("secret 'a''b'", copy!.SetupStatements[2], StringComparison.Ordinal);
    }

    [Fact]
    public void Published_schemas_are_valid_json_schema()
    {
        var c = new S3Connector();
        foreach (var s in new[] { c.ConnectionConfigSchema, c.DatasetConfigSchema })
        {
            var schema = Json.Schema.JsonSchema.FromText(s); // throws on malformed
            Assert.NotNull(schema);
        }
    }
}
