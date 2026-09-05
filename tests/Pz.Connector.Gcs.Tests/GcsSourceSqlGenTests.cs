using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Gcs.Tests;

/// <summary>Offline proof of the gcs SOURCE's whole data plane — which IS these strings:
/// URL composition under the `root:` rules, the per-format fragment shapes (the azure/s3 two-state
/// contract model), window wrapping, the date-template watermark cover, schema-inference signals,
/// and the shared secret. No docker/network — TryGetNativeScan is a pure probe. Byte-for-byte the s3
/// shapes over <c>gs://</c> URLs.</summary>
public sealed class GcsSourceSqlGenTests
{
    private static ConnectorConfig Conn(string? root = "my-bucket/raw") =>
        new(new Dictionary<string, object?>
        {
            ["auth"] = "hmac",
            ["key_id"] = "GOOGHMAC_TEST",
            ["secret"] = "HMAC_SECRET_VALUE",
            ["root"] = root,
        });

    private static DatasetSpec Ds(
        string dataset = "orders", string? format = null, string? path = null, string? bucket = null,
        Dictionary<string, string>? columns = null, string? cursor = null, string? value = null,
        string? upper = null, string? layout = null)
    {
        var options = new Dictionary<string, object?>();
        if (format is not null) options["format"] = format;
        if (path is not null) options["path"] = path;
        if (bucket is not null) options["bucket"] = bucket;
        if (columns is not null) options["columns"] = columns;
        if (layout is not null) options["layout"] = layout;
        return new DatasetSpec("lake", dataset, options)
        {
            WatermarkCursor = cursor,
            WatermarkValue = value,
            WatermarkUpperBound = upper,
        };
    }

    private static GcsSource Source(string? root = "my-bucket/raw") => new(Conn(root));

    [Fact]
    public void Default_path_is_root_prefix_entity_dot_format()
    {
        Assert.True(Source().TryGetNativeScan(Ds(), out var scan));
        Assert.Equal("read_parquet('gs://my-bucket/raw/orders.parquet')", scan!.SqlFragment);
        Assert.Equal("read_parquet", scan.Mechanism);
        Assert.False(scan.SchemaInferred);
    }

    [Fact]
    public void Dataset_bucket_override_drops_the_root_prefix()
    {
        Assert.True(Source().TryGetNativeScan(Ds(path: "in/x.parquet", bucket: "other"), out var scan));
        Assert.Equal("read_parquet('gs://other/in/x.parquet')", scan!.SqlFragment);
    }

    [Fact]
    public void No_root_and_no_bucket_is_the_named_error()
    {
        var ex = Assert.Throws<PzConnectorException>(() => Source(root: null).TryGetNativeScan(Ds(), out _));
        Assert.False(ex.IsTransient);
        Assert.Contains("'orders'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("root", ex.Message, StringComparison.Ordinal);
        Assert.Contains("bucket", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Setup_is_install_load_and_the_shared_source_secret()
    {
        Assert.True(Source().TryGetNativeScan(Ds(), out var scan));
        Assert.Equal(3, scan!.SetupStatements.Count);
        Assert.Equal("install httpfs", scan.SetupStatements[0]);
        Assert.Equal("load httpfs", scan.SetupStatements[1]);
        Assert.StartsWith("create or replace secret pz_gcs_src_lake_", scan.SetupStatements[2], StringComparison.Ordinal);
        Assert.Contains("key_id 'GOOGHMAC_TEST'", scan.SetupStatements[2], StringComparison.Ordinal);
    }

    [Fact]
    public void Contract_less_csv_auto_detects_and_signals_inference_with_a_sniff()
    {
        Assert.True(Source().TryGetNativeScan(Ds(format: "csv"), out var scan));
        Assert.Equal("read_csv('gs://my-bucket/raw/orders.csv', header = true, auto_detect = true)", scan!.SqlFragment);
        Assert.Equal("read_csv", scan.Mechanism);
        Assert.True(scan.SchemaInferred);
        Assert.Equal("sniff_csv('gs://my-bucket/raw/orders.csv')", scan.SniffFragment);
    }

    [Fact]
    public void Declared_csv_contract_renders_the_strict_columns_map()
    {
        var cols = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" };
        Assert.True(Source().TryGetNativeScan(Ds(format: "csv", columns: cols), out var scan));
        Assert.Equal(
            "read_csv('gs://my-bucket/raw/orders.csv', header = true, auto_detect = false, " +
            "columns = {'id': 'BIGINT', 'name': 'VARCHAR'})", scan!.SqlFragment);
        Assert.False(scan.SchemaInferred);
        Assert.Null(scan.SniffFragment);
    }

    [Fact]
    public void Contract_less_json_auto_detects_newline_delimited_without_a_sniff()
    {
        Assert.True(Source().TryGetNativeScan(Ds(format: "json", path: "logs/*.json"), out var scan));
        Assert.Equal(
            "read_json('gs://my-bucket/raw/logs/*.json', auto_detect = true, format = 'newline_delimited')",
            scan!.SqlFragment);
        Assert.True(scan.SchemaInferred);
        Assert.Null(scan.SniffFragment);
    }

    [Fact]
    public void Windowed_dataset_wraps_the_scan_with_both_bounds()
    {
        Assert.True(Source().TryGetNativeScan(Ds(cursor: "ts", value: "3", upper: "7"), out var scan));
        Assert.Equal(
            "(select * from read_parquet('gs://my-bucket/raw/orders.parquet') " +
            "where \"ts\" > '3' and \"ts\" <= '7')", scan!.SqlFragment);
    }

    [Fact]
    public void Plain_incremental_does_not_wrap_or_push_down()
    {
        Assert.True(Source().TryGetNativeScan(Ds(cursor: "ts", value: "3"), out var scan));
        Assert.Equal("read_parquet('gs://my-bucket/raw/orders.parquet')", scan!.SqlFragment);
    }

    [Fact]
    public void Templated_path_with_a_window_emits_the_cover_list()
    {
        Assert.True(Source().TryGetNativeScan(
            Ds(path: "events/{yyyy}/{MM}/{dd}/*.parquet", cursor: "event_time",
                value: "2026-07-11T00:00:00.000000", upper: "2026-07-12T00:00:00.000000"),
            out var scan));
        Assert.Contains(
            "read_parquet(['gs://my-bucket/raw/events/2026/07/11/*.parquet', " +
            "'gs://my-bucket/raw/events/2026/07/12/*.parquet']", scan!.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("\"event_time\" > '2026-07-11", scan.SqlFragment, StringComparison.Ordinal);
    }

    [Fact]
    public void Quotes_in_bucket_and_path_are_escaped()
    {
        Assert.True(Source(root: null).TryGetNativeScan(Ds(bucket: "bu'cket", path: "pa'th.parquet"), out var scan));
        Assert.Equal("read_parquet('gs://bu''cket/pa''th.parquet')", scan!.SqlFragment);
    }

    [Fact]
    public async Task GetSchema_returns_the_declared_contract_as_the_schema()
    {
        var cols = new Dictionary<string, string> { ["id"] = "bigint", ["placed_on"] = "date" };
        var schema = await Source().GetSchemaAsync(Ds(columns: cols), CancellationToken.None);

        Assert.Collection(schema.Schema.FieldsList,
            f => { Assert.Equal("id", f.Name); Assert.Equal(ArrowTypeId.Int64, f.DataType.TypeId); },
            f => { Assert.Equal("placed_on", f.Name); Assert.Equal(ArrowTypeId.Date32, f.DataType.TypeId); });
    }

    [Fact]
    public async Task GetSchema_without_a_contract_is_a_clear_permanent_refusal()
    {
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await Source().GetSchemaAsync(Ds(), CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("columns:", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Tsv_read_defaults_to_the_tsv_suffix_and_tab_delimiter()
    {
        Assert.True(Source().TryGetNativeScan(Ds(format: "tsv"), out var scan));
        Assert.Equal("read_csv('gs://my-bucket/raw/orders.tsv', header = true, auto_detect = true, delim = '\\t')", scan!.SqlFragment);
        Assert.Equal("sniff_csv('gs://my-bucket/raw/orders.tsv', delim = '\\t')", scan.SniffFragment);
    }

    [Fact]
    public void Json_array_layout_reads_with_format_array()
    {
        Assert.True(Source().TryGetNativeScan(Ds(format: "json", layout: "array"), out var scan));
        Assert.Equal("read_json('gs://my-bucket/raw/orders.json', auto_detect = true, format = 'array')", scan!.SqlFragment);
    }

    [Fact]
    public void Xlsx_read_installs_excel_and_reads_one_workbook()
    {
        Assert.True(Source().TryGetNativeScan(Ds(format: "xlsx"), out var scan));
        Assert.Equal("read_xlsx('gs://my-bucket/raw/orders.xlsx', header = true)", scan!.SqlFragment);
        Assert.Equal("read_xlsx", scan.Mechanism);
        // Assert.EndsWith on a collection does not exist in xunit -- assert the last two elements
        // explicitly; the secret statement comes first.
        Assert.Equal("install excel", scan.SetupStatements[^2]);
        Assert.Equal("load excel", scan.SetupStatements[^1]);
    }

    [Fact]
    public void Avro_read_with_a_contract_casts_and_installs_avro()
    {
        var cols = new Dictionary<string, string> { ["id"] = "bigint" };
        Assert.True(Source().TryGetNativeScan(Ds(format: "avro", columns: cols), out var scan));
        Assert.Equal("(select \"id\"::BIGINT as \"id\" from read_avro('gs://my-bucket/raw/orders.avro'))", scan!.SqlFragment);
        Assert.Contains("load avro", scan.SetupStatements);
    }

    [Fact]
    public void Xlsx_read_over_a_date_template_window_cover_is_PZ0361()
    {
        // A date-templated path with both watermark bounds present resolves to a multi-file cover
        // list -- xlsx reads exactly one workbook per entity, so it refuses rather than silently
        // reading one file of the matched set.
        var ex = Assert.Throws<PzConnectorException>(() => Source().TryGetNativeScan(
            Ds(format: "xlsx", path: "in/{yyyy}/{MM}/{dd}.xlsx", cursor: "d", value: "2026-01-01", upper: "2026-01-03"),
            out _));
        Assert.Contains("xlsx reads one workbook per entity", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PlanRead_refuses_the_universal_tier_with_PZ0312()
    {
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
            await Source().PlanReadAsync(Ds(), ReadHints.None, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.StartsWith("PZ0312", ex.Message, StringComparison.Ordinal);
    }
}
