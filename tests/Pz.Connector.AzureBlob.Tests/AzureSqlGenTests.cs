using Pz.Connectors.Abstractions;
using Pz.DuckDb;

namespace Pz.Connector.AzureBlob.Tests;

public sealed class AzureSqlGenTests
{
    private static ConnectorConfig Conn() =>
        new(new Dictionary<string, object?> { ["auth"] = "connection_string", ["connection_string"] = "cs" });

    private static OutputSpec Out(string mode = "replace", string container = "lake", string? path = "raw/orders",
        string format = "parquet", string? scheme = null, string? layout = null)
    {
        var o = new Dictionary<string, object?> { ["container"] = container, ["format"] = format };
        if (path is not null) o["path"] = path;
        if (scheme is not null) o["scheme"] = scheme;
        if (layout is not null) o["layout"] = layout;
        return new OutputSpec("sink", "data", mode, "fail_on_change", o);
    }

    [Fact]
    public void Copy_sql_targets_container_and_prefix()
    {
        var sink = new AzureSink(Conn());
        Assert.True(sink.TryGetNativeCopy(Out(), out var copy));
        Assert.Equal("copy (select * from {{source}}) to 'az://lake/raw/orders/data.parquet' (format parquet)", copy!.CopySql);
        Assert.Equal("install azure", copy.SetupStatements[0]);
        Assert.Equal("load azure", copy.SetupStatements[1]);
        Assert.StartsWith("create or replace secret pz_azure_", copy.SetupStatements[2], StringComparison.Ordinal);
        Assert.Empty(copy.Finalizations);
    }

    [Fact]
    public void Csv_copy_adds_header_clause()
    {
        var sink = new AzureSink(Conn());
        sink.TryGetNativeCopy(Out(format: "csv"), out var copy);
        Assert.Contains("(format csv, header)", copy!.CopySql, StringComparison.Ordinal);
    }

    [Fact]
    public void Append_mode_uses_unique_object_name()
    {
        var sink = new AzureSink(Conn());
        sink.TryGetNativeCopy(Out(mode: "append"), out var a);
        sink.TryGetNativeCopy(Out(mode: "append"), out var b);
        Assert.NotEqual(a!.CopySql, b!.CopySql);
    }

    [Fact]
    public void Abfss_scheme_renders_in_copy_target()
    {
        var sink = new AzureSink(Conn());
        sink.TryGetNativeCopy(Out(scheme: "abfss"), out var copy);
        Assert.Contains("to 'abfss://lake/raw/orders/data.parquet'", copy!.CopySql, StringComparison.Ordinal);
    }

    [Fact]
    public void Unknown_format_is_named_error()
    {
        var sink = new AzureSink(Conn());
        var ex = Assert.Throws<PzConnectorException>(() => sink.TryGetNativeCopy(Out(format: "prquet"), out _));
        Assert.False(ex.IsTransient);
        Assert.Contains("parquet", ex.Message, StringComparison.Ordinal);
        Assert.Contains("csv", ex.Message, StringComparison.Ordinal);
        Assert.StartsWith("PZ0361: output '", ex.Message, StringComparison.Ordinal);
        Assert.Contains("(supported: csv, json, parquet, tsv)", ex.Message, StringComparison.Ordinal);
    }

    private static DatasetSpec Ds(string format = "parquet", string container = "lake", string path = "in/*.parquet",
        string? cursor = null, string? value = null, string? upper = null, IReadOnlyDictionary<string, string>? columns = null,
        string? layout = null)
    {
        var o = new Dictionary<string, object?> { ["container"] = container, ["path"] = path, ["format"] = format };
        if (columns is not null) o["columns"] = columns;
        if (layout is not null) o["layout"] = layout;
        return new DatasetSpec("src", "orders", o) { WatermarkCursor = cursor, WatermarkValue = value, WatermarkUpperBound = upper };
    }

    [Fact]
    public void Native_scan_reads_parquet_at_url()
    {
        var src = new AzureSource(Conn());
        Assert.True(src.TryGetNativeScan(Ds(), out var scan));
        Assert.Equal("read_parquet('az://lake/in/*.parquet')", scan!.SqlFragment);
        Assert.Equal("install azure", scan.SetupStatements[0]);
        Assert.StartsWith("create or replace secret pz_azure_", scan.SetupStatements[2], StringComparison.Ordinal);
    }

    [Fact]
    public void NativeScan_csv_auto_detects_without_columns_contract()
    {
        var src = new AzureSource(Conn());
        Assert.True(src.TryGetNativeScan(Ds(format: "csv"), out var scan));
        Assert.Contains("auto_detect = true", scan!.SqlFragment, StringComparison.Ordinal);
        Assert.DoesNotContain("columns = {", scan.SqlFragment, StringComparison.Ordinal);
        Assert.DoesNotContain("types = {", scan.SqlFragment, StringComparison.Ordinal);
    }

    /// <summary>A partial contract behaves EXACTLY like a full one, mirroring LocalFiles'
    /// <c>Native_scan_partial_contract_behaves_the_same_as_a_full_one</c> -- the same strict, pruning
    /// `columns = {...}` fragment, no `types=`/`auto_detect=true` middle case. A declared contract,
    /// partial or full, means "this is the schema, prune to it".</summary>
    [Fact]
    public void NativeScan_csv_partial_contract_behaves_the_same_as_a_full_one()
    {
        var src = new AzureSource(Conn());
        var cols = new Dictionary<string, string> { ["id"] = "bigint" };
        Assert.True(src.TryGetNativeScan(Ds(format: "csv", path: "in/data.csv", columns: cols), out var scan));
        Assert.Contains("read_csv('az://lake/in/data.csv'", scan!.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("auto_detect = false", scan.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("columns = {", scan.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("'id': 'BIGINT'", scan.SqlFragment, StringComparison.Ordinal);
        Assert.DoesNotContain("types = {", scan.SqlFragment, StringComparison.Ordinal);
    }

    [Fact]
    public void Csv_native_scan_renders_columns_map_when_full_contract_present()
    {
        var src = new AzureSource(Conn());
        var cols = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" };
        Assert.True(src.TryGetNativeScan(Ds(format: "csv", path: "in/data.csv", columns: cols), out var scan));
        Assert.Contains("read_csv('az://lake/in/data.csv'", scan!.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("auto_detect = false", scan.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("columns = {", scan.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("'id': 'BIGINT'", scan.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("'name': 'VARCHAR'", scan.SqlFragment, StringComparison.Ordinal);
        Assert.DoesNotContain("types = {", scan.SqlFragment, StringComparison.Ordinal);
        Assert.DoesNotContain("auto_detect = true", scan.SqlFragment, StringComparison.Ordinal);
    }

    [Fact]
    public void Windowed_dataset_wraps_scan_with_upper_bound()
    {
        var src = new AzureSource(Conn());
        src.TryGetNativeScan(Ds(cursor: "ts", value: "3", upper: "7"), out var scan);
        Assert.Contains("<= '7'", scan!.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("\"ts\"", scan.SqlFragment, StringComparison.Ordinal);
    }

    [Fact]
    public void Plain_incremental_does_not_wrap()
    {
        var src = new AzureSource(Conn());
        src.TryGetNativeScan(Ds(cursor: "ts", value: "3"), out var scan);
        Assert.Equal("read_parquet('az://lake/in/*.parquet')", scan!.SqlFragment);
    }

    [Fact]
    public void NativeScan_templated_parquet_emits_cover_list()
    {
        var src = new AzureSource(Conn());
        Assert.True(src.TryGetNativeScan(
            Ds(path: "events/{yyyy}/{MM}/{dd}/*.parquet", cursor: "event_time",
                value: "2026-07-11T00:00:00.000000", upper: "2026-07-12T00:00:00.000000"),
            out var scan));
        Assert.Contains(
            "read_parquet(['az://lake/events/2026/07/11/*.parquet', 'az://lake/events/2026/07/12/*.parquet']",
            scan!.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("\"event_time\" > '2026-07-11", scan.SqlFragment, StringComparison.Ordinal); // window predicate still applied
    }

    [Fact]
    public void NativeScan_non_templated_unchanged()
    {
        var src = new AzureSource(Conn());
        Assert.True(src.TryGetNativeScan(Ds(path: "events/*.parquet"), out var scan));
        Assert.Contains("read_parquet('az://lake/events/*.parquet')", scan!.SqlFragment, StringComparison.Ordinal);
    }

    /// <summary>DuckDB's <c>read_json</c> has no `types=` named parameter to combine with
    /// `auto_detect = true` (empirically verified -- see
    /// <see cref="ReadJson_has_no_types_named_parameter"/>'s doc comment). csv and json are identical
    /// for a declared `columns:` contract, full or partial (see
    /// <see cref="Csv_native_scan_renders_columns_map_when_full_contract_present"/>): both render the
    /// `columns = {...}` map with no `auto_detect` at all.</summary>
    [Fact]
    public void NativeScan_json_renders_columns_map_when_full_contract_present()
    {
        var src = new AzureSource(Conn());
        var cols = new Dictionary<string, string> { ["id"] = "bigint", ["ts"] = "timestamp" };
        Assert.True(src.TryGetNativeScan(Ds(format: "json", path: "logs/*.ndjson", columns: cols), out var scan));
        Assert.Contains("read_json('az://lake/logs/*.ndjson'", scan!.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("format = 'newline_delimited'", scan.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("columns = {", scan.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("'id': 'BIGINT'", scan.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("'ts': 'TIMESTAMP'", scan.SqlFragment, StringComparison.Ordinal);
        Assert.DoesNotContain("types = {", scan.SqlFragment, StringComparison.Ordinal);
        Assert.DoesNotContain("auto_detect", scan.SqlFragment, StringComparison.Ordinal);
    }

    [Fact]
    public void NativeScan_json_auto_detects_without_columns_contract()
    {
        var src = new AzureSource(Conn());
        Assert.True(src.TryGetNativeScan(Ds(format: "json", path: "logs/*.ndjson"), out var scan));
        Assert.Contains("read_json('az://lake/logs/*.ndjson'", scan!.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("auto_detect = true", scan.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("format = 'newline_delimited'", scan.SqlFragment, StringComparison.Ordinal);
        Assert.DoesNotContain("columns = {", scan.SqlFragment, StringComparison.Ordinal);
        Assert.DoesNotContain("types = {", scan.SqlFragment, StringComparison.Ordinal);
    }

    /// <summary>A partial `columns:` on json does NOT get an auto-detected "rest" -- read_json has no
    /// `types=` parameter to layer over `auto_detect`, so a partial declaration renders exactly the
    /// same `columns = {...}` shape a full declaration would; only the declared columns are
    /// projected. csv's <see cref="NativeScan_csv_partial_contract_behaves_the_same_as_a_full_one"/>
    /// asserts the identical fact for csv, for a different reason (a deliberate scope reduction
    /// rather than json's `types=`-doesn't-exist constraint) -- both formats land on the same
    /// shape.</summary>
    [Fact]
    public void NativeScan_json_with_partial_columns_still_uses_columns_map_not_types()
    {
        var src = new AzureSource(Conn());
        var cols = new Dictionary<string, string> { ["id"] = "bigint" };
        Assert.True(src.TryGetNativeScan(Ds(format: "json", path: "logs/*.ndjson", columns: cols), out var scan));
        Assert.Contains("read_json('az://lake/logs/*.ndjson'", scan!.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("columns = {", scan.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("'id': 'BIGINT'", scan.SqlFragment, StringComparison.Ordinal);
        Assert.DoesNotContain("types = {", scan.SqlFragment, StringComparison.Ordinal);
        Assert.DoesNotContain("auto_detect", scan.SqlFragment, StringComparison.Ordinal);
    }

    /// <summary>Pins that the bundled DuckDB's <c>read_json</c> has NO <c>types=</c> named parameter at
    /// all -- unlike <c>read_csv</c>, which does accept `types=`+`auto_detect=true` (csv nonetheless does
    /// not use that combination for a declared contract -- see
    /// <see cref="Csv_native_scan_renders_columns_map_when_full_contract_present"/>). This is WHY
    /// <see cref="AzureSource.TryGetNativeScan"/>'s json branch uses the two-state
    /// `columns={...}`-or-`auto_detect=true` shape (see
    /// <see cref="NativeScan_json_with_partial_columns_still_uses_columns_map_not_types"/>), which csv
    /// shares too, for a different reason. Drives DuckDB's <c>read_json</c> directly against a local ndjson
    /// file (no Azure/Azurite involved -- the `az://` URL scheme and the `azure` extension are orthogonal
    /// to whether `types=` itself is a recognized parameter, so a local file is a cheaper, docker-free way
    /// to pin this DuckDB behavior). If a future DuckDB upgrade adds `types=` support to `read_json`, this
    /// test starts failing -- the intended signal to revisit json's two-state shape.</summary>
    [Fact]
    public async Task ReadJson_has_no_types_named_parameter()
    {
        var dir = Directory.CreateTempSubdirectory("pz-azure-readjson-typecheck-");
        try
        {
            var path = Path.Combine(dir.FullName, "partial.ndjson");
            await File.WriteAllTextAsync(path, "{\"id\": 1, \"qty\": 5, \"name\": \"Alice\"}\n");
            var escaped = path.Replace("'", "''", StringComparison.Ordinal);

            await using var duck = DuckSession.Open(Path.Combine(dir.FullName, "check.duckdb"));
            await duck.ExecuteAsync("create schema if not exists staging");

            // The message must actually name `types`: asserting only that something threw would also
            // pass for an unrelated failure (bad path, permissions, another binder-error class) and
            // silently defeat this pin's purpose.
            var ex = await Assert.ThrowsAnyAsync<Exception>(() => duck.ExecuteAsync(
                $"create table staging.bad as select * from read_json('{escaped}', " +
                "auto_detect = true, types = {'id': 'BIGINT'}, format = 'newline_delimited')"));
            Assert.Contains("types", ex.Message, StringComparison.OrdinalIgnoreCase);

            // Sanity: auto_detect alone (no types=) is what json's contract-less branch actually emits,
            // and it DOES work -- real DuckDB-inferred types, not an error.
            await duck.ExecuteAsync(
                $"create table staging.good as select * from read_json('{escaped}', " +
                "auto_detect = true, format = 'newline_delimited')");
            Assert.Equal("BIGINT", await duck.ScalarAsync<string>(
                "select data_type from information_schema.columns where table_schema = 'staging' and table_name = 'good' and column_name = 'id'"));
        }
        finally
        {
            try { dir.Delete(recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public void NativeCopy_json_emits_format_json()
    {
        var sink = new AzureSink(Conn());
        Assert.True(sink.TryGetNativeCopy(Out(format: "json"), out var copy));
        Assert.Contains("(format json)", copy!.CopySql, StringComparison.Ordinal);
        Assert.Contains("to 'az://lake/raw/orders/data.json'", copy.CopySql, StringComparison.Ordinal);
    }

    [Fact]
    public void Tsv_copy_targets_the_tsv_suffix_and_tab_delimiter()
    {
        var sink = new AzureSink(Conn());
        Assert.True(sink.TryGetNativeCopy(Out(format: "tsv"), out var copy));
        Assert.Equal(
            "copy (select * from {{source}}) to 'az://lake/raw/orders/data.tsv' " +
            "(format csv, header, delimiter '\\t')", copy!.CopySql);
    }

    [Fact]
    public void Json_array_layout_copies_with_format_json_array_true()
    {
        var sink = new AzureSink(Conn());
        Assert.True(sink.TryGetNativeCopy(Out(format: "json", layout: "array"), out var copy));
        Assert.EndsWith("(format json, array true)", copy!.CopySql, StringComparison.Ordinal);
    }

    [Fact]
    public void Tsv_read_defaults_to_the_tsv_suffix_and_tab_delimiter()
    {
        var src = new AzureSource(Conn());
        Assert.True(src.TryGetNativeScan(Ds(format: "tsv", path: "in/data.tsv"), out var scan));
        Assert.Equal(
            "read_csv('az://lake/in/data.tsv', header = true, auto_detect = true, delim = '\\t')",
            scan!.SqlFragment);
        Assert.Equal("sniff_csv('az://lake/in/data.tsv', delim = '\\t')", scan.SniffFragment);
    }

    [Fact]
    public void Json_array_layout_reads_with_format_array()
    {
        var src = new AzureSource(Conn());
        Assert.True(src.TryGetNativeScan(Ds(format: "json", path: "in/data.json", layout: "array"), out var scan));
        Assert.Equal(
            "read_json('az://lake/in/data.json', auto_detect = true, format = 'array')", scan!.SqlFragment);
    }

    [Fact]
    public void Windowed_hourly_layout_scans_the_minimal_cover_url_list()
    {
        // events/{yyyy}/{MM}/{dd}/{HH}/... with a window from 2026-01-02T10 to 2026-01-03T02:
        // partial first day => hour dirs 10..23, then hour dirs 00..02 of the next day (upper bucket
        // inclusive). Whole-day collapse is pinned separately in PathTemplateCoverTests.
        var src = new AzureSource(Conn());
        var spec = Ds(path: "events/{yyyy}/{MM}/{dd}/{HH}/*.parquet",
            cursor: "ts", value: "2026-01-02T10:00:00", upper: "2026-01-03T02:00:00");

        Assert.True(src.TryGetNativeScan(spec, out var scan));
        Assert.Contains("read_parquet([", scan!.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("'az://lake/events/2026/01/02/10/*.parquet'", scan.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("'az://lake/events/2026/01/02/23/*.parquet'", scan.SqlFragment, StringComparison.Ordinal);
        Assert.Contains("'az://lake/events/2026/01/03/02/*.parquet'", scan.SqlFragment, StringComparison.Ordinal);
        // Nothing before the window's lower bucket or past its upper bucket.
        Assert.DoesNotContain("2026/01/02/09", scan.SqlFragment, StringComparison.Ordinal);
        Assert.DoesNotContain("2026/01/03/03", scan.SqlFragment, StringComparison.Ordinal);
    }
    /// <summary>Mirrors LocalFiles — only contract-less csv/json scans let auto_detect invent the
    /// schema, so only they declare
    /// <see cref="NativeScan.SchemaInferred"/>; parquet carries its own exact schema.</summary>
    [Fact]
    public void NativeScan_schema_inferred_flags_only_contract_less_csv_and_json()
    {
        var src = new AzureSource(Conn());
        var cols = new Dictionary<string, string> { ["id"] = "bigint" };

        Assert.True(src.TryGetNativeScan(Ds(format: "csv", path: "in/data.csv"), out var csvInferred));
        Assert.True(csvInferred!.SchemaInferred);
        Assert.Equal("sniff_csv('az://lake/in/data.csv')", csvInferred.SniffFragment);
        Assert.True(src.TryGetNativeScan(Ds(format: "json", path: "in/data.ndjson"), out var jsonInferred));
        Assert.True(jsonInferred!.SchemaInferred);
        Assert.Null(jsonInferred.SniffFragment);

        Assert.True(src.TryGetNativeScan(Ds(format: "csv", path: "in/data.csv", columns: cols), out var csvContract));
        Assert.False(csvContract!.SchemaInferred);
        Assert.Null(csvContract.SniffFragment);
        Assert.True(src.TryGetNativeScan(Ds(format: "json", path: "in/data.ndjson", columns: cols), out var jsonContract));
        Assert.False(jsonContract!.SchemaInferred);

        Assert.True(src.TryGetNativeScan(Ds(), out var parquet));
        Assert.False(parquet!.SchemaInferred);
    }
}
