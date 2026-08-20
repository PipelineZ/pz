using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.DuckDb;

namespace Pz.Connector.LocalFiles.Tests;

/// <summary>json (NDJSON) alongside csv/parquet on the localfiles connector, mirroring azure's
/// shape — reads are native-only via <c>read_json</c> (the
/// <see cref="ParquetSource"/> precedent), the sink writes NDJSON via the shared toolkit codec on
/// the universal tier and <c>COPY ... (format json)</c> on the native tier. Same convention as the
/// sibling test classes: everything drives the public <see cref="ISourceConnector"/>/
/// <see cref="ISinkConnector"/> surface, no <c>InternalsVisibleTo</c>.</summary>
public sealed class JsonFormatTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-json-format-tests", Guid.NewGuid().ToString("N"));

    public JsonFormatTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private ConnectorConfig Config => new(new Dictionary<string, object?> { ["base_dir"] = _dir });

    [Fact]
    public void Dataset_schema_format_enum_includes_json()
    {
        var connector = new LocalFilesConnector();
        Assert.Contains("\"json\"", connector.DatasetConfigSchema);
    }

    [Fact]
    public async Task Json_native_scan_with_contract_emits_strict_columns_map()
    {
        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "events", new Dictionary<string, object?>
        {
            ["path"] = "events.json",
            ["format"] = "json",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        });

        var ok = source.TryGetNativeScan(spec, out var scan);

        Assert.True(ok);
        Assert.NotNull(scan);
        Assert.StartsWith("read_json(", scan!.SqlFragment);
        Assert.Contains("columns = {", scan.SqlFragment);
        Assert.Contains("'id': 'BIGINT'", scan.SqlFragment);
        Assert.Contains("'name': 'VARCHAR'", scan.SqlFragment);
        Assert.Contains("format = 'newline_delimited'", scan.SqlFragment);
        Assert.DoesNotContain("auto_detect", scan.SqlFragment);
        Assert.Equal("read_json", scan.Mechanism);
    }

    [Fact]
    public async Task Json_native_scan_auto_detects_without_columns_contract()
    {
        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "events", new Dictionary<string, object?>
        {
            ["format"] = "json",
        });

        var ok = source.TryGetNativeScan(spec, out var scan);

        Assert.True(ok);
        Assert.NotNull(scan);
        Assert.Contains("auto_detect = true", scan!.SqlFragment);
        Assert.Contains("format = 'newline_delimited'", scan.SqlFragment);
        Assert.DoesNotContain("columns = {", scan.SqlFragment);
        // No path: option — the entity names the file, extension from the format.
        Assert.Contains("events.json", scan.SqlFragment);
    }

    [Fact]
    public async Task Json_native_scan_wraps_fragment_with_window_bounds()
    {
        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "events", new Dictionary<string, object?>
        {
            ["path"] = "events.json",
            ["format"] = "json",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
        })
        {
            WatermarkCursor = "id",
            WatermarkValue = "10",
            WatermarkUpperBound = "20",
        };

        var ok = source.TryGetNativeScan(spec, out var scan);

        Assert.True(ok);
        Assert.NotNull(scan);
        Assert.StartsWith("(select * from read_json(", scan!.SqlFragment);
        Assert.Contains("\"id\" > '10' and \"id\" <= '20'", scan.SqlFragment);
        Assert.EndsWith(")", scan.SqlFragment);
    }

    [Fact]
    public async Task Json_native_scan_roundtrips_via_duckdb()
    {
        var path = Path.Combine(_dir, "events.json");
        await File.WriteAllTextAsync(path,
            """{"id":1,"name":"Alice"}""" + "\n" +
            """{"id":2,"name":null}""" + "\n" +
            """{"id":3,"name":"Bob"}""" + "\n");

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "events", new Dictionary<string, object?>
        {
            ["path"] = "events.json",
            ["format"] = "json",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        });

        var ok = source.TryGetNativeScan(spec, out var scan);
        Assert.True(ok);

        await using var duck = DuckSession.Open(Path.Combine(_dir, "native.duckdb"));
        await duck.ExecuteAsync("create schema if not exists staging");
        await duck.ExecuteAsync($"create table staging.t as select * from {scan!.SqlFragment}");

        Assert.Equal(3, await duck.ScalarAsync<long>("select count(*) from staging.t"));
        Assert.Equal("Alice", await duck.ScalarAsync<string>("select name from staging.t where id = 1"));
        Assert.Equal(0L, await duck.ScalarAsync<long>("select count(*) from staging.t where id = 2 and name is not null"));
    }

    /// <summary>Executes the contract-less (auto_detect) fragment against the real bundled DuckDB: a
    /// full contract is not required for localfiles json, and a fragment-string assertion alone must
    /// not be that claim's only net.</summary>
    [Fact]
    public async Task Json_native_scan_contract_less_auto_detect_roundtrips_via_duckdb()
    {
        var path = Path.Combine(_dir, "events.json");
        await File.WriteAllTextAsync(path,
            """{"id":1,"name":"Alice"}""" + "\n" +
            """{"id":2,"name":"Bob"}""" + "\n");

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "events", new Dictionary<string, object?>
        {
            ["path"] = "events.json",
            ["format"] = "json",
        });

        var ok = source.TryGetNativeScan(spec, out var scan);
        Assert.True(ok);

        await using var duck = DuckSession.Open(Path.Combine(_dir, "autodetect.duckdb"));
        await duck.ExecuteAsync("create schema if not exists staging");
        await duck.ExecuteAsync($"create table staging.t as select * from {scan!.SqlFragment}");

        Assert.Equal(2, await duck.ScalarAsync<long>("select count(*) from staging.t"));
        Assert.Equal("Bob", await duck.ScalarAsync<string>("select name from staging.t where id = 2"));
    }

    [Fact]
    public async Task Json_schema_is_the_declared_contract()
    {
        var path = Path.Combine(_dir, "events.json");
        await File.WriteAllTextAsync(path, """{"id":1,"name":"Alice"}""" + "\n");

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "events", new Dictionary<string, object?>
        {
            ["path"] = "events.json",
            ["format"] = "json",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        });

        var schema = await source.GetSchemaAsync(spec, CancellationToken.None);

        var byName = schema.Schema.FieldsList.ToDictionary(f => f.Name);
        Assert.Equal(2, schema.Schema.FieldsList.Count);
        Assert.Equal(Int64Type.Default, byName["id"].DataType);
        Assert.Equal(StringType.Default, byName["name"].DataType);
    }

    [Fact]
    public async Task Json_schema_without_contract_is_permanent_error()
    {
        var path = Path.Combine(_dir, "events.json");
        await File.WriteAllTextAsync(path, """{"id":1}""" + "\n");

        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "events", new Dictionary<string, object?>
        {
            ["path"] = "events.json",
            ["format"] = "json",
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.GetSchemaAsync(spec, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("columns", ex.Message);
        Assert.Contains("json", ex.Message);
    }

    [Fact]
    public async Task Json_schema_on_missing_file_is_permanent_error()
    {
        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "events", new Dictionary<string, object?>
        {
            ["path"] = "nope.json",
            ["format"] = "json",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.GetSchemaAsync(spec, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("json file not found", ex.Message);
        Assert.Contains("nope.json", ex.Message);
    }

    [Fact]
    public async Task Json_universal_plan_read_is_permanent_error()
    {
        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new DatasetSpec("files", "events", new Dictionary<string, object?>
        {
            ["path"] = "events.json",
            ["format"] = "json",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("PZ0312", ex.Message);
        Assert.Contains("native-scan only", ex.Message);
    }

    [Fact]
    public async Task Json_sink_native_copy_uses_format_json()
    {
        var connector = new LocalFilesConnector();
        await using var sink = await ((ISinkConnector)connector).OpenAsync(Config, CancellationToken.None);

        var spec = new OutputSpec("lake", "out", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "out", ["format"] = "json" });

        var ok = sink.TryGetNativeCopy(spec, out var copy);

        Assert.True(ok);
        Assert.NotNull(copy);
        Assert.Contains("(format json)", copy!.CopySql);
        Assert.Equal("COPY TO json", copy.Mechanism);
    }

    [Fact]
    public async Task Json_sink_universal_write_produces_ndjson()
    {
        var connector = new LocalFilesConnector();
        await using var sink = await ((ISinkConnector)connector).OpenAsync(Config, CancellationToken.None);

        var schema = new Schema(
        [
            new Field("id", Int64Type.Default, nullable: false),
            new Field("name", StringType.Default, nullable: true),
        ], null);

        var spec = new OutputSpec("lake", "out", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "out", ["format"] = "json" });

        var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None);
        await using (session)
        {
            var batch = new RecordBatch(schema,
            [
                new Int64Array.Builder().Append(1).Append(2).Build(),
                new StringArray.Builder().Append("Alice").AppendNull().Build(),
            ], 2);
            using (batch)
            {
                await session.WriteBatchAsync(batch, CancellationToken.None);
            }

            var result = await session.CommitAsync(CancellationToken.None);
            Assert.Equal(2, result.RowsWritten);
        }

        var finalPath = Path.Combine(_dir, "out", "out.json");
        Assert.True(File.Exists(finalPath));
        Assert.Empty(Directory.EnumerateDirectories(Path.Combine(_dir, "out"), ".pz-tmp-*"));

        var text = await File.ReadAllTextAsync(finalPath);
        Assert.EndsWith("\n", text); // LF-framed including a trailing newline (byte-stable-writer rule)
        var lines = text.Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(2, lines.Length);

        using var first = JsonDocument.Parse(lines[0]);
        Assert.Equal(1, first.RootElement.GetProperty("id").GetInt64());
        Assert.Equal("Alice", first.RootElement.GetProperty("name").GetString());

        using var second = JsonDocument.Parse(lines[1]);
        Assert.Equal(2, second.RootElement.GetProperty("id").GetInt64());
        Assert.Equal(JsonValueKind.Null, second.RootElement.GetProperty("name").ValueKind);
    }

    [Fact]
    public async Task Unknown_sink_format_error_names_all_three_formats()
    {
        var connector = new LocalFilesConnector();
        await using var sink = await ((ISinkConnector)connector).OpenAsync(Config, CancellationToken.None);

        var schema = new Schema([new Field("id", Int64Type.Default, nullable: false)], null);
        var spec = new OutputSpec("lake", "out", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["path"] = "out", ["format"] = "xml" });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await sink.BeginWriteAsync(spec, schema, CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Contains("'json'", ex.Message);
        Assert.Contains("xml", ex.Message);
    }
    /// <summary>Mirrors the csv flag — contract-less json auto-detects, so only it declares
    /// <see cref="NativeScan.SchemaInferred"/>.</summary>
    [Fact]
    public async Task Json_scan_declares_schema_inferred_only_when_contract_less()
    {
        var connector = new LocalFilesConnector();
        await using var source = await ((ISourceConnector)connector).OpenAsync(Config, CancellationToken.None);

        var contractLess = new DatasetSpec("files", "t", new Dictionary<string, object?>
        {
            ["path"] = "t.ndjson",
            ["format"] = "json",
        });
        Assert.True(source.TryGetNativeScan(contractLess, out var inferred));
        Assert.True(inferred!.SchemaInferred);
        Assert.Null(inferred.SniffFragment);

        var declared = new DatasetSpec("files", "t", new Dictionary<string, object?>
        {
            ["path"] = "t.ndjson",
            ["format"] = "json",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
        });
        Assert.True(source.TryGetNativeScan(declared, out var contracted));
        Assert.False(contracted!.SchemaInferred);
    }
}
