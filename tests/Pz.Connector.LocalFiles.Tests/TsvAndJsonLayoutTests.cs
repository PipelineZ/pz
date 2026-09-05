using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;
using Pz.DuckDb;

namespace Pz.Connector.LocalFiles.Tests;

public sealed class TsvAndJsonLayoutTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-tsv-tests", Guid.NewGuid().ToString("N"));

    public TsvAndJsonLayoutTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private ConnectorConfig Config => new(new Dictionary<string, object?> { ["base_dir"] = _dir });

    private static RecordBatch Batch()
    {
        var schema = new Schema([new Field("id", Int64Type.Default, true), new Field("name", StringType.Default, true)], null);
        return new RecordBatch(schema, [new Int64Array.Builder().Append(1).Append(2).Build(), new StringArray.Builder().Append("x").Append("y z").Build()], 2);
    }

    [Fact]
    public async Task Tsv_native_scan_reads_a_tab_separated_file_through_duckdb()
    {
        await File.WriteAllTextAsync(Path.Combine(_dir, "people.tsv"), "id\tname\n1\tx\n2\ty z\n");
        await using var source = await ((ISourceConnector)new LocalFilesConnector()).OpenAsync(Config, CancellationToken.None);
        var spec = new DatasetSpec("files", "people", new Dictionary<string, object?> { ["format"] = "tsv" });

        Assert.True(source.TryGetNativeScan(spec, out var scan));
        Assert.EndsWith(", delim = '\\t')", scan!.SqlFragment, StringComparison.Ordinal);
        await using var duck = DuckSession.Open(Path.Combine(_dir, "t.duckdb"));
        await duck.ExecuteAsync("create schema if not exists staging");
        await duck.ExecuteAsync($"create table staging.t as select * from {scan.SqlFragment}");
        var rows = await duck.ScalarAsync<long>("select count(*) from staging.t where name = 'y z'");
        Assert.Equal(1, rows);
    }

    [Fact]
    public async Task Tsv_universal_read_uses_the_tab_delimiter()
    {
        await File.WriteAllTextAsync(Path.Combine(_dir, "people.tsv"), "id\tname\n1\tx\n2\ty,z\n");
        await using var source = await ((ISourceConnector)new LocalFilesConnector()).OpenAsync(Config, CancellationToken.None);
        var spec = new DatasetSpec("files", "people", new Dictionary<string, object?>
        {
            ["format"] = "tsv",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        });

        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        var names = new List<string>();
        foreach (var p in partitions)
        {
            await foreach (var batch in p.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                var col = (StringArray)batch.Column(1);
                for (var i = 0; i < col.Length; i++) names.Add(col.GetString(i));
                batch.Dispose();
            }
        }

        Assert.Equal(["x", "y,z"], names);
    }

    [Fact]
    public async Task Tsv_universal_write_lands_a_tab_separated_file()
    {
        await using var sink = await ((ISinkConnector)new LocalFilesConnector()).OpenAsync(Config, CancellationToken.None);
        var spec = new OutputSpec("files", "people", "replace", "fail_on_change", new Dictionary<string, object?> { ["format"] = "tsv" });
        using var batch = Batch();
        await using var session = await sink.BeginWriteAsync(spec, batch.Schema, CancellationToken.None);
        await session.WriteBatchAsync(batch, CancellationToken.None);
        await session.CommitAsync(CancellationToken.None);

        Assert.Equal("id\tname\n1\tx\n2\ty z\n", await File.ReadAllTextAsync(Path.Combine(_dir, "people", "people.tsv")));
    }

    [Fact]
    public async Task Tsv_native_copy_uses_the_tab_delimiter_and_tsv_suffix()
    {
        await using var sink = await ((ISinkConnector)new LocalFilesConnector()).OpenAsync(Config, CancellationToken.None);
        var spec = new OutputSpec("files", "people", "replace", "fail_on_change", new Dictionary<string, object?> { ["format"] = "tsv" });
        Assert.True(sink.TryGetNativeCopy(spec, out var copy));
        Assert.Contains("(format csv, header, delimiter '\\t')", copy!.CopySql, StringComparison.Ordinal);
        Assert.EndsWith("people.tsv", copy.Finalizations[0].FinalPath, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Tsv_native_copy_executes_and_lands_bytes_identical_to_the_managed_writer()
    {
        await using var sink = await ((ISinkConnector)new LocalFilesConnector()).OpenAsync(Config, CancellationToken.None);
        var spec = new OutputSpec("files", "people", "replace", "fail_on_change", new Dictionary<string, object?> { ["format"] = "tsv" });
        Assert.True(sink.TryGetNativeCopy(spec, out var copy));

        var move = copy!.Finalizations[0];
        Directory.CreateDirectory(Path.GetDirectoryName(move.FinalPath)!);

        await using var duck = DuckSession.Open(Path.Combine(_dir, "copy.duckdb"));
        await duck.ExecuteAsync("create table src as select 1::bigint as id, 'x' as name union all select 2, 'y z'");
        await duck.ExecuteAsync(copy.CopySql.Replace("{{source}}", "src"));

        File.Move(move.TempPath, move.FinalPath);

        // Byte-identical to Tsv_universal_write_lands_a_tab_separated_file's managed-writer output --
        // the planner is free to choose either tier for the same dataset, so the two must agree exactly.
        Assert.Equal("id\tname\n1\tx\n2\ty z\n", await File.ReadAllTextAsync(move.FinalPath));
    }

    [Fact]
    public async Task Json_array_native_copy_executes_and_writes_an_array_readable_back_by_native_scan()
    {
        await using var sink = await ((ISinkConnector)new LocalFilesConnector()).OpenAsync(Config, CancellationToken.None);
        var spec = new OutputSpec("files", "events_native", "replace", "fail_on_change",
            new Dictionary<string, object?> { ["format"] = "json", ["layout"] = "array" });
        Assert.True(sink.TryGetNativeCopy(spec, out var copy));

        var move = copy!.Finalizations[0];
        Directory.CreateDirectory(Path.GetDirectoryName(move.FinalPath)!);

        await using var duck = DuckSession.Open(Path.Combine(_dir, "copyj.duckdb"));
        await duck.ExecuteAsync("create table src as select 1::bigint as id, 'x' as name union all select 2, 'y z'");
        await duck.ExecuteAsync(copy.CopySql.Replace("{{source}}", "src"));

        File.Move(move.TempPath, move.FinalPath);

        var json = await File.ReadAllTextAsync(move.FinalPath);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(JsonValueKind.Array, doc.RootElement.ValueKind);
        Assert.Equal(2, doc.RootElement.GetArrayLength());

        // The sink lands under <root>/<entity>/<entity>.json (a write's directory convention) while a
        // read with no path: resolves <root>/<entity>.<format> -- an explicit path: bridges the two so
        // the native scan reads the exact file the native copy just wrote.
        await using var source = await ((ISourceConnector)new LocalFilesConnector()).OpenAsync(Config, CancellationToken.None);
        var readSpec = new DatasetSpec("files", "events_native", new Dictionary<string, object?>
        {
            ["format"] = "json",
            ["layout"] = "array",
            ["path"] = "events_native/events_native.json",
        });
        Assert.True(source.TryGetNativeScan(readSpec, out var scan));
        await duck.ExecuteAsync("create schema if not exists staging");
        await duck.ExecuteAsync($"create table staging.t as select * from {scan!.SqlFragment}");
        Assert.Equal(2, await duck.ScalarAsync<long>("select count(*) from staging.t"));
    }

    [Fact]
    public async Task Json_array_native_scan_and_copy_roundtrip_through_duckdb()
    {
        await File.WriteAllTextAsync(Path.Combine(_dir, "events.json"), "[{\"id\": 1, \"name\": \"x\"}, {\"id\": 2, \"name\": \"y\"}]");
        await using var source = await ((ISourceConnector)new LocalFilesConnector()).OpenAsync(Config, CancellationToken.None);
        var spec = new DatasetSpec("files", "events", new Dictionary<string, object?> { ["format"] = "json", ["layout"] = "array" });
        Assert.True(source.TryGetNativeScan(spec, out var scan));
        Assert.Contains("format = 'array'", scan!.SqlFragment, StringComparison.Ordinal);

        await using var duck = DuckSession.Open(Path.Combine(_dir, "j.duckdb"));
        await duck.ExecuteAsync("create schema if not exists staging");
        await duck.ExecuteAsync($"create table staging.t as select * from {scan.SqlFragment}");
        Assert.Equal(2, await duck.ScalarAsync<long>("select count(*) from staging.t"));

        await using var sink = await ((ISinkConnector)new LocalFilesConnector()).OpenAsync(Config, CancellationToken.None);
        var outSpec = new OutputSpec("files", "events_out", "replace", "fail_on_change", new Dictionary<string, object?> { ["format"] = "json", ["layout"] = "array" });
        Assert.True(sink.TryGetNativeCopy(outSpec, out var copy));
        Assert.Contains("(format json, array true)", copy!.CopySql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Json_array_universal_write_is_PZ0361()
    {
        await using var sink = await ((ISinkConnector)new LocalFilesConnector()).OpenAsync(Config, CancellationToken.None);
        var spec = new OutputSpec("files", "events", "replace", "fail_on_change", new Dictionary<string, object?> { ["format"] = "json", ["layout"] = "array" });
        using var batch = Batch();
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await sink.BeginWriteAsync(spec, batch.Schema, CancellationToken.None));
        Assert.StartsWith("PZ0361: output 'events': json 'layout: array' is native-only", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Csv_without_delimiter_option_is_parsed_as_comma_even_when_the_file_is_not()
    {
        await File.WriteAllTextAsync(Path.Combine(_dir, "people.csv"), "id;name\n1;a\n");
        var contract = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" };

        await using var source = await ((ISourceConnector)new LocalFilesConnector()).OpenAsync(Config, CancellationToken.None);

        // No delimiter: -- Sylvan is forced to comma, so the file's one semicolon-joined header column
        // ("id;name") satisfies neither declared column.
        var noDelimiterSpec = new DatasetSpec("files", "people", new Dictionary<string, object?> { ["format"] = "csv", ["columns"] = contract });
        var noDelimiterPartitions = await source.PlanReadAsync(noDelimiterSpec, ReadHints.None, CancellationToken.None);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            foreach (var p in noDelimiterPartitions)
            {
                await foreach (var batch in p.ReadAsync(BatchOptions.Default, CancellationToken.None))
                {
                    batch.Dispose();
                }
            }
        });
        Assert.Contains("missing declared column", ex.Message, StringComparison.Ordinal);

        // delimiter: ";" makes the same bytes parse as two columns.
        var withDelimiterSpec = new DatasetSpec("files", "people", new Dictionary<string, object?>
        {
            ["format"] = "csv",
            ["delimiter"] = ";",
            ["columns"] = contract,
        });
        var withDelimiterPartitions = await source.PlanReadAsync(withDelimiterSpec, ReadHints.None, CancellationToken.None);
        var names = new List<string>();
        foreach (var p in withDelimiterPartitions)
        {
            await foreach (var batch in p.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                var col = (StringArray)batch.Column(1);
                for (var i = 0; i < col.Length; i++) names.Add(col.GetString(i));
                batch.Dispose();
            }
        }

        Assert.Equal(["a"], names);
    }

    [Fact]
    public void Dataset_schema_accepts_tsv_delimiter_and_layout()
    {
        var schema = new LocalFilesConnector().DatasetConfigSchema;
        Assert.Contains("\"tsv\"", schema, StringComparison.Ordinal);
        Assert.Contains("\"delimiter\"", schema, StringComparison.Ordinal);
        Assert.Contains("\"layout\"", schema, StringComparison.Ordinal);
    }
}
