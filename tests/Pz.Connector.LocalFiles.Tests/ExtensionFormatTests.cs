using Avro;
using Avro.File;
using Avro.Generic;
using Pz.Connectors.Abstractions;
using Pz.DuckDb;
using Pz.TestSupport;

namespace Pz.Connector.LocalFiles.Tests;

/// <summary>xlsx and avro need the DuckDB excel/avro extensions: one network fetch per DuckDB
/// version, then cached under ~/.duckdb. Skipped under PZ_TESTS_OFFLINE; run by CI's format-extensions job.</summary>
[Trait("Category", "DuckDbExtension")]
public sealed class ExtensionFormatTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-ext-format-tests", Guid.NewGuid().ToString("N"));

    public ExtensionFormatTests()
    {
        DockerFacts.SkipIfOffline();
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private ConnectorConfig Config => new(new Dictionary<string, object?> { ["base_dir"] = _dir });

    private static async Task RunSetupAsync(DuckSession duck, IReadOnlyList<string> statements)
    {
        foreach (var s in statements) await duck.ExecuteAsync(s);
    }

    [SkippableFact]
    public async Task Xlsx_native_copy_then_native_scan_roundtrips_with_sheet()
    {
        await using var duck = DuckSession.Open(Path.Combine(_dir, "x.duckdb"));
        await duck.ExecuteAsync("create table src as select 1::bigint as id, 'a' as name union all select 2, 'b'");

        await using var sink = await ((ISinkConnector)new LocalFilesConnector()).OpenAsync(Config, CancellationToken.None);
        var outSpec = new OutputSpec("files", "people", "replace", "fail_on_change", new Dictionary<string, object?> { ["format"] = "xlsx", ["sheet"] = "People" });
        Assert.True(sink.TryGetNativeCopy(outSpec, out var copy));
        Assert.Equal(["install excel", "load excel"], copy!.SetupStatements);
        Assert.Contains("(format xlsx, header true, sheet 'People')", copy.CopySql, StringComparison.Ordinal);
        Directory.CreateDirectory(Path.GetDirectoryName(copy.Finalizations[0].FinalPath)!);
        await RunSetupAsync(duck, copy.SetupStatements);
        await duck.ExecuteAsync(copy.CopySql.Replace("{{source}}", "src", StringComparison.Ordinal));
        File.Move(copy.Finalizations[0].TempPath, copy.Finalizations[0].FinalPath, overwrite: true);
        Assert.EndsWith(Path.Combine("people", "people.xlsx"), copy.Finalizations[0].FinalPath, StringComparison.Ordinal);

        await using var source = await ((ISourceConnector)new LocalFilesConnector()).OpenAsync(Config, CancellationToken.None);
        var spec = new DatasetSpec("files", "people", new Dictionary<string, object?>
        {
            ["path"] = "people/people.xlsx", ["format"] = "xlsx", ["sheet"] = "People",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["name"] = "varchar" },
        });
        Assert.True(source.TryGetNativeScan(spec, out var scan));
        Assert.Equal("read_xlsx", scan!.Mechanism);
        await RunSetupAsync(duck, scan.SetupStatements);
        await duck.ExecuteAsync($"create table back as select * from {scan.SqlFragment}");
        Assert.Equal(2, await duck.ScalarAsync<long>("select count(*) from back where name in ('a', 'b')"));
    }

    [SkippableFact]
    public async Task Avro_native_scan_reads_a_file_written_by_apache_avro()
    {
        var schema = (RecordSchema)Avro.Schema.Parse("""{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"name","type":["null","string"]}]}""");
        var path = Path.Combine(_dir, "users.avro");
        using (var writer = DataFileWriter<GenericRecord>.OpenWriter(new GenericDatumWriter<GenericRecord>(schema), path))
        {
            foreach (var (id, name) in new[] { (1L, "a"), (2L, (string?)null) })
            {
                var r = new GenericRecord(schema);
                r.Add("id", id);
                r.Add("name", name);
                writer.Append(r);
            }
        }

        await using var source = await ((ISourceConnector)new LocalFilesConnector()).OpenAsync(Config, CancellationToken.None);
        var spec = new DatasetSpec("files", "users", new Dictionary<string, object?> { ["format"] = "avro" });
        Assert.True(source.TryGetNativeScan(spec, out var scan));
        Assert.Equal("read_avro", scan!.Mechanism);
        Assert.False(scan.SchemaInferred);

        await using var duck = DuckSession.Open(Path.Combine(_dir, "a.duckdb"));
        await RunSetupAsync(duck, scan.SetupStatements);
        await duck.ExecuteAsync($"create table t as select * from {scan.SqlFragment}");
        Assert.Equal(1, await duck.ScalarAsync<long>("select count(*) from t where name is null"));
    }
}
