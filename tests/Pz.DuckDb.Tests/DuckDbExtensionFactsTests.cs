using Avro;
using Avro.File;
using Avro.Generic;
using Pz.DuckDb;
using Pz.TestSupport;

namespace Pz.DuckDb.Tests;

/// <summary>The excel/avro extension surface the format catalog renders against. Needs one network
/// fetch per DuckDB version (the extension cache lives under ~/.duckdb); skipped offline.</summary>
[Trait("Category", "DuckDbExtension")]
public sealed class DuckDbExtensionFactsTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "pz-duck-ext", Guid.NewGuid().ToString("N"));

    public DuckDbExtensionFactsTests()
    {
        DockerFacts.SkipIfOffline();
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort */ }
    }

    [SkippableFact]
    public async Task Excel_copy_to_xlsx_then_read_xlsx_with_sheet_and_header_roundtrips()
    {
        await using var duck = DuckSession.Open(Path.Combine(_dir, "x.duckdb"));
        await duck.ExecuteAsync("install excel");
        await duck.ExecuteAsync("load excel");
        var path = Path.Combine(_dir, "t.xlsx").Replace("'", "''");
        await duck.ExecuteAsync($"copy (select 1::bigint as id, 'a' as name union all select 2, 'b') to '{path}' (format xlsx, header true, sheet 'Data')");
        await duck.ExecuteAsync($"create table t as select * from read_xlsx('{path}', header = true, sheet = 'Data')");
        Assert.Equal(2, await duck.ScalarAsync<long>("select count(*) from t"));

        // A declared contract is applied as a projecting cast around read_xlsx (it has no columns=
        // parameter of its own — xlsx cells carry no static type, read_xlsx infers one from the sheet).
        await duck.ExecuteAsync($"create table c as (select \"id\"::BIGINT as \"id\", \"name\"::VARCHAR as \"name\" from read_xlsx('{path}', header = true))");
        Assert.Equal(2, await duck.ScalarAsync<long>("select count(*) from c"));

        // header = false yields generic spreadsheet cell-reference column names (A1, B1, ...), not a
        // positional name like col0/col1 — that is what "no header" looks like to a rendered contract.
        await duck.ExecuteAsync($"create table h as select * from read_xlsx('{path}', header = false)");
        Assert.Equal("A1", await duck.ScalarAsync<string>(
            "select column_name from (describe h) order by column_name limit 1"));
    }

    [SkippableFact]
    public async Task Avro_extension_loads_and_read_avro_accepts_a_list_literal()
    {
        await using var duck = DuckSession.Open(Path.Combine(_dir, "a.duckdb"));
        await duck.ExecuteAsync("install avro");
        await duck.ExecuteAsync("load avro");
        // read_avro on a nonexistent path must fail on the file, not on the function or its parameters.
        var ex = await Assert.ThrowsAnyAsync<Exception>(() => duck.ExecuteAsync("select * from read_avro(['/nonexistent/a.avro', '/nonexistent/b.avro'])"));
        Assert.DoesNotContain("Catalog Error", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("named parameter", ex.Message, StringComparison.Ordinal);

        // DuckDB has no avro writer, so the fixture read_avro reads back is written directly with
        // Apache.Avro: a two-row file, one row with a null in the nullable "name" field.
        var avroPath = Path.Combine(_dir, "r.avro");
        WriteAvroFixture(avroPath);
        var quoted = avroPath.Replace("'", "''");
        Assert.Equal(2, await duck.ScalarAsync<long>($"select count(*) from read_avro('{quoted}')"));
        Assert.Equal(1L, await duck.ScalarAsync<long>($"select count(*) from read_avro('{quoted}') where name is null"));
    }

    private static void WriteAvroFixture(string path)
    {
        const string schemaJson = """
            {
              "type": "record",
              "name": "PzAvroFixture",
              "fields": [
                { "name": "id", "type": "long" },
                { "name": "name", "type": ["null", "string"] }
              ]
            }
            """;
        var schema = (RecordSchema)Schema.Parse(schemaJson);

        using var stream = File.Create(path);
        using var writer = DataFileWriter<GenericRecord>.OpenWriter(new GenericDatumWriter<GenericRecord>(schema), stream);

        var withName = new GenericRecord(schema);
        withName.Add("id", 1L);
        withName.Add("name", "Alice");
        writer.Append(withName);

        var withoutName = new GenericRecord(schema);
        withoutName.Add("id", 2L);
        withoutName.Add("name", null);
        writer.Append(withoutName);

        writer.Flush();
    }
}
