using System.IO.Compression;
using System.Text;
using System.Text.Json.Nodes;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Memory;

namespace Pz.Connector.Http.Tests;

public class HttpCsvFileReaderTests
{
    private const string Label = FileReaderRows.Label;
    private static readonly DateTimeOffset FetchedAt = FileReaderRows.FetchedAt;

    private static HttpDatasetConfig Config(Dictionary<string, object?>? file = null,
        Dictionary<string, string>? columns = null, string? cursor = null, string? cursorType = null)
    {
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/f",
            ["file"] = file ?? new Dictionary<string, object?> { ["format"] = "csv" },
        };
        if (columns is not null)
        {
            options["columns"] = columns;
        }

        if (cursor is not null)
        {
            options["cursor"] = cursor;
            options["cursor_type"] = cursorType;
        }

        return HttpDatasetConfig.Parse(new DatasetSpec("s", "d", options));
    }

    private static Dictionary<string, object?> File(params (string Key, object? Value)[] pairs)
    {
        var file = new Dictionary<string, object?> { ["format"] = "csv" };
        foreach (var (k, v) in pairs)
        {
            file[k] = v;
        }

        return file;
    }

    private static Task<List<object?[]>> Rows(string text, HttpDatasetConfig config, string name = "a.csv")
        => Rows(Encoding.UTF8.GetBytes(text), config, name);

    private static Task<List<object?[]>> Rows(byte[] bytes, HttpDatasetConfig config, string name = "a.csv",
        BatchOptions? options = null)
        => FileReaderRows.ReadAsync(new HttpCsvFileReader(), bytes, config, name, options);

    private static async Task<PzConnectorException> Fails(byte[] bytes, HttpDatasetConfig config, string name = "a.csv")
        => await Assert.ThrowsAsync<PzConnectorException>(() => Rows(bytes, config, name));

    [Fact]
    public async Task Contract_csv_types_and_empty_is_null()
    {
        var config = Config(columns: new() { ["id"] = "int", ["name"] = "varchar", ["amount"] = "decimal" });

        var row = Assert.Single(await Rows("id,name,amount\n1,,2.5\n", config));

        Assert.Equal(1, row[0]);
        Assert.Null(row[1]);
        Assert.Equal(2.5m, row[2]);
    }

    [Fact]
    public async Task Missing_contract_column_fails()
    {
        var config = Config(columns: new() { ["id"] = "int", ["zip"] = "varchar" });

        var ex = await Fails(Encoding.UTF8.GetBytes("id,name\n1,a\n"), config, "export.csv");

        Assert.False(ex.IsTransient);
        Assert.Equal("s.d: file 'export.csv': missing declared column 'zip' in header", ex.Message);
    }

    [Fact]
    public async Task Bom_and_crlf_header()
    {
        var config = Config(columns: new() { ["id"] = "int", ["name"] = "varchar" });
        byte[] bytes = [.. Encoding.UTF8.GetPreamble(), .. Encoding.UTF8.GetBytes("id,name\r\n1,a\r\n")];

        var row = Assert.Single(await Rows(bytes, config));

        Assert.Equal(1, row[0]);
        Assert.Equal("a", row[1]);
    }

    [Fact]
    public async Task Tsv_and_skip_lines_and_no_header()
    {
        var config = Config(
            new() { ["format"] = "tsv", ["skip_lines"] = "2", ["header"] = false },
            new() { ["id"] = "int", ["name"] = "varchar" });

        var row = Assert.Single(await Rows("report\ngenerated x\n1\ta\n", config));

        Assert.Equal(1, row[0]);
        Assert.Equal("a", row[1]);
    }

    [Fact]
    public async Task No_header_short_row_fails_naming_the_line()
    {
        var config = Config(
            new() { ["format"] = "tsv", ["skip_lines"] = "1", ["header"] = false },
            new() { ["id"] = "int", ["name"] = "varchar" });

        var ex = await Fails(Encoding.UTF8.GetBytes("report\n1\ta\n2\n"), config);

        Assert.False(ex.IsTransient);
        Assert.Equal("s.d: file 'a.csv' line 3: expected 2 fields, got 1", ex.Message);
    }

    [Fact]
    public async Task No_header_invalid_value_names_line_and_column()
    {
        var config = Config(File(("header", false)), new() { ["id"] = "int", ["name"] = "varchar" });

        var ex = await Fails(Encoding.UTF8.GetBytes("1,a\nx,b\n"), config);

        Assert.Equal("s.d: file 'a.csv' line 2: column 'id': value 'x' is not a valid int", ex.Message);
    }

    [Fact]
    public async Task Utf16_with_bom()
    {
        var config = Config(columns: new() { ["id"] = "int", ["name"] = "varchar" });
        byte[] bytes = [.. Encoding.Unicode.GetPreamble(), .. Encoding.Unicode.GetBytes("id,name\n1,é€\n")];

        var row = Assert.Single(await Rows(bytes, config));

        Assert.Equal("é€", row[1]);
    }

    [Fact]
    public async Task Latin1_when_declared()
    {
        var config = Config(File(("encoding", "iso-8859-1")), new() { ["id"] = "int", ["name"] = "varchar" });

        var row = Assert.Single(await Rows(Encoding.Latin1.GetBytes("id,name\n1,café\n"), config));

        Assert.Equal("café", row[1]);
    }

    [Fact]
    public async Task Raw_mode_rows()
    {
        var config = Config();

        var rows = await Rows("id,name\n1,\n2,b\n", config);

        Assert.Equal(["payload", "pz_file", "pz_row", "pz_fetched_at"],
            FileOutput.Schema(config).FieldsList.Select(f => f.Name));
        Assert.Equal(2, rows.Count);
        Assert.Equal("""{"id":"1","name":""}""", rows[0][0]);
        Assert.Equal("""{"id":"2","name":"b"}""", rows[1][0]);
        Assert.All(rows, r => Assert.Equal("a.csv", r[1]));
        Assert.Equal([1L, 2L], rows.Select(r => r[2]));
        Assert.All(rows, r => Assert.Equal(FetchedAt, r[3]));
    }

    [Fact]
    public async Task Raw_mode_extra_field_fails_short_row_pads()
    {
        var config = Config(File(("skip_lines", "1")));

        var padded = Assert.Single(await Rows("report\nid,name\n1\n", config));
        Assert.Equal("""{"id":"1","name":""}""", padded[0]);

        var ex = await Fails(Encoding.UTF8.GetBytes("report\nid,name\n1,a\n2,b,extra\n"), config);
        Assert.False(ex.IsTransient);
        Assert.Equal("s.d: file 'a.csv' line 4: expected 2 fields, got 3", ex.Message);
    }

    [Fact]
    public async Task Raw_mode_no_header_names_c1()
    {
        var config = Config(File(("header", false)));

        var row = Assert.Single(await Rows("1,a\n", config));

        Assert.Equal("""{"c1":"1","c2":"a"}""", row[0]);
    }

    [Fact]
    public async Task Raw_mode_cursor_from_text()
    {
        var config = Config(cursor: "day", cursorType: "date");

        var row = Assert.Single(await Rows("day,v\n2026-11-07,x\n", config));

        Assert.Equal("day", FileOutput.Schema(config).FieldsList[4].Name);
        Assert.Equal(new DateOnly(2026, 11, 7), row[4]);
    }

    [Fact]
    public async Task Raw_mode_duplicate_header_fails()
    {
        var ex = await Fails(Encoding.UTF8.GetBytes("a,a\n1,2\n"), Config());

        Assert.Equal("s.d: file 'a.csv': duplicate column 'a' in header", ex.Message);
    }

    [Fact]
    public async Task File_column_is_last_and_constant()
    {
        var config = Config(File(("file_column", "src")), new() { ["id"] = "int", ["name"] = "varchar" });

        // A one-byte batch target closes a batch after every row, so the column is appended per batch.
        var rows = await Rows(Encoding.UTF8.GetBytes("id,name\n1,a\n2,b\n3,c\n"), config, "x/2026.csv",
            new BatchOptions(TargetBatchBytes: 1));

        Assert.Equal(["id", "name", "src"], FileOutput.Schema(config).FieldsList.Select(f => f.Name));
        Assert.Equal([1, 2, 3], rows.Select(r => r[0]));
        Assert.All(rows, r => Assert.Equal("x/2026.csv", r[2]));
    }

    [Fact]
    public void AppendFileColumn_new_batch_owns_old_arrays()
    {
        var allocator = new PooledNativeAllocator();
        var ids = new Int32Array.Builder().Append(1).Append(2).Build(allocator);
        var schema = new Schema([new Field("id", Int32Type.Default, true)], null);
        var old = new RecordBatch(schema, [ids], 2);
        Assert.True(allocator.RentedBytes > 0);
        var target = new Schema(
            [new Field("id", Int32Type.Default, true), new Field("src", StringType.Default, true)], null);

        using (var batch = FileOutput.AppendFileColumn(old, target, "a.csv"))
        {
            Assert.Same(target, batch.Schema);
            Assert.Same(ids, batch.Column(0));
            var src = Assert.IsType<StringArray>(batch.Column(1));
            Assert.Equal(["a.csv", "a.csv"], new[] { src.GetString(0), src.GetString(1) });
        }

        Assert.Equal(0, allocator.RentedBytes);
    }

    [Fact]
    public async Task Invalid_deflate_is_permanent_and_names_the_file()
    {
        var config = Config(columns: new() { ["id"] = "int" });
        var garbage = new DeflateStream(new MemoryStream([0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]), CompressionMode.Decompress);
        using var source = new FileSource("bad.csv", garbage);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () =>
        {
            await foreach (var batch in new HttpCsvFileReader().ReadAsync(
                source, config, FetchedAt, new BatchOptions(), Label, CancellationToken.None))
            {
                batch.Dispose();
            }
        });

        Assert.False(ex.IsTransient);
        Assert.StartsWith("s.d: file 'bad.csv': ", ex.Message);
        Assert.IsType<InvalidDataException>(ex.InnerException);
    }

    [Fact]
    public void RawRow_cursor_from_json_number_uses_contract_projector()
    {
        var config = Config(cursor: "n", cursorType: "bigint");
        var payload = JsonNode.Parse("""{"n":7}""")!.AsObject();

        var row = FileOutput.RawRow(payload, "a.json", 3, FetchedAt, config, "ctx");

        Assert.Equal(["""{"n":7}""", "a.json", 3L, FetchedAt, 7L], row);
    }
}
