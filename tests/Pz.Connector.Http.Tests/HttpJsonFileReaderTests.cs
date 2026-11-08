using System.Text;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

public class HttpJsonFileReaderTests
{
    private static readonly DateTimeOffset FetchedAt = FileReaderRows.FetchedAt;

    private static HttpDatasetConfig Config(string layout = "lines", Dictionary<string, string>? columns = null,
        string? items = null, string? fileColumn = null)
    {
        var file = new Dictionary<string, object?> { ["format"] = "json", ["layout"] = layout };
        if (fileColumn is not null)
        {
            file["file_column"] = fileColumn;
        }

        var options = new Dictionary<string, object?> { ["path"] = "/f", ["file"] = file };
        if (columns is not null)
        {
            options["columns"] = columns;
        }

        if (items is not null)
        {
            options["items"] = items;
        }

        return HttpDatasetConfig.Parse(new DatasetSpec("s", "d", options));
    }

    private static Task<List<object?[]>> Rows(string text, HttpDatasetConfig config, string name = "a.json",
        long maxDocumentBytes = 1024 * 1024)
        => FileReaderRows.ReadAsync(new HttpJsonFileReader(maxDocumentBytes), Encoding.UTF8.GetBytes(text), config,
            name);

    [Fact]
    public async Task Lines_contract_and_raw()
    {
        const string text = "{\"id\":1,\"name\":\"a\"}\n\n  \n{\"id\":2,\"extra\":true}\n";
        var contract = Config(columns: new() { ["id"] = "int", ["name"] = "varchar" }, fileColumn: "src");

        var rows = await Rows(text, contract, "x.ndjson");

        Assert.Equal(2, rows.Count);
        Assert.Equal([1, "a", "x.ndjson"], rows[0]);
        Assert.Equal([2, null, "x.ndjson"], rows[1]);

        var raw = await Rows(text, Config(), "x.ndjson");

        Assert.Equal(2, raw.Count);
        Assert.Equal("""{"id":1,"name":"a"}""", raw[0][0]);
        Assert.Equal("""{"id":2,"extra":true}""", raw[1][0]);
        Assert.All(raw, r => Assert.Equal("x.ndjson", r[1]));
        Assert.Equal([1L, 2L], raw.Select(r => r[2]));
        Assert.All(raw, r => Assert.Equal(FetchedAt, r[3]));
    }

    [Fact]
    public async Task Lines_bad_line_names_file_and_line()
    {
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            Rows("{\"id\":1}\n\n{\"id\":\n", Config(columns: new() { ["id"] = "int" }), "x.ndjson"));

        Assert.False(ex.IsTransient);
        Assert.Equal("s.d: file 'x.ndjson' line 3 is not valid JSON", ex.Message);
    }

    [Fact]
    public async Task Lines_crlf_and_last_line_without_newline()
    {
        var rows = await Rows("{\"id\":1}\r\n\r\n{\"id\":2}", Config(columns: new() { ["id"] = "int" }));

        Assert.Equal([[1], [2]], rows);
    }

    [Fact]
    public async Task Lines_over_cap_names_line_and_option()
    {
        var text = "{\"id\":1}\n" + new string(' ', 2048) + "{\"id\":2}";

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            Rows(text, Config(columns: new() { ["id"] = "int" }), "x.ndjson", maxDocumentBytes: 1024));

        Assert.False(ex.IsTransient);
        Assert.Equal("s.d: file 'x.ndjson' line 2 exceeds 'max_response_mb' (1024 bytes) for one json line",
            ex.Message);
    }

    [Fact]
    public async Task Document_with_items_pointer()
    {
        var config = Config("document", new() { ["id"] = "int", ["name"] = "varchar" }, "/data/rows");

        var rows = await Rows("""{"data":{"rows":[{"id":1,"name":"a"},{"id":2,"name":"b"}]}}""", config);

        Assert.Equal([[1, "a"], [2, "b"]], rows);
    }

    [Fact]
    public async Task Document_root_array_default_items()
    {
        var rows = await Rows("""[{"id":1},{"id":2}]""", Config("document"), "d.json");

        Assert.Equal(["""{"id":1}""", """{"id":2}"""], rows.Select(r => r[0]));
        Assert.Equal([1L, 2L], rows.Select(r => r[2]));
    }

    [Fact]
    public async Task Document_items_not_an_array_fails()
    {
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            Rows("""{"data":{"id":1}}""", Config("document", items: "/data"), "d.json"));

        Assert.False(ex.IsTransient);
        Assert.Equal("s.d: file 'd.json': items pointer '/data' does not resolve to an array", ex.Message);
    }

    [Fact]
    public async Task Document_over_cap_suggests_lines()
    {
        var text = "[" + string.Join(",", Enumerable.Repeat("""{"id":1234567890}""", 120)) + "]";
        Assert.True(text.Length > 2048);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            Rows(text, Config("document"), "big.json", maxDocumentBytes: 1024));

        Assert.False(ex.IsTransient);
        Assert.Contains("'big.json'", ex.Message);
        Assert.Contains("max_response_mb", ex.Message);
        Assert.Contains("layout: lines", ex.Message);
    }

    [Fact]
    public async Task Raw_non_object_record_is_wrapped()
    {
        var rows = await Rows("[1,2]", Config("document"));

        Assert.Equal(["""{"value":1}""", """{"value":2}"""], rows.Select(r => r[0]));
    }
}
