using System.Text;
using Apache.Arrow;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

public class HttpXmlFileReaderTests
{
    private const string Label = "s.d";
    private static readonly DateTimeOffset FetchedAt = new(2026, 11, 8, 9, 30, 0, TimeSpan.Zero);

    private static HttpDatasetConfig Config(string records, Dictionary<string, string>? columns = null,
        string? fileColumn = null)
    {
        var file = new Dictionary<string, object?> { ["format"] = "xml", ["records"] = records };
        if (fileColumn is not null)
        {
            file["file_column"] = fileColumn;
        }

        var options = new Dictionary<string, object?> { ["path"] = "/f", ["file"] = file };
        if (columns is not null)
        {
            options["columns"] = columns;
        }

        return HttpDatasetConfig.Parse(new DatasetSpec("s", "d", options));
    }

    private static Task<List<object?[]>> Rows(string text, HttpDatasetConfig config, string name = "a.xml")
        => Rows(Encoding.UTF8.GetBytes(text), config, name);

    private static async Task<List<object?[]>> Rows(byte[] bytes, HttpDatasetConfig config, string name = "a.xml")
    {
        var rows = new List<object?[]>();
        var expected = FileOutput.Schema(config);
        using var source = new FileSource(name, new MemoryStream(bytes));
        await foreach (var batch in new HttpXmlFileReader().ReadAsync(
            source, config, FetchedAt, new BatchOptions(), Label, CancellationToken.None))
        {
            using (batch)
            {
                Assert.Equal(expected.FieldsList.Select(f => f.Name), batch.Schema.FieldsList.Select(f => f.Name));
                for (var r = 0; r < batch.Length; r++)
                {
                    var row = new object?[batch.ColumnCount];
                    for (var c = 0; c < batch.ColumnCount; c++)
                    {
                        row[c] = batch.Column(c) switch
                        {
                            Int32Array a => a.GetValue(r),
                            Int64Array a => a.GetValue(r),
                            Decimal128Array a => a.GetValue(r),
                            StringArray a => a.GetString(r),
                            TimestampArray a => a.GetTimestamp(r),
                            var other => throw new InvalidOperationException(other.GetType().Name),
                        };
                    }

                    rows.Add(row);
                }
            }
        }

        return rows;
    }

    private static readonly Dictionary<string, string> Campaign =
        new() { ["id"] = "int", ["name"] = "varchar", ["budget"] = "decimal" };

    [Fact]
    public async Task Contract_children_and_attributes()
    {
        var config = Config("/report/campaigns/campaign", Campaign, "src");

        var row = Assert.Single(await Rows(
            """<report><campaigns><campaign id="7"><name>A</name><budget>1.50</budget></campaign></campaigns></report>""",
            config, "r.xml"));

        Assert.Equal([7, "A", 1.50m, "r.xml"], row);
    }

    [Fact]
    public async Task Element_wins_over_attribute()
    {
        var row = Assert.Single(await Rows("""<rs><r id="1"><id>2</id></r></rs>""",
            Config("/rs/r", new() { ["id"] = "int" })));

        Assert.Equal(2, row[0]);
    }

    [Fact]
    public async Task Missing_value_is_null()
    {
        var rows = await Rows("""<rs><r id="1"><name></name></r><r/></rs>""",
            Config("/rs/r", new() { ["id"] = "int", ["name"] = "varchar", ["budget"] = "decimal" }));

        Assert.Equal([[1, null, null], [null, null, null]], rows);
    }

    [Fact]
    public async Task Nested_child_in_contract_mode_fails()
    {
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Rows(
            """<rs><r><id>1</id><name><first>A</first></name></r></rs>""",
            Config("/rs/r", new() { ["id"] = "int", ["name"] = "varchar" }), "n.xml"));

        Assert.False(ex.IsTransient);
        Assert.StartsWith("s.d: file 'n.xml' record 1: column 'name'", ex.Message);
        Assert.Contains("raw mode", ex.Message);
    }

    [Fact]
    public async Task Records_path_ignores_other_branches()
    {
        const string xml = """
            <report>
              <archive><campaign id="9"/></archive>
              <campaigns><campaign id="1"/><other id="8"/><campaign id="2"/></campaigns>
              <campaign id="7"/>
              <archive><campaigns><campaign id="6"/></campaigns></archive>
            </report>
            """;

        var rows = await Rows(xml, Config("/report/campaigns/campaign", new() { ["id"] = "int" }));

        Assert.Equal([1, 2], rows.Select(r => r[0]));
    }

    [Fact]
    public async Task Namespaces_use_local_names()
    {
        const string xml = """
            <r:report xmlns:r="urn:r" xmlns="urn:x">
              <campaigns><campaign r:id="3"><r:name>A</r:name></campaign></campaigns>
            </r:report>
            """;

        var row = Assert.Single(await Rows(xml, Config("/report/campaigns/campaign", Campaign)));
        Assert.Equal([3, "A", null], row);

        var raw = Assert.Single(await Rows(xml, Config("/report/campaigns/campaign")));
        Assert.Equal("""{"@id":"3","name":"A"}""", raw[0]);
    }

    [Fact]
    public async Task Raw_payload_shape()
    {
        const string xml = """
            <rs>
              <r id="1">
                hello
                <tag>a</tag>
                <tag>b</tag>
                <empty/>
                <amount currency="EUR">1.5</amount>
                <nested><x>1</x></nested>
              </r>
              <r id="2"/>
            </rs>
            """;

        var rows = await Rows(xml, Config("/rs/r"), "p.xml");

        Assert.Equal(2, rows.Count);
        Assert.Equal(
            """{"@id":"1","tag":["a","b"],"empty":"","amount":{"@currency":"EUR","#text":"1.5"},"nested":{"x":"1"},"#text":"hello"}""",
            rows[0][0]);
        Assert.Equal("""{"@id":"2"}""", rows[1][0]);
        Assert.All(rows, r => Assert.Equal("p.xml", r[1]));
        Assert.Equal([1L, 2L], rows.Select(r => r[2]));
        Assert.All(rows, r => Assert.Equal(FetchedAt, r[3]));
    }

    [Fact]
    public async Task Doctype_is_refused()
    {
        const string xml = """
            <?xml version="1.0"?>
            <!DOCTYPE r [<!ENTITY x SYSTEM "file:///etc/passwd">]>
            <r><i>&x;</i></r>
            """;

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => Rows(xml, Config("/r/i"), "evil.xml"));

        Assert.False(ex.IsTransient);
        Assert.StartsWith("s.d: file 'evil.xml': ", ex.Message);
        Assert.IsType<System.Xml.XmlException>(ex.InnerException);
        Assert.Contains("DTD", ex.Message);
    }

    [Fact]
    public async Task Malformed_xml_is_permanent()
    {
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            Rows("<rs><r>1</rs>", Config("/rs/r"), "bad.xml"));

        Assert.False(ex.IsTransient);
        Assert.StartsWith("s.d: file 'bad.xml': ", ex.Message);
    }

    [Fact]
    public async Task Declared_encoding_honoured()
    {
        var bytes = Encoding.Latin1.GetBytes(
            """<?xml version="1.0" encoding="ISO-8859-1"?><rs><r><name>café</name></r></rs>""");

        var row = Assert.Single(await Rows(bytes, Config("/rs/r", new() { ["name"] = "varchar" })));

        Assert.Equal("café", row[0]);
    }
}
