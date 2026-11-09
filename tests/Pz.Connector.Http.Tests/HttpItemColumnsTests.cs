using System.Text;
using System.Text.Json.Nodes;
using Apache.Arrow;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

public class HttpItemColumnsTests
{
    private static readonly DateTimeOffset Fetched = FileReaderRows.FetchedAt;

    private static DatasetSpec Spec(Dictionary<string, object?> options) => new("s", "d", options);

    /// <summary>A for_each dataset whose item request is <c>/orders/{{ item.order_id }}/lines</c>.</summary>
    private static Dictionary<string, object?> ItemOptions(string[] fields, Dictionary<string, object?>? extra = null)
    {
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/orders/{{ item.order_id }}/lines",
            ["for_each"] = new Dictionary<string, object?>
            {
                ["path"] = "/orders",
                ["items"] = "/data",
                ["fields"] = fields.ToDictionary(f => f, f => (object?)("/" + f)),
            },
        };
        foreach (var (k, v) in extra ?? [])
        {
            options[k] = v;
        }

        return options;
    }

    private static ItemValues Item(string json, params string[] fields)
        => ItemValues.From(JsonNode.Parse(json), [.. fields.Select(f => (f, "/" + f))]);

    private static HttpConnectionConfig Connection(Uri baseUrl)
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(new Dictionary<string, object?>
        {
            ["base_url"] = baseUrl.ToString(),
        }), errors);
        Assert.Empty(errors);
        return connection!;
    }

    private static async Task<Schema> SchemaOf(DatasetSpec spec)
    {
        await using var source = new HttpSource(Connection(new Uri("https://api.example.com")));
        return (await source.GetSchemaAsync(spec, CancellationToken.None)).Schema;
    }

    private static void AssertSameSchema(Schema expected, Schema actual)
    {
        Assert.Equal(expected.FieldsList.Select(f => f.Name), actual.FieldsList.Select(f => f.Name));
        Assert.Equal(expected.FieldsList.Select(f => f.DataType.TypeId), actual.FieldsList.Select(f => f.DataType.TypeId));
    }

    /// <summary>Reads one item's JSON pages and checks every batch against GetSchemaAsync.</summary>
    private static async Task<(List<object?[]> Rows, Schema Schema)> ReadPages(Dictionary<string, object?> options,
        ItemValues? item, string body)
    {
        await using var server = new StubHttpServer();
        server.Map("/orders/o1/lines", _ => new StubResponse(200, body));
        var connection = Connection(server.BaseUrl);
        var spec = Spec(options);
        var config = HttpDatasetConfig.Parse(spec);
        using var client = HttpSource.CreateClient(connection);
        using var bare = HttpSource.CreateBareClient(connection);
        var partition = new HttpPartition(client, bare, connection, config, spec, TimeProvider.System) { Item = item };
        var schema = await SchemaOf(spec);
        var rows = new List<object?[]>();
        await foreach (var batch in partition.ReadAsync(new BatchOptions(), CancellationToken.None))
        {
            using (batch)
            {
                AssertSameSchema(schema, batch.Schema);
                for (var r = 0; r < batch.Length; r++)
                {
                    var row = new object?[batch.ColumnCount];
                    for (var c = 0; c < batch.ColumnCount; c++)
                    {
                        row[c] = FileReaderRows.Value(batch.Column(c), r);
                    }

                    rows.Add(row);
                }
            }
        }

        return (rows, schema);
    }

    private static Dictionary<string, object?> FileOptions(string format, string[] fields,
        Dictionary<string, string>? columns = null, Dictionary<string, object?>? extraFile = null,
        Dictionary<string, object?>? extra = null)
    {
        var file = new Dictionary<string, object?> { ["format"] = format };
        foreach (var (k, v) in extraFile ?? [])
        {
            file[k] = v;
        }

        var all = new Dictionary<string, object?> { ["file"] = file };
        if (columns is not null)
        {
            all["columns"] = columns;
        }

        foreach (var (k, v) in extra ?? [])
        {
            all[k] = v;
        }

        return ItemOptions(fields, all);
    }

    private static IFileFormatReader Reader(string format, ItemValues? item) => format switch
    {
        "csv" => new HttpCsvFileReader { Item = item },
        "json" => new HttpJsonFileReader(1024 * 1024) { Item = item },
        "xml" => new HttpXmlFileReader { Item = item },
        _ => throw new ArgumentOutOfRangeException(nameof(format)),
    };

    private static readonly Dictionary<string, (string Name, string Text)> FileBodies = new()
    {
        ["csv"] = ("a.csv", "qty\n5\n"),
        ["json"] = ("a.json", "{\"qty\":5}\n"),
        ["xml"] = ("a.xml", "<rows><row><qty>5</qty></row></rows>"),
    };

    /// <summary>Reads one file with an item; FileReaderRows checks every batch against FileOutput.Schema,
    /// and this also checks GetSchemaAsync agrees.</summary>
    private static async Task<List<object?[]>> ReadFile(string format, Dictionary<string, object?> options,
        ItemValues? item, string? text = null)
    {
        var spec = Spec(options);
        var config = HttpDatasetConfig.Parse(spec);
        AssertSameSchema(await SchemaOf(spec), FileOutput.Schema(config));
        var (name, body) = FileBodies[format];
        return await FileReaderRows.ReadAsync(Reader(format, item), Encoding.UTF8.GetBytes(text ?? body), config, name);
    }

    private static Dictionary<string, object?> XmlRecords() => new() { ["records"] = "/rows/row" };

    [Fact]
    public async Task Contract_columns_from_item_json_pages()
    {
        var options = ItemOptions(["order_id"], new()
        {
            ["columns"] = new Dictionary<string, string> { ["order_id"] = "varchar", ["qty"] = "int" },
        });

        var (rows, _) = await ReadPages(options, Item("""{"order_id":"o1"}""", "order_id"),
            """[{"qty":1},{"qty":2}]""");

        Assert.Equal([["o1", 1], ["o1", 2]], rows);
    }

    [Fact]
    public async Task Item_wins_over_child_field()
    {
        var options = ItemOptions(["order_id"], new()
        {
            ["columns"] = new Dictionary<string, string> { ["qty"] = "int", ["order_id"] = "varchar" },
        });

        var (rows, _) = await ReadPages(options, Item("""{"order_id":"o1"}""", "order_id"),
            """[{"qty":1,"order_id":"zzz"}]""");

        Assert.Equal([[1, "o1"]], rows);
    }

    [Fact]
    public async Task Item_values_convert_by_contract_type()
    {
        var options = ItemOptions(["order_id", "at", "n", "flag", "blank"], new()
        {
            ["columns"] = new Dictionary<string, string>
            {
                ["at"] = "timestamp", ["n"] = "bigint", ["flag"] = "boolean", ["blank"] = "varchar",
                ["missing"] = "int", ["qty"] = "int",
            },
        });
        var item = Item("""{"order_id":"o1","at":"2026-11-07T10:00:00Z","n":42,"flag":true,"blank":""}""",
            "order_id", "at", "n", "flag", "blank", "missing");

        var (rows, _) = await ReadPages(options, item, """[{"qty":7,"missing":3}]""");

        var row = Assert.Single(rows);
        Assert.Equal(new DateTimeOffset(2026, 11, 7, 10, 0, 0, TimeSpan.Zero), row[0]);
        Assert.Equal(42L, row[1]);
        Assert.Equal(true, row[2]);
        Assert.Null(row[3]); // a JSON string follows the text rules: empty is NULL
        Assert.Null(row[4]); // a declared field the list record lacks is NULL, never read from the child
        Assert.Equal(7, row[5]);
    }

    [Fact]
    public async Task Item_value_that_does_not_convert_names_the_item_and_column()
    {
        var options = ItemOptions(["order_id"], new()
        {
            ["columns"] = new Dictionary<string, string> { ["order_id"] = "int" },
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadPages(options, Item("""{"order_id":"o1"}""", "order_id"), "[{}]"));

        Assert.False(ex.IsTransient);
        Assert.Contains("item order_id=o1", ex.Message);
        Assert.Contains("column 'order_id'", ex.Message);
    }

    [Fact]
    public async Task Raw_pz_item_json_pages()
    {
        var item = Item("""{"order_id":"o1","n":5}""", "order_id", "n");

        var (rows, schema) = await ReadPages(ItemOptions(["order_id", "n"]), item, """[{"qty":1}]""");

        Assert.Equal(["payload", "pz_page", "pz_fetched_at", "pz_item"], schema.FieldsList.Select(f => f.Name));
        var row = Assert.Single(rows);
        Assert.Equal("""{"qty":1}""", row[0]);
        Assert.Equal(0, row[1]);
        Assert.Equal("""{"order_id":"o1","n":5}""", row[3]);
    }

    [Theory]
    [InlineData("csv")]
    [InlineData("json")]
    [InlineData("xml")]
    public async Task Raw_pz_item_files(string format)
    {
        var options = FileOptions(format, ["order_id", "n"], extraFile: format == "xml" ? XmlRecords() : null);
        var item = Item("""{"order_id":"o1","n":5}""", "order_id", "n");

        var rows = await ReadFile(format, options, item);

        Assert.Equal(["payload", "pz_file", "pz_row", "pz_fetched_at", "pz_item"],
            FileOutput.Schema(HttpDatasetConfig.Parse(Spec(options))).FieldsList.Select(f => f.Name));
        var row = Assert.Single(rows);
        Assert.Equal(FileBodies[format].Name, row[1]);
        Assert.Equal(1L, row[2]);
        Assert.Equal(Fetched, row[3]);
        Assert.Equal("""{"order_id":"o1","n":5}""", row[4]);
    }

    [Theory]
    [InlineData("csv")]
    [InlineData("json")]
    [InlineData("xml")]
    public async Task Contract_item_columns_files(string format)
    {
        var options = FileOptions(format, ["order_id"],
            columns: new() { ["order_id"] = "varchar", ["qty"] = "int" },
            extraFile: format == "xml"
                ? new() { ["records"] = "/rows/row", ["file_column"] = "src" }
                : new() { ["file_column"] = "src" });

        var rows = await ReadFile(format, options, Item("""{"order_id":"o1"}""", "order_id"));

        Assert.Equal([["o1", 5, FileBodies[format].Name]], rows);
    }

    [Fact]
    public async Task Csv_contract_item_column_not_in_header()
    {
        var options = FileOptions("csv", ["order_id", "export_id"],
            columns: new() { ["id"] = "int", ["export_id"] = "bigint", ["name"] = "varchar" });
        var item = Item("""{"order_id":"o1","export_id":77}""", "order_id", "export_id");

        var rows = await ReadFile("csv", options, item, "name,id\na,1\nb,2\n");

        Assert.Equal([[1, 77L, "a"], [2, 77L, "b"]], rows);
    }

    [Fact]
    public async Task Csv_positional_item_column_takes_no_field()
    {
        var options = FileOptions("csv", ["order_id"],
            columns: new() { ["order_id"] = "varchar", ["id"] = "int" },
            extraFile: new() { ["header"] = false });

        var rows = await ReadFile("csv", options, Item("""{"order_id":"o1"}""", "order_id"), "1\n2\n");

        Assert.Equal([["o1", 1], ["o1", 2]], rows);
    }

    [Fact]
    public async Task Cursor_from_item_both_modes()
    {
        var item = Item("""{"order_id":"o1","changed":"2026-11-07T10:00:00Z"}""", "order_id", "changed");
        var at = new DateTimeOffset(2026, 11, 7, 10, 0, 0, TimeSpan.Zero);

        var raw = ItemOptions(["order_id", "changed"], new()
        {
            ["cursor"] = "changed", ["cursor_type"] = "timestamp", ["cursor_pointer"] = "/changed",
        });
        var (rawRows, rawSchema) = await ReadPages(raw, item, """[{"changed":"1999-01-01T00:00:00Z"}]""");
        Assert.Equal(["payload", "pz_page", "pz_fetched_at", "pz_item", "changed"],
            rawSchema.FieldsList.Select(f => f.Name));
        Assert.Equal(at, Assert.Single(rawRows)[4]);

        var contract = ItemOptions(["order_id", "changed"], new()
        {
            ["columns"] = new Dictionary<string, string> { ["qty"] = "int", ["changed"] = "timestamp" },
            ["cursor"] = "changed",
        });
        var (contractRows, _) = await ReadPages(contract, item, """[{"qty":1,"changed":"1999-01-01T00:00:00Z"}]""");
        Assert.Equal([[1, at]], contractRows);

        var rawFile = FileOptions("json", ["order_id", "changed"],
            extra: new() { ["cursor"] = "changed", ["cursor_type"] = "timestamp" });
        var fileRows = await ReadFile("json", rawFile, item, "{\"changed\":\"1999-01-01T00:00:00Z\"}\n");
        Assert.Equal(at, Assert.Single(fileRows)[5]);
    }

    [Fact]
    public async Task Schema_matches_batches()
    {
        // ReadPages and ReadFile check every batch against GetSchemaAsync; this runs the shapes the
        // other tests don't: a raw cursor that is not a field, and a for_each dataset read with no item.
        var item = Item("""{"order_id":"o1"}""", "order_id");
        var raw = ItemOptions(["order_id"], new() { ["cursor"] = "id", ["cursor_type"] = "bigint" });
        var (rows, schema) = await ReadPages(raw, item, """[{"id":9}]""");
        Assert.Equal(["payload", "pz_page", "pz_fetched_at", "pz_item", "id"], schema.FieldsList.Select(f => f.Name));
        Assert.Equal(9L, Assert.Single(rows)[4]);

        foreach (var format in new[] { "csv", "json", "xml" })
        {
            var options = FileOptions(format, ["order_id"], extraFile: format == "xml" ? XmlRecords() : null,
                extra: new() { ["cursor"] = "qty", ["cursor_type"] = "int", ["cursor_pointer"] = "/qty" });
            var fileRows = await ReadFile(format, options, item);
            Assert.Equal(5, Assert.Single(fileRows)[5]);
        }
    }

    [Fact]
    public void Datasets_without_for_each_unchanged()
    {
        var plain = HttpDatasetConfig.Parse(Spec(new() { ["path"] = "/items" }));
        Assert.Equal(["payload", "pz_page", "pz_fetched_at"], RawEnvelope.Columns(plain).Keys);
        var cursor = HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/items", ["cursor"] = "id", ["cursor_type"] = "bigint",
        }));
        Assert.Equal(["payload", "pz_page", "pz_fetched_at", "id"], RawEnvelope.Columns(cursor).Keys);

        var file = HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/f", ["file"] = new Dictionary<string, object?> { ["format"] = "csv" },
            ["cursor"] = "id", ["cursor_type"] = "bigint",
        }));
        Assert.Equal(["payload", "pz_file", "pz_row", "pz_fetched_at", "id"], FileOutput.Columns(file).Keys);
        var row = FileOutput.RawRow(JsonNode.Parse("""{"id":3}""")!.AsObject(), "a.csv", 1, Fetched, file, "ctx");
        Assert.Equal(["{\"id\":3}", "a.csv", 1L, Fetched, 3L], row);

        var contract = HttpDatasetConfig.Parse(Spec(new()
        {
            ["path"] = "/f",
            ["file"] = new Dictionary<string, object?> { ["format"] = "csv", ["file_column"] = "src" },
            ["columns"] = new Dictionary<string, string> { ["id"] = "int" },
        }));
        Assert.Equal(["id", "src"], FileOutput.Columns(contract).Keys);
    }

    [Fact]
    public void Constant_columns_batch_owns_old_arrays()
    {
        var allocator = new Pz.Connectors.Abstractions.Memory.PooledNativeAllocator();
        var ids = new Int32Array.Builder().Append(1).Append(2).Build(allocator);
        var old = new RecordBatch(new Schema([new Field("id", Apache.Arrow.Types.Int32Type.Default, true)], null),
            [ids], 2);
        var target = new Schema(
        [
            new Field("k", Apache.Arrow.Types.Int64Type.Default, true),
            new Field("id", Apache.Arrow.Types.Int32Type.Default, true),
            new Field("src", Apache.Arrow.Types.StringType.Default, true),
        ], null);

        using (var batch = FileOutput.WithConstantColumns(old, target, [(0, 7L), (2, "a.csv")]))
        {
            Assert.Same(target, batch.Schema);
            Assert.Same(ids, batch.Column(1));
            Assert.Equal([7L, 7L], new[] { FileReaderRows.Value(batch.Column(0), 0), FileReaderRows.Value(batch.Column(0), 1) });
            Assert.Equal("a.csv", FileReaderRows.Value(batch.Column(2), 1));
        }

        Assert.Equal(0, allocator.RentedBytes);
    }
}
