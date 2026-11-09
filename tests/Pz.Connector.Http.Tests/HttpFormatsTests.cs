using System.Text;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

/// <summary>`watermark_format`, `epoch_unit` and nested contract columns (`column_pointers`, which pz
/// writes from the columns' long-form `pointer`).</summary>
public class HttpFormatsTests
{
    private const string Watermark = "2026-11-09T12:34:56.789";
    private const string Upper = "2026-11-10T00:00:00.5";

    private static Dictionary<string, object?> Incremental(Dictionary<string, object?>? extra = null)
    {
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/events",
            ["query"] = new Dictionary<string, object?> { ["since"] = "{{ watermark }}", ["until"] = "{{ window_upper }}" },
            ["cursor"] = "updated_at",
            ["cursor_type"] = "timestamp",
        };
        foreach (var (k, v) in extra ?? [])
        {
            options[k] = v;
        }

        return options;
    }

    private static DatasetSpec WithWatermark(Dictionary<string, object?> options, string cursor = "updated_at")
        => new("s", "d", options) { WatermarkCursor = cursor, WatermarkValue = Watermark, WatermarkUpperBound = Upper };

    private static async Task<(StubHttpServer Server, List<object?[]> Rows)> ReadAsync(DatasetSpec spec,
        string body = "[]", string route = "/events")
    {
        var server = new StubHttpServer();
        server.MapPrefix(route, _ => new StubResponse(200, body));
        var rows = new List<object?[]>();
        try
        {
            var connector = new HttpConnector();
            await using var source = await connector.OpenAsync(new ConnectorConfig(
                new Dictionary<string, object?> { ["base_url"] = server.BaseUrl.ToString() }), CancellationToken.None);
            var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
            await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                using (batch)
                {
                    for (var r = 0; r < batch.Length; r++)
                    {
                        rows.Add([.. Enumerable.Range(0, batch.ColumnCount).Select(c => FileReaderRows.Value(batch.Column(c), r))]);
                    }
                }
            }
        }
        catch
        {
            await server.DisposeAsync();
            throw;
        }

        return (server, rows);
    }

    private static string Errors(Dictionary<string, object?> options, string? cursor = null)
        => Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(
            new DatasetSpec("s", "d", options) { WatermarkCursor = cursor })).Message;

    private static string Query(StubHttpServer server) => Uri.UnescapeDataString(server.Requests.Single().Url.Query);

    // ---- watermark_format ----

    [Fact]
    public async Task Epoch_seconds_floors_the_watermark_and_rounds_the_upper_bound_up()
    {
        var (server, _) = await ReadAsync(WithWatermark(Incremental(new() { ["watermark_format"] = "epoch_s" })));
        await using var _s = server;

        Assert.Equal("?since=1794227696&until=1794268801", Query(server));
    }

    [Fact]
    public async Task Epoch_milliseconds_rounds_per_millisecond()
    {
        var (server, _) = await ReadAsync(WithWatermark(Incremental(new() { ["watermark_format"] = "epoch_ms" })));
        await using var _s = server;

        Assert.Equal("?since=1794227696789&until=1794268800500", Query(server));
    }

    [Fact]
    public async Task Custom_format_renders_in_the_query_and_the_path()
    {
        var options = Incremental(new() { ["watermark_format"] = "yyyy-MM-dd HH:mm" });
        options["path"] = "/events/{{ watermark }}";
        var (server, _) = await ReadAsync(WithWatermark(options), route: "/events/");
        await using var _s = server;

        Assert.Equal("?since=2026-11-09 12:34&until=2026-11-10 00:01", Query(server));
        Assert.Equal("/events/2026-11-09%2012%3A34", server.Requests.Single().Url.AbsolutePath);
    }

    [Fact]
    public async Task Without_watermark_format_the_rendering_is_unchanged()
    {
        var (server, _) = await ReadAsync(WithWatermark(Incremental()));
        await using var _s = server;

        Assert.Equal("?since=2026-11-09T12:34:56Z&until=2026-11-10T00:00:01Z", Query(server));
    }

    [Fact]
    public async Task Explicit_iso8601_renders_like_the_default()
    {
        var (server, _) = await ReadAsync(WithWatermark(Incremental(new() { ["watermark_format"] = "iso8601" })));
        await using var _s = server;

        Assert.Equal("?since=2026-11-09T12:34:56Z&until=2026-11-10T00:00:01Z", Query(server));
    }

    [Fact]
    public async Task First_run_still_omits_the_parameter()
    {
        var (server, _) = await ReadAsync(new DatasetSpec("s", "d", Incremental(new() { ["watermark_format"] = "epoch_s" }))
        {
            WatermarkCursor = "updated_at",
        });
        await using var _s = server;

        Assert.Equal("", server.Requests.Single().Url.Query);
    }

    [Fact]
    public async Task Epoch_format_is_a_json_number_in_a_whole_value_body_binding()
    {
        var options = Incremental(new()
        {
            ["watermark_format"] = "epoch_s",
            ["method"] = "post",
            ["body"] = new Dictionary<string, object?> { ["since"] = "{{ watermark }}", ["label"] = "after {{ watermark }}" },
        });
        options.Remove("query");
        var (server, _) = await ReadAsync(WithWatermark(options));
        await using var _s = server;

        Assert.Equal("""{"since":1794227696,"label":"after 1794227696"}""", server.Requests.Single().Body);
    }

    [Fact]
    public async Task Date_format_on_a_timestamp_body_is_a_string()
    {
        var options = Incremental(new()
        {
            ["watermark_format"] = "date",
            ["method"] = "post",
            ["body"] = new Dictionary<string, object?> { ["since"] = "{{ watermark }}" },
        });
        options.Remove("query");
        var (server, _) = await ReadAsync(WithWatermark(options));
        await using var _s = server;

        Assert.Equal("""{"since":"2026-11-09"}""", server.Requests.Single().Body);
    }

    [Fact]
    public async Task A_date_cursor_takes_a_date_only_custom_format()
    {
        var options = Incremental(new() { ["watermark_format"] = "yyyyMMdd", ["cursor_type"] = "date" });
        var spec = new DatasetSpec("s", "d", options)
        {
            WatermarkCursor = "updated_at", WatermarkValue = "2026-11-09", WatermarkUpperBound = "2026-11-30",
        };
        var (server, _) = await ReadAsync(spec);
        await using var _s = server;

        Assert.Equal("?since=20261109&until=20261130", Query(server));
    }

    [Fact]
    public async Task The_list_request_of_a_for_each_dataset_renders_the_same_format()
    {
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/orders/{{ item.id }}",
            ["watermark_format"] = "epoch_s",
            ["for_each"] = new Dictionary<string, object?>
            {
                ["path"] = "/events",
                ["query"] = new Dictionary<string, object?> { ["since"] = "{{ watermark }}" },
                ["fields"] = new Dictionary<string, object?> { ["id"] = "/id" },
            },
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["updated_at"] = "timestamp" },
        };
        var (server, _) = await ReadAsync(WithWatermark(options));
        await using var _s = server;

        Assert.Equal("?since=1794227696", Query(server));
    }

    [Theory]
    [InlineData("bigint")]
    [InlineData("varchar")]
    [InlineData("decimal")]
    public void Watermark_format_is_refused_on_a_numeric_or_text_cursor(string type)
        => Assert.Contains($"the cursor is '{type}'",
            Errors(Incremental(new() { ["watermark_format"] = "epoch_s", ["cursor_type"] = type })));

    [Fact]
    public void Watermark_format_is_refused_on_a_contract_numeric_cursor()
    {
        var options = Incremental(new()
        {
            ["watermark_format"] = "epoch_s",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
        });
        options.Remove("cursor");
        options.Remove("cursor_type");
        Assert.Contains("the cursor is 'bigint'", Errors(options, cursor: "id"));
    }

    [Theory]
    [InlineData("epoch_s")]
    [InlineData("iso8601")]
    [InlineData("yyyy-MM-dd HH")]
    public void A_date_cursor_refuses_time_formats(string format)
        => Assert.Contains("the cursor is a date",
            Errors(Incremental(new() { ["watermark_format"] = format, ["cursor_type"] = "date" })));

    [Fact]
    public void Watermark_format_without_a_reference_is_refused()
    {
        var options = Incremental(new() { ["watermark_format"] = "epoch_s" });
        options["query"] = new Dictionary<string, object?> { ["state"] = "all" };
        Assert.Contains("'watermark_format' has no effect", Errors(options));
    }

    [Fact]
    public void A_bad_custom_format_and_other_errors_are_reported_together()
    {
        var message = Errors(Incremental(new() { ["watermark_format"] = "HH:mm", ["epoch_unit"] = "us" }));
        Assert.Contains("skips the year", message);
        Assert.Contains("'epoch_unit' must be 's' or 'ms'", message);
    }

    [Fact]
    public void Watermark_format_is_refused_inside_for_each()
    {
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/orders/{{ item.id }}",
            ["for_each"] = new Dictionary<string, object?>
            {
                ["path"] = "/events",
                ["fields"] = new Dictionary<string, object?> { ["id"] = "/id" },
                ["watermark_format"] = "epoch_s",
                ["epoch_unit"] = "s",
            },
        };
        var message = Errors(options);
        Assert.Contains("for_each.watermark_format is not allowed", message);
        Assert.Contains("for_each.epoch_unit is not allowed", message);
    }

    [Fact]
    public async Task A_sql_declared_numeric_cursor_is_refused_at_run_time()
    {
        // No raw cursor and no contract column at parse time: the type is known only from the spec.
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/events",
            ["query"] = new Dictionary<string, object?> { ["since"] = "{{ watermark }}" },
            ["watermark_format"] = "epoch_s",
            ["columns"] = new Dictionary<string, string> { ["seq"] = "bigint" },
        };
        HttpDatasetConfig.Parse(new DatasetSpec("s", "d", options));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAsync(
            new DatasetSpec("s", "d", options) { WatermarkCursor = "seq", WatermarkValue = "41" }));
        Assert.False(ex.IsTransient);
        Assert.Contains("the cursor is 'bigint'", ex.Message);
    }

    [Fact]
    public async Task A_stored_watermark_that_is_not_a_timestamp_is_a_permanent_error_without_the_value()
    {
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAsync(
            new DatasetSpec("s", "d", Incremental(new() { ["watermark_format"] = "epoch_s" }))
            {
                WatermarkCursor = "updated_at", WatermarkValue = "not-a-time-SECRET",
            }));
        Assert.False(ex.IsTransient);
        Assert.Contains("cannot render the stored watermark", ex.Message);
        Assert.DoesNotContain("SECRET", ex.Message);
    }

    // ---- epoch_unit ----

    [Fact]
    public async Task Epoch_numbers_land_in_contract_timestamp_columns()
    {
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/events",
            ["epoch_unit"] = "ms",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["at"] = "timestamp" },
        };
        var (server, rows) = await ReadAsync(new DatasetSpec("s", "d", options),
            """[{"id":1,"at":1700000000123},{"id":2,"at":"2026-11-09T00:00:00Z"},{"id":3,"at":null}]""");
        await using var _s = server;

        Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_123), rows[0][1]);
        Assert.Equal(new DateTimeOffset(2026, 11, 9, 0, 0, 0, TimeSpan.Zero), rows[1][1]);
        Assert.Null(rows[2][1]);
    }

    [Fact]
    public async Task Epoch_numbers_land_in_a_raw_timestamp_cursor()
    {
        var options = Incremental(new() { ["epoch_unit"] = "s" });
        options.Remove("query");
        var (server, rows) = await ReadAsync(new DatasetSpec("s", "d", options), """[{"updated_at":1700000000}]""");
        await using var _s = server;

        Assert.Equal(DateTimeOffset.FromUnixTimeSeconds(1_700_000_000), Assert.Single(rows)[3]);
    }

    [Fact]
    public async Task An_epoch_number_without_epoch_unit_names_the_option()
    {
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/events",
            ["columns"] = new Dictionary<string, string> { ["at"] = "timestamp" },
        };
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(new DatasetSpec("s", "d", options), """[{"at":1700000000}]"""));
        Assert.False(ex.IsTransient);
        Assert.Contains("column 'at'", ex.Message);
        Assert.Contains("JSON number", ex.Message);
        Assert.Contains("'epoch_unit: s'", ex.Message);
    }

    [Fact]
    public async Task A_millisecond_value_read_as_seconds_suggests_ms()
    {
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/events",
            ["epoch_unit"] = "s",
            ["columns"] = new Dictionary<string, string> { ["at"] = "timestamp" },
        };
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAsync(new DatasetSpec("s", "d", options), """[{"at":1700000000123000}]"""));
        Assert.Contains("'epoch_unit: ms'", ex.Message);
    }

    [Fact]
    public async Task Epoch_unit_applies_to_item_sourced_timestamp_columns()
    {
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/orders/{{ item.id }}",
            ["epoch_unit"] = "s",
            ["for_each"] = new Dictionary<string, object?>
            {
                ["path"] = "/list",
                ["fields"] = new Dictionary<string, object?> { ["id"] = "/id", ["created"] = "/created" },
            },
            ["columns"] = new Dictionary<string, string> { ["created"] = "timestamp", ["n"] = "bigint" },
        };
        await using var server = new StubHttpServer();
        server.Map("/list", _ => new StubResponse(200, """[{"id":"o1","created":1700000000}]"""));
        server.Map("/orders/o1", _ => new StubResponse(200, """[{"n":5}]"""));
        var connector = new HttpConnector();
        await using var source = await connector.OpenAsync(new ConnectorConfig(
            new Dictionary<string, object?> { ["base_url"] = server.BaseUrl.ToString() }), CancellationToken.None);
        var partitions = await source.PlanReadAsync(new DatasetSpec("s", "d", options), ReadHints.None, CancellationToken.None);
        var rows = new List<object?[]>();
        await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            using (batch)
            {
                rows.Add([FileReaderRows.Value(batch.Column(0), 0), FileReaderRows.Value(batch.Column(1), 0)]);
            }
        }

        Assert.Equal([DateTimeOffset.FromUnixTimeSeconds(1_700_000_000), 5L], Assert.Single(rows));
    }

    [Fact]
    public void Epoch_unit_without_a_timestamp_is_refused()
        => Assert.Contains("'epoch_unit' has no effect", Errors(new Dictionary<string, object?>
        {
            ["path"] = "/events",
            ["epoch_unit"] = "s",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint" },
        }));

    [Fact]
    public void Epoch_unit_is_refused_on_a_text_file()
        => Assert.Contains("file.format csv has only text values", Errors(new Dictionary<string, object?>
        {
            ["path"] = "/export.csv",
            ["epoch_unit"] = "s",
            ["file"] = new Dictionary<string, object?> { ["format"] = "csv" },
            ["columns"] = new Dictionary<string, string> { ["at"] = "timestamp" },
        }));

    // ---- nested contract columns ----

    private static Dictionary<string, object?> Pointers(params (string Column, string Pointer)[] pointers)
        => pointers.ToDictionary(p => p.Column, p => (object?)p.Pointer);

    [Fact]
    public async Task Nested_columns_land_from_their_pointers()
    {
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/issues",
            ["items"] = "/issues",
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["status"] = "varchar", ["updated"] = "timestamp" },
            ["column_pointers"] = Pointers(("status", "/fields/status/name"), ("updated", "/fields/updated")),
            ["epoch_unit"] = "s",
        };
        var (server, rows) = await ReadAsync(new DatasetSpec("s", "d", options),
            """{"issues":[{"id":1,"fields":{"status":{"name":"Done"},"updated":1700000000}},{"id":2,"fields":{}}]}""",
            route: "/issues");
        await using var _s = server;

        Assert.Equal([1L, "Done", DateTimeOffset.FromUnixTimeSeconds(1_700_000_000)], rows[0]);
        Assert.Equal([2L, null, null], rows[1]);
    }

    [Fact]
    public async Task A_nested_cursor_drives_the_truncation_guard()
    {
        // max_pages cuts the crawl short; descending nested cursor values must be seen by the guard,
        // which reads the projected row, so the read refuses rather than advancing past skipped rows.
        var options = new Dictionary<string, object?>
        {
            ["path"] = "/events",
            ["max_pages"] = 1L,
            ["pagination"] = new Dictionary<string, object?> { ["strategy"] = "page" },
            ["columns"] = new Dictionary<string, string> { ["updated"] = "timestamp" },
            ["column_pointers"] = Pointers(("updated", "/attributes/updated_at")),
        };
        var ex = await Assert.ThrowsAsync<PzConnectorException>(() => ReadAsync(
            new DatasetSpec("s", "d", options) { WatermarkCursor = "updated" },
            """[{"attributes":{"updated_at":"2026-11-09T02:00:00Z"}},{"attributes":{"updated_at":"2026-11-09T01:00:00Z"}}]"""));
        Assert.Contains("max_pages", ex.Message);
    }

    [Fact]
    public async Task Nested_columns_read_from_json_files()
    {
        var config = HttpDatasetConfig.Parse(new DatasetSpec("s", "d", new Dictionary<string, object?>
        {
            ["path"] = "/export.json",
            ["file"] = new Dictionary<string, object?> { ["format"] = "json", ["layout"] = "lines" },
            ["columns"] = new Dictionary<string, string> { ["name"] = "varchar" },
            ["column_pointers"] = Pointers(("name", "/user/name")),
        }));
        var rows = await FileReaderRows.ReadAsync(new HttpJsonFileReader(1024 * 1024),
            Encoding.UTF8.GetBytes("{\"user\":{\"name\":\"ana\"}}\n{\"user\":null}\n"), config, "export.json");

        Assert.Equal(["ana", null], rows.Select(r => r[0]));
    }

    [Fact]
    public void Pointer_errors_are_reported_together()
    {
        var message = Errors(new Dictionary<string, object?>
        {
            ["path"] = "/orders/{{ item.id }}",
            ["for_each"] = new Dictionary<string, object?>
            {
                ["path"] = "/list",
                ["fields"] = new Dictionary<string, object?> { ["id"] = "/id" },
                ["column_pointers"] = Pointers(("id", "/x")),
            },
            ["columns"] = new Dictionary<string, string> { ["id"] = "bigint", ["n"] = "bigint" },
            ["column_pointers"] = Pointers(("id", "/order/id"), ("n", "count"), ("ghost", "/g")),
        });
        Assert.Contains("filled from the for_each field 'id'", message);
        Assert.Contains("column 'n': 'pointer' must be a JSON pointer", message);
        Assert.Contains("column 'ghost' has a 'pointer' but is not declared", message);
        Assert.Contains("for_each.column_pointers is not allowed", message);
    }

    [Fact]
    public void Pointers_are_refused_on_a_text_file()
        => Assert.Contains("file.format xml has none", Errors(new Dictionary<string, object?>
        {
            ["path"] = "/export.xml",
            ["file"] = new Dictionary<string, object?> { ["format"] = "xml", ["records"] = "row" },
            ["columns"] = new Dictionary<string, string> { ["name"] = "varchar" },
            ["column_pointers"] = Pointers(("name", "/user/name")),
        }));

    [Fact]
    public void The_dataset_schema_lists_the_new_options()
    {
        var schema = new HttpConnector().DatasetConfigSchema;
        Assert.Contains("\"column_pointers\"", schema);
        Assert.Contains("\"watermark_format\"", schema);
        Assert.Contains("\"epoch_unit\"", schema);
    }
}
