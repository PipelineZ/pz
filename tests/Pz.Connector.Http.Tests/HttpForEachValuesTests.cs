using System.Text.Json;
using System.Text.Json.Nodes;
using Json.Schema;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

/// <summary><c>for_each.values</c> (a literal item list in place of a list request) and
/// <c>for_each.dedupe</c> (skip a list record whose fields were already seen), sequential and parallel,
/// with checkpoints and validation.</summary>
public sealed class HttpForEachValuesTests : IAsyncLifetime
{
    private readonly StubHttpServer _server = new();
    private readonly List<string> _notices = [];

    public Task InitializeAsync() => Task.CompletedTask;

    public async Task DisposeAsync() => await _server.DisposeAsync();

    private static Dictionary<string, object?> Map(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    private static List<object?> List(params object?[] items) => [.. items];

    private HttpSource Source()
    {
        var errors = new List<string>();
        var connection = HttpConnectionConfig.Parse(new ConnectorConfig(
            new Dictionary<string, object?> { ["base_url"] = _server.BaseUrl.ToString() }), errors);
        Assert.Empty(errors);
        var source = new HttpSource(connection!, TimeProvider.System);
        source.UseNotice(_notices.Add);
        return source;
    }

    private async Task<(List<object?[]> Rows, List<string> Tokens)> Read(Dictionary<string, object?> options,
        string? resumeFrom = null)
    {
        await using var source = Source();
        var partitions = await source.PlanReadAsync(new DatasetSpec("s", "d", options), ReadHints.None,
            CancellationToken.None);
        var partition = Assert.IsAssignableFrom<ICheckpointingPartition>(Assert.Single(partitions));
        if (resumeFrom is not null)
        {
            Assert.True(partition.TryResumeFrom(resumeFrom));
        }

        var rows = new List<object?[]>();
        var tokens = new List<string>();
        await foreach (var batch in partition.ReadAsync(new BatchOptions(), CancellationToken.None))
        {
            using (batch)
            {
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

            if (partition.TryGetCheckpoint(out var token) && token is not null)
            {
                tokens.Add(token);
            }
        }

        return (rows, tokens);
    }

    private static string Message(Dictionary<string, object?> options)
        => Assert.Throws<PzConnectorException>(() => HttpDatasetConfig.Parse(new DatasetSpec("s", "d", options))).Message;

    private List<string> Paths() => [.. _server.Requests.Select(r => r.Url.PathAndQuery)];

    /// <summary>/regions/{r}/sales serves one row named after the region.</summary>
    private void MapSales()
        => _server.MapPrefix("/regions/", r => new StubResponse(200,
            $$"""{"data":[{"sale":"{{r.Url.AbsolutePath.Split('/')[2]}}"}]}"""));

    private static Dictionary<string, object?> RegionOptions(Dictionary<string, object?>? forEachExtra = null,
        params string[] regions)
    {
        var forEach = Map(("values", List([.. regions.Select(r => (object?)Map(("region", r)))])));
        foreach (var (k, v) in forEachExtra ?? [])
        {
            forEach[k] = v;
        }

        return Map(("path", "/regions/{{ item.region }}/sales"), ("items", "/data"), ("for_each", forEach));
    }

    [Fact]
    public async Task Values_drive_one_item_request_each_in_order()
    {
        MapSales();

        var (rows, _) = await Read(RegionOptions(null, "eu", "us", "apac"));

        Assert.Equal(["/regions/eu/sales", "/regions/us/sales", "/regions/apac/sales"], Paths());
        Assert.Equal(3, rows.Count);
        Assert.Equal("""{"region":"eu"}""", rows[0][3]);
        Assert.Equal("""{"sale":"apac"}""", rows[2][0]);
        Assert.Empty(_notices);
    }

    [Fact]
    public async Task Values_of_every_scalar_kind_expand_like_list_fields()
    {
        _server.Map("/stats", _ => new StubResponse(200, """{"data":[{"n":1}]}"""));

        var (rows, _) = await Read(Map(("path", "/stats"), ("items", "/data"),
            ("query", Map(("id", "{{ item.id }}"), ("on", "{{ item.on }}"), ("name", "{{ item.name }}"))),
            ("for_each", Map(("values", List(
                Map(("id", 42L), ("on", true), ("name", "a b")),
                Map(("id", 1.5), ("on", false), ("name", "x"))))))));

        Assert.Equal(["/stats?id=42&on=true&name=a%20b", "/stats?id=1.5&on=false&name=x"], Paths());
        Assert.Equal("""{"id":42,"on":true,"name":"a b"}""", rows[0][3]);
    }

    [Fact]
    public async Task Values_run_in_parallel()
    {
        MapSales();

        var (rows, _) = await Read(RegionOptions(Map(("concurrency", 3L)), "eu", "us", "apac", "latam"));

        Assert.Equal(4, rows.Count);
        Assert.Equal(["apac", "eu", "latam", "us"], rows.Select(r => JsonNode.Parse((string)r[0]!)!["sale"]!.GetValue<string>()).Order());
    }

    [Fact]
    public async Task Values_checkpoint_names_the_literal_list_and_resumes_at_the_index()
    {
        MapSales();
        var options = RegionOptions(null, "eu", "us", "apac");

        var (_, tokens) = await Read(options);
        Assert.True(HttpCheckpointToken.TryDecodeForEach(tokens[0], out var position));
        Assert.Equal("values", position.List);
        Assert.Equal(1, position.Index);

        var before = _server.Requests.Count;
        var (rows, _) = await Read(options, resumeFrom: tokens[0]);

        Assert.Equal(2, rows.Count);
        Assert.Equal(["/regions/us/sales", "/regions/apac/sales"], Paths().Skip(before));
    }

    [Fact]
    public async Task A_values_dataset_refuses_a_list_request_token_and_a_list_dataset_refuses_a_values_token()
    {
        await using var source = Source();
        var values = Assert.IsAssignableFrom<ICheckpointingPartition>(Assert.Single(await source.PlanReadAsync(
            new DatasetSpec("s", "d", RegionOptions(null, "eu")), ReadHints.None, CancellationToken.None)));
        var listed = Assert.IsAssignableFrom<ICheckpointingPartition>(Assert.Single(await source.PlanReadAsync(
            new DatasetSpec("s", "d", Map(("path", "/r/{{ item.region }}"),
                ("for_each", Map(("path", "/regions"), ("fields", Map(("region", "/id"))))))),
            ReadHints.None, CancellationToken.None)));

        var listToken = HttpCheckpointToken.EncodeForEach(new Uri(_server.BaseUrl, "/regions").ToString(), 0, null);
        var valuesToken = HttpCheckpointToken.EncodeForEach("values", 0, null);

        Assert.True(values.TryResumeFrom(valuesToken));
        Assert.False(values.TryResumeFrom(listToken));
        Assert.False(listed.TryResumeFrom(valuesToken));
    }

    [Fact]
    public async Task Dedupe_skips_records_already_seen_across_list_pages_and_counts_them()
    {
        _server.Map("/orders", r => r.Url.Query.Contains("cursor=X")
            ? new StubResponse(200, """{"data":[{"id":"o2","x":9},{"id":"o3"}]}""")
            : new StubResponse(200, """{"data":[{"id":"o1"},{"id":"o2"},{"id":"o1"}],"next":"X"}"""));
        _server.MapPrefix("/orders/", r => new StubResponse(200,
            $$"""{"lines":[{"sku":"{{r.Url.AbsolutePath.Split('/')[2]}}"}]}"""));

        var (rows, _) = await Read(Map(("path", "/orders/{{ item.order_id }}/lines"), ("items", "/lines"),
            ("for_each", Map(("path", "/orders"), ("items", "/data"), ("fields", Map(("order_id", "/id"))),
                ("dedupe", true),
                ("pagination", Map(("strategy", "cursor"), ("pointer", "/next"), ("param", "cursor")))))));

        Assert.Equal(["/orders", "/orders/o1/lines", "/orders/o2/lines", "/orders?cursor=X", "/orders/o3/lines"], Paths());
        Assert.Equal(3, rows.Count);
        var notice = Assert.Single(_notices);
        Assert.Contains("for_each.dedupe skipped 2 duplicate list records", notice);
        Assert.Contains("(first: item order_id=o1)", notice);
    }

    [Fact]
    public async Task Dedupe_works_in_parallel_and_duplicates_do_not_count_toward_max_items()
    {
        MapSales();

        var (rows, _) = await Read(RegionOptions(Map(("dedupe", true), ("concurrency", 2L)),
            "eu", "eu", "us", "eu", "us", "apac"));

        Assert.Equal(3, rows.Count);
        Assert.Equal(3, _server.Requests.Count);
        Assert.Contains(_notices, n => n.Contains("skipped 3 duplicate list records"));
    }

    [Fact]
    public async Task Dedupe_compares_json_values_so_a_string_and_a_number_differ()
    {
        _server.Map("/stats", _ => new StubResponse(200, """{"data":[{"n":1}]}"""));

        var (rows, _) = await Read(Map(("path", "/stats"), ("items", "/data"), ("query", Map(("id", "{{ item.id }}"))),
            ("for_each", Map(("dedupe", true), ("values", List(Map(("id", "1")), Map(("id", 1L)), Map(("id", 1L))))))));

        Assert.Equal(2, rows.Count);
        Assert.Contains(_notices, n => n.Contains("skipped 1 duplicate"));
    }

    [Fact]
    public async Task Without_dedupe_a_parent_listed_twice_is_fetched_twice()
    {
        MapSales();

        var (rows, _) = await Read(RegionOptions(null, "eu", "eu"));

        Assert.Equal(2, rows.Count);
        Assert.Empty(_notices);
    }

    [Fact]
    public void Values_are_validated_all_at_once()
    {
        var message = Message(Map(("path", "/r/{{ item.region }}"), ("for_each", Map(
            ("values", List(Map(("region", "eu")), Map(("region", "us"), ("extra", "x")), "plain",
                Map(("Bad-Name", "x")), Map(("region", List("a"))))),
            ("path", "/regions"), ("fields", Map(("region", "/id"))), ("max_items", 2L), ("dedupe", "yes")))));

        Assert.Contains("for_each.values[1] has keys extra, region; every entry must have the keys of the first (region)", message);
        Assert.Contains("for_each.values[2] must be a map", message);
        Assert.Contains("for_each.values[3] key 'Bad-Name' is not valid", message);
        Assert.Contains("for_each.values[4].region must be a string, number, boolean or null", message);
        Assert.Contains("for_each.path cannot be combined with for_each.values", message);
        Assert.Contains("for_each.fields cannot be combined with for_each.values", message);
        Assert.Contains("for_each.max_items cannot be combined with for_each.values", message);
        Assert.Contains("for_each.dedupe must be a boolean", message);
    }

    [Theory]
    [InlineData("empty")]
    [InlineData("scalar")]
    [InlineData("empty-map")]
    public void Values_must_be_a_non_empty_list_of_non_empty_maps(string kind)
    {
        object? values = kind switch
        {
            "empty" => List(),
            "scalar" => "eu",
            _ => List(Map()),
        };

        var message = Message(Map(("path", "/r"), ("for_each", Map(("values", values)))));

        Assert.Contains(kind == "empty-map" ? "for_each.values[0] must declare at least one field"
            : "for_each.values must be a non-empty list of maps", message);
    }

    [Fact]
    public void An_item_reference_must_name_a_values_key()
    {
        var message = Message(Map(("path", "/r/{{ item.zone }}"), ("for_each", Map(("values", List(Map(("region", "eu"))))))));

        Assert.Contains("zone", message);
    }

    [Fact]
    public void Schema_accepts_values_without_path_or_fields()
    {
        var schema = JsonSchema.FromText(new HttpConnector().DatasetConfigSchema);

        Assert.True(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/r/{{ item.region }}","for_each":{"values":[{"region":"eu"},{"region":"us"}],"dedupe":true,"concurrency":2}}""")).IsValid);
        Assert.True(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/x","for_each":{"path":"/o","fields":{"id":"/id"},"dedupe":false}}""")).IsValid);
        Assert.False(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/x","for_each":{"dedupe":true}}""")).IsValid);
        Assert.False(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/x","for_each":{"values":[]}}""")).IsValid);
        Assert.False(schema.Evaluate(JsonSerializer.Deserialize<JsonElement>(
            """{"path":"/x","for_each":{"values":[{"a":{"b":1}}]}}""")).IsValid);
    }

    [Fact]
    public async Task Parallel_values_checkpoint_at_barriers_of_four_times_the_concurrency()
    {
        MapSales();
        var regions = Enumerable.Range(1, 10).Select(i => $"r{i}").ToArray();
        var options = RegionOptions(Map(("concurrency", 2L)), regions);

        var (rows, tokens) = await Read(options);

        Assert.Equal(10, rows.Count);
        // Chunks of 8 items: one barrier after r1..r8 names the rest; the last chunk ends the read.
        var position = Assert.Single(tokens.Select(t => HttpCheckpointToken.TryDecodeForEach(t, out var p) ? p : null));
        Assert.Equal("values@8", position!.List);
        Assert.Equal(0, position.Index);

        var before = _server.Requests.Count;
        var (resumed, _) = await Read(options, resumeFrom: tokens[0]);

        Assert.Equal(2, resumed.Count);
        Assert.Equal(["/regions/r10/sales", "/regions/r9/sales"], Paths().Skip(before).Order());
    }

    [Fact]
    public async Task A_values_offset_token_is_accepted_only_within_the_list()
    {
        await using var source = Source();
        var partition = Assert.IsAssignableFrom<ICheckpointingPartition>(Assert.Single(await source.PlanReadAsync(
            new DatasetSpec("s", "d", RegionOptions(null, "eu", "us")), ReadHints.None, CancellationToken.None)));

        Assert.True(partition.TryResumeFrom(HttpCheckpointToken.EncodeForEach("values@2", 0, null)));
        Assert.False(partition.TryResumeFrom(HttpCheckpointToken.EncodeForEach("values@3", 0, null)));
        Assert.False(partition.TryResumeFrom(HttpCheckpointToken.EncodeForEach("values@-1", 0, null)));
        Assert.False(partition.TryResumeFrom(HttpCheckpointToken.EncodeForEach("values@x", 0, null)));
    }

    [Fact]
    public async Task A_sequential_resume_offset_token_reads_from_that_offset()
    {
        MapSales();

        var options = RegionOptions(null, "eu", "us", "apac");
        var anchor = HttpDatasetConfig.Parse(new DatasetSpec("s", "d", options)).ForEach!.ValuesAnchor;

        var (rows, _) = await Read(options,
            resumeFrom: HttpCheckpointToken.EncodeForEach(new("values@1", 1, null, anchor)));

        Assert.Equal(["/regions/apac/sales"], Paths());
        Assert.Single(rows);
        Assert.Empty(_notices);
    }

    [Fact]
    public async Task A_values_offset_token_from_an_edited_list_restarts_at_the_first_value_with_a_notice()
    {
        MapSales();
        // The checkpoint was taken while the list was eu, us, apac, latam; "eu" has since been removed, so
        // offset 2 would now skip "latam".
        var before = HttpDatasetConfig.Parse(new DatasetSpec("s", "d",
            RegionOptions(Map(("concurrency", 2L)), "eu", "us", "apac", "latam"))).ForEach!.ValuesAnchor;
        var token = HttpCheckpointToken.EncodeForEach(new("values@2", 0, null, before));

        var (rows, _) = await Read(RegionOptions(Map(("concurrency", 2L)), "us", "apac", "latam"), resumeFrom: token);

        Assert.Equal(3, rows.Count);
        Assert.Equal(["/regions/apac/sales", "/regions/latam/sales", "/regions/us/sales"], Paths().Order());
        var notice = Assert.Single(_notices);
        Assert.Contains("for_each.values changed since the checkpoint", notice);
        Assert.Contains("from the first value", notice);
    }

    [Fact]
    public async Task A_values_offset_token_without_an_anchor_restarts_too()
    {
        MapSales();

        var (rows, _) = await Read(RegionOptions(null, "eu", "us"),
            resumeFrom: HttpCheckpointToken.EncodeForEach("values@1", 0, null));

        Assert.Equal(2, rows.Count);
        Assert.Single(_notices);
    }

    [Fact]
    public async Task Parallel_values_barrier_tokens_carry_the_list_anchor()
    {
        MapSales();
        var options = RegionOptions(Map(("concurrency", 1L * 2)), [.. Enumerable.Range(1, 9).Select(i => $"r{i}")]);

        var (_, tokens) = await Read(options);

        Assert.True(HttpCheckpointToken.TryDecodeForEach(Assert.Single(tokens), out var position));
        Assert.Equal(HttpDatasetConfig.Parse(new DatasetSpec("s", "d", options)).ForEach!.ValuesAnchor, position.Anchor);
    }

    /// <summary>/orders lists o1, o2, o1, o3 on one page; items have one line each.</summary>
    private void MapDuplicatedOrders()
    {
        _server.Map("/orders", _ => new StubResponse(200, """{"data":[{"id":"o1"},{"id":"o2"},{"id":"o1"},{"id":"o3"}]}"""));
        _server.MapPrefix("/orders/", r => new StubResponse(200,
            $$"""{"lines":[{"sku":"{{r.Url.AbsolutePath.Split('/')[2]}}"}]}"""));
    }

    private static Dictionary<string, object?> DedupedOrders(long concurrency)
        => Map(("path", "/orders/{{ item.order_id }}/lines"), ("items", "/lines"),
            ("for_each", Map(("path", "/orders"), ("items", "/data"), ("fields", Map(("order_id", "/id"))),
                ("dedupe", true), ("concurrency", concurrency))));

    [Theory]
    [InlineData(1L)]
    [InlineData(2L)]
    public async Task A_resume_seeds_dedupe_with_the_records_before_its_position(long concurrency)
    {
        MapDuplicatedOrders();
        var listUrl = new Uri(_server.BaseUrl, "/orders").ToString();
        // Resume at record 2 (the second o1): o1 was read before the resume position on this list page.
        var token = HttpCheckpointToken.EncodeForEach(listUrl, 2, null);

        var (rows, _) = await Read(DedupedOrders(concurrency), resumeFrom: token);

        Assert.Equal(["/orders", "/orders/o3/lines"], Paths());
        Assert.Single(rows);
        Assert.Contains(_notices, n => n.Contains("for_each.dedupe skipped 1 duplicate list records (first: item order_id=o1)"));
    }

    [Fact]
    public async Task Values_with_dedupe_resume_and_skip_a_duplicate_of_a_value_before_the_position()
    {
        MapSales();

        var (rows, _) = await Read(RegionOptions(Map(("dedupe", true)), "eu", "us", "eu", "apac"),
            resumeFrom: HttpCheckpointToken.EncodeForEach("values", 2, null));

        Assert.Equal(["/regions/apac/sales"], Paths());
        Assert.Single(rows);
        Assert.Contains(_notices, n => n.Contains("skipped 1 duplicate"));
    }
}
