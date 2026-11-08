using Apache.Arrow;
using System.Text.Json.Nodes;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;
using Xunit;

namespace Pz.Connector.Http.Tests;

/// <summary>Checkpoint tokens of POST reads: the v2 token format, resume at a body offset, and the
/// refusals (foreign host, method mismatch, garbled payload).</summary>
public class HttpPostCheckpointTests
{
    private static Dictionary<string, object?> Map(params (string Key, object? Value)[] entries)
        => entries.ToDictionary(e => e.Key, e => e.Value);

    private static Dictionary<string, object?> OffsetPost() => Map(
        ("path", "/_search"), ("method", "post"), ("items", "/hits"),
        ("body", Map(("q", "x"))), ("columns", new Dictionary<string, string> { ["id"] = "bigint" }),
        ("pagination", Map(("strategy", "offset"), ("location", "body"), ("param", "/from"),
            ("size_param", "/size"), ("size", 2L))));

    private static async Task<(ISource Source, ICheckpointingPartition Partition)> OpenAsync(
        StubHttpServer server, Dictionary<string, object?> options)
    {
        var connection = new Dictionary<string, object?> { ["base_url"] = server.BaseUrl.ToString() };
        var source = await new HttpConnector().OpenAsync(new ConnectorConfig(connection), CancellationToken.None);
        var partitions = await source.PlanReadAsync(new DatasetSpec("s", "d", options), ReadHints.None,
            CancellationToken.None);
        return (source, Assert.IsAssignableFrom<ICheckpointingPartition>(partitions[0]));
    }

    private static int FromOf(StubRequest request) => JsonNode.Parse(request.Body)!["from"]!.GetValue<int>();

    [Fact]
    public void Get_tokens_stay_plain_urls()
    {
        var request = new HttpPageRequest(HttpMethod.Get, new Uri("https://h.example/a?page=2"), null, null);

        var token = HttpCheckpointToken.Encode(request, isPost: false);

        Assert.Equal("https://h.example/a?page=2", token);
        Assert.True(HttpCheckpointToken.TryDecode(token, false, HttpMethod.Get, out var back));
        Assert.Equal(request.Url, back!.Url);
    }

    [Fact]
    public void Post_token_round_trips_url_and_body()
    {
        var json = new HttpPageRequest(HttpMethod.Post, new Uri("https://h.example/s"),
            JsonNode.Parse("""{"from":4,"q":{"a":[1,2]}}"""), null);
        var text = new HttpPageRequest(HttpMethod.Post, new Uri("https://h.example/s"), null, "a=1&b=ü");

        var jsonToken = HttpCheckpointToken.Encode(json, isPost: true);
        var textToken = HttpCheckpointToken.Encode(text, isPost: true);

        Assert.StartsWith("v2:", jsonToken);
        Assert.True(HttpCheckpointToken.TryDecode(jsonToken, true, HttpMethod.Post, out var j));
        Assert.True(j!.SameAs(json));
        Assert.Null(j.TextBody);
        Assert.True(HttpCheckpointToken.TryDecode(textToken, true, HttpMethod.Post, out var t));
        Assert.True(t!.SameAs(text));
        Assert.Null(t.JsonBody);
    }

    [Fact]
    public async Task Resume_with_a_body_offset_continues_at_that_offset()
    {
        await using var server = new StubHttpServer();
        var all = Enumerable.Range(1, 5).Select(i => $$"""{"id":{{i}}}""").ToArray();
        server.Map("/_search", req =>
            new StubResponse(200, $$"""{"hits":[{{string.Join(',', all.Skip(FromOf(req)).Take(2))}}]}"""));

        var (source, first) = await OpenAsync(server, OffsetPost());
        await using var _ = source;
        string? token = null;
        var ids = new List<long>();
        await foreach (var batch in first.ReadAsync(new BatchOptions(MaxRowsPerBatch: 1), CancellationToken.None))
        {
            ids.Add(((Int64Array)batch.Column(0)).GetValue(0)!.Value);
            batch.Dispose();
            if (ids.Count == 2 && first.TryGetCheckpoint(out var offered))
            {
                token = offered;
                break;
            }
        }

        Assert.NotNull(token);
        var sent = server.Requests.Count;
        var (source2, second) = await OpenAsync(server, OffsetPost());
        await using var _2 = source2;
        Assert.True(second.TryResumeFrom(token!));
        await foreach (var batch in second.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            var column = (Int64Array)batch.Column(0);
            for (var i = 0; i < batch.Length; i++)
            {
                ids.Add(column.GetValue(i)!.Value);
            }

            batch.Dispose();
        }

        Assert.Equal(2, FromOf(server.Requests[sent]));
        Assert.Equal([1L, 2, 3, 4, 5], ids);
    }

    [Fact]
    public async Task A_post_token_to_a_foreign_host_is_refused()
    {
        await using var server = new StubHttpServer();
        var (source, partition) = await OpenAsync(server, OffsetPost());
        await using var _ = source;
        var foreign = HttpCheckpointToken.Encode(new HttpPageRequest(HttpMethod.Post,
            new Uri("https://evil.example/_search"), JsonNode.Parse("""{"from":2}"""), null), isPost: true);

        Assert.False(partition.TryResumeFrom(foreign));
    }

    [Fact]
    public async Task Tokens_are_refused_across_method_changes()
    {
        await using var server = new StubHttpServer();
        var (source, post) = await OpenAsync(server, OffsetPost());
        await using var _ = source;
        var (source2, get) = await OpenAsync(server, Map(("path", "/g")));
        await using var _2 = source2;
        var url = new Uri(server.BaseUrl, "/g?page=2");

        var getToken = HttpCheckpointToken.Encode(new HttpPageRequest(HttpMethod.Get, url, null, null), false);
        var postToken = HttpCheckpointToken.Encode(new HttpPageRequest(HttpMethod.Post, url,
            JsonNode.Parse("""{"from":2}"""), null), true);

        Assert.False(post.TryResumeFrom(getToken));
        Assert.False(get.TryResumeFrom(postToken));
        Assert.True(post.TryResumeFrom(postToken));
        Assert.True(get.TryResumeFrom(getToken));
    }

    [Theory]
    [InlineData("v2:!!!")]
    [InlineData("v2:")]
    [InlineData("v2:bm90IGpzb24")]
    [InlineData("v2:W10")]
    [InlineData("v2:eyJ1cmwiOjUsImJvZHkiOm51bGx9")]
    [InlineData("v3:e30")]
    public void Garbled_v2_token_is_refused_not_thrown(string token)
        => Assert.False(HttpCheckpointToken.TryDecode(token, true, HttpMethod.Post, out _));
}
