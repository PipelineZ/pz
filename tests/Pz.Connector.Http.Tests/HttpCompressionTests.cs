using System.IO.Compression;
using System.Text;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit;

namespace Pz.Connector.Http.Tests;

/// <summary>Compressed responses: the connector asks for them and unpacks them, and the
/// 'max_response_mb' cap applies to the unpacked size.</summary>
public class HttpCompressionTests
{
    private static async Task<long> ReadAllAsync(StubHttpServer server, Dictionary<string, object?>? connectionExtra = null)
    {
        var connection = new Dictionary<string, object?> { ["base_url"] = server.BaseUrl.ToString() };
        foreach (var (k, v) in connectionExtra ?? []) connection[k] = v;
        await using var source = await new HttpConnector().OpenAsync(new ConnectorConfig(connection),
            CancellationToken.None);
        var partitions = await source.PlanReadAsync(
            new DatasetSpec("s", "d", new Dictionary<string, object?> { ["path"] = "/items" }), ReadHints.None,
            CancellationToken.None);
        long rows = 0;
        await foreach (var batch in partitions[0].ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        return rows;
    }

    private static byte[] Compress(string text, string encoding)
    {
        using var buffer = new MemoryStream();
        using (Stream zip = encoding == "br"
            ? new BrotliStream(buffer, CompressionLevel.SmallestSize)
            : new GZipStream(buffer, CompressionLevel.SmallestSize))
        {
            zip.Write(Encoding.UTF8.GetBytes(text));
        }

        return buffer.ToArray();
    }

    private static StubResponse Compressed(string json, string encoding) =>
        new(200, "", new Dictionary<string, string> { ["Content-Encoding"] = encoding })
        {
            RawBody = Compress(json, encoding),
        };

    [Fact]
    public async Task Requests_ask_for_compressed_responses()
    {
        await using var server = new StubHttpServer();
        server.Map("/items", _ => new StubResponse(200, """[{"id":1}]"""));

        await ReadAllAsync(server);

        var acceptEncoding = Assert.Single(server.Requests).Headers["Accept-Encoding"];
        Assert.Contains("gzip", acceptEncoding);
        Assert.Contains("br", acceptEncoding);
    }

    [Theory]
    [InlineData("gzip")]
    [InlineData("br")]
    public async Task A_compressed_response_is_unpacked_and_lands_its_rows(string encoding)
    {
        await using var server = new StubHttpServer();
        server.Map("/items", _ => Compressed("""[{"id":1},{"id":2},{"id":3}]""", encoding));

        Assert.Equal(3, await ReadAllAsync(server));
    }

    [Fact]
    public async Task The_response_cap_applies_to_the_unpacked_size()
    {
        // ~3 MiB of JSON compresses to a few KiB: a cap on the wire size alone would let a small
        // compressed response expand far past what the author allowed into memory.
        var big = "[" + string.Join(',', Enumerable.Repeat("""{"pad":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}""", 75_000)) + "]";
        await using var server = new StubHttpServer();
        var response = Compressed(big, "gzip");
        Assert.True(response.RawBody!.Length < 1024 * 1024);
        server.Map("/items", _ => response);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(() =>
            ReadAllAsync(server, new() { ["max_response_mb"] = 1L }));

        Assert.False(ex.IsTransient);
        Assert.Contains("max_response_mb", ex.Message);
    }
}
