using System.Net;
using Pz.Connectors.Sdk.Telemetry;

namespace Pz.Connectors.Sdk.Tests;

/// <summary>The connector's copy of the headers file handler behaves like the host's: the file is read before every
/// request, the body is gzipped, and a bad file is reported once without its content.</summary>
public sealed class HeadersFileHandlerTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("pz-sdk-headers-").FullName;
    private readonly List<HttpRequestMessage> _seen = [];
    private readonly List<string> _notes = [];

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private HttpClient Client(string? path) =>
        new(new HeadersFileHandler(path, _notes.Add) { InnerHandler = new Capture(_seen) });

    [Fact]
    public void Reads_the_file_before_every_request_on_the_synchronous_path()
    {
        var path = Path.Combine(_dir, "h");
        File.WriteAllText(path, "Authorization=Bearer one\n");
        using var client = Client(path);
        client.Send(new HttpRequestMessage(HttpMethod.Post, "http://x/t") { Content = new ByteArrayContent([1]) });
        File.WriteAllText(path, "Authorization=Bearer two\n");
        client.Send(new HttpRequestMessage(HttpMethod.Post, "http://x/t") { Content = new ByteArrayContent([1]) });

        Assert.Equal("Bearer one", _seen[0].Headers.GetValues("Authorization").Single());
        Assert.Equal("Bearer two", _seen[1].Headers.GetValues("Authorization").Single());
        Assert.Equal("gzip", _seen[1].Content!.Headers.ContentEncoding.Single());
        Assert.Empty(_notes);
    }

    [Fact]
    public async Task A_malformed_line_notes_once_and_never_echoes_the_secret()
    {
        var path = Path.Combine(_dir, "h");
        await File.WriteAllTextAsync(path, "Authorization Bearer secret-value\n");
        using var client = Client(path);
        await client.PostAsync("http://x/t", new ByteArrayContent([1]));
        await client.PostAsync("http://x/t", new ByteArrayContent([1]));

        var note = Assert.Single(_notes);
        Assert.DoesNotContain("secret-value", note, StringComparison.Ordinal);
        Assert.False(_seen[0].Headers.Contains("Authorization"));
    }

    private sealed class Capture(List<HttpRequestMessage> seen) : HttpMessageHandler
    {
        protected override HttpResponseMessage Send(HttpRequestMessage request, CancellationToken ct)
        {
            seen.Add(request);
            return new HttpResponseMessage(HttpStatusCode.OK);
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            seen.Add(request);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
        }
    }
}
