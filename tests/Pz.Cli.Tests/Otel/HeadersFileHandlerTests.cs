using System.Net;
using Pz.Cli.Otel;

namespace Pz.Cli.Tests.Otel;

public sealed class HeadersFileHandlerTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("pz-headers-").FullName;
    private readonly List<HttpRequestMessage> _seen = [];
    private readonly List<string> _notes = [];

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private HttpClient Client(string? path) =>
        new(new HeadersFileHandler(path, _notes.Add) { InnerHandler = new Capture(_seen) });

    [Fact]
    public async Task Reads_the_file_before_every_request()
    {
        var path = Path.Combine(_dir, "h");
        await File.WriteAllTextAsync(path, "# token\nAuthorization=Bearer one\n\n");
        using var client = Client(path);
        await client.PostAsync("http://x/v1/traces", new ByteArrayContent([1, 2, 3]));
        await File.WriteAllTextAsync(path, "Authorization=Bearer two\n");
        await client.PostAsync("http://x/v1/traces", new ByteArrayContent([1, 2, 3]));

        Assert.Equal("Bearer one", _seen[0].Headers.GetValues("Authorization").Single());
        Assert.Equal("Bearer two", _seen[1].Headers.GetValues("Authorization").Single());
        Assert.Empty(_notes);
    }

    [Fact]
    public void The_synchronous_send_path_reads_the_file_too()
    {
        // The OTLP exporter sends synchronously (HttpClient.Send), so this path must behave like SendAsync.
        var path = Path.Combine(_dir, "h");
        File.WriteAllText(path, "Authorization=Bearer sync\n");
        using var client = Client(path);
        client.Send(new HttpRequestMessage(HttpMethod.Post, "http://x/v1/traces") { Content = new ByteArrayContent([1]) });

        Assert.Equal("Bearer sync", _seen[0].Headers.GetValues("Authorization").Single());
        Assert.Equal("gzip", _seen[0].Content!.Headers.ContentEncoding.Single());
    }

    [Fact]
    public async Task Gzips_the_body()
    {
        using var client = Client(null);
        await client.PostAsync("http://x/v1/traces", new ByteArrayContent(new byte[4096]));
        Assert.Equal("gzip", _seen[0].Content!.Headers.ContentEncoding.Single());
    }

    [Fact]
    public async Task A_missing_file_sends_without_it_and_notes_once_without_content()
    {
        using var client = Client(Path.Combine(_dir, "absent"));
        await client.PostAsync("http://x/v1/traces", new ByteArrayContent([1]));
        await client.PostAsync("http://x/v1/traces", new ByteArrayContent([1]));

        Assert.Equal(2, _seen.Count);
        Assert.False(_seen[0].Headers.Contains("Authorization"));
        var note = Assert.Single(_notes);
        Assert.Contains("absent", note, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_malformed_line_notes_once_and_never_echoes_the_secret()
    {
        var path = Path.Combine(_dir, "h");
        await File.WriteAllTextAsync(path, "Authorization Bearer secret-value\n");
        using var client = Client(path);
        await client.PostAsync("http://x/v1/traces", new ByteArrayContent([1]));

        var note = Assert.Single(_notes);
        Assert.DoesNotContain("secret-value", note, StringComparison.Ordinal);
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
