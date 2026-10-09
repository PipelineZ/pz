using System.IO.Compression;

namespace Pz.Cli.Otel;

/// <summary>The OTLP/HTTP exporter's client handler: gzips each body (Azure Monitor caps a request at 1 MB) and sets
/// the headers read from <c>path</c> just before the request goes out, so a token the caller rewrites mid-run is used by
/// the next export. A missing, unreadable or malformed file sends the request without it and says so once per run,
/// naming the file and the problem, never its content.</summary>
public sealed class HeadersFileHandler(string? path, Action<string> notice) : DelegatingHandler
{
    private int _noted;

    protected override HttpResponseMessage Send(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        Prepare(request);
        return base.Send(request, cancellationToken);
    }

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        Prepare(request);
        return base.SendAsync(request, cancellationToken);
    }

    private void Prepare(HttpRequestMessage request)
    {
        if (request.Content is { } content && content.Headers.ContentEncoding.Count == 0)
            request.Content = Gzip(content);
        if (path is null) return;
        foreach (var (name, value) in Read())
        {
            request.Headers.Remove(name);
            request.Headers.TryAddWithoutValidation(name, value);
        }
    }

    private IEnumerable<(string Name, string Value)> Read()
    {
        string[] lines;
        try { lines = File.ReadAllLines(path!); }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            Note($"telemetry headers file '{path}' could not be read ({ex.GetType().Name}); exporting without it");
            return [];
        }

        var headers = new List<(string, string)>();
        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i].Trim();
            if (line.Length == 0 || line.StartsWith('#')) continue;
            var eq = line.IndexOf('=');
            var name = eq > 0 ? line[..eq].Trim() : "";
            if (name.Length == 0 || !IsToken(name))
            {
                Note($"telemetry headers file '{path}' line {i + 1} is not Name=value; exporting without it");
                return [];
            }
            headers.Add((name, line[(eq + 1)..].Trim()));
        }
        return headers;
    }

    private static bool IsToken(string name) => name.All(c => c > 32 && c < 127 && !"()<>@,;:\\\"/[]?={}".Contains(c));

    private void Note(string text)
    {
        if (Interlocked.Exchange(ref _noted, 1) == 0) notice(text);
    }

    private static ByteArrayContent Gzip(HttpContent content)
    {
        using var buffer = new MemoryStream();
        using (var gzip = new GZipStream(buffer, CompressionLevel.Fastest, leaveOpen: true))
            content.CopyTo(gzip, null, CancellationToken.None);
        var zipped = new ByteArrayContent(buffer.ToArray());
        foreach (var header in content.Headers) zipped.Headers.TryAddWithoutValidation(header.Key, header.Value);
        zipped.Headers.ContentEncoding.Add("gzip");
        zipped.Headers.ContentLength = null;
        return zipped;
    }
}
