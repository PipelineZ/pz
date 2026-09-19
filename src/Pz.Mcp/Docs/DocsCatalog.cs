using System.Text;
using System.Text.RegularExpressions;

namespace Pz.Mcp.Docs;

/// <summary>One documentation page as the catalog knows it. <c>Slug</c> is the stable identifier a
/// caller passes to pz_docs_get (for example <c>concepts/data-plane</c>); <c>Body</c> is the page's
/// full markdown, and is empty until the full text has been fetched.</summary>
public sealed record DocPage(string Slug, string Title, string Description, string Url, string Group)
{
    public string Body { get; init; } = string.Empty;
}

/// <summary>Reads the published documentation from the site rather than from files shipped inside
/// this assembly.
///
/// The docs used to be embedded resources. They are fetched now because the documentation lives on
/// the website, and a tool that carried its own copy would answer from whatever was true when the
/// user's version of pz was built — quietly wrong for anyone not on the latest release. The cost is
/// that these tools need network access, which they report honestly instead of degrading to silence.
///
/// Two endpoints back this, and their formats are a contract the site deliberately keeps stable:
///   /llms.txt       an index: "- [Title](url): description" lines under "## Group" headings.
///   /llms-full.txt  every page's markdown, each introduced by "===== pz-doc: slug | url =====".
///
/// Set PZ_DOCS_URL to point at a mirror (an internal copy of the site, or a file:// tree) when the
/// public site is not reachable. That is the supported answer for air-gapped use. A <c>file:</c> URL
/// names a directory holding the same two files the site serves over http -- resolved as a root:
/// every fetched path is combined against it and refused if normalization would walk it outside that
/// root, since a page name can in principle come from a tool caller (<see cref="FetchFileAsync"/>).
///
/// One instance holds one process's cache: the index and the full text are each fetched at most
/// once, because an agent typically searches several times in a session and the full text is large.
/// Every fetch, over either transport, is capped (<see cref="DefaultMaxResponseBytes"/> unless told otherwise) -- a caller gets a
/// coded refusal (PZ0610) rather than an unbounded read or a silent truncation.
/// </summary>
public sealed class DocsCatalog
{
    public const string DefaultBaseUrl = "https://pipelinez.dev";
    public const string BaseUrlEnvironmentVariable = "PZ_DOCS_URL";

    /// <summary>The largest response (`llms.txt`, `llms-full.txt`) this catalog reads by default, over
    /// either transport. The whole corpus is a few megabytes; this leaves room to grow and still bounds
    /// what a misconfigured mirror can make pz hold in memory.</summary>
    public const long DefaultMaxResponseBytes = 25 * 1024 * 1024;

    // "- [Title](url)" with an optional ": description" tail. The description is optional because a
    // page without a leading prose paragraph produces no summary, and dropping the whole line for
    // that would silently hide a real page.
    private static readonly Regex IndexLine = new(
        @"^-\s+\[(?<title>[^\]]+)\]\((?<url>[^)]+)\)(?::\s*(?<desc>.*))?$",
        RegexOptions.Compiled);

    private static readonly Regex FullTextDelimiter = new(
        @"^=====\s+pz-doc:\s*(?<slug>\S+)\s*\|\s*(?<url>\S+)\s+=====$",
        RegexOptions.Compiled | RegexOptions.Multiline);

    private readonly HttpClient _http;
    private readonly long _maxResponseBytes;
    private readonly string _baseUrl;
    private IReadOnlyList<DocPage>? _index;
    private IReadOnlyDictionary<string, string>? _bodies;

    public DocsCatalog(HttpClient http, string? baseUrl = null, long maxResponseBytes = DefaultMaxResponseBytes)
    {
        _http = http;
        _maxResponseBytes = maxResponseBytes;
        _baseUrl = (baseUrl
            ?? Environment.GetEnvironmentVariable(BaseUrlEnvironmentVariable)
            ?? DefaultBaseUrl).TrimEnd('/');
    }

    /// <summary>The base this instance reads from — surfaced so an error can name the URL that
    /// could not be reached rather than leaving the user to guess which host pz wanted.</summary>
    public string BaseUrl => _baseUrl;

    /// <summary>Every published page, index order preserved (the site groups them so that concepts
    /// precede how-tos). Fetched once per instance.</summary>
    public async Task<IReadOnlyList<DocPage>> IndexAsync(CancellationToken ct)
    {
        if (_index is { } cached)
        {
            return cached;
        }

        var text = await FetchAsync("/llms.txt", ct).ConfigureAwait(false);
        _index = ParseIndex(text, _baseUrl);
        return _index;
    }

    /// <summary>One page with its <see cref="DocPage.Body"/> populated, or null when no page carries
    /// that slug. Fetching the full text pulls every page at once — it is one document by design, and
    /// a per-page fetch would be a request per call for no benefit once cached.</summary>
    public async Task<DocPage?> GetAsync(string slug, CancellationToken ct)
    {
        var index = await IndexAsync(ct).ConfigureAwait(false);
        var page = index.FirstOrDefault(p => string.Equals(p.Slug, slug, StringComparison.OrdinalIgnoreCase));
        if (page is null)
        {
            return null;
        }

        var bodies = await BodiesAsync(ct).ConfigureAwait(false);
        return bodies.TryGetValue(page.Slug, out var body) ? page with { Body = body } : page;
    }

    /// <summary>Every page with its body attached, for searching across full text.</summary>
    public async Task<IReadOnlyList<DocPage>> AllWithBodiesAsync(CancellationToken ct)
    {
        var index = await IndexAsync(ct).ConfigureAwait(false);
        var bodies = await BodiesAsync(ct).ConfigureAwait(false);
        return index
            .Select(p => bodies.TryGetValue(p.Slug, out var body) ? p with { Body = body } : p)
            .ToList();
    }

    private async Task<IReadOnlyDictionary<string, string>> BodiesAsync(CancellationToken ct)
    {
        if (_bodies is { } cached)
        {
            return cached;
        }

        var text = await FetchAsync("/llms-full.txt", ct).ConfigureAwait(false);
        _bodies = ParseFullText(text);
        return _bodies;
    }

    private async Task<string> FetchAsync(string path, CancellationToken ct)
    {
        // file: is a directory mirror read straight off disk -- HttpClient has no concept of it at
        // all (GetAsync throws NotSupportedException for any non-http(s) scheme), so it must be
        // handled before anything here touches _http.
        if (Uri.TryCreate(_baseUrl, UriKind.Absolute, out var baseUri) && baseUri.Scheme == Uri.UriSchemeFile)
        {
            return await FetchFileAsync(baseUri, path, ct).ConfigureAwait(false);
        }

        var url = _baseUrl + path;
        try
        {
            // Headers first: the body is pulled through the limit below, never buffered whole.
            using var response = await _http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, ct)
                .ConfigureAwait(false);
            response.EnsureSuccessStatusCode();
            // A declared length over the limit is refused without reading anything.
            if (response.Content.Headers.ContentLength is { } declared && declared > _maxResponseBytes)
            {
                throw new DocsResponseTooLargeException(url, declared, _maxResponseBytes);
            }

            await using var body = await response.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
            return await ReadBoundedAsync(body, url, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException && !ct.IsCancellationRequested)
        {
            // Wrapped rather than surfaced raw: the handler turns this into a PZ-coded envelope, and
            // the message has to say WHICH url failed for a mirror misconfiguration to be diagnosable.
            throw new DocsUnavailableException(url, ex);
        }
    }

    /// <summary>Reads <paramref name="body"/> as UTF-8, stopping the moment it has yielded more than the
    /// limit -- a response with no declared length (chunked) is measured as it arrives, not afterwards.</summary>
    private async Task<string> ReadBoundedAsync(Stream body, string url, CancellationToken ct)
    {
        using var buffered = new MemoryStream();
        var chunk = new byte[81920];
        int read;
        while ((read = await body.ReadAsync(chunk, ct).ConfigureAwait(false)) > 0)
        {
            buffered.Write(chunk, 0, read);
            if (buffered.Length > _maxResponseBytes)
            {
                throw new DocsResponseTooLargeException(url, buffered.Length, _maxResponseBytes);
            }
        }

        buffered.Position = 0;
        using var reader = new StreamReader(buffered, Encoding.UTF8);
        return await reader.ReadToEndAsync(ct).ConfigureAwait(false);
    }

    /// <summary>Resolves <paramref name="path"/> (always one of the two fixed corpus paths today, but
    /// treated as caller-influenced on principle -- see the class doc) under <paramref name="baseUri"/>'s
    /// local directory and reads it. Refuses a resolved path that normalizes outside that root, and
    /// maps every I/O/permission/path failure to <see cref="DocsUnavailableException"/> (PZ0607) -- the
    /// same user-facing failure an unreachable http mirror produces, named types only, so a genuine
    /// defect elsewhere still surfaces as PZ0609 rather than being misdiagnosed here.</summary>
    private async Task<string> FetchFileAsync(Uri baseUri, string path, CancellationToken ct)
    {
        var url = _baseUrl + path;
        var root = Path.GetFullPath(baseUri.LocalPath);
        var resolved = Path.GetFullPath(Path.Combine(root, path.TrimStart('/')));
        if (!string.Equals(resolved, root, StringComparison.Ordinal) &&
            !resolved.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.Ordinal))
        {
            throw new DocsUnavailableException(url,
                new UnauthorizedAccessException($"resolved path escapes the configured docs root '{root}'"));
        }

        try
        {
            var info = new FileInfo(resolved);
            if (!info.Exists)
            {
                throw new FileNotFoundException($"no such file: '{resolved}'", resolved);
            }

            if (info.Length > _maxResponseBytes)
            {
                throw new DocsResponseTooLargeException(url, info.Length, _maxResponseBytes);
            }

            return await File.ReadAllTextAsync(resolved, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or NotSupportedException
            or ArgumentException)
        {
            throw new DocsUnavailableException(url, ex);
        }
    }

    internal static IReadOnlyList<DocPage> ParseIndex(string text, string baseUrl)
    {
        var pages = new List<DocPage>();
        var group = string.Empty;

        foreach (var raw in text.Split('\n'))
        {
            var line = raw.TrimEnd('\r');
            if (line.StartsWith("## ", StringComparison.Ordinal))
            {
                group = line[3..].Trim();
                continue;
            }

            var match = IndexLine.Match(line.Trim());
            if (!match.Success)
            {
                continue;
            }

            var url = match.Groups["url"].Value.Trim();
            var slug = SlugFor(url, baseUrl);
            if (slug.Length == 0)
            {
                // The "Full text" pointer at the bottom of llms.txt is a link like any other, but it
                // is the corpus itself rather than a page in it.
                continue;
            }

            pages.Add(new DocPage(
                Slug: slug,
                Title: match.Groups["title"].Value.Trim(),
                Description: match.Groups["desc"].Success ? match.Groups["desc"].Value.Trim() : string.Empty,
                Url: url,
                Group: group));
        }

        return pages;
    }

    internal static IReadOnlyDictionary<string, string> ParseFullText(string text)
    {
        var bodies = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var matches = FullTextDelimiter.Matches(text);
        for (var i = 0; i < matches.Count; i++)
        {
            var start = matches[i].Index + matches[i].Length;
            var end = i + 1 < matches.Count ? matches[i + 1].Index : text.Length;
            bodies[matches[i].Groups["slug"].Value] = text[start..end].Trim();
        }

        return bodies;
    }

    private static string SlugFor(string url, string baseUrl)
    {
        string path;
        if (url.StartsWith(baseUrl, StringComparison.OrdinalIgnoreCase))
        {
            // Preferred: strips a base that carries its own path prefix, so a site published under a
            // subdirectory yields the same slugs as one published at a root.
            path = url[baseUrl.Length..];
        }
        else if (Uri.TryCreate(url, UriKind.Absolute, out var absolute))
        {
            // A mirror serves copies of the site's files, and those files keep the canonical host in
            // every link -- copying them does not rewrite their contents. So a page's host is not
            // required to match the host it was fetched from; the slug is the path either way.
            path = absolute.AbsolutePath;
        }
        else
        {
            path = url;
        }

        path = path.Trim('/');
        // llms-full.txt and any other non-page asset are not pages.
        return path.EndsWith(".txt", StringComparison.OrdinalIgnoreCase) ? string.Empty : path;
    }
}

/// <summary>The documentation site could not be reached. Carries the URL so the tool's error names
/// it — the most common cause is a PZ_DOCS_URL mirror that is wrong or down, which is
/// undiagnosable from a bare "network error".</summary>
public sealed class DocsUnavailableException(string url, Exception inner)
    : Exception($"could not reach the documentation at {url}", inner)
{
    public string Url { get; } = url;
}

/// <summary>A documentation response exceeded the catalog's size limit. Deliberately its own type, not
/// folded into <see cref="DocsUnavailableException"/> — the source WAS reached, so "could not reach" and
/// its mirror-misconfiguration hint would misdiagnose the real cause. <paramref name="sizeBytes"/> is
/// the declared size when the source declared one, otherwise how much had arrived when pz stopped.</summary>
public sealed class DocsResponseTooLargeException(string url, long sizeBytes, long limitBytes)
    : Exception($"the documentation response from {url} is over pz's {limitBytes}-byte limit " +
        $"({sizeBytes} bytes and counting)")
{
    public string Url { get; } = url;

    public long LimitBytes { get; } = limitBytes;
}
