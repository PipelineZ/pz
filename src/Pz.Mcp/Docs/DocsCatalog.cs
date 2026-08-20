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
/// public site is not reachable. That is the supported answer for air-gapped use.
///
/// One instance holds one process's cache: the index and the full text are each fetched at most
/// once, because an agent typically searches several times in a session and the full text is large.
/// </summary>
public sealed class DocsCatalog
{
    public const string DefaultBaseUrl = "https://pipelinez.dev";
    public const string BaseUrlEnvironmentVariable = "PZ_DOCS_URL";

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
    private readonly string _baseUrl;
    private IReadOnlyList<DocPage>? _index;
    private IReadOnlyDictionary<string, string>? _bodies;

    public DocsCatalog(HttpClient http, string? baseUrl = null)
    {
        _http = http;
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
        var url = _baseUrl + path;
        try
        {
            using var response = await _http.GetAsync(url, ct).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();
            return await response.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is HttpRequestException or TaskCanceledException && !ct.IsCancellationRequested)
        {
            // Wrapped rather than surfaced raw: the handler turns this into a PZ-coded envelope, and
            // the message has to say WHICH url failed for a mirror misconfiguration to be diagnosable.
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
