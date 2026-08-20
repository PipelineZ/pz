using System.Text.Json;
using Pz.Core.Validation;
using Pz.Mcp.Docs;

namespace Pz.Mcp.Handlers;

/// <summary>pz_docs_list / pz_docs_search / pz_docs_get — read-only access to the published
/// documentation.
///
/// These replaced the embedded doc resources. The documentation is published on the website, so
/// serving it from there means an agent reads what is currently true rather than whatever shipped
/// with the user's build of pz. The trade is that these three tools need network access; they are the
/// only ones that do, and they say so with PZ0607 rather than returning an empty result that reads
/// like "no such documentation".
///
/// Project-independent by design: unlike every other tool here, none of these takes a projectDir.
/// An agent can consult the documentation before a project exists, which is exactly when it is most
/// useful (scaffolding, "what does sync: mean", picking a connector).</summary>
internal static class DocsTools
{
    /// <summary>pz_docs_list: every published page — slug, title, one-line summary, group, URL.
    /// The cheap call an agent makes first to see what documentation exists at all.</summary>
    internal static async Task<string> ListAsync(DocsCatalog catalog, CancellationToken ct)
    {
        try
        {
            var pages = await catalog.IndexAsync(ct).ConfigureAwait(false);
            return ToolEnvelope.Ok(json =>
            {
                json.WriteStartArray("docs");
                foreach (var page in pages)
                {
                    WritePage(json, page, includeBody: false);
                }
                json.WriteEndArray();
            });
        }
        catch (DocsUnavailableException ex)
        {
            return Unavailable(ex);
        }
    }

    /// <summary>pz_docs_search: keyword search across titles, summaries, headings, code and prose,
    /// best matches first, each with a few matching lines so an agent can judge relevance before
    /// spending a pz_docs_get on the whole page.</summary>
    internal static async Task<string> SearchAsync(
        DocsCatalog catalog, string query, int limit, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(query))
        {
            return ToolEnvelope.Errors([new PzError(
                PzErrorCode.McpDocsRequestInvalid,
                "search query was empty",
                File: null,
                Line: null,
                Hint: "Pass a query — a term, an error code like PZ0214, or an option name like force_universal.")]);
        }

        try
        {
            var pages = await catalog.AllWithBodiesAsync(ct).ConfigureAwait(false);
            var hits = DocsSearch.Rank(pages, query, Math.Clamp(limit, 1, 50));
            return ToolEnvelope.Ok(json =>
            {
                json.WriteString("query", query);
                json.WriteStartArray("hits");
                foreach (var hit in hits)
                {
                    json.WriteStartObject();
                    json.WriteString("slug", hit.Page.Slug);
                    json.WriteString("title", hit.Page.Title);
                    json.WriteString("url", hit.Page.Url);
                    json.WriteNumber("score", hit.Score);
                    json.WriteStartArray("excerpts");
                    foreach (var excerpt in hit.Excerpts)
                    {
                        json.WriteStringValue(excerpt);
                    }
                    json.WriteEndArray();
                    json.WriteEndObject();
                }
                json.WriteEndArray();
            });
        }
        catch (DocsUnavailableException ex)
        {
            return Unavailable(ex);
        }
    }

    /// <summary>pz_docs_get: one page's full markdown by slug, as pz_docs_list and pz_docs_search
    /// report it.</summary>
    internal static async Task<string> GetAsync(DocsCatalog catalog, string slug, CancellationToken ct)
    {
        try
        {
            var page = await catalog.GetAsync(slug.Trim().Trim('/'), ct).ConfigureAwait(false);
            if (page is null)
            {
                return ToolEnvelope.Errors([new PzError(
                    PzErrorCode.McpDocsRequestInvalid,
                    $"no documentation page with slug '{slug}'",
                    File: null,
                    Line: null,
                    Hint: "Call pz_docs_list for the available slugs, or pz_docs_search to find one by keyword.")]);
            }

            return ToolEnvelope.Ok(json => WritePage(json, page, includeBody: true, propertyName: "doc"));
        }
        catch (DocsUnavailableException ex)
        {
            return Unavailable(ex);
        }
    }

    /// <param name="propertyName">Names the object when it is written as a field of the envelope
    /// (pz_docs_get); null when it is an element of the docs array (pz_docs_list).</param>
    private static void WritePage(Utf8JsonWriter json, DocPage page, bool includeBody, string? propertyName = null)
    {
        if (propertyName is null) { json.WriteStartObject(); } else { json.WriteStartObject(propertyName); }
        json.WriteString("slug", page.Slug);
        json.WriteString("title", page.Title);
        if (page.Description.Length > 0) { json.WriteString("description", page.Description); }
        if (page.Group.Length > 0) { json.WriteString("group", page.Group); }
        json.WriteString("url", page.Url);
        if (includeBody) { json.WriteString("markdown", page.Body); }
        json.WriteEndObject();
    }

    private static string Unavailable(DocsUnavailableException ex) =>
        ToolEnvelope.Errors([new PzError(
            PzErrorCode.McpDocsUnavailable,
            $"could not reach the documentation at {ex.Url}",
            File: null,
            Line: null,
            Hint: "The documentation tools need network access; every other pz tool works offline. "
                + $"Set {DocsCatalog.BaseUrlEnvironmentVariable} to a reachable mirror of the site if "
                + "this machine cannot reach the public one.")]);
}
