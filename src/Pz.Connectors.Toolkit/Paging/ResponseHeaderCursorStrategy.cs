using System.Text.Json.Nodes;
using Pz.Connectors.Abstractions;

namespace Pz.Connectors.Toolkit.Paging;

/// <summary>Cursor paging whose token arrives in a response header (<c>X-Next-Cursor</c>,
/// <c>x-ms-continuation</c>) and goes back as the query parameter <paramref name="param"/>. A missing or
/// empty header ends the crawl, so an empty page that still carries a token is a gap, not the end.</summary>
public sealed class ResponseHeaderCursorStrategy(string header, string param) : IPageStrategy
{
    public bool StopsOnEmptyPage => false;

    public Uri? NextRequestUri(Uri current, HttpResponseMessage response, JsonNode? body)
        => ReadToken(response, header) is { } token ? QueryString.With(current, param, token) : null;

    /// <summary>The token in <paramref name="name"/> (matched case-insensitively), or null when the
    /// header is absent or empty. A header sent more than once is a permanent error: which value is the
    /// cursor would be a guess. The message never shows a value.</summary>
    public static string? ReadToken(HttpResponseMessage response, string name)
    {
        if (!response.Headers.TryGetValues(name, out var values)
            && !response.Content.Headers.TryGetValues(name, out values))
        {
            return null;
        }

        var all = values.ToList();
        if (all.Count > 1)
        {
            throw new PzConnectorException(
                $"pagination header '{name}' appears {all.Count} times in the response; the next-page token " +
                "must be a single value (check 'pagination.header')", isTransient: false);
        }

        return all.Count == 0 || string.IsNullOrEmpty(all[0]) ? null : all[0];
    }
}
