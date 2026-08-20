using System.Globalization;
using System.Text.Json.Nodes;

namespace Pz.Connectors.Toolkit.Paging;

public sealed class PageParamsStrategy(string param, int start, string? sizeParam, int? size) : IPageStrategy
{
    private int _pagesAdvanced;

    public Uri FirstRequestUri(Uri firstUri)
    {
        var first = QueryString.With(firstUri, param, start.ToString(CultureInfo.InvariantCulture));
        if (sizeParam is not null && size is { } s)
        {
            first = QueryString.With(first, sizeParam, s.ToString(CultureInfo.InvariantCulture));
        }

        return first;
    }

    public Uri? NextRequestUri(Uri current, HttpResponseMessage response, JsonNode? body)
    {
        _pagesAdvanced++;
        // A resumed attempt re-enters mid-crawl from a persisted next-link with a FRESH
        // instance, so the current request's own param value — not the counter — is the only
        // truthful position. The counter is the fallback for a current URI that lacks the
        // param (never one this strategy built: FirstRequestUri and this method always stamp it).
        var nextPage = TryGetPage(current, out var currentPage)
            ? currentPage + 1
            : start + _pagesAdvanced;
        var next = QueryString.With(current, param, nextPage.ToString(CultureInfo.InvariantCulture));
        if (sizeParam is not null && size is { } s)
        {
            next = QueryString.With(next, sizeParam, s.ToString(CultureInfo.InvariantCulture));
        }

        return next;
    }

    private bool TryGetPage(Uri uri, out int page)
    {
        var prefix = Uri.EscapeDataString(param) + "=";
        foreach (var pair in uri.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            if (pair.StartsWith(prefix, StringComparison.Ordinal))
            {
                return int.TryParse(Uri.UnescapeDataString(pair[prefix.Length..]),
                    NumberStyles.Integer, CultureInfo.InvariantCulture, out page);
            }
        }

        page = 0;
        return false;
    }
}
