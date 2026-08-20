using System.Text.Json.Nodes;

namespace Pz.Connectors.Toolkit.Paging;

/// <summary>Follows the RFC 8288 "Link" response header to the link-value whose <c>rel</c>
/// parameter contains the "next" relation type. Parameter order is not significant, params may
/// be quoted or unquoted, and a quoted rel value may carry multiple space-separated relation
/// types (e.g. <c>rel="next last"</c>). The target URI-reference may be relative to the current
/// request; a malformed link value is treated as "no next page" rather than thrown.</summary>
public sealed class LinkHeaderStrategy : IPageStrategy
{
    /// <summary>The absence of a rel="next" link is this strategy's own end-of-feed signal, so an
    /// empty page that still carries one is a gap to cross, not the end of the crawl.</summary>
    public bool StopsOnEmptyPage => false;

    public Uri? NextRequestUri(Uri current, HttpResponseMessage response, JsonNode? body)
    {
        if (!response.Headers.TryGetValues("Link", out var values))
        {
            return null;
        }

        foreach (var value in values)
        {
            var next = FindNextLink(current, value);
            if (next is not null)
            {
                return next;
            }
        }

        return null;
    }

    private static Uri? FindNextLink(Uri current, string headerValue)
    {
        foreach (var linkValue in headerValue.Split(','))
        {
            var segments = linkValue.Split(';');
            var uriSegment = segments[0].Trim();
            if (uriSegment.Length < 2 || uriSegment[0] != '<' || uriSegment[^1] != '>')
            {
                continue;
            }

            var isNext = false;
            for (var i = 1; i < segments.Length; i++)
            {
                var (key, paramValue) = SplitParam(segments[i]);
                if (!string.Equals(key, "rel", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var relations = paramValue.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (Array.Exists(relations, r => string.Equals(r, "next", StringComparison.OrdinalIgnoreCase)))
                {
                    isNext = true;
                }

                break;
            }

            if (!isNext)
            {
                continue;
            }

            var uriReference = uriSegment[1..^1];
            return Uri.TryCreate(current, uriReference, out var resolved) ? resolved : null;
        }

        return null;
    }

    private static (string Key, string Value) SplitParam(string segment)
    {
        var trimmed = segment.Trim();
        var eq = trimmed.IndexOf('=');
        if (eq < 0)
        {
            return (trimmed, string.Empty);
        }

        var key = trimmed[..eq].Trim();
        var value = trimmed[(eq + 1)..].Trim();
        if (value.Length >= 2 && value[0] == '"' && value[^1] == '"')
        {
            value = value[1..^1];
        }

        return (key, value);
    }
}
