using System.Text.Json;
using System.Text.Json.Nodes;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Json;

namespace Pz.Connectors.Toolkit.Paging;

/// <summary>Follows a next-page URL the endpoint embeds in the response body — OData/Microsoft Graph
/// <c>@odata.nextLink</c>, JSON:API <c>links.next</c>, and the many APIs with a top-level <c>next</c>.
/// The URL is used as-is (it already carries the server's own continuation state); a relative one
/// resolves against the current request. A missing, null or empty value ends the crawl. The caller
/// must still check the returned URI against the connection's allowed hosts: the body is
/// endpoint-controlled.</summary>
public sealed class NextUrlStrategy(string pointer) : IPageStrategy
{
    /// <summary>A missing next link is this strategy's own end-of-feed signal, so an empty page that
    /// still carries one is a gap to cross, not the end of the crawl.</summary>
    public bool StopsOnEmptyPage => false;

    public Uri? NextRequestUri(Uri current, HttpResponseMessage response, JsonNode? body)
    {
        if (body is null || !JsonPointer.TryResolve(body, pointer, out var node) || node is null)
        {
            return null;
        }

        // Anything but a string means the pointer is aimed at the wrong node. Treating that as "no
        // next page" would land the first page alone and report success.
        if (node.GetValueKind() != JsonValueKind.String)
        {
            throw new PzConnectorException(
                $"pagination pointer '{pointer}' resolved to a JSON {node.GetValueKind().ToString().ToLowerInvariant()}, " +
                "not a URL string; point 'pagination.pointer' at the next-page link itself", isTransient: false);
        }

        var link = node.GetValue<string>();
        if (link.Length == 0)
        {
            return null;
        }

        // Only the authority is named in the error: the rest of the link can embed a secret token.
        if (!Uri.TryCreate(current, link, out var next))
        {
            throw new PzConnectorException(
                $"the next-page link at '{pointer}' from {current.GetLeftPart(UriPartial.Authority)} " +
                "is not a valid URL", isTransient: false);
        }

        return next;
    }
}
