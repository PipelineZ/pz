using System.Text.Json.Nodes;
using Pz.Connectors.Toolkit.Json;

namespace Pz.Connectors.Toolkit.Paging;

public sealed class CursorTokenStrategy(string pointer, string param) : IPageStrategy
{
    /// <summary>A missing/empty cursor token is this strategy's own end-of-feed signal, so an empty
    /// page that still carries a token is a gap to cross, not the end of the crawl.</summary>
    public bool StopsOnEmptyPage => false;

    public Uri? NextRequestUri(Uri current, HttpResponseMessage response, JsonNode? body)
    {
        if (body is null || !JsonPointer.TryResolve(body, pointer, out var node) || node is null)
        {
            return null;
        }

        var token = node.GetValueKind() == System.Text.Json.JsonValueKind.String
            ? node.GetValue<string>()
            : node.ToJsonString();
        return string.IsNullOrEmpty(token) ? null : QueryString.With(current, param, token);
    }
}
