using System.Text.Json.Nodes;

namespace Pz.Connectors.Toolkit.Paging;

/// <summary>Computes the next page request from the current one. Null = pagination exhausted.
/// Instances may be stateful; construct one per partition read. Strategies never terminate on
/// content: whether an empty items page ends the crawl is <see cref="StopsOnEmptyPage"/>.</summary>
public interface IPageStrategy
{
    Uri? NextRequestUri(Uri current, HttpResponseMessage response, JsonNode? body);

    /// <summary>Whether an empty items page means "the crawl is over". True only for strategies with
    /// no other termination signal — <see cref="PageParamsStrategy"/> walks page numbers forever, so
    /// an empty page is the only end it has. A strategy that reports its own exhaustion (no Link
    /// header, no cursor token) must return false: for those, an empty page in the MIDDLE of a feed
    /// is a gap, not the end, and stopping there drops every row behind it with no error. Microsoft
    /// Graph delta feeds and filtered GitHub queries both serve empty pages that carry a next link.
    /// Defaults to true so a third-party strategy need not declare it.</summary>
    bool StopsOnEmptyPage => true;

    /// <summary>Decorates the crawl's FIRST request with the strategy's own parameters (e.g. the
    /// page-number strategy pins <c>param=start</c> and the size params so the API's default page
    /// size can never skip or re-deliver rows before page two). Never applied to resume links or
    /// sync-token replays — those URLs already embed their full request state. Default: identity.</summary>
    Uri FirstRequestUri(Uri firstUri) => firstUri;
}
