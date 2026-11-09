using System.Security.Cryptography;
using System.Text;
using System.Text.Json.Nodes;

namespace Pz.Connector.Http;

/// <summary>One page request of a read: method, URL and the already-rendered body (a JSON tree for a
/// map body, text for a string or form body, neither for GET). Instances are never mutated; paging
/// produces a new request with a new body tree. Nothing here may put the body in a message.</summary>
internal sealed record HttpPageRequest(HttpMethod Method, Uri Url, JsonNode? JsonBody, string? TextBody)
{
    /// <summary>The page token sent in a request header (<c>pagination.location: header</c>), already
    /// checked to be a value a header can carry; null for every other read and for a crawl's first request.
    /// It is the only header a request carries of its own: connection headers and auth are added at send
    /// time and never stored here.</summary>
    public (string Name, string Value)? PageHeader { get; init; }

    /// <summary>Identity of the whole request: two requests with the same key fetch the same page.
    /// Body paging keeps the URL fixed and moves the body, so a URL-only identity would call every
    /// body-paged request a repeat (or, compared with <c>==</c>, never a change). Header paging keeps both
    /// and moves the page header, so it is part of the identity when present.</summary>
    public string Key
    {
        get
        {
            var key = PageHeader is { } h ? $"{Method.Method} {Url}\n{h.Name}: {h.Value}" : $"{Method.Method} {Url}";
            return BodyText is { } body ? $"{key}\n{body}" : key;
        }
    }

    /// <summary>SHA-256 hex digest of <see cref="Key"/>: a fixed-size stand-in for the whole request, so
    /// a set of up to tens of thousands of requests does not hold every serialised body.</summary>
    public string KeyDigest => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(Key)));

    private string? BodyText => JsonBody?.ToJsonString() ?? TextBody;

    /// <summary>True when <paramref name="other"/> asks for the same page. URLs compare with
    /// <see cref="Uri"/> equality (fragment-insensitive), which is what the GET guards have always
    /// used; the body compares as its serialised text.</summary>
    public bool SameAs(HttpPageRequest other)
        => Method == other.Method && Url == other.Url && BodyText == other.BodyText
           && PageHeader?.Value == other.PageHeader?.Value
           && string.Equals(PageHeader?.Name, other.PageHeader?.Name, StringComparison.OrdinalIgnoreCase);

    /// <summary>A fresh message for one send (an <see cref="HttpRequestMessage"/> cannot be sent
    /// twice). The body is attached only when this request carries one.</summary>
    public HttpRequestMessage ToMessage(HttpBody? body)
    {
        var message = new HttpRequestMessage(Method, Url);
        if (PageHeader is { } header)
        {
            message.Headers.TryAddWithoutValidation(header.Name, header.Value);
        }

        if (body is not null && (JsonBody is not null || TextBody is not null))
        {
            message.Content = HttpBody.ToContent(JsonBody, TextBody, body);
        }

        return message;
    }

    /// <summary>The request a redirect hop sends: 307 and 308 keep the method and body; 301, 302 and
    /// 303 switch to GET without a body. Every hop keeps the page header: it is the crawl's position,
    /// like the URL.</summary>
    public HttpPageRequest RedirectedTo(Uri target, int status)
        => status is 307 or 308
            ? this with { Url = target }
            : new HttpPageRequest(HttpMethod.Get, target, null, null) { PageHeader = PageHeader };
}
