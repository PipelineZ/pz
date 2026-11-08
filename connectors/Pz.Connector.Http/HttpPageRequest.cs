using System.Text.Json.Nodes;

namespace Pz.Connector.Http;

/// <summary>One page request of a read: method, URL and the already-rendered body (a JSON tree for a
/// map body, text for a string or form body, neither for GET). Instances are never mutated; paging
/// produces a new request with a new body tree. Nothing here may put the body in a message.</summary>
internal sealed record HttpPageRequest(HttpMethod Method, Uri Url, JsonNode? JsonBody, string? TextBody)
{
    /// <summary>Identity of the whole request: two requests with the same key fetch the same page.
    /// Body paging keeps the URL fixed and moves the body, so a URL-only identity would call every
    /// body-paged request a repeat (or, compared with <c>==</c>, never a change).</summary>
    public string Key
        => BodyText is { } body ? $"{Method.Method} {Url}\n{body}" : $"{Method.Method} {Url}";

    private string? BodyText => JsonBody?.ToJsonString() ?? TextBody;

    /// <summary>True when <paramref name="other"/> asks for the same page. URLs compare with
    /// <see cref="Uri"/> equality (fragment-insensitive), which is what the GET guards have always
    /// used; the body compares as its serialised text.</summary>
    public bool SameAs(HttpPageRequest other)
        => Method == other.Method && Url == other.Url && BodyText == other.BodyText;

    /// <summary>A fresh message for one send (an <see cref="HttpRequestMessage"/> cannot be sent
    /// twice). The body is attached only when this request carries one.</summary>
    public HttpRequestMessage ToMessage(HttpBody? body)
    {
        var message = new HttpRequestMessage(Method, Url);
        if (body is not null && (JsonBody is not null || TextBody is not null))
        {
            message.Content = HttpBody.ToContent(JsonBody, TextBody, body);
        }

        return message;
    }

    /// <summary>The request a redirect hop sends: 307 and 308 keep the method and body; 301, 302 and
    /// 303 switch to GET without a body.</summary>
    public HttpPageRequest RedirectedTo(Uri target, int status)
        => status is 307 or 308 ? this with { Url = target } : new HttpPageRequest(HttpMethod.Get, target, null, null);
}
