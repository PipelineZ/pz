using System.Text.Json;
using System.Text.Json.Nodes;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Json;
using Pz.Connectors.Toolkit.Paging;

namespace Pz.Connector.Http;

/// <summary>Where a cursor's next-page token is read from: the body at <see cref="Pointer"/>, or the response
/// header <see cref="Header"/>. Exactly one is set.</summary>
internal sealed record PageTokenSource(string? Pointer, string? Header)
{
    /// <summary>The token as JSON: the node at the pointer, or the header value as a JSON string. Null when
    /// the token is missing, null or an empty string, which ends the crawl.</summary>
    public JsonNode? Read(HttpResponseMessage? response, JsonNode? body)
    {
        if (Header is not null)
        {
            return response is not null && ResponseHeaderCursorStrategy.ReadToken(response, Header) is { } value
                ? JsonValue.Create(value)
                : null;
        }

        if (body is null || !JsonPointer.TryResolve(body, Pointer!, out var node) || node is null
            || (node is JsonValue v && v.TryGetValue(out string? text) && text.Length == 0))
        {
            return null;
        }

        return node;
    }

    /// <summary>The token as text: a string as itself, any other JSON value as its JSON text (the way the
    /// query-string cursor has always sent one).</summary>
    public string? ReadText(HttpResponseMessage? response, JsonNode? body)
        => Read(response, body) is { } token
            ? token.GetValueKind() == JsonValueKind.String ? token.GetValue<string>() : token.ToJsonString()
            : null;
}

/// <summary>Cursor paging that sends the token in the request header <see cref="Header"/>
/// (`pagination.location: header`): every request after the first is the first one's URL and body with
/// that header set to the latest token.</summary>
internal sealed record HeaderTokenPaging(string Header, PageTokenSource Source)
{
    /// <summary>Request header names pz or the HTTP stack owns: credentials, cookies and framing. An
    /// endpoint-chosen value in any of them would replace or corrupt what pz sends. Content headers
    /// (<c>Content-*</c>, <c>Expires</c>, <c>Allow</c>, ...) are refused separately by
    /// <see cref="IsRequestHeader"/>, which asks the HTTP stack itself.</summary>
    internal static readonly HashSet<string> ReservedHeaders = new(StringComparer.OrdinalIgnoreCase)
    {
        "Authorization", "Proxy-Authorization", "Cookie", "Host", "Transfer-Encoding", "Connection", "Expect", "TE",
        "Upgrade",
    };

    /// <summary>Whether <paramref name="name"/> can go on a request's own headers. The HTTP stack refuses the
    /// names it treats as content headers there, so a page token in one would never be sent.</summary>
    public static bool IsRequestHeader(string name)
    {
        using var probe = new HttpRequestMessage();
        return probe.Headers.TryAddWithoutValidation(name, "probe");
    }

    /// <summary>The page header a request would not send: <see cref="ReservedHeaders"/> or one of
    /// the connection's own (<c>headers:</c> or the <c>api_key</c> auth header), or null. Known before the first
    /// request, so the read is refused at plan time.</summary>
    public string? CollisionWith(HttpConnectionConfig connection)
    {
        if (connection.Headers.Keys.Any(k => string.Equals(k, Header, StringComparison.OrdinalIgnoreCase)))
        {
            return "the connection's 'headers:'";
        }

        if (string.Equals(connection.AuthHeader, Header, StringComparison.OrdinalIgnoreCase))
        {
            return "the connection's api_key auth ('auth.header')";
        }

        // The session sign-in replaces its header on every request, so the page token would be dropped unseen.
        return connection.SessionInject?.Header is { } injected
            && string.Equals(injected, Header, StringComparison.OrdinalIgnoreCase)
            ? "the connection's session sign-in ('auth.inject.header')"
            : null;
    }

    /// <summary>The next request, or null when the response carries no token. A token a header cannot carry
    /// (a control character such as CR or LF, or anything outside ASCII) fails permanently rather than being
    /// altered; the message names the header, never the token.</summary>
    public HttpPageRequest? Next(HttpPageRequest current, HttpResponseMessage response, JsonNode? body, string label)
    {
        if (Source.ReadText(response, body) is not { } token)
        {
            return null;
        }

        if (HasEdgeWhitespace(token))
        {
            throw new PzConnectorException(
                $"{label}: the next-page token cannot be sent in header '{Header}': it has leading or trailing " +
                "whitespace, which HTTP strips from a header value; read the token from another field, or send it " +
                "with 'location: query'", isTransient: false);
        }

        if (!IsHeaderValue(token))
        {
            throw new PzConnectorException(
                $"{label}: the next-page token cannot be sent in header '{Header}': it contains a control or " +
                "non-ASCII character; read the token from another field, or send it with 'location: query'",
                isTransient: false);
        }

        return current with { PageHeader = (Header, token) };
    }

    /// <summary>Visible ASCII, with space and tab inside but not at either end: what a header value carries
    /// unchanged (HTTP strips whitespace at the ends).</summary>
    public static bool IsHeaderValue(string value)
        => !HasEdgeWhitespace(value) && value.All(c => c == '\t' || c is >= ' ' and <= '~');

    private static bool HasEdgeWhitespace(string value)
        => value.Length > 0 && (value[0] is ' ' or '\t' || value[^1] is ' ' or '\t');

    /// <summary>An RFC 9110 token: what a header field name may contain.</summary>
    public static bool IsHeaderName(string name)
        => name.Length > 0 && name.All(c => c is >= 'a' and <= 'z' or >= 'A' and <= 'Z' or >= '0' and <= '9'
            || "!#$%&'*+-.^_`|~".Contains(c));
}
