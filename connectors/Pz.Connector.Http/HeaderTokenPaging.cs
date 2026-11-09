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
    /// <summary>Header names pz or the HTTP stack owns: credentials, cookies, framing and the body's
    /// description. An endpoint-chosen value in any of them would replace or corrupt what pz sends.</summary>
    internal static readonly HashSet<string> ReservedHeaders = new(StringComparer.OrdinalIgnoreCase)
    {
        "Authorization", "Proxy-Authorization", "Cookie", "Host", "Content-Length", "Content-Type",
        "Content-Encoding", "Transfer-Encoding", "Connection", "Expect", "TE", "Upgrade",
    };

    /// <summary>The next request, or null when the response carries no token. A token a header cannot carry
    /// (a control character such as CR or LF, or anything outside ASCII) fails permanently rather than being
    /// altered; the message names the header, never the token.</summary>
    public HttpPageRequest? Next(HttpPageRequest current, HttpResponseMessage response, JsonNode? body, string label)
    {
        if (Source.ReadText(response, body) is not { } token)
        {
            return null;
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

    /// <summary>Visible ASCII, space and tab: what a header value can carry without encoding.</summary>
    public static bool IsHeaderValue(string value)
        => value.All(c => c == '\t' || c is >= ' ' and <= '~');

    /// <summary>An RFC 9110 token: what a header field name may contain.</summary>
    public static bool IsHeaderName(string name)
        => name.Length > 0 && name.All(c => c is >= 'a' and <= 'z' or >= 'A' and <= 'Z' or >= '0' and <= '9'
            || "!#$%&'*+-.^_`|~".Contains(c));
}
