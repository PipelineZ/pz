using System.Buffers.Text;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Pz.Connector.Http;

/// <summary>The resume token of a read. A GET token is the next page's URL, byte for byte. A POST
/// token is <c>v2:</c> + base64url(UTF-8 JSON <c>{"url":...,"body":...}</c>), where a JSON body is a
/// JSON object and a text or form body a JSON string. Tokens live only in the run's local staging
/// state; neither a token nor its decoded body may appear in a message. A token of the other
/// method's shape, a different version or a malformed payload is refused, never thrown. A for_each
/// read's token is <c>v3:</c> + base64url(UTF-8 JSON <c>{"list":...,"index":n,"item":...}</c>): the list
/// page's token (in the list request's form), the record index on that page, and the item crawl's next
/// request token (in the item request's form) or null to start the item from its first request. Optional
/// keys follow, each only when set: <c>anchor</c> (see <see cref="ForEachToken"/>), <c>skipped</c> and
/// <c>first_skipped</c>, and <c>rule_skipped</c> (true once an <c>on_status</c> skip rule skipped an item, so a
/// resumed read still reports the skip to the engine). A request that carries a page token header (<c>pagination.location: header</c>) is
/// <c>v4:</c> + base64url(UTF-8 JSON <c>{"url":...,"body":...,"header":{"name":...,"value":...}}</c>) whatever
/// its method, the body as in v2 (null for a GET). Only that one header is stored: connection headers and
/// auth are never part of a request until it is sent.</summary>
internal static class HttpCheckpointToken
{
    private const string PostPrefix = "v2:";
    private const string ForEachPrefix = "v3:";
    private const string HeaderPrefix = "v4:";

    /// <summary>The body shape a dataset sends: none, a JSON map, or text (a string or a form).</summary>
    public enum BodyKind { None, Json, Text }

    /// <summary>The kind of body <paramref name="body"/> declares.</summary>
    public static BodyKind KindOf(HttpBody? body)
        => body is null ? BodyKind.None : body.IsJson ? BodyKind.Json : BodyKind.Text;

    /// <param name="strip">Query parameters removed from the URL first: a secret the sign-in adds to every
    /// request (a session token) must not be stored, and is added back when the resumed request is sent.</param>
    public static string Encode(HttpPageRequest request, bool isPost, IReadOnlyCollection<string>? strip = null)
    {
        if (strip is { Count: > 0 })
        {
            request = request with { Url = WithoutQueryParams(request.Url, strip) };
        }

        if (!isPost && request.PageHeader is null)
        {
            return request.Url.ToString();
        }

        var body = request.JsonBody?.DeepClone() ?? (request.TextBody is { } text ? JsonValue.Create(text) : null);
        var payload = new JsonObject { ["url"] = request.Url.ToString(), ["body"] = body };
        if (request.PageHeader is not { } header)
        {
            return PostPrefix + Base64Url.EncodeToString(Encoding.UTF8.GetBytes(payload.ToJsonString()));
        }

        payload["header"] = new JsonObject { ["name"] = header.Name, ["value"] = header.Value };
        return HeaderPrefix + Base64Url.EncodeToString(Encoding.UTF8.GetBytes(payload.ToJsonString()));
    }

    private static Uri WithoutQueryParams(Uri url, IReadOnlyCollection<string> names)
    {
        if (url.Query.Length == 0)
        {
            return url;
        }

        // A server may spell the name in another case or percent-encode it (%73id); every pair whose decoded name
        // matches, in any case, goes.
        var kept = url.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries)
            .Where(pair => !names.Contains(DecodedName(pair), StringComparer.OrdinalIgnoreCase));
        return new UriBuilder(url) { Query = string.Join('&', kept) }.Uri;
    }

    private static string DecodedName(string pair) => DecodedQueryName(pair.Split('=', 2)[0]);

    /// <summary>A query parameter name as the query string compares it: <c>+</c> as a space, percent-escapes
    /// decoded (left as written when malformed). The one decoder for every comparison of query names, so a
    /// session token the checkpoint strips is the same one the plan checks against pagination.</summary>
    internal static string DecodedQueryName(string name)
    {
        name = name.Replace('+', ' ');
        try
        {
            return Uri.UnescapeDataString(name);
        }
        catch (UriFormatException)
        {
            return name;
        }
    }

    /// <summary>Decodes a token for a request of <paramref name="method"/> and body kind
    /// <paramref name="expected"/>. <paramref name="pageHeader"/> is the dataset's page token header, when it
    /// pages by one: a v4 token must then name that header (any case), and the plain form of its method
    /// (a first page, which has no token yet) is accepted too. Without it every v4 token is refused.</summary>
    public static bool TryDecode(string token, bool isPost, HttpMethod method, BodyKind expected,
        out HttpPageRequest? request, string? pageHeader = null)
    {
        request = null;
        if (token.StartsWith(HeaderPrefix, StringComparison.Ordinal))
        {
            return pageHeader is not null && TryDecodePayload(token, HeaderPrefix, method, expected, pageHeader,
                out request);
        }

        if (!isPost)
        {
            // "v3:" parses as an absolute URI of scheme v3, so a for_each token is refused by name.
            if (token.StartsWith(PostPrefix, StringComparison.Ordinal)
                || token.StartsWith(ForEachPrefix, StringComparison.Ordinal)
                || !Uri.TryCreate(token, UriKind.Absolute, out var url))
            {
                return false;
            }

            request = new HttpPageRequest(method, url, null, null);
            return true;
        }

        return token.StartsWith(PostPrefix, StringComparison.Ordinal)
            && TryDecodePayload(token, PostPrefix, method, expected, pageHeader: null, out request);
    }

    private static bool TryDecodePayload(string token, string prefix, HttpMethod method, BodyKind expected,
        string? pageHeader, out HttpPageRequest? request)
    {
        request = null;
        try
        {
            var json = JsonNode.Parse(Base64Url.DecodeFromChars(token.AsSpan(prefix.Length)));
            if (json is not JsonObject obj
                || obj["url"] is not JsonValue urlNode
                || !urlNode.TryGetValue<string>(out var urlText)
                || !Uri.TryCreate(urlText, UriKind.Absolute, out var target))
            {
                return false;
            }

            (string, string)? header = null;
            if (pageHeader is not null)
            {
                // The value goes back on the wire as a header, so it is held to the same rule as a fresh token.
                if (obj["header"] is not JsonObject headerNode
                    || headerNode["name"] is not JsonValue nameNode || !nameNode.TryGetValue<string>(out var name)
                    || !string.Equals(name, pageHeader, StringComparison.OrdinalIgnoreCase)
                    || headerNode["value"] is not JsonValue valueNode || !valueNode.TryGetValue<string>(out var value)
                    || value.Length == 0 || !HeaderTokenPaging.IsHeaderValue(value))
                {
                    return false;
                }

                header = (pageHeader, value);
            }

            // The token's body replaces the configured one on resume, so its kind must be the one the
            // dataset declares; otherwise it would bypass the declared content type.
            switch (obj["body"])
            {
                case null when expected == BodyKind.None:
                    request = new HttpPageRequest(method, target, null, null) { PageHeader = header };
                    return true;
                case JsonObject map when expected == BodyKind.Json:
                    request = new HttpPageRequest(method, target, map.DeepClone(), null) { PageHeader = header };
                    return true;
                case JsonValue value when expected == BodyKind.Text && value.TryGetValue<string>(out var text):
                    request = new HttpPageRequest(method, target, null, text) { PageHeader = header };
                    return true;
                default:
                    return false;
            }
        }
        catch (Exception e) when (e is FormatException or JsonException or ArgumentException or InvalidOperationException)
        {
            return false;
        }
    }

    /// <summary>A for_each read's position. <paramref name="Anchor"/> is <see cref="Anchor(ItemValues)"/> of
    /// the record the position was taken at: the record at <paramref name="Index"/> while its item is in
    /// progress (<paramref name="Item"/> set), else the record just finished, at <c>Index - 1</c>. A resume
    /// that finds another record there restarts the list page. <paramref name="Skipped"/> and
    /// <paramref name="FirstSkipped"/> are the `on_missing: skip` count and first identity so far, so a
    /// retry still reports skips made before its resume position.</summary>
    internal sealed record ForEachToken(string List, int Index, string? Item, string? Anchor = null,
        int Skipped = 0, string? FirstSkipped = null, bool RuleSkipped = false);

    /// <summary>A record's anchor: base64url SHA-256 of its <c>pz_item</c> JSON.</summary>
    public static string Anchor(ItemValues values)
        => Base64Url.EncodeToString(SHA256.HashData(Encoding.UTF8.GetBytes(values.ToJsonString())));

    public static string EncodeForEach(string listToken, int index, string? itemToken)
        => EncodeForEach(new ForEachToken(listToken, index, itemToken));

    public static string EncodeForEach(ForEachToken token)
    {
        var payload = new JsonObject { ["list"] = token.List, ["index"] = token.Index, ["item"] = token.Item };
        if (token.Anchor is not null)
        {
            payload["anchor"] = token.Anchor;
        }

        if (token.Skipped > 0)
        {
            payload["skipped"] = token.Skipped;
        }

        if (token.FirstSkipped is not null)
        {
            payload["first_skipped"] = token.FirstSkipped;
        }

        if (token.RuleSkipped)
        {
            payload["rule_skipped"] = true;
        }

        return ForEachPrefix + Base64Url.EncodeToString(Encoding.UTF8.GetBytes(payload.ToJsonString()));
    }

    public static bool TryDecodeForEach(string token, out string listToken, out int index, out string? itemToken)
    {
        var ok = TryDecodeForEach(token, out var decoded);
        listToken = decoded?.List ?? "";
        index = decoded?.Index ?? 0;
        itemToken = decoded?.Item;
        return ok;
    }

    /// <summary>Splits a <c>v3:</c> token into its parts; the embedded tokens are not decoded here (each
    /// has its own request's method and body kind). A negative index or count, an optional key of the
    /// wrong type, or a malformed payload is refused.</summary>
    public static bool TryDecodeForEach(string token, [NotNullWhen(true)] out ForEachToken? decoded)
    {
        decoded = null;
        if (!token.StartsWith(ForEachPrefix, StringComparison.Ordinal))
        {
            return false;
        }

        try
        {
            if (JsonNode.Parse(Base64Url.DecodeFromChars(token.AsSpan(ForEachPrefix.Length))) is not JsonObject obj
                || obj["list"] is not JsonValue listNode
                || !listNode.TryGetValue<string>(out var list)
                || obj["index"] is not JsonValue indexNode
                || !indexNode.TryGetValue<int>(out var at)
                || at < 0
                || !TryOptionalString(obj, "item", out var item)
                || !TryOptionalString(obj, "anchor", out var anchor)
                || !TryOptionalString(obj, "first_skipped", out var firstSkipped))
            {
                return false;
            }

            var skipped = 0;
            if (obj["skipped"] is { } skippedNode
                && (skippedNode is not JsonValue skippedValue || !skippedValue.TryGetValue(out skipped) || skipped < 0))
            {
                return false;
            }

            var ruleSkipped = false;
            if (obj["rule_skipped"] is { } ruleNode
                && (ruleNode is not JsonValue ruleValue || !ruleValue.TryGetValue(out ruleSkipped)))
            {
                return false;
            }

            decoded = new ForEachToken(list, at, item, anchor, skipped, firstSkipped, ruleSkipped);
            return true;
        }
        catch (Exception e) when (e is FormatException or JsonException or ArgumentException or InvalidOperationException)
        {
            return false;
        }
    }

    /// <summary>An absent or null key is null; anything but a JSON string is refused.</summary>
    private static bool TryOptionalString(JsonObject obj, string key, out string? value)
    {
        value = null;
        return obj[key] is not { } node || (node is JsonValue v && v.TryGetValue(out value));
    }
}
