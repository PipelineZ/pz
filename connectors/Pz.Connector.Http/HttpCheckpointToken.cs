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
/// <c>first_skipped</c>.</summary>
internal static class HttpCheckpointToken
{
    private const string PostPrefix = "v2:";
    private const string ForEachPrefix = "v3:";

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

        if (!isPost)
        {
            return request.Url.ToString();
        }

        var body = request.JsonBody?.DeepClone() ?? (request.TextBody is { } text ? JsonValue.Create(text) : null);
        var payload = new JsonObject { ["url"] = request.Url.ToString(), ["body"] = body };
        return PostPrefix + Base64Url.EncodeToString(Encoding.UTF8.GetBytes(payload.ToJsonString()));
    }

    private static Uri WithoutQueryParams(Uri url, IReadOnlyCollection<string> names)
    {
        if (url.Query.Length == 0)
        {
            return url;
        }

        var escaped = names.Select(Uri.EscapeDataString).ToList();
        var kept = url.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries)
            .Where(pair => !escaped.Any(name => pair == name || pair.StartsWith(name + "=", StringComparison.Ordinal)));
        return new UriBuilder(url) { Query = string.Join('&', kept) }.Uri;
    }

    public static bool TryDecode(string token, bool isPost, HttpMethod method, BodyKind expected,
        out HttpPageRequest? request)
    {
        request = null;
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

        if (!token.StartsWith(PostPrefix, StringComparison.Ordinal))
        {
            return false;
        }

        try
        {
            var json = JsonNode.Parse(Base64Url.DecodeFromChars(token.AsSpan(PostPrefix.Length)));
            if (json is not JsonObject obj
                || obj["url"] is not JsonValue urlNode
                || !urlNode.TryGetValue<string>(out var urlText)
                || !Uri.TryCreate(urlText, UriKind.Absolute, out var target))
            {
                return false;
            }

            // The token's body replaces the configured one on resume, so its kind must be the one the
            // dataset declares; otherwise it would bypass the declared content type.
            switch (obj["body"])
            {
                case null when expected == BodyKind.None:
                    request = new HttpPageRequest(method, target, null, null);
                    return true;
                case JsonObject map when expected == BodyKind.Json:
                    request = new HttpPageRequest(method, target, map.DeepClone(), null);
                    return true;
                case JsonValue value when expected == BodyKind.Text && value.TryGetValue<string>(out var text):
                    request = new HttpPageRequest(method, target, null, text);
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
        int Skipped = 0, string? FirstSkipped = null);

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

            decoded = new ForEachToken(list, at, item, anchor, skipped, firstSkipped);
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
