using System.Buffers.Text;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Pz.Connector.Http;

/// <summary>The resume token of a read. A GET token is the next page's URL, byte for byte. A POST
/// token is <c>v2:</c> + base64url(UTF-8 JSON <c>{"url":...,"body":...}</c>), where a JSON body is a
/// JSON object and a text or form body a JSON string. Tokens live only in the run's local staging
/// state; neither a token nor its decoded body may appear in a message. A token of the other
/// method's shape, a different version or a malformed payload is refused, never thrown.</summary>
internal static class HttpCheckpointToken
{
    private const string PostPrefix = "v2:";

    /// <summary>The body shape a dataset sends: none, a JSON map, or text (a string or a form).</summary>
    public enum BodyKind { None, Json, Text }

    /// <summary>The kind of body <paramref name="body"/> declares.</summary>
    public static BodyKind KindOf(HttpBody? body)
        => body is null ? BodyKind.None : body.IsJson ? BodyKind.Json : BodyKind.Text;

    public static string Encode(HttpPageRequest request, bool isPost)
    {
        if (!isPost)
        {
            return request.Url.ToString();
        }

        var body = request.JsonBody?.DeepClone() ?? (request.TextBody is { } text ? JsonValue.Create(text) : null);
        var payload = new JsonObject { ["url"] = request.Url.ToString(), ["body"] = body };
        return PostPrefix + Base64Url.EncodeToString(Encoding.UTF8.GetBytes(payload.ToJsonString()));
    }

    public static bool TryDecode(string token, bool isPost, HttpMethod method, BodyKind expected,
        out HttpPageRequest? request)
    {
        request = null;
        if (!isPost)
        {
            if (token.StartsWith(PostPrefix, StringComparison.Ordinal)
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
}
