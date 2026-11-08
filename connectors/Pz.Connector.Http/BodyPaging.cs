using System.Globalization;
using System.Text.Json.Nodes;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Toolkit.Json;

namespace Pz.Connector.Http;

/// <summary>Pagination whose position lives in a POST read's JSON body (`pagination.location: body`).
/// Every method returns a new tree; the rendered body it is handed is never mutated. A fresh instance
/// resumes mid-crawl because the position is read back from the current request's body.</summary>
internal abstract class BodyPaging
{
    /// <summary>True when an empty page ends the crawl (page and offset); a cursor ends on its token.</summary>
    public abstract bool StopsOnEmptyPage { get; }

    /// <summary>The body for the first request.</summary>
    public abstract JsonNode First(JsonNode body);

    /// <summary>The body for the next request, or null when the crawl is over.</summary>
    public abstract JsonNode? Next(JsonNode currentBody, JsonNode? responseBody, int recordsReturned);

    /// <summary>Builds the strategy for `pagination.location: body`; null when the location is the query
    /// string or the strategy is URL-driven (`link_header`, `next_url`). Problems are appended to
    /// <paramref name="errors"/>.</summary>
    public static BodyPaging? Parse(IReadOnlyDictionary<string, object?> pagination, string strategy,
        JsonNode? bodyTemplate, string itemsPointer, List<string> errors)
    {
        string? Get(string key) => pagination.TryGetValue(key, out var v) ? YamlScalarText.Of(v) : null;

        var location = Get("location");
        if (location is not (null or "query" or "body"))
        {
            errors.Add($"pagination 'location' must be 'query' or 'body', got '{location}'");
            return null;
        }

        if (location != "body" || strategy is "link_header" or "next_url")
        {
            return null;
        }

        if (strategy is not ("page" or "offset" or "cursor"))
        {
            return null;
        }

        if (bodyTemplate is null)
        {
            errors.Add("pagination 'location: body' requires 'body' to be a JSON map (form and string " +
                "bodies cannot carry pagination)");
            return null;
        }

        var param = PointerOption("param", required: true);
        switch (strategy)
        {
            case "page":
            {
                var sizeParam = PointerOption("size_param", required: false);
                return param is null ? null
                    : new BodyPagePaging(param, StartOf(1), sizeParam, SizeOf());
            }
            case "offset":
            {
                var sizeParam = PointerOption("size_param", required: false);
                return param is null ? null
                    : new BodyOffsetPaging(param, StartOf(0), sizeParam, SizeOf(), itemsPointer);
            }
            default:
                // The response pointer and the presence of 'pointer'/'param' are validated with the
                // URL-side options; only the body target is checked here.
                return param is null || Get("pointer") is not { } pointer
                    ? null
                    : new BodyCursorPaging(param, pointer);
        }

        string? PointerOption(string key, bool required)
        {
            var value = Get(key);
            if (value is null)
            {
                if (required)
                {
                    errors.Add($"pagination '{key}' is required with 'location: body' (a JSON pointer " +
                        "into the body, e.g. '/page')");
                }

                return null;
            }

            if (value.Length == 0 || value[0] != '/'
                || JsonPointerWriter.TrySet(bodyTemplate.DeepClone(), value, JsonValue.Create(0)) is false)
            {
                errors.Add($"pagination '{key}' ('{value}') is not a JSON pointer the body can hold: it " +
                    "must name an existing member or array element of 'body', or add the last segment " +
                    "to an existing object");
                return null;
            }

            return value;
        }

        // 'start' and 'size' are validated by the URL-side parse; unreadable values fall back here.
        long StartOf(long fallback)
            => long.TryParse(Get("start"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var s)
                && s >= 0 ? s : fallback;

        int? SizeOf()
            => int.TryParse(Get("size"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var s)
                && s > 0 ? s : null;
    }

    protected static JsonNode Write(JsonNode body, string pointer, JsonNode? value)
    {
        var copy = body.DeepClone();
        // Parse proved the pointer settable against the configured body; a rendered body has the
        // same shape, so a failure here is a programming error.
        return JsonPointerWriter.TrySet(copy, pointer, value)
            ? copy
            : throw new InvalidOperationException($"pagination pointer '{pointer}' cannot be set in the request body");
    }

    /// <summary>The position the current request carries at <paramref name="pointer"/>; absent means
    /// the crawl has not stamped one yet. A present value that is not an integer would silently repeat
    /// pages, so it fails permanently (the message names the pointer, never the value).</summary>
    protected static long ReadPosition(JsonNode body, string pointer, long fallback)
    {
        if (!JsonPointer.TryResolve(body, pointer, out var node))
        {
            return fallback;
        }

        if (node is JsonValue v)
        {
            if (v.TryGetValue(out long whole))
            {
                return whole;
            }

            if (v.TryGetValue(out double d) && Math.Floor(d) == d && Math.Abs(d) < 9e15)
            {
                return (long)d;
            }
        }

        throw new PzConnectorException(
            $"pagination.param '{pointer}' in the request body is not an integer " +
            "(fix the 'body' template so the paged position is a whole number)", isTransient: false);
    }

    protected static JsonNode WriteSize(JsonNode body, string? sizeParam, int? size)
        => sizeParam is not null && size is { } s ? Write(body, sizeParam, JsonValue.Create(s)) : body;
}

internal sealed class BodyPagePaging(string param, long start, string? sizeParam, int? size) : BodyPaging
{
    public override bool StopsOnEmptyPage => true;

    public override JsonNode First(JsonNode body)
        => WriteSize(Write(body, param, JsonValue.Create(start)), sizeParam, size);

    public override JsonNode? Next(JsonNode currentBody, JsonNode? responseBody, int recordsReturned)
    {
        if (recordsReturned == 0)
        {
            return null;
        }

        var page = ReadPosition(currentBody, param, start);
        return WriteSize(Write(currentBody, param, JsonValue.Create(page + 1)), sizeParam, size);
    }
}

internal sealed class BodyOffsetPaging(string param, long start, string? sizeParam, int? size, string itemsPointer)
    : BodyPaging
{
    public override bool StopsOnEmptyPage => true;

    public override JsonNode First(JsonNode body)
        => WriteSize(Write(body, param, JsonValue.Create(start)), sizeParam, size);

    public override JsonNode? Next(JsonNode currentBody, JsonNode? responseBody, int recordsReturned)
    {
        // Advance by the rows the page actually returned, not the requested size; a clamped limit
        // would otherwise skip rows. Items are counted from the response when it is available.
        var returned = responseBody is not null && JsonPointer.TryResolve(responseBody, itemsPointer, out var items)
            && items is JsonArray array
                ? array.Count
                : recordsReturned;
        if (returned == 0)
        {
            return null;
        }

        var position = ReadPosition(currentBody, param, start);
        return WriteSize(Write(currentBody, param, JsonValue.Create(position + returned)), sizeParam, size);
    }
}

internal sealed class BodyCursorPaging(string param, string responsePointer) : BodyPaging
{
    public override bool StopsOnEmptyPage => false;

    public override JsonNode First(JsonNode body) => body.DeepClone();

    public override JsonNode? Next(JsonNode currentBody, JsonNode? responseBody, int recordsReturned)
    {
        if (responseBody is null || !JsonPointer.TryResolve(responseBody, responsePointer, out var token)
            || token is null
            || (token is JsonValue v && v.TryGetValue(out string? text) && string.IsNullOrEmpty(text))
            || (token is JsonArray { Count: 0 }) || (token is JsonObject { Count: 0 }))
        {
            return null;
        }

        return Write(currentBody, param, token.DeepClone());
    }
}

/// <summary>RFC 6901 pointer writer (the toolkit's <see cref="JsonPointer"/> only reads).</summary>
internal static class JsonPointerWriter
{
    /// <summary>Sets an existing member or element, or adds the last segment to an existing object.
    /// Returns false when the parent does not exist, is not a container, or an array index is out of
    /// range.</summary>
    public static bool TrySet(JsonNode root, string pointer, JsonNode? value)
    {
        if (pointer.Length < 2 || pointer[0] != '/')
        {
            return false;
        }

        var segments = pointer[1..].Split('/');
        var current = root;
        for (var i = 0; i < segments.Length - 1; i++)
        {
            var segment = Unescape(segments[i]);
            switch (current)
            {
                case JsonObject obj when obj.TryGetPropertyValue(segment, out var child) && child is not null:
                    current = child;
                    break;
                case JsonArray arr when int.TryParse(segment, NumberStyles.None, CultureInfo.InvariantCulture, out var index)
                    && index < arr.Count && arr[index] is { } element:
                    current = element;
                    break;
                default:
                    return false;
            }
        }

        var last = Unescape(segments[^1]);
        switch (current)
        {
            case JsonObject obj:
                obj[last] = value;
                return true;
            case JsonArray arr when int.TryParse(last, NumberStyles.None, CultureInfo.InvariantCulture, out var index)
                && index < arr.Count:
                arr[index] = value;
                return true;
            default:
                return false;
        }
    }

    private static string Unescape(string segment) => segment.Replace("~1", "/").Replace("~0", "~");
}
