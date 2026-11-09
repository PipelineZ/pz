using System.Globalization;
using System.Text.Json.Nodes;
using Pz.Connectors.Toolkit.Json;

namespace Pz.Connectors.Toolkit.Paging;

/// <summary>Offset/limit paging: the next request's <paramref name="param"/> is the current offset
/// plus the number of records the current page actually returned (read at
/// <paramref name="itemsPointer"/>), not plus the requested size. An API that silently clamps the
/// limit (asked 100, served 50) would otherwise have every page skip the rows between what it served
/// and what was asked. An empty page ends the crawl.</summary>
public sealed class OffsetStrategy(string param, long start, string? sizeParam, int? size, string itemsPointer)
    : IPageStrategy
{
    private long? _offset;

    public Uri FirstRequestUri(Uri firstUri)
    {
        _offset = start;
        return Stamp(firstUri, start);
    }

    public Uri? NextRequestUri(Uri current, HttpResponseMessage response, JsonNode? body)
    {
        var returned = body is not null && JsonPointer.TryResolve(body, itemsPointer, out var items)
            && items is JsonArray array
                ? array.Count
                : 0;
        if (returned == 0)
        {
            return null;
        }

        // A resumed attempt re-enters mid-crawl from a persisted next-link with a FRESH instance, so
        // the current request's own offset — not the running total — is the truthful position.
        var position = TryGetOffset(current, out var currentOffset) ? currentOffset : _offset ?? start;
        _offset = position + returned;
        return Stamp(current, _offset.Value);
    }

    private Uri Stamp(Uri uri, long offset)
    {
        var stamped = QueryString.With(uri, param, offset.ToString(CultureInfo.InvariantCulture));
        if (sizeParam is not null && size is { } s)
        {
            stamped = QueryString.With(stamped, sizeParam, s.ToString(CultureInfo.InvariantCulture));
        }

        return stamped;
    }

    private bool TryGetOffset(Uri uri, out long offset)
    {
        var prefix = Uri.EscapeDataString(param) + "=";
        foreach (var pair in uri.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            if (pair.StartsWith(prefix, StringComparison.Ordinal))
            {
                return long.TryParse(Uri.UnescapeDataString(pair[prefix.Length..]),
                    NumberStyles.Integer, CultureInfo.InvariantCulture, out offset);
            }
        }

        offset = 0;
        return false;
    }
}
