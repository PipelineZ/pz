namespace Pz.Connectors.Toolkit.Paging;

internal static class QueryString
{
    /// <summary>Sets or replaces one query parameter, RFC 3986-escaping name and value.</summary>
    public static Uri With(Uri uri, string name, string value)
    {
        var encodedName = Uri.EscapeDataString(name);
        var encodedValue = Uri.EscapeDataString(value);
        var query = uri.Query.TrimStart('?');
        var pairs = query
            .Split('&', StringSplitOptions.RemoveEmptyEntries)
            .Where(p => !p.StartsWith(encodedName + "=", StringComparison.Ordinal) && p != encodedName)
            .Append($"{encodedName}={encodedValue}");
        var newQuery = string.Join('&', pairs);

        var builder = new UriBuilder(uri) { Query = newQuery };
        return builder.Uri;
    }
}
