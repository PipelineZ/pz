namespace Pz.Connectors.Toolkit.Paging;

/// <summary>Query-string editing for request URLs.</summary>
public static class QueryString
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

    /// <summary>Sets a secret query parameter (an api key, a session token), first removing every pair whose name,
    /// decoded (<c>+</c> as a space, percent-escapes undone), equals <paramref name="name"/> in any letter case. An
    /// API that echoes the secret into its next links under another spelling (<c>API_KEY</c>, <c>%61pi_key</c>)
    /// would otherwise get it twice.</summary>
    public static Uri WithSecret(Uri uri, string name, string value)
    {
        var kept = uri.Query.TrimStart('?')
            .Split('&', StringSplitOptions.RemoveEmptyEntries)
            .Where(p => !string.Equals(DecodedName(p), name, StringComparison.OrdinalIgnoreCase));
        var cleaned = new UriBuilder(uri) { Query = string.Join('&', kept) }.Uri;
        return With(cleaned, name, value);
    }

    private static string DecodedName(string pair)
    {
        var name = pair.Split('=', 2)[0].Replace('+', ' ');
        try
        {
            return Uri.UnescapeDataString(name);
        }
        catch (UriFormatException)
        {
            return name;
        }
    }
}
