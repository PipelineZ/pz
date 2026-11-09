namespace Pz.Connector.Http;

/// <summary>The header-name checks every configured header name goes through (a page token header, a session
/// <c>auth.inject.header</c> or login header, the <c>retry_after</c> header). The connector's JSON schema patterns
/// mirror them.</summary>
internal static class HttpHeaderNames
{
    /// <summary>A non-empty RFC 9110 token: what a header field name may contain.</summary>
    public static bool IsToken(string name)
        => name.Length > 0 && name.All(c => char.IsAsciiLetterOrDigit(c) || "!#$%&'*+-.^_`|~".Contains(c));

    /// <summary>Whether <paramref name="name"/> can go on a request's own headers. The HTTP stack refuses an invalid
    /// name there, and the names it treats as content headers (<c>Content-Type</c>, <c>Expires</c>, <c>Allow</c>,
    /// ...), which a request without a body never sends.</summary>
    public static bool IsRequestHeader(string name)
    {
        if (name.Length == 0)
        {
            return false;
        }

        using var probe = new HttpRequestMessage();
        return probe.Headers.TryAddWithoutValidation(name, "probe");
    }
}
