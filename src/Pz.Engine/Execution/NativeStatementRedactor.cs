using System.Text.RegularExpressions;

namespace Pz.Engine.Execution;

/// <summary>Renders a native setup statement safely for errors/logs: the first two
/// whitespace-separated tokens, uppercased, plus an ellipsis — never the statement body, which may
/// contain credentials (CREATE SECRET ...).</summary>
public static partial class NativeStatementRedactor
{
    public static string Describe(string sql)
    {
        var tokens = sql.Split((char[]?)null, 3, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return tokens.Length switch
        {
            0 => "…",
            1 => $"{tokens[0].ToUpperInvariant()} …",
            _ => $"{tokens[0].ToUpperInvariant()} {tokens[1].ToUpperInvariant()} …",
        };
    }

    /// <summary>Isolates the leading error-type/summary line(s) of a native engine message: a
    /// parser/binder/catalog error's <c>LINE &lt;n&gt;: ...</c> context block echoes the offending
    /// statement verbatim (including any inline secret literal, e.g. a malformed <c>CREATE SECRET ...
    /// 'value'</c>, and — load-bearing for <see cref="DuckTransientErrors.IsTransient"/> — any URL or
    /// filename from the failing SQL) — everything from the first line starting with "LINE " to the end of
    /// the message is dropped, keeping only the summary. Shared by <see cref="SanitizeEngineMessage"/>
    /// (redaction) and <see cref="DuckTransientErrors.IsTransient"/> (transient classification) so both
    /// agree on exactly the same boundary; classification must never see the raw SQL echo, which is where
    /// unbounded "HTTP"/status-code substring matches misfire (e.g. "https://..." URLs, "part-00500.parquet"
    /// shard filenames).</summary>
    internal static string SplitSummary(string message)
    {
        var lines = message.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        var summaryLines = new List<string>();
        foreach (var line in lines)
        {
            if (line.TrimStart().StartsWith("LINE ", StringComparison.Ordinal))
            {
                break;
            }

            summaryLines.Add(line);
        }

        return string.Join('\n', summaryLines).Trim();
    }

    /// <summary>Sanitizes a DuckDB engine exception message before it can reach a NodeResult/log: drops the
    /// <c>LINE</c>-context block (see <see cref="SplitSummary"/>), then masks single-quoted literals in the
    /// summary (e.g. an identifier DuckDB quotes back in a Binder Error) to <c>'***'</c> as defense in depth.
    /// Two literal shapes are structural rather than data and stay readable, because they are what explains a
    /// native IO failure: an object/file URL (<c>'az://lake/raw/x.parquet'</c>), kept with its userinfo,
    /// query string (a SAS token) and fragment masked; and the nested <c>Original error message: '...'</c>
    /// an extension wraps around its transport error, kept with every URL in it scrubbed the same way and
    /// every credential-named <c>key=value</c> pair (<c>AccountKey=</c>, <c>sig=</c>, <c>password=</c>, ...)
    /// masked.</summary>
    public static string SanitizeEngineMessage(string message)
    {
        ArgumentNullException.ThrowIfNull(message);

        var summary = SplitSummary(message);
        return QuotedLiteral().Replace(summary, m =>
        {
            var body = m.Groups["body"].Value;
            if (m.Groups["nested"].Success)
            {
                return $"{m.Groups["nested"].Value}'{ScrubFreeText(body)}'";
            }

            return IsUrl(body) ? $"'{ScrubUrl(body)}'" : "'***'";
        });
    }

    private static bool IsUrl(string text) => UrlLiteral().IsMatch(text);

    /// <summary>Keeps scheme, host and path; masks userinfo (<c>user:password@</c>) and drops the query and
    /// fragment behind a <c>?***</c> marker, since a SAS token or presigned signature rides the query.</summary>
    private static string ScrubUrl(string url)
    {
        var schemeEnd = url.IndexOf("://", StringComparison.Ordinal) + 3;
        var tailStart = url.IndexOfAny(['?', '#'], schemeEnd);
        var head = tailStart < 0 ? url : url[..tailStart];

        var authorityEnd = head.IndexOf('/', schemeEnd);
        var authority = authorityEnd < 0 ? head[schemeEnd..] : head[schemeEnd..authorityEnd];
        var at = authority.LastIndexOf('@');
        if (at >= 0)
        {
            head = string.Concat(head.AsSpan(0, schemeEnd), "***", head.AsSpan(schemeEnd + at));
        }

        return tailStart < 0 ? head : head + "?***";
    }

    private static string ScrubFreeText(string text)
    {
        var scrubbed = EmbeddedUrl().Replace(text, m => ScrubUrl(m.Value));
        return CredentialPair().Replace(scrubbed, m => $"{m.Groups["key"].Value}=***");
    }

    [GeneratedRegex(@"(?<nested>Original error message:\s*)?'(?<body>[^']*)'", RegexOptions.IgnoreCase)]
    private static partial Regex QuotedLiteral();

    [GeneratedRegex(@"^[A-Za-z][A-Za-z0-9+.\-]*://\S+$")]
    private static partial Regex UrlLiteral();

    [GeneratedRegex(@"[A-Za-z][A-Za-z0-9+.\-]*://[^\s'""]+")]
    private static partial Regex EmbeddedUrl();

    [GeneratedRegex(
        @"\b(?<key>AccountKey|SharedAccessSignature|SharedAccessKey|sig|password|pwd|token|secret|client_secret|access_key|secret_key|key_id|session_token)=[^;&\s]+",
        RegexOptions.IgnoreCase)]
    private static partial Regex CredentialPair();
}
