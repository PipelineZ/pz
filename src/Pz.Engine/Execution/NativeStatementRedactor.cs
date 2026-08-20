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
    /// <c>LINE</c>-context block (see <see cref="SplitSummary"/>), then masks any single-quoted literal
    /// remaining in the summary (e.g. an identifier DuckDB quotes back in a Binder Error) to <c>'***'</c> as
    /// defense in depth.</summary>
    public static string SanitizeEngineMessage(string message)
    {
        ArgumentNullException.ThrowIfNull(message);

        var summary = SplitSummary(message);
        return QuotedLiteral().Replace(summary, "'***'");
    }

    [GeneratedRegex(@"'[^']*'")]
    private static partial Regex QuotedLiteral();
}
