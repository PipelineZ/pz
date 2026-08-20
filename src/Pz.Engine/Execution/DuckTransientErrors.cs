using System.Text.RegularExpressions;

namespace Pz.Engine.Execution;

/// <summary>Classifies a native-path (DuckDB CTAS/COPY/setup-statement) failure message as transient or
/// permanent for the retry/circuit-breaker machinery (<see
/// cref="Pz.Connectors.Abstractions.PzConnectorException.IsTransient"/>). Wrap sites keep calling this on
/// the RAW engine message (<c>ex.Message</c>) — the classifier extracts its own pre-LINE summary via <see
/// cref="NativeStatementRedactor.SplitSummary"/> before matching, so callers stay one-argument simple.
/// Sanitization (<see cref="NativeStatementRedactor.SanitizeEngineMessage"/>) applies separately, to
/// whatever gets rethrown.
///
/// Classifying the raw message directly is unsafe: a Parser/Binder/Catalog error's <c>LINE &lt;n&gt;: ...</c>
/// context block echoes the failing SQL verbatim, and unbounded substring matching against that block
/// misfires — "HTTP" matches inside "https://..." URLs, and a transient status-code digit sequence (e.g.
/// "500") matches inside an unrelated shard filename (e.g. "part-00500.parquet"). Matching the summary only
/// removes that whole class of false positive.
///
/// Even restricted to the summary, an "HTTP token present ANYWHERE, code token present ANYWHERE" rule
/// still collides with URL numeric path segments that sit in the summary
/// itself — DuckDB's httpfs errors embed the failing URL directly in the (LINE-less) summary, e.g. <c>HTTP
/// GET error on 'https://example.com/500/file.parquet' (HTTP 404)</c>: the "500" in the URL path is
/// word-boundaried and would satisfy an "anywhere" rule even though the real trailing status is 404. Hence
/// <b>adjacency</b> — the code must appear immediately after an HTTP token, shape <c>HTTP\s+&lt;code&gt;\b</c>
/// (see <see cref="TransientHttpAdjacent"/>). No intervening word is allowed between the HTTP token and the
/// code: the repo has no fixture or comment establishing an "HTTP Error 502"-style (HTTP + one filler word +
/// code) shape as something DuckDB actually emits, and the two shapes that DO need to keep matching — the
/// trailing <c>(HTTP 503)</c> marker and the bare <c>HTTP 429 Too Many Requests</c> line — are both strictly
/// adjacent. Strict adjacency also keeps the matching surface as small as possible, which is the
/// conservative direction for a classifier whose false positives cause retry storms. (Pinned false in
/// tests: an "HTTP Error 502" filler-word shape.) Adjacency also excludes an all-caps
/// "HTTPS" collision a bare token rule would allow (see <see cref="TransientHttpAdjacent"/>'s doc): "HTTPS"
/// is never followed by whitespace, so it can never satisfy the adjacency shape regardless of case.
///
/// The pattern list below is CLOSED by construction — every case-insensitive
/// shape is enumerated here and pinned by <c>DuckTransientErrorsTests</c>'s full matrix (every true case,
/// every pinned-false case). Anything unmatched stays <c>false</c> (permanent). This is deliberately
/// conservative: a misclassified permanent error means retry storms, so growing this list beyond the
/// enumerated shapes is a reviewable finding, not a routine addition.
///
/// Accepted residuals (deliberate, not defended against):
/// - Bare "timeout" matched against the summary can still match a non-network shape that happens to use the
///   word (e.g. a hypothetical "Statement timeout configuration error"). Accepted deliberately —
///   genuine timeouts are the single most common transient shape, and the retry policy's attempt cap
///   plus the circuit breaker bound the cost of the rare false positive.
/// - A summary containing MORE THAN ONE adjacency-shaped "HTTP &lt;code&gt;" occurrence classifies transient
///   if ANY occurrence is a transient code, even if a later occurrence in the same summary carries a
///   different (permanent) status. Real DuckDB httpfs errors carry exactly one terminal status per
///   exception — a summary with two independent status markers is not a shape DuckDB emits — so this is
///   accepted as a residual of substring-based summary matching rather than chased with structured message
///   parsing, which is out of scope for a CLOSED pattern list. Pinned true in tests to document the known,
///   accepted behavior.</summary>
internal static class DuckTransientErrors
{
    /// <summary>HTTP status codes DuckDB's httpfs extension surfaces for a transient object-store/network
    /// condition — 5xx server errors plus 408 (request timeout) and 429 (too many requests). Matched only
    /// immediately adjacent to an HTTP token (see <see cref="TransientHttpAdjacent"/>), so an unrelated
    /// digit sequence (e.g. a byte count, or digits embedded in a URL path segment or filename) can never
    /// match, whether or not the word "HTTP" appears elsewhere in the same summary. 403/404 are deliberately
    /// absent: both are permanent (forbidden/not-found), pinned false in tests.</summary>
    private static readonly string[] TransientHttpCodes = ["500", "502", "503", "504", "408", "429"];

    /// <summary>Non-HTTP transient shapes: connection-establishment/reset/generic-connection failures, and
    /// both timeout spellings DuckDB/its extensions use. Matched as plain substrings of the summary — these
    /// phrases are specific enough that word-boundary tightening isn't needed.</summary>
    private static readonly string[] TransientPhrases =
    [
        "connection refused",
        "connection reset",
        "connection error",
        "could not establish connection",
        "timed out",
        "timeout",
    ];

    /// <summary>Matches an HTTP status code immediately following an HTTP token: <c>HTTP\s+&lt;code&gt;\b</c>,
    /// built once from <see cref="TransientHttpCodes"/> so the code list stays the single source of truth.
    /// The "HTTP" token itself is boundaried — the
    /// character before the match (if any) must not be a letter, and the character after it (if any) must
    /// not be a lowercase letter — so "HTTP 503" and "HTTPException 503" (CamelCase continuation) both
    /// anchor the token, but "https://.../503" does not: the lowercase "s" continuing "http" reads as the
    /// same token, not a boundary. Adjacency then additionally requires whitespace directly after that
    /// token, which independently excludes "HTTPS" (uppercase "S" isn't lowercase, so it passes the token
    /// boundary check, but it's never followed by whitespace) and any URL/path segment where the code
    /// follows a non-whitespace separator (e.g. "HTTP-500", "?retry=503").</summary>
    private static readonly Regex TransientHttpAdjacent = new(
        $@"(?<![A-Za-z])(?i:HTTP)(?![a-z])\s+(?:{string.Join('|', TransientHttpCodes)})\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public static bool IsTransient(string message)
    {
        ArgumentNullException.ThrowIfNull(message);

        var summary = NativeStatementRedactor.SplitSummary(message);

        return TransientHttpAdjacent.IsMatch(summary) ||
            TransientPhrases.Any(phrase => summary.Contains(phrase, StringComparison.OrdinalIgnoreCase));
    }
}
