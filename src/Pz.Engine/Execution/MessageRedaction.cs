using Pz.Connectors.Abstractions;
using Pz.Core.Validation;

namespace Pz.Engine.Execution;

/// <summary>A thin, NAMED seam over <see cref="NativeStatementRedactor"/> so raw-message call sites
/// express intent ("redact this before it reaches a user/log/artifact") rather than reaching for a
/// helper whose name reads as SQL-specific. Delegates entirely to
/// <see cref="NativeStatementRedactor.SanitizeEngineMessage"/>:
/// drops a DuckDB parser/binder's verbatim "LINE n: ..." statement-echo block, then masks any
/// single-quoted literal remaining in the summary to <c>'***'</c>.
///
/// This is METADATA-level hardening, NOT a PII-proof guarantee: it strips the two shapes DuckDB/engine
/// exceptions are known to leak through (a verbatim statement echo, a single-quoted literal), and
/// nothing else. A message that embeds sensitive data outside those shapes -- inside a double-quoted
/// identifier, a connector-specific error format with no quoting at all, free text, a stack trace, etc.
/// -- passes through completely unchanged. Callers that need an actual data-loss-prevention guarantee
/// must not rely on this seam alone.</summary>
public static class MessageRedaction
{
    public static string Redact(string message) => NativeStatementRedactor.SanitizeEngineMessage(message);

    /// <summary>TRUST BOUNDARY: <see cref="IsTrusted"/> exceptions -- PipelineZ's own <see cref="PzConnectorException"/>,
    /// <see cref="PzConfigException"/>, and <see cref="PzValidationException"/>
    /// -- are developer-authored and ALREADY sanitized at their native sites before being wrapped (e.g.
    /// <see cref="NativeSetup.ExecuteSetupAsync"/> and <see cref="SourceLoadExecutor"/>'s native-scan
    /// catch both run <see cref="NativeStatementRedactor.SanitizeEngineMessage"/> on the raw engine text
    /// BEFORE embedding it in a <see cref="PzConnectorException"/> message -- the standing discipline). Their
    /// <c>Message</c> is passed through UNREDACTED so operator-facing identifiers (a column/table/dataset
    /// name quoted in a Pz-authored error) stay actionable instead of being masked to <c>'***'</c>
    /// alongside genuinely sensitive data.
    ///
    /// Every other exception -- a raw/foreign one (DuckDBException, NpgsqlException, IOException, or any
    /// arbitrary <see cref="Exception"/> that didn't pass through a Pz wrapper) -- may still echo raw
    /// SQL, a statement fragment, or other data verbatim, so its message is redacted via
    /// <see cref="Redact(string)"/>.
    ///
    /// The guarantee is unchanged by this narrowing: METADATA-level hardening, not a PII-proof
    /// guarantee. A Pz exception that -- contrary to the discipline above -- embeds unsanitized raw
    /// engine/data text would pass through unredacted.</summary>
    public static string Redact(Exception ex) => IsTrusted(ex) ? ex.Message : Redact(ex.Message);

    private static bool IsTrusted(Exception ex) =>
        ex is PzConnectorException or PzConfigException or PzValidationException;
}
