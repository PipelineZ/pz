using Pz.Connectors.Abstractions;
using Pz.Core.Validation;
using Pz.DuckDb;

namespace Pz.Engine.Execution;

/// <summary>Shared by both native executor branches: runs one connector-authored setup statement
/// (secrets, extension installs) on the run's DuckDB session before the scan/copy statement itself.
/// Any failure is translated into a PZ0311 whose message never echoes the statement body — see <see
/// cref="NativeStatementRedactor"/> — since setup statements are TRUSTED connector code that may embed
/// credentials (e.g. CREATE SECRET).
///
/// Transience is classified by <see cref="DuckTransientErrors"/> rather than fixed to <c>false</c>
/// — an extension install (the "install" hint below) genuinely can fail on a transient network condition
/// (connection reset/timeout downloading httpfs), which is exactly the retry/breaker path this
/// classification exists to unlock.</summary>
internal static class NativeSetup
{
    internal static async Task ExecuteSetupAsync(IDuckSession duck, string statement, CancellationToken ct)
    {
        try
        {
            await duck.ExecuteAsync(statement, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            var described = NativeStatementRedactor.Describe(statement);
            var hint = statement.TrimStart().StartsWith("install", StringComparison.OrdinalIgnoreCase)
                ? " (first use of an object-store path needs network access to install the DuckDB httpfs extension)"
                : string.Empty;
            // The inner engine message MUST be sanitized (never the raw ex.Message): a DuckDB
            // parser/binder error's "LINE <n>: ..." context block echoes the offending statement
            // verbatim, which — for a malformed CREATE SECRET — would otherwise echo the secret
            // literal straight into this NodeResult/log.
            var sanitized = NativeStatementRedactor.SanitizeEngineMessage(ex.Message);
            throw new PzConnectorException(
                $"{PzErrorCode.NativeSetupFailed}: native setup statement failed: {described} — {sanitized}{hint}",
                isTransient: DuckTransientErrors.IsTransient(ex.Message), innerException: ex);
        }
    }
}
