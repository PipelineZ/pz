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
/// classification exists to unlock.
///
/// Production reaches this only through <see cref="NativeSetupLedger"/> — the two native executor
/// branches issue setup statements via <see cref="RunContext.SetupLedger"/>, never directly — so this
/// method itself has no once-per-run memory; connector/e2e test suites that call it straight are
/// exercising one statement in isolation, not the per-run dedupe.</summary>
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
            var hint = InstallHint(statement);
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

    /// <summary>Names the extension a failing <c>install &lt;extension&gt;</c> statement was
    /// installing, so the hint reads as advice about the extension actually being fetched rather than
    /// a generic httpfs guess — every native-only connector ships its own extension (quack, ducklake,
    /// duckdb, motherduck, …) and only httpfs is specifically about object storage.</summary>
    private static string InstallHint(string statement)
    {
        const string prefix = "install";
        var trimmed = statement.TrimStart();
        if (!trimmed.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
        {
            return string.Empty;
        }

        var extension = trimmed[prefix.Length..].TrimStart();
        var end = extension.IndexOf(' ');
        if (end >= 0)
        {
            extension = extension[..end];
        }

        return extension.Equals("httpfs", StringComparison.OrdinalIgnoreCase)
            ? " (first use of an object-store path needs network access to install the DuckDB httpfs extension)"
            : $" (first use needs network access to install the DuckDB {extension} extension)";
    }
}
