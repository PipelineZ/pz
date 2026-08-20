namespace Pz.PackageManagement.Hosting;

/// <summary>Raised for connector-hosting failures. <see cref="Code"/> is one of the PZ03xx connector
/// error codes defined in <c>Pz.Core.Validation.PzErrorCode</c> (kept out of this assembly's
/// dependency graph — Pz.PackageManagement references only Pz.Connectors.Abstractions).</summary>
public sealed class ConnectorHostException(string code, string message, string? hint = null) : Exception(message)
{
    public string Code { get; } = code;   // "PZ0304" | "PZ0305" | "PZ0306" | "PZ0307"
    public string? Hint { get; } = hint;
}
