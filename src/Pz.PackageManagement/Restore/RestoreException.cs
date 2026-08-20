namespace Pz.PackageManagement.Restore;

/// <summary>Raised for restore failures. Codes are the PZ032x literals pinned by
/// Pz.Core.Validation.PzErrorCode (same pattern as ConnectorHostException — this assembly
/// must not depend on Pz.Core). These literals are NOT auto-synced with the registry -- see the
/// comment above PzErrorCode.FloatingVersionRejected.</summary>
public sealed class RestoreException(string code, string message, string? hint = null) : Exception(message)
{
    public string Code { get; } = code;   // "PZ0320" | "PZ0321" | "PZ0322" | "PZ0323"
    public string? Hint { get; } = hint;
}
