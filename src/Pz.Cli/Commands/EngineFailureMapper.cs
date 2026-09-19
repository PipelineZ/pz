using Pz.Core.Validation;

namespace Pz.Cli.Commands;

/// <summary>Fingerprints an exception that escaped ordinary command handling (`pz run`, `pz retry`,
/// `pz test`, `pz connector test`, and the `pz mcp` tools that share <see cref="RunCommand.ExecuteRun"/>)
/// into one of three local I/O failures with a next step, instead of forwarding the raw .NET exception
/// text under the generic <see cref="PzErrorCode.UnexpectedEngineFailure"/> (PZ0500).
///
/// <para>Checked by exception type and HResult only, never by message text -- a wrong diagnosis (e.g.
/// misreading a network hiccup as "check your disk") replaces the real error. Anything that is not one
/// of these three exact shapes stays PZ0500 with its own message: a defect in pz must stay a fatal error
/// with the information needed to report it, not a friendly guess.</para></summary>
public static class EngineFailureMapper
{
    // POSIX ENOSPC (no space left on device): .NET on Linux/macOS keeps the raw errno as the
    // exception's HResult rather than translating it to a Win32-style HRESULT -- verified against a
    // real write into a size-capped tmpfs.
    private const int PosixEnospc = 28;

    // Windows ERROR_DISK_FULL (0x70) and ERROR_HANDLE_DISK_FULL (0x27), each raised by a different
    // Win32 file API, both translated to an HRESULT by OR-ing the FACILITY_WIN32 bits (0x8007_0000)
    // onto the raw error code.
    private const int Win32DiskFull = unchecked((int)0x80070070);
    private const int Win32HandleDiskFull = unchecked((int)0x80070027);

    // Windows ERROR_SHARING_VIOLATION: another process holds the file open without sharing it. Linux's
    // advisory locking model has no equivalent errno for a plain open()/write(), so this HResult is
    // Windows-only in practice.
    private const int Win32SharingViolation = unchecked((int)0x80070020);

    public static PzError? TryMap(Exception ex) => ex switch
    {
        UnauthorizedAccessException => new PzError(
            PzErrorCode.EngineAccessDenied,
            $"permission denied: {ex.Message}",
            null, null,
            "check filesystem permissions on the project directory and .pz, then try again"),

        IOException { HResult: PosixEnospc or Win32DiskFull or Win32HandleDiskFull } => new PzError(
            PzErrorCode.EngineDiskFull,
            $"disk full: {ex.Message}",
            null, null,
            "free disk space under the project directory (including its .pz subfolder), then try again"),

        IOException { HResult: Win32SharingViolation } => new PzError(
            PzErrorCode.EngineFileLocked,
            $"file locked: {ex.Message}",
            null, null,
            "close whatever other process has the file open, then try again"),

        _ => null,
    };
}
