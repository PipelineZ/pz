using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;

namespace Pz.TestSupport;

/// <summary>Makes a real filesystem operation fail for the current user, on unix and Windows alike, so
/// a test can drive the product's own failure handling rather than a mock of it. Each block is lifted
/// when the returned value is disposed.</summary>
public static class FileSystemBlocks
{
    private const UnixFileMode ReadExecute = UnixFileMode.UserRead | UnixFileMode.UserExecute;
    private const UnixFileMode ReadWriteExecute = ReadExecute | UnixFileMode.UserWrite;

    /// <summary>Creating a file or directory directly inside <paramref name="dir"/> fails with
    /// <see cref="UnauthorizedAccessException"/>. Unix: the directory loses its write bit. Windows: a
    /// deny rule for the current user on the directory itself (not inherited by its contents), which
    /// wins over every allow rule it inherits.</summary>
    public static IDisposable DenyCreatingChildren(string dir)
    {
        if (OperatingSystem.IsWindows())
        {
            return DenyOnWindows(dir, FileSystemRights.CreateFiles | FileSystemRights.CreateDirectories);
        }

        return RemoveWriteBit(dir);
    }

    /// <summary>Deleting <paramref name="file"/>, or a directory tree holding it, fails. Unix: the file's
    /// directory loses its write bit, since unlinking needs write on the parent (so creating siblings
    /// fails too), and the failure is <see cref="UnauthorizedAccessException"/>. Windows: an open handle
    /// that does not share delete, and the failure is an <see cref="IOException"/> (a sharing
    /// violation). Code under test that tolerates a failed delete must catch both.</summary>
    public static IDisposable BlockDeleting(string file)
    {
        if (OperatingSystem.IsWindows())
        {
            var handle = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.Read);
            return new Unblock(handle.Dispose);
        }

        return RemoveWriteBit(Path.GetDirectoryName(Path.GetFullPath(file))!);
    }

    [UnsupportedOSPlatform("windows")]
    private static Unblock RemoveWriteBit(string dir)
    {
        File.SetUnixFileMode(dir, ReadExecute);
        return new Unblock(() => File.SetUnixFileMode(dir, ReadWriteExecute));
    }

    [SupportedOSPlatform("windows")]
    private static Unblock DenyOnWindows(string dir, FileSystemRights rights)
    {
        var info = new DirectoryInfo(dir);
        var rule = new FileSystemAccessRule(
            WindowsIdentity.GetCurrent().User!, rights, InheritanceFlags.None, PropagationFlags.None,
            AccessControlType.Deny);

        var security = info.GetAccessControl();
        security.AddAccessRule(rule);
        info.SetAccessControl(security);

        return new Unblock(() =>
        {
            var current = info.GetAccessControl();
            current.RemoveAccessRuleSpecific(rule);
            info.SetAccessControl(current);
        });
    }

    /// <summary>Runs its action at most once, so a test may lift a block early (to "fix" the
    /// failure mid-test) and still dispose it in a <c>finally</c>.</summary>
    private sealed class Unblock(Action lift) : IDisposable
    {
        private Action? _lift = lift;

        public void Dispose() => Interlocked.Exchange(ref _lift, null)?.Invoke();
    }
}
