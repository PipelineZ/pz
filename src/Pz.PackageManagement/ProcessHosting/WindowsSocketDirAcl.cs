using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;

namespace Pz.PackageManagement.ProcessHosting;

/// <summary>The Windows half of the socket directory's owner-only guarantee. A Windows AF_UNIX socket
/// is a file whose DACL is checked on connect, and a new file takes its DACL from its directory by
/// inheritance -- so an ordinary project tree (where <c>Authenticated Users</c> commonly hold Modify)
/// would let any local user dial a connector's control socket, which carries credentials. The Unix
/// mode bits the POSIX path sets mean nothing here; this replaces the directory's DACL with a
/// protected one (inheritance from the parent cut) granting only the current user, inherited by the
/// sockets the child creates inside it.</summary>
[SupportedOSPlatform("windows")]
internal static class WindowsSocketDirAcl
{
    public static void RestrictToCurrentUser(string directory)
    {
        using var identity = WindowsIdentity.GetCurrent();
        var user = identity.User
            ?? throw new InvalidOperationException("the current Windows identity has no user SID");

        var security = new DirectorySecurity();
        security.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);
        security.AddAccessRule(new FileSystemAccessRule(
            user,
            FileSystemRights.FullControl,
            InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
            PropagationFlags.None,
            AccessControlType.Allow));
        new DirectoryInfo(directory).SetAccessControl(security);
    }
}
