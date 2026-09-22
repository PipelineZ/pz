using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using Microsoft.Win32.SafeHandles;

namespace Pz.PackageManagement.ProcessHosting;

/// <summary>Windows-only backstop for the case <see cref="ConnectorProcess"/>'s own shutdown ladder
/// cannot reach: THIS process (the pz host) dying ungracefully -- a crash, a debugger detach, an
/// operator's <c>taskkill /f</c> -- before any of its own code runs, including
/// <see cref="ConnectorProcess.DisposeAsync"/>'s process-tree kill. On Linux, a spawned child could ask
/// the kernel to <c>prctl(PR_SET_PDEATHSIG, SIGKILL)</c> itself, but that call has to run in the CHILD
/// before it execs -- something only the child's own entrypoint (a connector built on an SDK) can do for
/// itself, and only .NET's <see cref="Process.Start"/> here cannot arrange. A Windows Job Object is the
/// mirror-image primitive the HOST can set up instead: the job handle is process-local kernel state, so
/// when this process's handle table is torn down -- by any means, not just an orderly close -- the OS
/// closes the job handle too, and <see cref="JobObjectLimitKillOnJobClose"/> means that closure kills
/// every process still assigned to the job. Unlike a connector self-exiting on its own control-connection
/// loss (see <c>ControlConnectionWatch</c> in <c>Pz.Connectors.Sdk</c>), this covers every spawned
/// connector, hand-rolled or SDK-built, without needing the connector's own cooperation.</summary>
[SupportedOSPlatform("windows")]
internal static partial class WindowsJobObject
{
    private const int JobObjectExtendedLimitInformation = 9;
    private const uint JobObjectLimitKillOnJobClose = 0x2000;

    /// <summary>Creates a new job object with <see cref="JobObjectLimitKillOnJobClose"/> set and
    /// assigns <paramref name="process"/> to it. Returns null (rather than throwing) on any failure --
    /// a process whose parent PID has been reused, one already assigned to a job that forbids nesting on
    /// an older Windows without <c>JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK</c>, or any other Win32 failure
    /// leaves the child bound only by the ordinary shutdown ladder, exactly as it would be without this
    /// backstop -- never a reason to fail the spawn that already succeeded.</summary>
    public static SafeJobObjectHandle? AssignToNewJob(Process process)
    {
        SafeJobObjectHandle? job = null;
        try
        {
            job = SafeJobObjectHandle.Create();

            var info = default(JOBOBJECT_EXTENDED_LIMIT_INFORMATION);
            info.BasicLimitInformation.LimitFlags = JobObjectLimitKillOnJobClose;
            var size = (uint)Marshal.SizeOf<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>();
            if (!SetInformationJobObject(job, JobObjectExtendedLimitInformation, ref info, size))
            {
                job.Dispose();
                return null;
            }

            if (!AssignProcessToJobObject(job, process.SafeHandle))
            {
                job.Dispose();
                return null;
            }

            return job;
        }
        catch
        {
            job?.Dispose();
            return null;
        }
    }

    [LibraryImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool SetInformationJobObject(
        SafeJobObjectHandle job, int jobObjectInfoClass,
        ref JOBOBJECT_EXTENDED_LIMIT_INFORMATION jobObjectInfo, uint jobObjectInfoLength);

    [LibraryImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool AssignProcessToJobObject(SafeJobObjectHandle job, SafeHandle process);

    // Field order/types mirror the Win32 JOBOBJECT_BASIC_LIMIT_INFORMATION and
    // JOBOBJECT_EXTENDED_LIMIT_INFORMATION structs exactly (see winnt.h): SIZE_T/ULONG_PTR fields are
    // pointer-width (nuint), everything else is fixed-width. Only LimitFlags is ever set; every other
    // field stays zeroed (the struct's default), which Win32 reads as "no limit" for that field.
    [StructLayout(LayoutKind.Sequential)]
    private struct JOBOBJECT_BASIC_LIMIT_INFORMATION
    {
        public long PerProcessUserTimeLimit;
        public long PerJobUserTimeLimit;
        public uint LimitFlags;
        public nuint MinimumWorkingSetSize;
        public nuint MaximumWorkingSetSize;
        public uint ActiveProcessLimit;
        public nuint Affinity;
        public uint PriorityClass;
        public uint SchedulingClass;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct IO_COUNTERS
    {
        public ulong ReadOperationCount;
        public ulong WriteOperationCount;
        public ulong OtherOperationCount;
        public ulong ReadTransferCount;
        public ulong WriteTransferCount;
        public ulong OtherTransferCount;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct JOBOBJECT_EXTENDED_LIMIT_INFORMATION
    {
        public JOBOBJECT_BASIC_LIMIT_INFORMATION BasicLimitInformation;
        public IO_COUNTERS IoInfo;
        public nuint ProcessMemoryLimit;
        public nuint JobMemoryLimit;
        public nuint PeakProcessMemoryUsed;
        public nuint PeakJobMemoryUsed;
    }
}

/// <summary>Owns one Win32 job object handle. <see cref="ReleaseHandle"/> is the entire point of this
/// type: closing the last handle to a job created with <c>JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE</c> kills
/// every process still assigned to it, so this handle staying open for exactly this process's lifetime
/// -- released only on an explicit <see cref="SafeHandle.Dispose()"/> or, just as surely, whenever the
/// OS reclaims this process's handle table on any exit, graceful or not -- is what makes the backstop
/// work.</summary>
[SupportedOSPlatform("windows")]
internal sealed partial class SafeJobObjectHandle : SafeHandleZeroOrMinusOneIsInvalid
{
    // Public, not private: the LibraryImport source generator constructs this type itself when
    // marshalling CreateJobObjectW's HANDLE return value back into a SafeJobObjectHandle, and needs an
    // accessible parameterless constructor to do it -- nothing else is meant to call this directly, use
    // Create() instead.
    public SafeJobObjectHandle() : base(ownsHandle: true)
    {
    }

    public static SafeJobObjectHandle Create()
    {
        var handle = CreateJobObjectW(IntPtr.Zero, null);
        if (handle.IsInvalid)
        {
            throw new Win32Exception(Marshal.GetLastPInvokeError());
        }

        return handle;
    }

    protected override bool ReleaseHandle() => CloseHandle(handle);

    [LibraryImport("kernel32.dll", EntryPoint = "CreateJobObjectW", SetLastError = true,
        StringMarshalling = StringMarshalling.Utf16)]
    private static partial SafeJobObjectHandle CreateJobObjectW(IntPtr lpJobAttributes, string? lpName);

    [LibraryImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static partial bool CloseHandle(IntPtr handle);
}
