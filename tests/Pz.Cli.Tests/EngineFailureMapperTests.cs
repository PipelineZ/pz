using System.Runtime.Versioning;
using Pz.Cli;
using Pz.Cli.Commands;

namespace Pz.Cli.Tests;

/// <summary>Offline, no-repro unit tests for <see cref="EngineFailureMapper.TryMap"/> over synthetic
/// exceptions built with a specific <c>HResult</c> -- disk-full and file-locked are OS-signaled purely
/// through that field, and a real ERROR_SHARING_VIOLATION only ever happens on Windows, so a constructed
/// <see cref="IOException"/> is the only portable way to prove the classifier recognizes it. See
/// <see cref="RunCommandFailureMappingTests"/> for a real permission-denied repro through `pz run`.</summary>
public sealed class EngineFailureMapperTests
{
    [Fact]
    public void UnauthorizedAccessException_maps_to_PZ0531()
    {
        var ex = new UnauthorizedAccessException("Access to the path '/work/.pz' is denied.");

        var mapped = EngineFailureMapper.TryMap(ex);

        Assert.NotNull(mapped);
        Assert.Equal("PZ0531", mapped!.Code);
        Assert.Contains("/work/.pz", mapped.Message, StringComparison.Ordinal);
        Assert.NotNull(mapped.Hint);
    }

    [Fact]
    public void IOException_with_posix_ENOSPC_HResult_maps_to_PZ0532()
    {
        // Reproduced for real: writing into a size-capped tmpfs mounted via an unprivileged user
        // namespace raises exactly this shape -- IOException, HResult 28, message naming the path.
        var ex = new IOException("No space left on device : '/work/.pz/runs/x/staging.duckdb'", 28);

        var mapped = EngineFailureMapper.TryMap(ex);

        Assert.NotNull(mapped);
        Assert.Equal("PZ0532", mapped!.Code);
        Assert.Contains("staging.duckdb", mapped.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(unchecked((int)0x80070070))] // Windows ERROR_DISK_FULL
    [InlineData(unchecked((int)0x80070027))] // Windows ERROR_HANDLE_DISK_FULL
    public void IOException_with_windows_disk_full_HResult_maps_to_PZ0532(int hresult)
    {
        var ex = new IOException("There is not enough space on the disk.", hresult);

        var mapped = EngineFailureMapper.TryMap(ex);

        Assert.NotNull(mapped);
        Assert.Equal("PZ0532", mapped!.Code);
    }

    [Fact]
    public void IOException_with_sharing_violation_HResult_maps_to_PZ0533()
    {
        var ex = new IOException(
            "The process cannot access the file 'C:\\work\\.pz\\runs\\x\\staging.duckdb' because it is being used by another process.",
            unchecked((int)0x80070020));

        var mapped = EngineFailureMapper.TryMap(ex);

        Assert.NotNull(mapped);
        Assert.Equal("PZ0533", mapped!.Code);
    }

    [Fact]
    public void Unrelated_IOException_is_not_mapped()
    {
        // A plain IOException with none of the three named HResults must stay unmapped -- it is a
        // defect in pz, not a diagnosable local I/O condition, and must not be guessed at.
        var ex = new IOException("connection reset");

        Assert.Null(EngineFailureMapper.TryMap(ex));
    }

    [Fact]
    public void Unrelated_exception_type_is_not_mapped()
    {
        Assert.Null(EngineFailureMapper.TryMap(new InvalidOperationException("boom")));
    }
}

/// <summary>Real repro: a read-only project directory makes `pz run` fail creating
/// <c>.pz/runs/&lt;id&gt;</c>, which used to forward the raw <see cref="UnauthorizedAccessException"/>
/// text under the generic PZ0500 -- now fingerprinted to PZ0531 with a next step.
///
/// Unix permission bits only: <see cref="File.SetUnixFileMode"/> is a no-op fiction on Windows (and the
/// repo's CI/dev environment is Linux per https://pipelinez.dev/concepts/architecture-overview/), same
/// reasoning <see cref="RunRetentionFailureTests"/> already uses for the same trick.</summary>
[SupportedOSPlatform("linux")]
[Collection("console-and-env-serialized")]
public sealed class RunCommandFailureMappingTests
{
    [Fact]
    public void Read_only_project_directory_is_PZ0531_not_raw_PZ0500_text()
    {
        var work = Path.Combine(Path.GetTempPath(), "pz-engine-failure-tests", Guid.NewGuid().ToString("N"));
        CopyTree(Path.Combine(AppContext.BaseDirectory, "TemplatesSample"), work);
        try
        {
            // No write permission on the project root itself blocks creating .pz/runs/<id> under it.
            File.SetUnixFileMode(work, UnixFileMode.UserRead | UnixFileMode.UserExecute);

            var stderr = RunAndCaptureStderr(["run", "--project", work, "--all"]);

            Assert.Contains("PZ0531", stderr);
            Assert.Contains(work, stderr);
            Assert.DoesNotContain("PZ0500", stderr);
        }
        finally
        {
            try
            {
                File.SetUnixFileMode(work,
                    UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            }
            catch { /* best-effort */ }

            try { Directory.Delete(work, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private static void CopyTree(string from, string to)
    {
        Directory.CreateDirectory(to);
        foreach (var file in Directory.EnumerateFiles(from, "*", SearchOption.AllDirectories))
        {
            var dest = Path.Combine(to, Path.GetRelativePath(from, file));
            Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
            File.Copy(file, dest, overwrite: true);
        }
    }

    private static string RunAndCaptureStderr(string[] args)
    {
        var stderr = new StringWriter();
        var original = Console.Error;
        Console.SetError(stderr);
        try { CliApp.Build().Parse(args).Invoke(); }
        finally { Console.SetError(original); }
        return stderr.ToString();
    }
}
