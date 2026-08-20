using Pz.Engine.Execution;

namespace Pz.Engine.Tests.Execution;

/// <summary>The live-run guard. .NET on Unix enforces
/// <see cref="FileShare"/> with advisory <c>flock</c>, which is per-open-file-description — so a second
/// exclusive open fails even from within the same process, and these facts need no second process.</summary>
public sealed class RunDirLockTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "pz-rundirlock-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch { /* best-effort cleanup */ }
    }

    private string NewDir()
    {
        var dir = Path.Combine(_root, Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public void Held_lock_makes_the_directory_not_free()
    {
        var dir = NewDir();
        using var held = RunDirLock.Acquire(dir);

        Assert.False(RunDirLock.IsFree(dir));
    }

    [Fact]
    public void Released_lock_makes_the_directory_free_again()
    {
        var dir = NewDir();
        var held = RunDirLock.Acquire(dir);
        Assert.False(RunDirLock.IsFree(dir));

        held.Dispose();

        Assert.True(RunDirLock.IsFree(dir));
    }

    [Fact]
    public void Directory_without_a_lock_file_is_free()
    {
        // A run dir written by a pz version predating the lock: no owner is possible, so it is sweepable.
        var dir = NewDir();

        Assert.True(RunDirLock.IsFree(dir));
        Assert.False(File.Exists(Path.Combine(dir, RunDirLock.FileName)));
    }

    [Fact]
    public void Probing_never_creates_a_lock_file()
    {
        // FileMode.Open, not OpenOrCreate: probing must not litter a .lock into a dir the sweep then keeps.
        var dir = NewDir();

        RunDirLock.IsFree(dir);

        Assert.Empty(Directory.EnumerateFileSystemEntries(dir));
    }

    [Fact]
    public void Missing_directory_is_free()
    {
        Assert.True(RunDirLock.IsFree(Path.Combine(_root, "does-not-exist")));
    }

    [Fact]
    public void Acquire_creates_the_directory_and_the_lock_file()
    {
        var dir = Path.Combine(_root, Guid.NewGuid().ToString("N"));

        using var held = RunDirLock.Acquire(dir);

        Assert.True(File.Exists(Path.Combine(dir, RunDirLock.FileName)));
    }

    [Fact]
    public void Dispose_is_idempotent()
    {
        var dir = NewDir();
        var held = RunDirLock.Acquire(dir);

        held.Dispose();
        held.Dispose();

        Assert.True(RunDirLock.IsFree(dir));
    }
}
