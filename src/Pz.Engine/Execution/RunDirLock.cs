namespace Pz.Engine.Execution;

/// <summary>An exclusive, OS-held marker that a live process owns a
/// directory. Taken by `pz run`/`pz retry` on the run dir and by `pz restore` on its tmp workdir, held
/// for that process's lifetime; probed by `pz clean` before it deletes anything.
///
/// The OS releases the lock when the holder exits, INCLUDING on SIGKILL — which is the whole point.
/// <c>run_results.json</c> carries status "running" during a run but a crashed run carries it forever, so
/// a status check leaks crashed runs permanently and any age-based tiebreak clobbers a legitimately long
/// backfill. The lock has neither failure mode: a crashed run's lock is free, so it sweeps normally.
///
/// .NET enforces <see cref="FileShare.None"/> with advisory <c>flock</c> on Unix and native share modes
/// on Windows, so this is correct on both — unlike a bare delete attempt, which fails loudly on Windows
/// (sharing violation) but silently succeeds on Linux, unlinking a file the writer still holds open.</summary>
public sealed class RunDirLock : IDisposable
{
    public const string FileName = ".lock";

    private readonly FileStream _stream;
    private bool _disposed;

    private RunDirLock(FileStream stream) => _stream = stream;

    /// <summary>Takes the lock for the calling process. Dispose (or process exit) releases it.</summary>
    public static RunDirLock Acquire(string dir)
    {
        Directory.CreateDirectory(dir);
        return new RunDirLock(new FileStream(
            Path.Combine(dir, FileName), FileMode.OpenOrCreate, FileAccess.Write, FileShare.None));
    }

    /// <summary>True when no live process holds <paramref name="dir"/> — i.e. it is safe to delete from.
    ///
    /// Opens with <see cref="FileMode.Open"/>, never <c>OpenOrCreate</c>: probing must not litter a
    /// <c>.lock</c> into every pre-lock run dir that the staging-only sweep then keeps. A missing lock
    /// file or a missing directory means no owner is possible, which is free, not held.</summary>
    public static bool IsFree(string dir)
    {
        try
        {
            using var probe = new FileStream(
                Path.Combine(dir, FileName), FileMode.Open, FileAccess.Write, FileShare.None);
            return true;
        }
        catch (FileNotFoundException)
        {
            return true; // no lock file -> no owner possible
        }
        catch (DirectoryNotFoundException)
        {
            return true; // nothing there to own
        }
        catch (IOException)
        {
            return false; // held by a live process (sharing violation / flock contention)
        }
        catch (UnauthorizedAccessException)
        {
            return false; // cannot prove it is free -> treat as held and skip it
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _stream.Dispose();
    }
}
