namespace Pz.Core.Artifacts;

/// <summary>Publishes a file by writing it aside and renaming it into place. Overlapping pz processes
/// in one project all publish to the same <c>.pz/target</c>: opening the final path directly makes the
/// second writer fail on the first one's open handle, and lets a reader see a half-written file. A
/// same-directory rename is atomic on every supported platform, so each writer succeeds, the last one
/// wins whole, and a reader sees one writer's complete bytes or the previous file — never a mix.</summary>
public static class AtomicFile
{
    /// <summary>Roughly half a second of retrying in all, far longer than one replace takes.</summary>
    private const int MoveAttempts = 20;

    public static void Write(string path, Action<Stream> write)
    {
        // Same directory as the target, so the rename never crosses a volume and stays atomic.
        var tmpPath = $"{path}.{Guid.NewGuid():N}.tmp";
        var moved = false;
        try
        {
            using (var stream = new FileStream(tmpPath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
            {
                write(stream);
            }

            MoveIntoPlace(tmpPath, path);
            moved = true;
        }
        finally
        {
            if (!moved)
            {
                try { File.Delete(tmpPath); } catch { /* best-effort cleanup — never mask the real exception */ }
            }
        }
    }

    /// <summary>A Windows replace is not free-standing the way a unix rename is: it is refused (access
    /// denied, or a sharing violation) while another writer is replacing the same file, whose old copy
    /// is still being deleted, or while a reader holds the file open without delete sharing. Both last
    /// only as long as that other operation, so the move is retried briefly there. A failure that
    /// outlasts every attempt — a directory that really is read-only — surfaces unchanged.</summary>
    private static void MoveIntoPlace(string tmpPath, string path)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                File.Move(tmpPath, path, overwrite: true);
                return;
            }
            catch (Exception ex) when (OperatingSystem.IsWindows() && attempt < MoveAttempts
                && ex is UnauthorizedAccessException or IOException)
            {
                Thread.Sleep(Math.Min(attempt * 2, 50));
            }
        }
    }

    public static void WriteAllText(string path, string contents) =>
        Write(path, stream =>
        {
            // No BOM, matching File.WriteAllText: these files are byte-stable artifacts.
            using var writer = new StreamWriter(stream, new System.Text.UTF8Encoding(false), leaveOpen: true);
            writer.Write(contents);
        });
}
