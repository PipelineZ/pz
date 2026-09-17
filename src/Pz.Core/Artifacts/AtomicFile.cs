namespace Pz.Core.Artifacts;

/// <summary>Publishes a file by writing it aside and renaming it into place. Overlapping pz processes
/// in one project all publish to the same <c>.pz/target</c>: opening the final path directly makes the
/// second writer fail on the first one's open handle, and lets a reader see a half-written file. A
/// same-directory rename is atomic on every supported platform, so each writer succeeds, the last one
/// wins whole, and a reader sees one writer's complete bytes or the previous file — never a mix.</summary>
public static class AtomicFile
{
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

            File.Move(tmpPath, path, overwrite: true);
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

    public static void WriteAllText(string path, string contents) =>
        Write(path, stream =>
        {
            // No BOM, matching File.WriteAllText: these files are byte-stable artifacts.
            using var writer = new StreamWriter(stream, new System.Text.UTF8Encoding(false), leaveOpen: true);
            writer.Write(contents);
        });
}
