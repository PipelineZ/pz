using Renci.SshNet;
using Renci.SshNet.Common;
using Renci.SshNet.Sftp;

namespace Pz.Connector.Sftp;

/// <summary>Thin adapter from <see cref="ISftpFileSystem"/> onto a connected SSH.NET
/// <see cref="SftpClient"/>. Owns the client's lifetime — <see cref="Dispose"/> disconnects and
/// disposes it — and, when given one, the <see cref="IDisposable"/> auth bundle
/// (<see cref="SftpAuth"/>) the client was built with, since that is the only place left holding a
/// reference to it once <see cref="SftpClientFactory.Connect"/> returns.</summary>
internal sealed class SftpFileSystem(SftpClient client, IDisposable? auth = null) : ISftpFileSystem
{
    /// <summary>Regular files under <paramref name="directory"/>, yielded as
    /// <c>&lt;directory as passed&gt;/&lt;entry name&gt;</c> — relative to whatever form the caller
    /// passed, NEVER <c>ISftpFile.FullName</c>. SSH.NET builds <c>FullName</c> from the server's
    /// <c>realpath</c> of the listed directory, so under a chrooted OpenSSH server (atmoz/sftp and
    /// friends) a relative <paramref name="directory"/> like "upload/x" comes back rooted at
    /// "/upload/x", which then fails to match the caller's relative glob pattern.</summary>
    public IEnumerable<string> ListFiles(string directory, bool recursive)
    {
        // A bare glob pattern with no directory part (e.g. "*.csv") resolves to a static prefix with
        // no '/', which SftpPaths.ListMatches then hands here as "" -- not a valid SFTP listing
        // target. "." (the login-relative current directory) is the correct empty-prefix root.
        if (directory.Length == 0)
        {
            directory = ".";
        }

        var pending = new Queue<string>();
        pending.Enqueue(directory);

        while (pending.Count > 0)
        {
            var current = pending.Dequeue();
            IEnumerable<ISftpFile> entries;
            try
            {
                entries = client.ListDirectory(current);
            }
            catch (SftpPathNotFoundException)
            {
                // A missing directory yields no entries — the no-match error belongs to the
                // caller, which knows the dataset name.
                continue;
            }

            foreach (var entry in entries)
            {
                if (entry.Name is "." or "..")
                {
                    continue;
                }

                var joined = Join(current, entry.Name);
                if (entry.IsRegularFile)
                {
                    yield return joined;
                }
                else if (recursive && entry.IsDirectory)
                {
                    pending.Enqueue(joined);
                }
            }
        }
    }

    /// <summary>Joins a listing directory and an entry name in the caller's own form: "." yields the
    /// bare name (no "./" prefix); a directory already ending in '/' is not doubled.</summary>
    private static string Join(string directory, string name) =>
        directory == "." ? name : directory.EndsWith('/') ? directory + name : $"{directory}/{name}";

    public Stream OpenRead(string path) => client.OpenRead(path);

    public Stream OpenWrite(string path) => client.Open(path, FileMode.Create, FileAccess.Write);

    public void Rename(string oldPath, string newPath) => client.RenameFile(oldPath, newPath);

    public void Delete(string path) => client.DeleteFile(path);

    // Exists(path) alone answers true for directories too; a regular-file guard is what makes
    // this "does the FILE exist" rather than "does anything exist at this path".
    public bool FileExists(string path) => client.Exists(path) && client.Get(path).IsRegularFile;

    // Mirror of FileExists's guard, inverted: Exists(path) alone answers true for regular files too.
    public bool DirectoryExists(string path) => client.Exists(path) && client.Get(path).IsDirectory;

    public void CreateDirectories(string path)
    {
        var segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var current = path.StartsWith('/') ? "/" : "";
        foreach (var segment in segments)
        {
            current = current is "" or "/" ? current + segment : $"{current}/{segment}";
            if (!client.Exists(current))
            {
                client.CreateDirectory(current);
            }
        }
    }

    public void Dispose()
    {
        try
        {
            if (client.IsConnected)
            {
                client.Disconnect();
            }
        }
        catch
        {
            // Best-effort: the client is being torn down either way, and Dispose below releases
            // the underlying resources regardless of whether the polite disconnect succeeded.
        }

        client.Dispose();
        auth?.Dispose();
    }
}
