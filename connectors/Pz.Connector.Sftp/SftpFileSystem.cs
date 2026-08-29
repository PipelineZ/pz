using Renci.SshNet;
using Renci.SshNet.Common;
using Renci.SshNet.Sftp;

namespace Pz.Connector.Sftp;

/// <summary>Thin adapter from <see cref="ISftpFileSystem"/> onto a connected SSH.NET
/// <see cref="SftpClient"/>. Owns the client's lifetime — <see cref="Dispose"/> disconnects and
/// disposes it.</summary>
internal sealed class SftpFileSystem(SftpClient client) : ISftpFileSystem
{
    public IEnumerable<string> ListFiles(string directory, bool recursive)
    {
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

                if (entry.IsRegularFile)
                {
                    yield return entry.FullName;
                }
                else if (recursive && entry.IsDirectory)
                {
                    pending.Enqueue(entry.FullName);
                }
            }
        }
    }

    public Stream OpenRead(string path) => client.OpenRead(path);

    public Stream OpenWrite(string path) => client.Open(path, FileMode.Create, FileAccess.Write);

    public void Rename(string oldPath, string newPath) => client.RenameFile(oldPath, newPath);

    public void Delete(string path) => client.DeleteFile(path);

    // Exists(path) alone answers true for directories too; a regular-file guard is what makes
    // this "does the FILE exist" rather than "does anything exist at this path".
    public bool FileExists(string path) => client.Exists(path) && client.Get(path).IsRegularFile;

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
    }
}
