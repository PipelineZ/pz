namespace Pz.Connector.Sftp;

/// <summary>The connector's whole view of a remote SFTP server. Synchronous by design — SSH.NET's
/// SFTP surface is synchronous, and connector calls already run on pool threads — and one instance
/// is ONE connection: never share an instance across concurrently-executing partitions/sessions
/// (a single SSH channel serializes requests; concurrent use is a correctness bug, not a slowdown).
/// All paths are absolute-or-login-relative remote paths with '/' separators.</summary>
internal interface ISftpFileSystem : IDisposable
{
    /// <summary>Regular files under <paramref name="directory"/>, full paths, '.'/'..' excluded;
    /// recursive walks subdirectories breadth-first. A missing directory yields no entries (the
    /// no-match error belongs to the caller, which knows the dataset name).</summary>
    IEnumerable<string> ListFiles(string directory, bool recursive);

    Stream OpenRead(string path);          // seekable (SSH.NET SftpFileStream)
    Stream OpenWrite(string path);         // create-or-truncate
    void Rename(string oldPath, string newPath);
    void Delete(string path);
    bool FileExists(string path);
    void CreateDirectories(string path);   // mkdir -p
}
