using Renci.SshNet.Common;

namespace Pz.Connector.Sftp.Tests;

/// <summary>In-memory <see cref="ISftpFileSystem"/> for protocol-level tests: source/sink partition
/// discovery, rename-into-place delivery, mkdir -p. Keyed and ordered by full path so ListFiles's
/// prefix-walk is deterministic without a real BFS traversal.</summary>
internal sealed class FakeSftpFileSystem : ISftpFileSystem
{
    private readonly SortedDictionary<string, byte[]> _files = new(StringComparer.Ordinal);
    private readonly HashSet<string> _directories = new(StringComparer.Ordinal);

    /// <summary>Operation log in call order, e.g. "rename:a->b", "delete:x", "mkdir:a/b",
    /// "open-write:x", "open-read:x" — lets a protocol test assert ordering/occurrence without
    /// reaching into file contents.</summary>
    public List<string> Operations { get; } = [];

    /// <summary>Per-operation fault injection: called with the operation's log entry (see
    /// <see cref="Operations"/>) before it runs; a non-null result is thrown instead of the normal
    /// behavior.</summary>
    public Func<string, Exception?>? FailOn { get; set; }

    public void Seed(string path, byte[] content) => _files[path] = content;

    public void SeedDirectory(string path) => _directories.Add(path);

    public IEnumerable<string> ListFiles(string directory, bool recursive)
    {
        Guard($"list:{directory}");

        var prefix = directory.EndsWith('/') ? directory : directory + "/";
        foreach (var path in _files.Keys)
        {
            if (!path.StartsWith(prefix, StringComparison.Ordinal))
            {
                continue;
            }

            var remainder = path[prefix.Length..];
            if (recursive || !remainder.Contains('/'))
            {
                yield return path;
            }
        }
    }

    public Stream OpenRead(string path)
    {
        Guard($"open-read:{path}");

        if (!_files.TryGetValue(path, out var content))
        {
            throw new SftpPathNotFoundException($"No such file: '{path}'", path);
        }

        return new MemoryStream(content, writable: false);
    }

    public Stream OpenWrite(string path)
    {
        Guard($"open-write:{path}");
        return new WriteBackStream(this, path);
    }

    public void Rename(string oldPath, string newPath)
    {
        Guard($"rename:{oldPath}->{newPath}");

        if (!_files.Remove(oldPath, out var content))
        {
            throw new SftpPathNotFoundException($"No such file: '{oldPath}'", oldPath);
        }

        // Real SFTP rename semantics: the target must not already exist.
        if (!_files.TryAdd(newPath, content))
        {
            _files[oldPath] = content;   // undo the remove — the rename never happened
            throw new SftpPermissionDeniedException($"rename target already exists: '{newPath}'");
        }
    }

    public void Delete(string path)
    {
        Guard($"delete:{path}");

        if (!_files.Remove(path))
        {
            throw new SftpPathNotFoundException($"No such file: '{path}'", path);
        }
    }

    public bool FileExists(string path)
    {
        Guard($"exists:{path}");
        return _files.ContainsKey(path);
    }

    public void CreateDirectories(string path)
    {
        Guard($"mkdir:{path}");
        _directories.Add(path);
    }

    public void Dispose()
    {
    }

    private void Guard(string operation)
    {
        Operations.Add(operation);
        if (FailOn?.Invoke(operation) is { } ex)
        {
            throw ex;
        }
    }

    /// <summary>A write-target MemoryStream that copies its buffered bytes back into the fake's
    /// dictionary on Dispose — mirroring SftpFileStream's create-or-truncate-then-flush-on-close
    /// behavior without a real connection.</summary>
    private sealed class WriteBackStream(FakeSftpFileSystem owner, string path) : MemoryStream
    {
        private bool _flushed;

        protected override void Dispose(bool disposing)
        {
            if (disposing && !_flushed)
            {
                owner._files[path] = ToArray();
                _flushed = true;
            }

            base.Dispose(disposing);
        }
    }
}
