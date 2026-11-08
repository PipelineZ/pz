using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http;

/// <summary>A per-partition temp directory, `Path.GetTempPath()/pz-http-&lt;guid&gt;/`, holding the
/// downloaded body. Archive members are never extracted into it; the download is the only file.
/// Dispose in a `finally` so the directory goes on success, failure and cancellation.</summary>
internal sealed class DownloadScratch : IDisposable
{
    private const long MiB = 1024 * 1024;

    public DownloadScratch()
    {
        Directory = Path.Combine(Path.GetTempPath(), $"pz-http-{Guid.NewGuid():N}");
        System.IO.Directory.CreateDirectory(Directory);
    }

    public string Directory { get; }

    /// <summary>Copies <paramref name="body"/> to `download.bin` and returns its path. Throws a
    /// permanent error as soon as more than <paramref name="maxBytes"/> arrive; the partial file stays
    /// until <see cref="Dispose"/>.</summary>
    public Task<string> SaveAsync(Stream body, long maxBytes, string label, CancellationToken ct)
        => SaveAsync(body, maxBytes, label, idle: null, ct);

    /// <summary>As <see cref="SaveAsync(Stream, long, string, CancellationToken)"/>, with each read of
    /// <paramref name="body"/> bounded by <paramref name="idle"/> when set.</summary>
    public async Task<string> SaveAsync(Stream body, long maxBytes, string label, IdleRead? idle,
        CancellationToken ct)
    {
        var path = Path.Combine(Directory, "download.bin");
        await using var file = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None,
            bufferSize: 0, useAsync: true);
        var buffer = new byte[81920];
        long total = 0;
        int n;
        while ((n = idle is null
                   ? await body.ReadAsync(buffer, ct).ConfigureAwait(false)
                   : await idle.ReadAsync(body, buffer, ct).ConfigureAwait(false)) > 0)
        {
            total += n;
            if (total > maxBytes)
            {
                throw new PzConnectorException(
                    $"{label}: download exceeds 'max_download_mb' ({maxBytes / MiB} MiB)", isTransient: false);
            }

            await file.WriteAsync(buffer.AsMemory(0, n), ct).ConfigureAwait(false);
        }

        return path;
    }

    public void Dispose()
    {
        try
        {
            System.IO.Directory.Delete(Directory, recursive: true);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            // Best effort (already gone, or a file still held open): cleanup must not mask the
            // partition's outcome.
        }
    }
}
