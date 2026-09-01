using Google.Apis.Upload;
using Google.Cloud.Storage.V1;

namespace Pz.Connector.Gcs.Tests;

/// <summary>In-memory <see cref="StorageClient"/> for offline session-protocol tests: captures every
/// completed upload's bucket/name/bytes and can be armed to fail. Only the one member the write
/// sessions call is overridden; anything else keeps the base's NotImplementedException, so a session
/// reaching for a new SDK member fails loudly here first.</summary>
internal sealed class FakeStorageClient : StorageClient
{
    public List<(string Bucket, string Name, byte[] Content)> Uploads { get; } = [];
    public Exception? ThrowOnUpload { get; set; }
    public int UploadAttempts { get; private set; }
    public bool Disposed { get; private set; }

    public override void Dispose() => Disposed = true;

    public override async Task<Google.Apis.Storage.v1.Data.Object> UploadObjectAsync(
        string bucket, string objectName, string contentType, Stream source,
        UploadObjectOptions? options = null, CancellationToken cancellationToken = default,
        IProgress<IUploadProgress>? progress = null)
    {
        UploadAttempts++;
        if (ThrowOnUpload is not null)
        {
            throw ThrowOnUpload;
        }

        using var buffer = new MemoryStream();
        await source.CopyToAsync(buffer, cancellationToken);
        Uploads.Add((bucket, objectName, buffer.ToArray()));
        return new Google.Apis.Storage.v1.Data.Object { Bucket = bucket, Name = objectName };
    }
}
