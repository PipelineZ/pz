using Pz.Connector.Http;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

public class DownloadScratchTests
{
    private const long MiB = 1024 * 1024;

    [Fact]
    public async Task Save_copies_the_body()
    {
        using var scratch = new DownloadScratch();
        var body = new byte[300_000];
        Random.Shared.NextBytes(body);

        var path = await scratch.SaveAsync(new MemoryStream(body), MiB, "s.d", CancellationToken.None);

        Assert.Equal(Path.Combine(scratch.Directory, "download.bin"), path);
        Assert.Equal(body, await File.ReadAllBytesAsync(path));
        Assert.StartsWith(Path.Combine(Path.GetTempPath(), "pz-http-"), scratch.Directory);
    }

    [Fact]
    public async Task Save_enforces_max_bytes_mid_stream()
    {
        var scratch = new DownloadScratch();
        var body = new CountingStream(new MemoryStream(new byte[2 * MiB]));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            () => scratch.SaveAsync(body, MiB, "s.d", CancellationToken.None));

        Assert.False(ex.IsTransient);
        Assert.Equal("s.d: download exceeds 'max_download_mb' (1 MiB)", ex.Message);
        Assert.True(body.BytesRead < 2 * MiB, "the copy stops once the cap is passed");
        Assert.True(File.Exists(Path.Combine(scratch.Directory, "download.bin")));

        scratch.Dispose();
        Assert.False(Directory.Exists(scratch.Directory));
    }

    [Fact]
    public async Task Save_accepts_exactly_max_bytes()
    {
        using var scratch = new DownloadScratch();
        var path = await scratch.SaveAsync(new MemoryStream(new byte[MiB]), MiB, "s.d", CancellationToken.None);
        Assert.Equal(MiB, new FileInfo(path).Length);
    }

    [Fact]
    public void Dispose_removes_directory()
    {
        var scratch = new DownloadScratch();
        Assert.True(Directory.Exists(scratch.Directory));
        File.WriteAllText(Path.Combine(scratch.Directory, "x.txt"), "x");
        Directory.CreateDirectory(Path.Combine(scratch.Directory, "sub"));

        scratch.Dispose();
        scratch.Dispose();

        Assert.False(Directory.Exists(scratch.Directory));
    }

    private sealed class CountingStream(Stream inner) : Stream
    {
        public long BytesRead { get; private set; }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override int Read(byte[] buffer, int offset, int count)
        {
            var n = inner.Read(buffer, offset, Math.Min(count, 64 * 1024));
            BytesRead += n;
            return n;
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            var n = await inner.ReadAsync(buffer[..Math.Min(buffer.Length, 64 * 1024)], cancellationToken);
            BytesRead += n;
            return n;
        }
    }
}
