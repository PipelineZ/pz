using System.IO.Compression;
using System.Text;
using Pz.Connector.Http;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http.Tests;

public sealed class ArchiveReaderTests : IDisposable
{
    private const long MiB = 1024 * 1024;
    private readonly string _dir = Directory.CreateTempSubdirectory("pz-archive-test-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private static HttpFileConfig Config(string? members = null, HttpArchive archive = HttpArchive.Auto,
        bool ifMissingEmpty = false)
        => new(HttpFileFormat.Csv) { Members = members, Archive = archive, IfMissingEmpty = ifMissingEmpty };

    private static DecompressionBudget Budget(long bytes = 64 * MiB) => new(bytes, "s.d");

    private string Zip(params (string Name, string? Text)[] entries)
    {
        var path = Path.Combine(_dir, $"{Guid.NewGuid():N}.zip");
        using (var zip = ZipFile.Open(path, ZipArchiveMode.Create))
        {
            foreach (var (name, text) in entries)
            {
                var entry = zip.CreateEntry(name);
                if (text is not null)
                {
                    using var w = new StreamWriter(entry.Open());
                    w.Write(text);
                }
            }
        }

        return path;
    }

    private string ZipOfZeros(params (string Name, long Bytes)[] entries)
    {
        var path = Path.Combine(_dir, $"{Guid.NewGuid():N}.zip");
        using var zip = ZipFile.Open(path, ZipArchiveMode.Create);
        foreach (var (name, bytes) in entries)
        {
            using var s = zip.CreateEntry(name, CompressionLevel.Fastest).Open();
            WriteZeros(s, bytes);
        }

        return path;
    }

    private string Gzip(string name, long zeros = 0, string? text = null)
    {
        var path = Path.Combine(_dir, name);
        using var gz = new GZipStream(File.Create(path), CompressionLevel.Fastest);
        if (text is not null)
        {
            gz.Write(Encoding.UTF8.GetBytes(text));
        }

        WriteZeros(gz, zeros);
        return path;
    }

    private static void WriteZeros(Stream s, long bytes)
    {
        var block = new byte[64 * 1024];
        for (var left = bytes; left > 0; left -= block.Length)
        {
            s.Write(block, 0, (int)Math.Min(block.Length, left));
        }
    }

    private static List<(string Name, string Text)> ReadAll(IEnumerable<FileSource> sources)
    {
        var result = new List<(string, string)>();
        foreach (var source in sources)
        {
            using (source)
            {
                using var reader = new StreamReader(source.Stream, leaveOpen: true);
                result.Add((source.Name, reader.ReadToEnd()));
            }
        }

        return result;
    }

    private static long Drain(Stream s)
    {
        var buffer = new byte[81920];
        long total = 0;
        int n;
        while ((n = s.Read(buffer)) > 0)
        {
            total += n;
        }

        return total;
    }

    [Fact]
    public void Sniffs_zip_gzip_and_plain()
    {
        var zip = Zip(("a.csv", "zip-a"));
        var gz = Gzip("data.csv.gz", text: "gzip-data");
        var plain = Path.Combine(_dir, "plain.csv");
        File.WriteAllText(plain, "plain-data");

        Assert.Equal([("a.csv", "zip-a")], ReadAll(ArchiveReader.Open(zip, Config("*.csv"), "x.zip", Budget(), "s.d")));
        Assert.Equal([("data.csv", "gzip-data")], ReadAll(ArchiveReader.Open(gz, Config(), "data.csv.gz", Budget(), "s.d")));
        Assert.Equal([("plain.csv", "plain-data")], ReadAll(ArchiveReader.Open(plain, Config(), "plain.csv", Budget(), "s.d")));
    }

    [Fact]
    public void Empty_zip_is_sniffed_as_zip()
    {
        var empty = Zip();
        Assert.Equal([0x50, 0x4B, 0x05, 0x06], File.ReadAllBytes(empty)[..4]);

        Assert.Empty(ReadAll(ArchiveReader.Open(empty, Config("*.csv", ifMissingEmpty: true), "x.zip", Budget(), "s.d")));
        var ex = Assert.Throws<PzConnectorException>(
            () => ReadAll(ArchiveReader.Open(empty, Config("*.csv"), "x.zip", Budget(), "s.d")));
        Assert.Contains("no zip entry matches", ex.Message);
    }

    [Fact]
    public void Explicit_archive_mode_overrides_sniffing()
    {
        var gz = Gzip("data.gz", text: "gzip-data");
        var sources = ReadAll(ArchiveReader.Open(gz, Config(archive: HttpArchive.None), "data.gz", Budget(), "s.d"));
        Assert.Equal("data.gz", Assert.Single(sources).Name);

        var plain = Path.Combine(_dir, "plain.csv");
        File.WriteAllText(plain, "plain-data");
        var ex = Assert.Throws<PzConnectorException>(
            () => ReadAll(ArchiveReader.Open(plain, Config("*.csv", HttpArchive.Zip), "plain.csv", Budget(), "s.d")));
        Assert.False(ex.IsTransient);
        Assert.StartsWith("s.d: ", ex.Message);
        Assert.Contains("zip", ex.Message);
    }

    [Fact]
    public void Members_in_name_order()
    {
        var zip = Zip(("b.csv", "B"), ("a.csv", "A"), ("c.json", "C"));

        Assert.Equal(["a.csv", "b.csv"], ArchiveReader.MatchingMembers(zip, Config("*.csv")));
        Assert.Equal([("a.csv", "A"), ("b.csv", "B")],
            ReadAll(ArchiveReader.Open(zip, Config("*.csv"), "x.zip", Budget(), "s.d")));
    }

    [Fact]
    public void Skips_directories_and_macosx()
    {
        var zip = Zip(("d/", null), ("__MACOSX/._s.csv", "fork"), ("__MACOSX/d/._s.csv", "fork"), ("d/s.csv", "real"));

        Assert.Equal(["d/s.csv"], ArchiveReader.MatchingMembers(zip, Config("**/*.csv")));
        Assert.Equal([("d/s.csv", "real")], ReadAll(ArchiveReader.Open(zip, Config("**/*.csv"), "x.zip", Budget(), "s.d")));
    }

    [Fact]
    public void Traversal_names_are_just_names()
    {
        // The zip sits two levels below a fresh root, so "../../evil.csv" resolved against the zip's
        // directory would land in the root.
        var root = Path.Combine(_dir, "root");
        var inner = Path.Combine(root, "a", "b");
        Directory.CreateDirectory(inner);
        var zip = Path.Combine(inner, "x.zip");
        File.Move(Zip(("../../evil.csv", "evil-data"), ("/abs.csv", "abs-data")), zip);

        string[] Listing() => [.. Directory.GetFileSystemEntries(root, "*", SearchOption.AllDirectories)
            .Concat(Directory.GetFileSystemEntries(Path.GetTempPath(), "evil.csv"))
            .Order(StringComparer.Ordinal)];
        var before = Listing();

        var read = ReadAll(ArchiveReader.Open(zip, Config("**/*.csv"), "x.zip", Budget(), "s.d"));

        Assert.Equal([("../../evil.csv", "evil-data"), ("/abs.csv", "abs-data")], read);
        Assert.Equal(before, Listing());
    }

    [Fact]
    public void No_match_errors_or_empty()
    {
        var zip = Zip(("a.csv", "A"), ("b.csv", "B"));

        var ex = Assert.Throws<PzConnectorException>(
            () => ReadAll(ArchiveReader.Open(zip, Config("x*.csv"), "x.zip", Budget(), "s.d")));
        Assert.False(ex.IsTransient);
        Assert.StartsWith("s.d: ", ex.Message);
        Assert.Contains("x*.csv", ex.Message);
        Assert.Contains("a.csv, b.csv", ex.Message);

        Assert.Empty(ArchiveReader.Open(zip, Config("x*.csv", ifMissingEmpty: true), "x.zip", Budget(), "s.d"));
        Assert.Empty(ArchiveReader.MatchingMembers(zip, Config("x*.csv")));
    }

    [Fact]
    public void Zip_without_members_errors_listing_up_to_20_entries()
    {
        var zip = Zip([.. Enumerable.Range(0, 25).Select(i => ($"f{i:D2}.csv", (string?)"x"))]);

        var ex = Assert.Throws<PzConnectorException>(
            () => ReadAll(ArchiveReader.Open(zip, Config(), "x.zip", Budget(), "s.d")));
        Assert.False(ex.IsTransient);
        Assert.Contains("file.members", ex.Message);
        Assert.Contains("f00.csv", ex.Message);
        Assert.Contains("f19.csv", ex.Message);
        Assert.DoesNotContain("f20.csv", ex.Message);
        Assert.Contains("5 more", ex.Message);

        Assert.Throws<PzConnectorException>(
            () => ReadAll(ArchiveReader.Open(zip, Config(archive: HttpArchive.Zip), "x.zip", Budget(), "s.d")));
    }

    [Fact]
    public void Zip_bomb_stops_at_budget()
    {
        var zip = ZipOfZeros(("bomb.csv", 50 * MiB));
        Assert.True(new FileInfo(zip).Length < MiB);

        // The enumeration stays open while the member is read: finishing it closes the archive.
        using var sources = ArchiveReader.Open(zip, Config("*.csv"), "x.zip", Budget(MiB), "s.d").GetEnumerator();
        Assert.True(sources.MoveNext());
        using var source = sources.Current;
        var ex = Assert.Throws<PzConnectorException>(() => Drain(source.Stream));

        Assert.False(ex.IsTransient);
        Assert.Equal("s.d: decompressed content exceeds 20 × 'max_download_mb' (1 MiB)", ex.Message);
    }

    [Fact]
    public void Gzip_bomb_stops_at_budget()
    {
        var gz = Gzip("bomb.csv.gz", zeros: 50 * MiB);
        Assert.True(new FileInfo(gz).Length < MiB);

        // The enumeration stays open while the member is read: finishing it closes the archive.
        using var sources = ArchiveReader.Open(gz, Config(), "bomb.csv.gz", Budget(MiB), "s.d").GetEnumerator();
        Assert.True(sources.MoveNext());
        using var source = sources.Current;
        var ex = Assert.Throws<PzConnectorException>(() => Drain(source.Stream));

        Assert.False(ex.IsTransient);
        Assert.Contains("'max_download_mb'", ex.Message);
    }

    [Fact]
    public void Budget_is_shared_across_members()
    {
        var zip = ZipOfZeros(("a.csv", 600 * 1024), ("b.csv", 600 * 1024));
        using var sources = ArchiveReader.Open(zip, Config("*.csv"), "x.zip", Budget(MiB), "s.d").GetEnumerator();

        Assert.True(sources.MoveNext());
        using (sources.Current)
        {
            Assert.Equal(600 * 1024, Drain(sources.Current.Stream));
        }

        Assert.True(sources.MoveNext());
        using (sources.Current)
        {
            var ex = Assert.Throws<PzConnectorException>(() => Drain(sources.Current.Stream));
            Assert.Contains("'max_download_mb'", ex.Message);
        }
    }

    [Fact]
    public async Task Budget_counts_async_reads()
    {
        var budget = Budget(1000);
        await using var s = budget.Wrap(new MemoryStream(new byte[1000]));
        Assert.Equal(1000, await s.ReadAtLeastAsync(new byte[4096], 4096, throwOnEndOfStream: false));

        await using var t = budget.Wrap(new MemoryStream(new byte[1]));
        await Assert.ThrowsAsync<PzConnectorException>(async () => await t.ReadAtLeastAsync(new byte[4096], 4096, throwOnEndOfStream: false));
    }

    [Fact]
    public void Encrypted_entries_are_refused()
    {
        var zip = Zip(("a.csv", "A"));
        var bytes = File.ReadAllBytes(zip);
        SetEncryptedFlag(bytes, [0x50, 0x4B, 0x03, 0x04], flagOffset: 6);
        SetEncryptedFlag(bytes, [0x50, 0x4B, 0x01, 0x02], flagOffset: 8);
        File.WriteAllBytes(zip, bytes);

        var ex = Assert.Throws<PzConnectorException>(
            () => ReadAll(ArchiveReader.Open(zip, Config("*.csv"), "x.zip", Budget(), "s.d")));
        Assert.False(ex.IsTransient);
        Assert.Contains("encrypted zip entries are not supported", ex.Message);
        Assert.Contains("a.csv", ex.Message);
    }

    private static void SetEncryptedFlag(byte[] zip, byte[] signature, int flagOffset)
    {
        var at = zip.AsSpan().IndexOf(signature);
        Assert.True(at >= 0);
        zip[at + flagOffset] |= 1;
    }

    [Fact]
    public void Archive_file_is_released_after_enumeration()
    {
        var zip = Zip(("a.csv", "A"), ("b.csv", "B"));

        // Abandoned after the first member: the iterator's finally still closes the archive.
        using (var e = ArchiveReader.Open(zip, Config("*.csv"), "x.zip", Budget(), "s.d").GetEnumerator())
        {
            Assert.True(e.MoveNext());
            e.Current.Dispose();
        }

        // An exclusive open fails while any handle to the archive is still open.
        using var exclusive = new FileStream(zip, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
    }
}
