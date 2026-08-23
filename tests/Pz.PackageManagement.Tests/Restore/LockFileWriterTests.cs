using Pz.PackageManagement.Restore;

namespace Pz.PackageManagement.Tests.Restore;

public sealed class LockFileWriterTests
{
    private static LockedAsset Lib(string file) => new(file, $"lib/net10.0/{file}");

    private static LockFile Sample() => new(LockFileWriter.CurrentVersion, "linux-x64",
    [
        new LockedPackage("Zeta", "2.0.0", new string('b', 128), new LockedAssets([Lib("z.dll")], [])),
        new LockedPackage("Alpha", "1.0.0", new string('a', 128), new LockedAssets(
            [Lib("a2.dll"), Lib("a1.dll")],
            [new LockedAsset("n.so", "runtimes/linux-x64/native/n.so")]))
    ]);

    [Fact]
    public void Write_is_byte_stable_and_sorted()
    {
        var dir = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"))).FullName;
        var p1 = Path.Combine(dir, "one.json");
        var p2 = Path.Combine(dir, "two.json");
        LockFileWriter.Write(Sample(), p1);
        LockFileWriter.Write(Sample(), p2);
        var bytes = File.ReadAllBytes(p1);
        Assert.Equal(bytes, File.ReadAllBytes(p2));
        var text = System.Text.Encoding.UTF8.GetString(bytes);
        Assert.True(text.IndexOf("Alpha", StringComparison.Ordinal) < text.IndexOf("Zeta", StringComparison.Ordinal));
        Assert.True(text.IndexOf("a1.dll", StringComparison.Ordinal) < text.IndexOf("a2.dll", StringComparison.Ordinal));
        Assert.EndsWith("\n", text);
        Assert.DoesNotContain("\r", text);
    }

    [Fact]
    public void Read_roundtrips_and_missing_is_null()
    {
        var dir = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"))).FullName;
        var path = Path.Combine(dir, "pz.lock.json");
        Assert.Null(LockFileWriter.Read(path));
        LockFileWriter.Write(Sample(), path);
        var read = LockFileWriter.Read(path)!;
        Assert.Equal(2, read.Packages.Count);
        Assert.Equal("Alpha", read.Packages[0].Id);
        Assert.Equal("runtimes/linux-x64/native/n.so", read.Packages[0].Assets.Native[0].ArchivePath);
    }

    /// <summary>A lock written before assets carried archive paths names no archive path for anything,
    /// so it cannot be upgraded in place — and its asset entries are bare strings where this schema
    /// expects objects, which would otherwise surface as unhelpful "malformed JSON". The version must be
    /// diagnosed as the version, so the reader is told to re-restore rather than to go looking for a
    /// syntax error.</summary>
    [Fact]
    public void Read_rejects_an_older_schema_version_naming_the_version()
    {
        var dir = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"))).FullName;
        var path = Path.Combine(dir, "pz.lock.json");
        File.WriteAllText(path, """
            {
              "version": 1,
              "rid": "linux-x64",
              "packages": [
                { "id": "Alpha", "version": "1.0.0", "sha512": "aa", "requested": true,
                  "assets": { "lib": ["a.dll"], "native": [] } }
              ]
            }
            """);

        var ex = Assert.Throws<RestoreException>(() => LockFileWriter.Read(path));

        Assert.Equal("PZ0321", ex.Code);
        Assert.Contains("schema version 1", ex.Message);
        Assert.Contains($"version {LockFileWriter.CurrentVersion}", ex.Message);
        Assert.Contains("pz restore", ex.Hint!);
    }

    /// <summary>A present-but-broken lock file must NOT silently
    /// surface as "no lock file" (that would incorrectly proceed as if never restored), mirroring
    /// <c>ManifestReader.TryRead</c>'s dto-null pattern. Two distinct inputs land in PZ0321 via two
    /// distinct code paths in <see cref="LockFileWriter.Read"/>: the literal JSON token `null`
    /// deserializes cleanly to a null <see cref="LockFile"/>, caught by the post-deserialize null
    /// check (message "empty or 'null' JSON document"); an empty file has zero JSON tokens, so
    /// <see cref="System.Text.Json.JsonSerializer"/> throws <see cref="System.Text.Json.JsonException"/>
    /// before deserialization completes, caught by the earlier catch arm (message wraps the
    /// JsonException's own text). Both must resolve to PZ0321, never to null.</summary>
    [Fact]
    public void Read_rejects_literal_null_content_with_PZ0321()
    {
        var dir = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"))).FullName;
        var path = Path.Combine(dir, "pz.lock.json");
        File.WriteAllText(path, "null");

        var ex = Assert.Throws<RestoreException>(() => LockFileWriter.Read(path));

        Assert.Equal("PZ0321", ex.Code);
        Assert.Contains("empty or 'null'", ex.Message);
    }

    /// <summary>See the doc comment on <see cref="Read_rejects_literal_null_content_with_PZ0321"/>: an
    /// empty file takes the OTHER path to PZ0321 — zero bytes have zero JSON tokens, so
    /// <see cref="System.Text.Json.JsonSerializer.Deserialize{TValue}(byte[], System.Text.Json.JsonSerializerOptions?)"/>
    /// throws <see cref="System.Text.Json.JsonException"/> directly, hitting the malformed-JSON catch
    /// arm rather than the post-deserialize null check — a different message, same PZ0321 code.</summary>
    [Fact]
    public void Read_returns_error_for_empty_file()
    {
        var dir = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), "pz-tests", Guid.NewGuid().ToString("N"))).FullName;
        var path = Path.Combine(dir, "pz.lock.json");
        File.WriteAllBytes(path, []);

        var ex = Assert.Throws<RestoreException>(() => LockFileWriter.Read(path));

        Assert.Equal("PZ0321", ex.Code);
        Assert.Contains("pz.lock.json is malformed", ex.Message);
        Assert.DoesNotContain("empty or 'null'", ex.Message);
    }
}
