using System.Text.Json;

namespace Pz.PackageManagement.Restore;

/// <summary>Byte-stable serialization for <see cref="LockFile"/>: explicit property
/// order, 2-space indent, LF line endings, a trailing newline byte, and packages/asset lists sorted
/// ordinal — so two restores of the same requirements produce an identical <c>pz.lock.json</c>.</summary>
public static class LockFileWriter
{
    private static readonly JsonSerializerOptions ReadOptions = new() { PropertyNameCaseInsensitive = true };

    public static void Write(LockFile lockFile, string path)
    {
        using var stream = File.Create(path);
        using var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true, IndentSize = 2, NewLine = "\n" });

        writer.WriteStartObject();
        writer.WriteNumber("version", lockFile.Version);
        writer.WriteString("rid", lockFile.Rid);
        writer.WriteStartArray("packages");

        foreach (var package in lockFile.Packages.OrderBy(p => p.Id, StringComparer.Ordinal))
        {
            writer.WriteStartObject();
            writer.WriteString("id", package.Id);
            writer.WriteString("version", package.Version);
            writer.WriteString("sha512", package.Sha512);
            writer.WriteBoolean("requested", package.Requested);
            writer.WriteStartObject("assets");
            WriteSortedStringArray(writer, "lib", package.Assets.Lib);
            WriteSortedStringArray(writer, "native", package.Assets.Native);
            writer.WriteEndObject();
            writer.WriteEndObject();
        }

        writer.WriteEndArray();
        writer.WriteEndObject();
        writer.Flush();
        stream.WriteByte((byte)'\n');
    }

    private static void WriteSortedStringArray(Utf8JsonWriter writer, string propertyName, IReadOnlyList<string> values)
    {
        writer.WriteStartArray(propertyName);
        foreach (var value in values.OrderBy(v => v, StringComparer.Ordinal))
        {
            writer.WriteStringValue(value);
        }

        writer.WriteEndArray();
    }

    /// <summary>Returns null when <paramref name="path"/> does not exist. Throws
    /// <see cref="RestoreException"/> with code PZ0321 when the file exists but is not valid JSON matching
    /// the lock schema.</summary>
    public static LockFile? Read(string path)
    {
        if (!File.Exists(path))
        {
            return null;
        }

        LockFile? lockFile;
        try
        {
            var bytes = File.ReadAllBytes(path);
            lockFile = JsonSerializer.Deserialize<LockFile>(bytes, ReadOptions);
        }
        catch (JsonException ex)
        {
            throw new RestoreException(
                "PZ0321",
                $"pz.lock.json is malformed: {ex.Message}",
                "run 'pz restore' to regenerate it");
        }

        if (lockFile is null)
        {
            throw new RestoreException(
                "PZ0321",
                "pz.lock.json is malformed: empty or 'null' JSON document",
                "run 'pz restore' to regenerate it");
        }

        return lockFile;
    }
}
