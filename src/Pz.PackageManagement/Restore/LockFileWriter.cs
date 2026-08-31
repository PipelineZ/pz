using System.Text.Json;

namespace Pz.PackageManagement.Restore;

/// <summary>Byte-stable serialization for <see cref="LockFile"/>: explicit property
/// order, 2-space indent, LF line endings, a trailing newline byte, and packages/asset lists sorted
/// ordinal — so two restores of the same requirements produce an identical <c>pz.lock.json</c>.</summary>
public static class LockFileWriter
{
    /// <summary>Schema version this build writes and is the only one it reads. Bumped to 2 when assets
    /// grew from bare file names to <c>{ file, archivePath }</c> pairs (see <see cref="LockedAsset"/>);
    /// a version-1 lock names no archive paths at all, so it cannot be upgraded in place and is
    /// rejected in favour of a regenerating <c>pz restore</c>.</summary>
    public const int CurrentVersion = 2;

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
            WriteSortedAssetArray(writer, "lib", package.Assets.Lib);
            WriteSortedAssetArray(writer, "native", package.Assets.Native);
            writer.WriteEndObject();
            writer.WriteEndObject();
        }

        writer.WriteEndArray();
        writer.WriteEndObject();
        writer.Flush();
        stream.WriteByte((byte)'\n');
    }

    private static void WriteSortedAssetArray(Utf8JsonWriter writer, string propertyName, IReadOnlyList<LockedAsset> assets)
    {
        writer.WriteStartArray(propertyName);
        foreach (var asset in assets.OrderBy(a => a.File, StringComparer.Ordinal).ThenBy(a => a.ArchivePath, StringComparer.Ordinal))
        {
            writer.WriteStartObject();
            writer.WriteString("file", asset.File);
            writer.WriteString("archivePath", asset.ArchivePath);
            writer.WriteEndObject();
        }

        writer.WriteEndArray();
    }

    /// <summary>Returns null when <paramref name="path"/> does not exist. Throws
    /// <see cref="RestoreException"/> with code PZ0321 when the file exists but is not valid JSON matching
    /// the lock schema, or declares a schema version this build does not write.</summary>
    public static LockFile? Read(string path)
    {
        if (!File.Exists(path))
        {
            return null;
        }

        var bytes = File.ReadAllBytes(path);

        // The version is read on its own pass FIRST: an older lock's asset entries are bare strings
        // where this schema expects objects, so a single-pass deserialize would fail as "malformed"
        // and hide the one thing the reader actually needs to be told.
        VersionProbe? probe;
        try
        {
            probe = JsonSerializer.Deserialize(bytes, PackageManagementJsonContext.Default.VersionProbe);
        }
        catch (JsonException ex)
        {
            throw Malformed(ex.Message);
        }

        if (probe is null)
        {
            throw new RestoreException(
                "PZ0321",
                "pz.lock.json is malformed: empty or 'null' JSON document",
                "run 'pz restore' to regenerate it");
        }

        if (probe.Version != CurrentVersion)
        {
            throw new RestoreException(
                "PZ0321",
                $"pz.lock.json declares schema version {probe.Version}, but this pz writes version " +
                $"{CurrentVersion}; the lock cannot be upgraded in place because an older lock records " +
                "no per-asset archive paths",
                "run 'pz restore' to regenerate it");
        }

        try
        {
            return JsonSerializer.Deserialize(bytes, PackageManagementJsonContext.Default.LockFile)!;
        }
        catch (JsonException ex)
        {
            throw Malformed(ex.Message);
        }
    }

    private static RestoreException Malformed(string detail) => new(
        "PZ0321",
        $"pz.lock.json is malformed: {detail}",
        "run 'pz restore' to regenerate it");

    /// <summary>Reads nothing but <c>version</c>, so a lock written by a different schema version is
    /// diagnosed as such rather than as malformed JSON.</summary>
    internal sealed record VersionProbe(int Version);
}
