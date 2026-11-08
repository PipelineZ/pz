using System.IO.Compression;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Http;

/// <summary>One file to parse: a zip member, the gunzipped download, or the download itself.
/// <see cref="Name"/> is a label only (the member's full path or the URL's last segment) and is never
/// used as a file-system path. Disposing it disposes <see cref="Stream"/>.</summary>
internal sealed record FileSource(string Name, Stream Stream) : IDisposable
{
    public void Dispose() => Stream.Dispose();
}

/// <summary>Reads a downloaded file as zip, gzip or plain. Members are streamed straight out of the
/// archive and never written to disk, so an entry name (`../../x`, `/abs`) is only ever a label.</summary>
internal static class ArchiveReader
{
    private const int ListedEntries = 20;

    private enum Kind { Zip, Gzip, None }

    /// <summary>The zip entries <paramref name="config"/> selects, in ordinal full-name order (empty
    /// when nothing matches or the file is not a zip). A zip read without `members` is a permanent
    /// error.</summary>
    public static IReadOnlyList<string> MatchingMembers(string file, HttpFileConfig config)
    {
        if (KindOf(file, config) != Kind.Zip)
        {
            return [];
        }

        using var zip = OpenZip(file, label: null);
        return [.. Select(zip, config, label: null).Select(e => e.FullName)];
    }

    /// <summary>The files to parse, in order. Lazy: each <see cref="FileSource"/> is opened as it is
    /// enumerated, and the consumer disposes it before moving to the next (a zip reads its members
    /// from one underlying file). The archive is closed when enumeration completes or is abandoned.
    /// Zip and gzip streams are wrapped by <paramref name="budget"/>; a plain file is not
    /// decompressed and is already capped by `max_download_mb`.</summary>
    /// <param name="fallbackName">The name for a non-zip file (the URL path's last segment); a gzip
    /// drops a trailing `.gz`.</param>
    public static IEnumerable<FileSource> Open(string file, HttpFileConfig config, string fallbackName,
        DecompressionBudget budget, string label)
    {
        switch (KindOf(file, config))
        {
            case Kind.Gzip:
                var name = fallbackName.EndsWith(".gz", StringComparison.OrdinalIgnoreCase)
                    ? fallbackName[..^3]
                    : fallbackName;
                yield return new FileSource(name,
                    budget.Wrap(new GZipStream(File.OpenRead(file), CompressionMode.Decompress)));
                yield break;
            case Kind.None:
                yield return new FileSource(fallbackName, File.OpenRead(file));
                yield break;
        }

        var zip = OpenZip(file, label);
        try
        {
            var members = Select(zip, config, label);
            if (members.Count == 0)
            {
                if (config.IfMissingEmpty)
                {
                    yield break;
                }

                throw Permanent(label, $"no zip entry matches file.members '{config.Members}' " +
                    $"(entries: {Listing(Candidates(zip))})");
            }

            if (members.FirstOrDefault(e => e.IsEncrypted) is { } encrypted)
            {
                throw Permanent(label, $"encrypted zip entries are not supported ('{encrypted.FullName}')");
            }

            foreach (var entry in members)
            {
                yield return new FileSource(entry.FullName, budget.Wrap(OpenEntry(entry, label)));
            }
        }
        finally
        {
            zip.Dispose();
        }
    }

    private static Kind KindOf(string file, HttpFileConfig config)
    {
        switch (config.Archive)
        {
            case HttpArchive.Zip: return Kind.Zip;
            case HttpArchive.Gzip: return Kind.Gzip;
            case HttpArchive.None: return Kind.None;
        }

        Span<byte> head = stackalloc byte[4];
        using var stream = File.OpenRead(file);
        var n = stream.ReadAtLeast(head, head.Length, throwOnEndOfStream: false);
        if (n == 4 && head is [0x50, 0x4B, 0x03, 0x04])
        {
            return Kind.Zip;
        }

        return n >= 2 && head[0] == 0x1F && head[1] == 0x8B ? Kind.Gzip : Kind.None;
    }

    private static ZipArchive OpenZip(string file, string? label)
    {
        var stream = File.OpenRead(file);
        try
        {
            return new ZipArchive(stream, ZipArchiveMode.Read, leaveOpen: false);
        }
        catch (InvalidDataException e)
        {
            stream.Dispose();
            throw Permanent(label, $"the download is not a readable zip archive ({e.Message})");
        }
    }

    private static Stream OpenEntry(ZipArchiveEntry entry, string label)
    {
        try
        {
            return entry.Open();
        }
        catch (InvalidDataException e)
        {
            throw Permanent(label, $"zip entry '{entry.FullName}' cannot be read ({e.Message})");
        }
    }

    /// <summary>Entries that can hold data: no directory entries and nothing under macOS's
    /// `__MACOSX/` resource-fork folder, ordered by full name.</summary>
    private static List<ZipArchiveEntry> Candidates(ZipArchive zip)
        => [.. zip.Entries
            .Where(e => e.Name.Length > 0 && !e.FullName.StartsWith("__MACOSX/", StringComparison.Ordinal))
            .OrderBy(e => e.FullName, StringComparer.Ordinal)];

    private static List<ZipArchiveEntry> Select(ZipArchive zip, HttpFileConfig config, string? label)
    {
        var candidates = Candidates(zip);
        if (config.Members is not { } glob)
        {
            throw Permanent(label, "the download is a zip archive; set file.members to choose its entries " +
                $"(entries: {Listing(candidates)})");
        }

        return [.. candidates.Where(e => MemberGlob.IsMatch(glob, e.FullName))];
    }

    private static string Listing(List<ZipArchiveEntry> entries)
    {
        if (entries.Count == 0)
        {
            return "none";
        }

        var listed = string.Join(", ", entries.Take(ListedEntries).Select(e => e.FullName));
        return entries.Count > ListedEntries ? $"{listed}, and {entries.Count - ListedEntries} more" : listed;
    }

    private static PzConnectorException Permanent(string? label, string message)
        => new(label is null ? message : $"{label}: {message}", isTransient: false);
}
