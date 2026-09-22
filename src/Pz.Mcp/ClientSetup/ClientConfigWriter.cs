using System.Text.Json;
using System.Text.Json.Nodes;
using Pz.Core.Validation;

namespace Pz.Mcp.ClientSetup;

/// <summary>Result of one <see cref="ClientConfigWriter.Apply"/> call: the file written, and whether it
/// already had a `pz` server entry (updated in place) or gained a new one.</summary>
public sealed record ClientConfigOutcome(string File, bool Replaced);

/// <summary>Merge-preserving JSON read-modify-write for one MCP client config file: parses
/// the existing file (a missing file starts from an empty object), preserves every sibling top-level key
/// and every sibling entry under <paramref name="topLevelKey"/> untouched, sets exactly
/// `&lt;topLevelKey&gt;.&lt;serverName&gt;` to a freshly built entry object, and writes back
/// deterministically -- 2-space indented, LF line endings, one trailing newline, atomic temp-file +
/// rename so a crash mid-write can never leave a half-written file in place. An existing file that fails
/// to parse even tolerantly (see below) is refused outright
/// (<see cref="PzErrorCode.McpClientConfigInvalid"/>, PZ0605) -- the file is never touched, never
/// clobbered.
///
/// <c>.vscode/mcp.json</c> and friends are editor-authored and legally carry `//`/`/* */` comments and
/// trailing commas (JSONC), which <see cref="JsonNode"/> cannot round-trip: parsing one, merging in the
/// entry, and serializing back would silently delete every comment. So a file is read TWICE -- strict
/// first (the common case, and the only case that ever reaches <see cref="Write"/>), then, only if that
/// fails, again tolerant of comments/trailing commas purely to tell the two failure modes apart. A file
/// that parses only tolerantly is refused the same as a plain rewrite
/// (<see cref="PzErrorCode.McpClientConfigHasComments"/>, PZ0611) but with a next step that pastes in the
/// exact entry to add by hand, since pz already built it and the comments are real content worth
/// keeping. A file that fails even the tolerant parse is genuinely broken JSON and keeps the existing
/// PZ0605 refusal.</summary>
public static class ClientConfigWriter
{
    public static ClientConfigOutcome Apply(
        string filePath, string topLevelKey, string serverName, Action<JsonObject> writeEntry)
    {
        var root = ReadExisting(filePath, out var hasComments);

        var entry = new JsonObject();
        writeEntry(entry);

        if (hasComments)
        {
            throw HasComments(filePath, topLevelKey, serverName, entry);
        }

        if (root[topLevelKey] is not JsonObject topLevel)
        {
            topLevel = new JsonObject();
            root[topLevelKey] = topLevel;
        }

        var replaced = topLevel.ContainsKey(serverName);
        topLevel[serverName] = entry;

        Write(filePath, root);
        return new ClientConfigOutcome(filePath, replaced);
    }

    private static JsonObject ReadExisting(string filePath, out bool hasComments)
    {
        hasComments = false;
        if (!File.Exists(filePath))
        {
            return new JsonObject();
        }

        var text = File.ReadAllText(filePath);
        try
        {
            return JsonNode.Parse(text) as JsonObject ?? throw Invalid(filePath);
        }
        catch (JsonException)
        {
            // Not strict JSON -- try again tolerant of comments/trailing commas before concluding the
            // file is simply broken.
        }

        JsonNode? tolerant;
        try
        {
            tolerant = JsonNode.Parse(text, documentOptions: new JsonDocumentOptions
            {
                CommentHandling = JsonCommentHandling.Skip,
                AllowTrailingCommas = true,
            });
        }
        catch (JsonException)
        {
            throw Invalid(filePath);
        }

        if (tolerant is not JsonObject obj)
        {
            throw Invalid(filePath);
        }

        hasComments = true;
        return obj;
    }

    private static PzConfigException Invalid(string filePath) => new(new PzError(
        PzErrorCode.McpClientConfigInvalid,
        $"'{filePath}' is not valid JSON -- refusing to overwrite it",
        filePath, null,
        "fix or remove the file by hand, then re-run pz mcp init"));

    private static PzConfigException HasComments(
        string filePath, string topLevelKey, string serverName, JsonObject entry)
    {
        // entry is freshly built and not yet parented anywhere (Apply throws this before ever
        // assigning it under topLevel), so it is safe to parent here without cloning.
        var snippet = new JsonObject { [serverName] = entry }
            .ToJsonString(new JsonSerializerOptions { WriteIndented = true })
            .Replace("\r\n", "\n");
        return new(new PzError(
            PzErrorCode.McpClientConfigHasComments,
            $"'{filePath}' contains comments or trailing commas (JSONC) that pz cannot preserve while " +
            $"merging in the \"{serverName}\" entry -- refusing to rewrite it automatically.",
            filePath, null,
            $"add this entry under \"{topLevelKey}\" in '{filePath}' by hand:\n{snippet}"));
    }

    private static void Write(string filePath, JsonObject root)
    {
        var fullPath = Path.GetFullPath(filePath);
        var dir = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrEmpty(dir))
        {
            Directory.CreateDirectory(dir);
        }

        var json = root.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
        var text = json.Replace("\r\n", "\n") + "\n";

        var tmp = fullPath + ".tmp-" + Guid.NewGuid().ToString("N");
        File.WriteAllText(tmp, text);
        File.Move(tmp, fullPath, overwrite: true);
    }
}
