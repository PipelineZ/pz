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
/// to parse as JSON (or does not parse to a JSON object) is refused outright
/// (<see cref="PzErrorCode.McpClientConfigInvalid"/>, PZ0605) -- the file is never touched, never
/// clobbered.</summary>
public static class ClientConfigWriter
{
    public static ClientConfigOutcome Apply(
        string filePath, string topLevelKey, string serverName, Action<JsonObject> writeEntry)
    {
        var root = ReadExisting(filePath);

        if (root[topLevelKey] is not JsonObject topLevel)
        {
            topLevel = new JsonObject();
            root[topLevelKey] = topLevel;
        }

        var replaced = topLevel.ContainsKey(serverName);
        var entry = new JsonObject();
        writeEntry(entry);
        topLevel[serverName] = entry;

        Write(filePath, root);
        return new ClientConfigOutcome(filePath, replaced);
    }

    private static JsonObject ReadExisting(string filePath)
    {
        if (!File.Exists(filePath))
        {
            return new JsonObject();
        }

        var text = File.ReadAllText(filePath);
        JsonNode? parsed;
        try
        {
            parsed = JsonNode.Parse(text);
        }
        catch (JsonException)
        {
            throw Invalid(filePath);
        }

        return parsed as JsonObject ?? throw Invalid(filePath);
    }

    private static PzConfigException Invalid(string filePath) => new(new PzError(
        PzErrorCode.McpClientConfigInvalid,
        $"'{filePath}' is not valid JSON -- refusing to overwrite it",
        filePath, null,
        "fix or remove the file by hand, then re-run pz mcp init"));

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
