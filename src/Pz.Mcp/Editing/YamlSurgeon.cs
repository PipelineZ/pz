using Pz.Core.Validation;
using YamlDotNet.RepresentationModel;

namespace Pz.Mcp.Editing;

/// <summary>Surgical, comment-preserving edits on a YAML mapping file. Locates a block by key
/// path using YamlDotNet's node marks (<see cref="YamlStream"/>/<see cref="YamlMappingNode"/>), then
/// splices LINES of the raw file text — every byte outside the spliced range is untouched, so user
/// comments/ordering/quoting/blank lines survive exactly. Inserted text is expected to come from
/// <see cref="CanonicalYaml"/> (2-space indent, LF, always ending with a trailing newline). Writes are
/// atomic: a temp file in the same directory, then <see cref="File.Replace(string, string, string?)"/>
/// (or <see cref="File.Move(string, string, bool)"/> when the destination does not yet exist).</summary>
public static class YamlSurgeon
{
    /// <summary>Insert <paramref name="canonicalBlock"/> (already indented for depth
    /// <c>path.Length</c>) as a new entry named <paramref name="key"/> of the mapping at
    /// <paramref name="path"/> (e.g. <c>["connections"]</c>), placed after that mapping's last existing
    /// entry. Creates every mapping named along <paramref name="path"/> that does not yet exist —
    /// including the file itself — and any intermediate mapping whose key exists but currently holds no
    /// mapping value (e.g. a bare <c>connections:</c> with nothing under it). Throws
    /// <see cref="PzConfigException"/> with code <see cref="PzErrorCode.McpMutationTarget"/> (PZ0602)
    /// if <paramref name="key"/> already exists at <paramref name="path"/>.</summary>
    public static void InsertMappingEntry(string filePath, string[] path, string key, string canonicalBlock)
    {
        var text = File.Exists(filePath) ? File.ReadAllText(filePath) : string.Empty;
        var index = new LineIndex(text);
        var root = ParseRootMapping(text);

        int anchorLine;
        int wrapperStartDepth;
        var ancestorLinks = new Dictionary<YamlMappingNode, (YamlMappingNode Parent, YamlNode Key)>();

        if (root is null)
        {
            // Nothing structured exists yet (missing file, empty file, or comment-only content):
            // synthesize the whole path as wrapper headers, appended after whatever text is already
            // there (0 for a genuinely empty/missing file).
            anchorLine = index.TotalContentLines;
            wrapperStartDepth = 0;
        }
        else
        {
            var current = root;
            var depth = 0;
            YamlMappingNode? stoppedAtNonMappingParent = null;
            YamlNode? stoppedAtNonMappingKey = null;

            while (depth < path.Length)
            {
                var childKey = FindKey(current, path[depth]);
                if (childKey is null)
                {
                    break; // this segment, and everything after it, must be created
                }

                if (current.Children[childKey] is YamlMappingNode childMapping)
                {
                    ancestorLinks[childMapping] = (current, childKey);
                    current = childMapping;
                    depth++;
                    continue;
                }

                // The key exists but holds a scalar/sequence (e.g. bare "connections:" with no value) —
                // treat it as an empty mapping: insert right after this key's own line.
                stoppedAtNonMappingParent = current;
                stoppedAtNonMappingKey = childKey;
                break;
            }

            if (depth == path.Length)
            {
                if (FindKey(current, key) is not null)
                {
                    var existingLine = (int)FindKey(current, key)!.Start.Line;
                    throw MutationTargetError(filePath, existingLine,
                        $"Cannot insert '{key}': it already exists at {string.Join('.', path)}.",
                        "use ReplaceMappingEntry instead of InsertMappingEntry");
                }

                anchorLine = AppendAnchor(current, ancestorLinks, index);
                wrapperStartDepth = path.Length;
            }
            else if (stoppedAtNonMappingKey is not null)
            {
                anchorLine = EndLineOfEntry(stoppedAtNonMappingParent!, stoppedAtNonMappingKey, ancestorLinks, index);
                wrapperStartDepth = depth + 1;
            }
            else
            {
                anchorLine = AppendAnchor(current, ancestorLinks, index);
                wrapperStartDepth = depth;
            }
        }

        var insertText = BuildWrapperText(path, wrapperStartDepth) + canonicalBlock;
        var spliceStart = index.EndOffsetInclusive(anchorLine);
        if (spliceStart == text.Length && text.Length > 0 && text[^1] != '\n')
        {
            insertText = "\n" + insertText;
        }

        var newText = text[..spliceStart] + insertText + text[spliceStart..];
        AtomicWrite(filePath, newText);
    }

    /// <summary>Replace the whole block of <paramref name="path"/>+<paramref name="key"/> (its key line
    /// through the last line of its nested content) with <paramref name="canonicalBlock"/>. Returns
    /// <see langword="true"/> when the replaced range contained a <c>#</c> character on any line — a
    /// simple <c>line.Contains('#')</c> heuristic (it does not distinguish a real comment from a `#`
    /// inside a quoted scalar), so it can false-positive; callers should treat a <see langword="true"/>
    /// result as "note that a comment may have been dropped", not a guarantee. Throws
    /// <see cref="PzConfigException"/> (PZ0602) if the key does not exist at <paramref name="path"/>.</summary>
    public static bool ReplaceMappingEntry(string filePath, string[] path, string key, string canonicalBlock)
    {
        var (text, index, mapping, keyNode, ancestorLinks) = ResolveExistingTarget(filePath, path, key);

        var startLine = (int)keyNode.Start.Line;
        var endLine = EndLineOfEntry(mapping, keyNode, ancestorLinks, index);

        var hadComment = false;
        for (var line = startLine; line <= endLine; line++)
        {
            if (index.GetLineText(line).Contains('#'))
            {
                hadComment = true;
                break;
            }
        }

        var spliceStart = index.StartOffset(startLine);
        var spliceEnd = index.EndOffsetInclusive(endLine);
        var newText = text[..spliceStart] + canonicalBlock + text[spliceEnd..];
        AtomicWrite(filePath, newText);
        return hadComment;
    }

    /// <summary>Delete the block of <paramref name="path"/>+<paramref name="key"/>, including its own
    /// trailing newline; every line outside the block is untouched. Throws
    /// <see cref="PzConfigException"/> (PZ0602) if the key does not exist at <paramref name="path"/>
    /// (including when the file itself is missing).</summary>
    public static void RemoveMappingEntry(string filePath, string[] path, string key)
    {
        var (text, index, mapping, keyNode, ancestorLinks) = ResolveExistingTarget(filePath, path, key);

        var startLine = (int)keyNode.Start.Line;
        var endLine = EndLineOfEntry(mapping, keyNode, ancestorLinks, index);

        var spliceStart = index.StartOffset(startLine);
        var spliceEnd = index.EndOffsetInclusive(endLine);
        var newText = text[..spliceStart] + text[spliceEnd..];
        AtomicWrite(filePath, newText);
    }

    // ------------------------------------------------------------------------------------------------
    // Shared resolution for Replace/Remove: the whole path + key must already exist.
    // ------------------------------------------------------------------------------------------------

    private static (string Text, LineIndex Index, YamlMappingNode Mapping, YamlNode KeyNode,
        Dictionary<YamlMappingNode, (YamlMappingNode Parent, YamlNode Key)> AncestorLinks)
        ResolveExistingTarget(string filePath, string[] path, string key)
    {
        var text = File.Exists(filePath) ? File.ReadAllText(filePath) : string.Empty;
        var index = new LineIndex(text);
        var root = ParseRootMapping(text);
        var ancestorLinks = new Dictionary<YamlMappingNode, (YamlMappingNode Parent, YamlNode Key)>();

        var current = root;
        foreach (var segment in path)
        {
            var childKey = current is null ? null : FindKey(current, segment);
            if (current is null || childKey is null || current.Children[childKey] is not YamlMappingNode childMapping)
            {
                throw MutationTargetError(filePath, null,
                    $"Cannot find '{string.Join('.', path)}' in {filePath}: no such mapping.",
                    "check the path, or use InsertMappingEntry to create it");
            }

            ancestorLinks[childMapping] = (current, childKey);
            current = childMapping;
        }

        var keyNode = current is null ? null : FindKey(current, key);
        if (current is null || keyNode is null)
        {
            throw MutationTargetError(filePath, null,
                $"Cannot find '{key}' at {string.Join('.', path)} in {filePath}: nothing to modify.",
                "check the key name, or use InsertMappingEntry to add it");
        }

        return (text, index, current, keyNode, ancestorLinks);
    }

    private static PzConfigException MutationTargetError(string filePath, int? line, string message, string hint) =>
        new(new PzError(PzErrorCode.McpMutationTarget, message, filePath, line, hint));

    // ------------------------------------------------------------------------------------------------
    // Wrapper-header synthesis for path segments that don't exist yet.
    // ------------------------------------------------------------------------------------------------

    private static string BuildWrapperText(string[] path, int startDepth)
    {
        if (startDepth >= path.Length)
        {
            return string.Empty;
        }

        var sb = new System.Text.StringBuilder();
        for (var i = startDepth; i < path.Length; i++)
        {
            sb.Append(new string(' ', i * 2)).Append(path[i]).Append(":\n");
        }

        return sb.ToString();
    }

    // ------------------------------------------------------------------------------------------------
    // Node lookup + extent computation.
    // ------------------------------------------------------------------------------------------------

    private static YamlMappingNode? ParseRootMapping(string text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return null;
        }

        var stream = new YamlStream();
        using var reader = new StringReader(text);
        stream.Load(reader);
        if (stream.Documents.Count == 0)
        {
            return null;
        }

        return stream.Documents[0].RootNode as YamlMappingNode;
    }

    private static YamlNode? FindKey(YamlMappingNode mapping, string name)
    {
        foreach (var candidate in mapping.Children.Keys)
        {
            if (candidate is YamlScalarNode scalar && scalar.Value == name)
            {
                return candidate;
            }
        }

        return null;
    }

    /// <summary>Where to append a NEW last child of <paramref name="mapping"/>: after its current last
    /// child's whole block, or — if <paramref name="mapping"/> has no children yet — right after the
    /// key line that points at it (recursing further up if that key is itself a mapping's last child),
    /// bottoming out at end-of-file when <paramref name="mapping"/> is the empty document root.</summary>
    private static int AppendAnchor(
        YamlMappingNode mapping,
        Dictionary<YamlMappingNode, (YamlMappingNode Parent, YamlNode Key)> ancestorLinks,
        LineIndex index)
    {
        if (mapping.Children.Count > 0)
        {
            var lastKey = mapping.Children.Keys.Last();
            return EndLineOfEntry(mapping, lastKey, ancestorLinks, index);
        }

        if (ancestorLinks.TryGetValue(mapping, out var link))
        {
            return EndLineOfEntry(link.Parent, link.Key, ancestorLinks, index);
        }

        return index.TotalContentLines;
    }

    /// <summary>The last physical line (1-based, inclusive) belonging to <paramref name="keyNode"/>'s
    /// entry inside <paramref name="parent"/>. YamlDotNet's own <c>End</c> mark on a block-mapping value
    /// under-reports trailing lines, so this is computed instead as: the line before the next sibling
    /// key's <c>Start.Line</c> (recursing to the enclosing mapping's own extent, the same way, when
    /// <paramref name="keyNode"/> is the last entry — bottoming out at end-of-file for the document
    /// root); then trimmed backward to the last non-blank line whose indentation is strictly deeper
    /// than <paramref name="keyNode"/>'s own indentation, so trailing blank lines and same-or-shallower
    /// comments/siblings (which are not this entry's nested content) are excluded.</summary>
    private static int EndLineOfEntry(
        YamlMappingNode parent,
        YamlNode keyNode,
        Dictionary<YamlMappingNode, (YamlMappingNode Parent, YamlNode Key)> ancestorLinks,
        LineIndex index)
    {
        var siblings = parent.Children.Keys.ToList();
        var position = siblings.FindIndex(k => ReferenceEquals(k, keyNode));

        int rawEnd;
        if (position >= 0 && position < siblings.Count - 1)
        {
            rawEnd = (int)siblings[position + 1].Start.Line - 1;
        }
        else if (ancestorLinks.TryGetValue(parent, out var link))
        {
            rawEnd = EndLineOfEntry(link.Parent, link.Key, ancestorLinks, index);
        }
        else
        {
            rawEnd = index.TotalContentLines;
        }

        var keyIndent = (int)keyNode.Start.Column - 1;
        var line = rawEnd;
        while (line >= (int)keyNode.Start.Line)
        {
            var content = index.GetLineText(line);
            var isBlank = string.IsNullOrWhiteSpace(content);
            if (!isBlank)
            {
                var indent = content.Length - content.TrimStart(' ').Length;
                if (indent > keyIndent)
                {
                    break;
                }
            }

            line--;
        }

        return Math.Max(line, (int)keyNode.Start.Line);
    }

    // ------------------------------------------------------------------------------------------------
    // Line offsets over the raw text (1-based line numbers, matching YamlDotNet marks).
    // ------------------------------------------------------------------------------------------------

    private sealed class LineIndex
    {
        private readonly string _text;
        private readonly List<int> _lineStarts = [0];

        public LineIndex(string text)
        {
            _text = text;
            for (var i = 0; i < text.Length; i++)
            {
                if (text[i] == '\n')
                {
                    _lineStarts.Add(i + 1);
                }
            }
        }

        /// <summary>Number of "real" content lines — the trailing phantom empty line implied by a final
        /// newline does not count.</summary>
        public int TotalContentLines =>
            _text.Length == 0 ? 0 : _text[^1] == '\n' ? _lineStarts.Count - 1 : _lineStarts.Count;

        public int StartOffset(int oneBasedLine) => _lineStarts[oneBasedLine - 1];

        /// <summary>Offset right after <paramref name="oneBasedLine"/>'s content, including its own
        /// trailing newline character when present — i.e. the splice point for "insert/replace/remove
        /// through the end of this line".</summary>
        public int EndOffsetInclusive(int oneBasedLine)
        {
            if (oneBasedLine <= 0)
            {
                return 0;
            }

            return oneBasedLine < _lineStarts.Count ? _lineStarts[oneBasedLine] : _text.Length;
        }

        public string GetLineText(int oneBasedLine)
        {
            var start = _lineStarts[oneBasedLine - 1];
            var endExclusive = oneBasedLine < _lineStarts.Count ? _lineStarts[oneBasedLine] - 1 : _text.Length;
            if (endExclusive < start)
            {
                endExclusive = start;
            }

            return _text.Substring(start, endExclusive - start);
        }
    }

    // ------------------------------------------------------------------------------------------------
    // Atomic write.
    // ------------------------------------------------------------------------------------------------

    private static void AtomicWrite(string filePath, string content)
    {
        var fullPath = Path.GetFullPath(filePath);
        var dir = Path.GetDirectoryName(fullPath)!;
        Directory.CreateDirectory(dir);

        var tempPath = Path.Combine(dir, $".{Path.GetFileName(fullPath)}.{Guid.NewGuid():N}.tmp");
        File.WriteAllText(tempPath, content);

        try
        {
            if (File.Exists(fullPath))
            {
                File.Replace(tempPath, fullPath, null);
            }
            else
            {
                File.Move(tempPath, fullPath, overwrite: true);
            }
        }
        catch
        {
            if (File.Exists(tempPath))
            {
                File.Delete(tempPath);
            }

            throw;
        }
    }
}
