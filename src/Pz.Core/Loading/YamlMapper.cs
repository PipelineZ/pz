using System.Globalization;
using Pz.Core.Validation;
using YamlDotNet.Core;
using YamlDotNet.RepresentationModel;

namespace Pz.Core.Loading;

/// <summary>Called for every scalar as it is converted, text and whether it was written PLAIN (not
/// quoted, not a block style) together with the key path leading to it (the connection name and
/// option key for connections.yml, the top-level project.yml key for project.yml, ...). Returns the
/// text to use instead -- typically an env-var substitution, or the text unchanged. Text the
/// interpolator changed is typed only when <c>isPlain</c> is true AND the typed value writes back as
/// exactly that text: a quoted <c>"${PORT}"</c> is substituted but stays a string, a bare
/// <c>${PORT}</c> with <c>PORT=5432</c> becomes the integer 5432, and a bare <c>${PIN}</c> with
/// <c>PIN=0123</c> stays the string "0123" -- substituted text is a secret as often as a port, and
/// nobody reviewing the YAML ever saw it.</summary>
public delegate string YamlScalarInterpolator(string text, bool isPlain, IReadOnlyList<string> path);

/// <summary>
/// Loads a YAML file into a dynamic object tree (<see cref="Dictionary{TKey,TValue}"/> /
/// <see cref="List{T}"/> / scalars) rather than deserializing into fixed classes.
/// </summary>
public static class YamlMapper
{
    /// <summary>
    /// Loads <paramref name="path"/> as YAML. <paramref name="relativePath"/> is the
    /// project-relative path used when reporting a syntax error.
    /// </summary>
    /// <exception cref="PzConfigException">
    /// Thrown with a <see cref="PzErrorCode.YamlShape"/> error when the file contains malformed YAML.
    /// Callers are expected to catch this at the file-load boundary and aggregate the error rather
    /// than letting it abort loading of the rest of the project.
    /// </exception>
    public static Dictionary<string, object?> LoadFile(string path, string relativePath) =>
        LoadFile(path, relativePath, interpolate: null);

    /// <summary>Loads <paramref name="path"/> the same way <see cref="LoadFile(string,string)"/> does,
    /// but runs <paramref name="interpolate"/> over every scalar's raw text before the plain/quoted
    /// typing decision -- see <see cref="YamlScalarInterpolator"/>. Used by loaders that substitute
    /// <c>${VAR}</c> references and need a whole-value reference retyped by its substituted shape.</summary>
    public static Dictionary<string, object?> LoadFile(string path, string relativePath,
        YamlScalarInterpolator? interpolate)
    {
        using var reader = new StreamReader(path);
        var yamlStream = new YamlStream();

        try
        {
            yamlStream.Load(reader);
        }
        catch (YamlException ex)
        {
            var line = ex.Start.Line > 0 ? (int?)ex.Start.Line : null;
            throw new PzConfigException(new PzError(
                PzErrorCode.YamlShape,
                $"Malformed YAML: {ex.Message}",
                relativePath,
                line,
                "fix the YAML syntax near this location"));
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            // YamlDotNet's scanner throws non-YamlException types on some malformed inputs (e.g.
            // InvalidOperationException on a multiline plain scalar inside an unclosed flow sequence).
            throw new PzConfigException(new PzError(
                PzErrorCode.YamlShape,
                $"Malformed YAML: {ex.Message}",
                relativePath,
                null,
                "fix the YAML syntax in this file"));
        }

        if (yamlStream.Documents.Count > 1)
        {
            throw new PzConfigException(new PzError(
                PzErrorCode.YamlShape,
                $"Malformed YAML: this file has {yamlStream.Documents.Count} YAML documents " +
                "(separated by '---'); pz reads only one document per file.",
                relativePath,
                null,
                "remove the extra document(s), or split them into separate files"));
        }

        if (yamlStream.Documents.Count == 0)
        {
            // A genuinely empty file, not a shape problem: "no content" reads the same as "no keys".
            return new Dictionary<string, object?>();
        }

        // A document that holds only `---`, comments or an explicit null says as little as an empty
        // file does -- a connections.yml with everything commented out is this, and is not a mistake.
        if (yamlStream.Documents[0].RootNode is YamlScalarNode
            { Style: ScalarStyle.Plain or ScalarStyle.Any, Value: null or "" or "~" or "null" })
        {
            return new Dictionary<string, object?>();
        }

        var state = new ConversionState(relativePath, interpolate);
        var converted = Convert(yamlStream.Documents[0].RootNode, state);
        if (converted is not Dictionary<string, object?> dict)
        {
            var rootLine = yamlStream.Documents[0].RootNode.Start.Line;
            var line = rootLine > 0 ? (int?)rootLine : null;
            throw new PzConfigException(new PzError(
                PzErrorCode.YamlShape,
                $"Malformed YAML: the document's root must be a mapping of keys to values " +
                $"(got {DescribeRootShape(converted)}).",
                relativePath,
                line,
                "start the file with `key: value` pairs, not a list or a bare scalar"));
        }

        return dict;
    }

    private static string DescribeRootShape(object? converted) => converted switch
    {
        List<object?> => "a list",
        _ => "a scalar",
    };

    /// <summary>An alias makes the parsed node graph shared — and, for a self-referencing anchor,
    /// cyclic — so the graph-to-tree conversion below needs two guards: the current recursion path
    /// (reference identity — YamlNode's own Equals is deep and would itself recurse on a cycle),
    /// and a total-values budget, since each alias occurrence expands to a fresh subtree.
    /// <see cref="PathSegments"/> is the key path to the scalar currently being converted, mutated as a
    /// stack by <see cref="ConvertMapping"/> -- it exists purely to hand <see cref="Interpolate"/> the
    /// context it needs to decide whether a given scalar is even eligible for substitution (e.g.
    /// connections.yml's <c>entities:</c> subtree is deliberately not).</summary>
    private sealed class ConversionState(string relativePath, YamlScalarInterpolator? interpolate)
    {
        public const int MaxValues = 1_000_000;

        public readonly HashSet<YamlNode> Path = new(ReferenceEqualityComparer.Instance);
        public readonly string RelativePath = relativePath;
        public readonly YamlScalarInterpolator? Interpolate = interpolate;
        public readonly List<string> PathSegments = [];
        public int Values;
    }

    private static object? Convert(YamlNode node, ConversionState state)
    {
        if (++state.Values > ConversionState.MaxValues)
        {
            throw new PzConfigException(new PzError(
                PzErrorCode.YamlShape,
                $"Malformed YAML: anchor/alias expansion produces more than {ConversionState.MaxValues:N0} values",
                state.RelativePath,
                null,
                "inline the repeated data instead of multiplying it through aliases"));
        }

        switch (node)
        {
            case YamlScalarNode scalar:
                // Only a PLAIN scalar is typed. Quotes and block styles are how YAML says "a string":
                // re-typing them turns a password "0123456" into 123456 and a connector version
                // "1.10" into 1.1 — a different package — and undoes the quoting the authoring tools
                // add around number-like strings precisely so that they survive. Interpolation runs
                // BEFORE that decision, on the raw text, so a substituted plain scalar is typed by its
                // OWN resulting shape exactly as an authored one would be, while a substituted quoted
                // scalar still becomes the substituted text but is never handed to ConvertScalar.
                var isPlain = scalar.Style is ScalarStyle.Plain or ScalarStyle.Any;
                var text = state.Interpolate is null || scalar.Value is null
                    ? scalar.Value
                    : state.Interpolate(scalar.Value, isPlain, state.PathSegments);
                if (!isPlain)
                {
                    return text;
                }

                // Text the author typed is typed as YAML says. Text that arrived by substitution is
                // somebody's port as often as somebody's password, and the author never saw it: it is
                // typed only when that loses nothing.
                return text == scalar.Value ? ConvertScalar(text) : ConvertLossless(text);
            case YamlMappingNode or YamlSequenceNode:
                if (!state.Path.Add(node))
                {
                    var line = node.Start.Line > 0 ? (int?)node.Start.Line : null;
                    throw new PzConfigException(new PzError(
                        PzErrorCode.YamlShape,
                        "Malformed YAML: an alias refers to a node inside its own anchor, forming a cycle",
                        state.RelativePath,
                        line,
                        "remove the self-referencing alias"));
                }

                object? converted = node is YamlMappingNode mapping
                    ? ConvertMapping(mapping, state)
                    : ((YamlSequenceNode)node).Children.Select(c => Convert(c, state)).ToList();
                state.Path.Remove(node);
                return converted;
            default:
                return null;
        }
    }

    private static Dictionary<string, object?> ConvertMapping(YamlMappingNode mapping, ConversionState state)
    {
        var dict = new Dictionary<string, object?>();
        foreach (var (key, value) in mapping.Children)
        {
            var keyText = key is YamlScalarNode keyScalar ? keyScalar.Value ?? string.Empty : key.ToString();
            state.PathSegments.Add(keyText);
            dict[keyText] = Convert(value, state);
            state.PathSegments.RemoveAt(state.PathSegments.Count - 1);
        }

        return dict;
    }

    /// <summary><see cref="ConvertScalar"/>, kept only when the typed value writes back as exactly
    /// <paramref name="value"/>: "5432" is the integer 5432, while "0123456" (123456), "1.10" (1.1) and
    /// "1e5" (100000) stay the strings they are. A connector reading the option as text therefore sees
    /// the same characters whichever way it was typed.</summary>
    private static object? ConvertLossless(string? value) => ConvertScalar(value) switch
    {
        long l when l.ToString(CultureInfo.InvariantCulture) == value => l,
        double d when d.ToString("R", CultureInfo.InvariantCulture) == value => d,
        bool b => b,
        _ => value,
    };

    private static object? ConvertScalar(string? value)
    {
        if (value is null)
        {
            return null;
        }

        if (long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var longValue))
        {
            return longValue;
        }

        if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var doubleValue))
        {
            return doubleValue;
        }

        if (value is "true" or "false")
        {
            return value == "true";
        }

        return value;
    }
}
