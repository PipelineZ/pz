using System.Globalization;
using Pz.Core.Validation;
using YamlDotNet.Core;
using YamlDotNet.RepresentationModel;

namespace Pz.Core.Loading;

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
    public static Dictionary<string, object?> LoadFile(string path, string relativePath)
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

        if (yamlStream.Documents.Count == 0)
        {
            return new Dictionary<string, object?>();
        }

        var state = new ConversionState(relativePath);
        var converted = Convert(yamlStream.Documents[0].RootNode, state);
        return converted as Dictionary<string, object?> ?? new Dictionary<string, object?>();
    }

    /// <summary>An alias makes the parsed node graph shared — and, for a self-referencing anchor,
    /// cyclic — so the graph-to-tree conversion below needs two guards: the current recursion path
    /// (reference identity — YamlNode's own Equals is deep and would itself recurse on a cycle),
    /// and a total-values budget, since each alias occurrence expands to a fresh subtree.</summary>
    private sealed class ConversionState(string relativePath)
    {
        public const int MaxValues = 1_000_000;

        public readonly HashSet<YamlNode> Path = new(ReferenceEqualityComparer.Instance);
        public readonly string RelativePath = relativePath;
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
                return ConvertScalar(scalar.Value);
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
            dict[keyText] = Convert(value, state);
        }

        return dict;
    }

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
