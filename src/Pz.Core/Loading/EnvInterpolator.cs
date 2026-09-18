using System.Text.RegularExpressions;
using Pz.Core.Validation;

namespace Pz.Core.Loading;

/// <summary>
/// Substitutes <c>${NAME}</c> references with values from an injected environment dictionary.
/// A literal <c>${</c> that must NOT be read as a reference is written <c>$${</c> (e.g. <c>$${NAME}</c>
/// -&gt; the literal text <c>${NAME}</c>) -- the escape collapses to a single <c>$</c>, and everything
/// after it (including the closing <c>}</c>) is ordinary text, so it never touches the reference regex.
/// </summary>
public static partial class EnvInterpolator
{
    [GeneratedRegex(@"\$\$\{|\$\{([A-Za-z_][A-Za-z0-9_]*)\}")]
    private static partial Regex EnvVarPattern();

    public static string Interpolate(string value, IReadOnlyDictionary<string, string> env, string file, List<PzError> errors)
    {
        return EnvVarPattern().Replace(value, match =>
        {
            if (!match.Groups[1].Success)
            {
                // Matched the "$${" escape, not a reference: collapse to a literal "${" and leave the
                // rest of the text (including the closing "}") untouched.
                return "${";
            }

            var name = match.Groups[1].Value;
            if (env.TryGetValue(name, out var replacement))
            {
                return replacement;
            }

            errors.Add(new PzError(
                PzErrorCode.UndeclaredEnvVar,
                $"Undeclared environment variable '{name}' referenced in {file}.",
                file,
                null,
                $"Set the {name} environment variable before running pz, or remove the reference from {file}."));
            return match.Value;
        });
    }

    /// <summary>True when <paramref name="text"/> contains at least one genuine <c>${VAR}</c> reference
    /// (never true for an escaped <c>$${</c> literal). For a caller that wants to flag a reference it
    /// will not substitute, without performing the substitution itself.</summary>
    public static bool ContainsReference(string text) =>
        EnvVarPattern().Matches(text).Any(m => m.Groups[1].Success);

    /// <summary>
    /// Walks a dynamic YAML object tree (dictionaries/lists/scalars), interpolating every string scalar.
    /// </summary>
    public static object? InterpolateTree(object? node, IReadOnlyDictionary<string, string> env, string file, List<PzError> errors)
    {
        switch (node)
        {
            case string s:
                return Interpolate(s, env, file, errors);
            case Dictionary<string, object?> dict:
                var mapped = new Dictionary<string, object?>();
                foreach (var (key, value) in dict)
                {
                    mapped[key] = InterpolateTree(value, env, file, errors);
                }

                return mapped;
            case List<object?> list:
                return list.Select(item => InterpolateTree(item, env, file, errors)).ToList();
            default:
                return node;
        }
    }
}
