using System.Globalization;
using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Loading;

/// <summary>Loads a project directory (project.yml, connections.yml, pipelines and sidecars) into a
/// <see cref="PzProject"/>, aggregating every error. The parsers live in the sibling partial files, one per
/// file or block; the YAML accessors they share are at the end of this one.</summary>
public static partial class ProjectLoader
{
    private const string ProjectFileName = "project.yml";

    private static PzError MissingProjectFileError() =>
        new(PzErrorCode.YamlShape, $"{ProjectFileName} is missing.", ProjectFileName, null,
            "run this from a pz project directory (or pass --project <dir>); to start a new " +
            "project, run 'pz init <name>'");

    // env: injected (never Environment.GetEnvironmentVariables directly) for testability
    public static PzProject Load(string projectDir, IReadOnlyDictionary<string, string> env,
        IReadOnlyDictionary<string, object?>? varOverrides = null)
    {
        var errors = new List<PzError>();
        var warnings = new List<PzWarning>();

        WarnIfYamlSiblingIgnored(projectDir, Path.Combine(projectDir, "project.yaml"), warnings);
        WarnIfYamlSiblingIgnored(projectDir, Path.Combine(projectDir, "connections.yaml"), warnings);

        var (name, version, connectors, vars, engine, retention, state, onSourceDrift, extensions) =
            LoadProjectFile(projectDir, env, errors);
        var connections = ConnectionsLoader.Load(projectDir, env, errors, warnings);
        RetiredConnectionDirectories.Refuse(projectDir, errors);
        var pipelines = LoadPipelines(projectDir, errors, warnings);

        ValidateStateConnection(state, connections, errors, warnings);

        var mergedVars = new Dictionary<string, object?>(vars);
        if (varOverrides is not null)
        {
            foreach (var (key, value) in varOverrides)
            {
                mergedVars[key] = value;
            }
        }

        if (errors.Count > 0)
        {
            throw new PzValidationException(errors);
        }

        return new PzProject(name, version, engine, mergedVars, connectors, connections, pipelines, retention, state,
            onSourceDrift)
        {
            Warnings = warnings,
            Extensions = extensions,
        };
    }

    /// <summary>Resolves ONLY <c>state:</c> (from project.yml plus the environment) and whatever
    /// credentials it names -- no pipelines, no checks, no retired-directory refusal, and no
    /// connections.yml at all unless <c>state.connection</c> names an entry.
    ///
    /// This exists because `pz state` and `pz clean` are the verbs you reach for when something is
    /// ALREADY broken. Routing them through <see cref="Load"/> to learn the backend would let a
    /// malformed connections.yml -- or any pipeline error -- block inspecting watermarks and freeing
    /// disk, which is precisely the situation they exist for. Under
    /// <c>backend: local</c> nothing beyond project.yml's own <c>state:</c> key is read, so both verbs
    /// keep their no-project-load, no-network property; under a remote
    /// backend they legitimately load what they need to reach the store.
    ///
    /// Errors aggregate and surface as <see cref="PzValidationException"/>, exactly as
    /// <see cref="Load"/>'s do, so callers render them identically.</summary>
    public static (string ProjectName, StateConfig State, IReadOnlyList<ConnectionDef> Connections) LoadStateOnly(
        string projectDir, IReadOnlyDictionary<string, string> env)
    {
        const string relativePath = ProjectFileName;
        var path = Path.Combine(projectDir, relativePath);
        if (!File.Exists(path))
        {
            throw new PzValidationException([MissingProjectFileError()]);
        }

        Dictionary<string, object?> yaml;
        try
        {
            yaml = YamlMapper.LoadFile(path, relativePath);
        }
        catch (PzConfigException ex)
        {
            throw new PzValidationException([ex.Error]);
        }

        var errors = new List<PzError>();
        var state = ParseStateConfig(yaml, env, relativePath, errors);

        // connections.yml is read only when state.connection names an entry in it -- that is the one
        // thing in that file this path can possibly need. Warnings are discarded: this path exists so
        // `pz state`/`pz clean` work when something else is already broken, not to report on it.
        IReadOnlyList<ConnectionDef> connections = state.Connection is null
            ? []
            : ConnectionsLoader.Load(projectDir, env, errors, []);
        ValidateStateConnection(state, connections, errors, []);

        if (errors.Count > 0)
        {
            throw new PzValidationException(errors);
        }

        return (TryGetString(yaml, "name") ?? string.Empty, state, connections);
    }

    /// <summary>Refuses every key in <paramref name="dict"/> that is not in <paramref name="allowed"/>,
    /// one aggregated <see cref="PzError"/> per unknown key, with a near-miss suggestion
    /// (<see cref="NearMiss.Find"/>) when the key is one edit -- or a case difference -- away
    /// from an allowed one. The one "unknown key" check every YAML block with a closed key set shares,
    /// so a typo reads and sounds the same wherever it is caught. <paramref name="where"/> is the
    /// message prefix (e.g. <c>"project.yml: "</c>, <c>"connection 'x' engine.breaker: "</c>) and is
    /// reused verbatim as the hint's lead-in.</summary>
    internal static void RefuseUnknownKeys(Dictionary<string, object?> dict, IReadOnlyList<string> allowed,
        string code, string relativePath, string where, List<PzError> errors)
    {
        foreach (var key in dict.Keys.Where(k => !allowed.Contains(k, StringComparer.Ordinal))
            .OrderBy(k => k, StringComparer.Ordinal))
        {
            var nearMiss = NearMiss.Find(allowed, key);
            errors.Add(new PzError(code,
                $"{where}unknown key '{key}'" + (nearMiss is null ? "" : $" -- did you mean '{nearMiss}'") + ".",
                relativePath, null,
                $"{where}accepts: {string.Join(", ", allowed.OrderBy(a => a, StringComparer.Ordinal))}"));
        }
    }

    /// <summary>A value that is present but not readable as its declared shape is an error, never a
    /// silent fallback to the default -- a typo'd <c>threads: banana</c> must not quietly run on 4
    /// threads. A bodyless key (null or empty scalar) still means "the default".</summary>
    private static bool PresentButNot<T>(Dictionary<string, object?> dict, string key) =>
        dict.TryGetValue(key, out var value) && value is not (null or "") && value is not T;

    internal static string RelativePath(string projectDir, string fullPath) =>
        Path.GetRelativePath(projectDir, fullPath).Replace(Path.DirectorySeparatorChar, '/');

    // Typed scalars are rendered the way YAML spells them, on any host: an invariant `.` decimal
    // whatever the machine's culture, and lowercase booleans rather than .NET's `True`.
    internal static string? TryGetString(Dictionary<string, object?> dict, string key) =>
        dict.TryGetValue(key, out var value)
            ? value switch
            {
                null => null,
                string s => s,
                bool b => b ? "true" : "false",
                IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
                _ => value.ToString(),
            }
            : null;

    // Both integer shapes: YAML scalars arrive as long, Scriban call-site kwargs as int. source()
    // reuses these same parsers for its `retry:`/`sync:` sub-blocks, so a long-only match would
    // silently refuse every integer written at a call site.
    internal static int? TryGetInt(Dictionary<string, object?> dict, string key) =>
        dict.TryGetValue(key, out var value) switch
        {
            // Range-checked: an unchecked (int) cast would silently wrap an out-of-range long.
            true when value is long and >= int.MinValue and <= int.MaxValue => (int)(long)value,
            true when value is int intValue => intValue,
            _ => null,
        };

    private static bool? TryGetBool(Dictionary<string, object?> dict, string key) =>
        dict.TryGetValue(key, out var value) && value is bool boolValue ? boolValue : null;

    internal static Dictionary<string, object?> GetDict(Dictionary<string, object?> dict, string key) =>
        dict.TryGetValue(key, out var value) && value is Dictionary<string, object?> nested
            ? nested
            : new Dictionary<string, object?>();

    private static List<object?> GetList(Dictionary<string, object?> dict, string key) =>
        dict.TryGetValue(key, out var value) && value is List<object?> list ? list : new List<object?>();
}
