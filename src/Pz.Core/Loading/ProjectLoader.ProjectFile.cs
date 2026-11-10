using System.Globalization;
using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Loading;

/// <summary>project.yml: the top-level file, <c>engine:</c>, <c>retention:</c>, <c>on_source_drift:</c> and <c>engine.breaker:</c>.</summary>
public static partial class ProjectLoader
{
    private static (string Name, string Version, IReadOnlyList<ConnectorRequirement> Connectors,
        Dictionary<string, object?> Vars, EngineConfig Engine,
        RetentionConfig? Retention, StateConfig State, DriftPolicy OnSourceDrift,
        IReadOnlyList<string> Extensions) LoadProjectFile(
            string projectDir, IReadOnlyDictionary<string, string> env, List<PzError> errors)
    {
        const string relativePath = ProjectFileName;
        var path = Path.Combine(projectDir, relativePath);

        var name = string.Empty;
        var version = string.Empty;
        var connectors = new List<ConnectorRequirement>();
        var vars = new Dictionary<string, object?>();
        var engine = new EngineConfig();
        var retention = (RetentionConfig?)new RetentionConfig(DefaultKeepLast);
        var state = StateConfig.Default;
        var onSourceDrift = DriftPolicy.Ignore;
        IReadOnlyList<string> extensions = [];

        if (!File.Exists(path))
        {
            errors.Add(MissingProjectFileError());
            return (name, version, connectors, vars, engine, retention, state, onSourceDrift, extensions);
        }

        Dictionary<string, object?> yaml;
        try
        {
            // Only the top-level `vars:` block is interpolated -- every other project.yml key (engine,
            // retention, state, ...) never has been, and a bare ${VAR} elsewhere stays literal text.
            yaml = YamlMapper.LoadFile(path, relativePath, (text, _, segments) =>
                segments.Count > 0 && segments[0] == "vars"
                    ? EnvInterpolator.Interpolate(text, env, relativePath, errors)
                    : text);
        }
        catch (PzConfigException ex)
        {
            errors.Add(ex.Error);
            return (name, version, connectors, vars, engine, retention, state, onSourceDrift, extensions);
        }

        if (TryGetString(yaml, "name") is { } nameValue)
        {
            name = nameValue;
        }
        else
        {
            errors.Add(new PzError(PzErrorCode.YamlShape, "project.yml is missing required field 'name'.", relativePath, null,
                "create project.yml with name: and version:"));
        }

        if (TryGetString(yaml, "version") is { } versionValue)
        {
            version = versionValue;
        }
        else
        {
            errors.Add(new PzError(PzErrorCode.YamlShape, "project.yml is missing required field 'version'.", relativePath, null,
                "create project.yml with name: and version:"));
        }

        if (PresentButNot<List<object?>>(yaml, "connectors"))
        {
            errors.Add(new PzError(PzErrorCode.YamlShape,
                "project.yml: 'connectors' must be a list of package/version mappings.",
                relativePath, null,
                "connectors:\n  - package: Pz.Connector.LocalFiles\n    version: 0.1.0"));
        }

        foreach (var entry in GetList(yaml, "connectors"))
        {
            if (entry is not Dictionary<string, object?> connectorDict)
            {
                errors.Add(new PzError(PzErrorCode.YamlShape,
                    $"project.yml: connectors entry '{entry}' must be a package/version mapping.",
                    relativePath, null,
                    "- package: Pz.Connector.LocalFiles\n    version: 0.1.0"));
                continue;
            }

            // A bare `1.10` is the YAML number 1.1, and what was typed cannot be recovered from it.
            // Restoring 1.1 where 1.10 was meant is a different package, so the number is refused.
            if (connectorDict.TryGetValue("version", out var rawVersion) && rawVersion is double)
            {
                errors.Add(new PzError(PzErrorCode.YamlShape,
                    $"project.yml: connectors entry '{TryGetString(connectorDict, "package")}' has an unquoted " +
                    $"decimal version, which YAML reads as the number {TryGetString(connectorDict, "version")} " +
                    "(a bare 1.10 is 1.1).",
                    relativePath, null,
                    "quote it — version: \"1.10\""));
                continue;
            }

            connectors.Add(new ConnectorRequirement(
                TryGetString(connectorDict, "package") ?? string.Empty,
                TryGetString(connectorDict, "version") ?? string.Empty));
        }

        if (yaml.ContainsKey("feeds"))
        {
            errors.Add(new PzError(PzErrorCode.FeedsRemoved,
                "project.yml field 'feeds' was removed; feeds are host configuration now.",
                relativePath, null,
                "set PZ_FEEDS or pass --feeds to pz restore"));
        }

        RefuseUnknownProjectKeys(yaml, relativePath, errors);

        if (PresentButNot<Dictionary<string, object?>>(yaml, "vars"))
        {
            errors.Add(new PzError(PzErrorCode.VarsInvalid,
                "project.yml: 'vars' must be a mapping of name to value.",
                relativePath, null, "vars:\n  min_amount: 10"));
        }

        // Already interpolated (and, for a whole-value plain scalar, retyped) by the LoadFile call above.
        vars = GetDict(yaml, "vars");

        if (PresentButNot<Dictionary<string, object?>>(yaml, "engine"))
        {
            errors.Add(new PzError(PzErrorCode.InvalidEngineConfig,
                "project.yml: 'engine' must be a mapping of engine options.",
                relativePath, null, "engine:\n  threads: 4"));
        }

        engine = ParseEngineConfig(GetDict(yaml, "engine"), relativePath, errors);
        retention = ParseRetentionConfig(yaml, relativePath, errors);
        state = ParseStateConfig(yaml, env, relativePath, errors);
        onSourceDrift = ParseDriftPolicy(yaml, relativePath, errors);
        extensions = ParseExtensions(yaml, relativePath, errors);

        return (name, version, connectors, vars, engine, retention, state, onSourceDrift, extensions);
    }

    /// <summary><c>extensions:</c> -- a list of DuckDB extension names. A name is lowercase letters,
    /// digits and underscores, the form DuckDB's own repositories use; anything else (a mis-cased or
    /// padded name included) would only surface later as a confusing "extension not found".</summary>
    private static IReadOnlyList<string> ParseExtensions(Dictionary<string, object?> yaml, string relativePath,
        List<PzError> errors)
    {
        const string example = "extensions:\n  - jev";
        if (PresentButNot<List<object?>>(yaml, "extensions"))
        {
            errors.Add(new PzError(PzErrorCode.YamlShape,
                "project.yml: 'extensions' must be a list of DuckDB extension names.", relativePath, null, example));
            return [];
        }

        var names = new List<string>();
        foreach (var item in GetList(yaml, "extensions"))
        {
            if (item is not string name || !IsExtensionName(name))
            {
                var text = Convert.ToString(item, CultureInfo.InvariantCulture) ?? string.Empty;
                var lower = text.Trim().ToLowerInvariant();
                errors.Add(new PzError(PzErrorCode.YamlShape,
                    $"project.yml: extensions entry '{text}' is not a DuckDB extension name " +
                    "(lowercase letters, digits and underscores).",
                    relativePath, null, IsExtensionName(lower) ? $"extensions:\n  - {lower}" : example));
                continue;
            }

            if (names.Contains(name, StringComparer.Ordinal))
            {
                errors.Add(new PzError(PzErrorCode.YamlShape,
                    $"project.yml: extension '{name}' is listed more than once.", relativePath, null,
                    "list each extension once"));
                continue;
            }

            names.Add(name);
        }

        return names;

        static bool IsExtensionName(string name) =>
            name.Length > 0 && name.All(c => c is (>= 'a' and <= 'z') or (>= '0' and <= '9') or '_');
    }

    /// <summary>"pz" is the documented engine-version-constraint key; accepted here because projects
    /// written to the docs must keep loading, though no constraint check reads it yet. "feeds" is
    /// listed so that <see cref="LoadProjectFile"/> can report its dedicated error instead of the
    /// generic unknown-key one.</summary>
    private static readonly string[] KnownProjectKeys =
        ["name", "version", "pz", "connectors", "extensions", "vars", "engine", "retention", "state", "on_source_drift", "feeds"];

    /// <summary>A mistyped project.yml key is refused rather than silently ignored, matching
    /// connections.yml. <c>outputs:</c> gets the targeted retirement code (PZ0347) instead of the
    /// generic unknown-key error.</summary>
    private static void RefuseUnknownProjectKeys(Dictionary<string, object?> yaml, string relativePath,
        List<PzError> errors)
    {
        if (yaml.ContainsKey("outputs"))
        {
            errors.Add(new PzError(PzErrorCode.RetiredOutputsBlock,
                "project.yml: the 'outputs' block is retired -- there are no per-target output profiles.",
                relativePath, null,
                "declare each place as a top-level connection in connections.yml"));
        }

        RefuseUnknownKeys(yaml.Where(kv => kv.Key != "outputs").ToDictionary(kv => kv.Key, kv => kv.Value),
            KnownProjectKeys, PzErrorCode.YamlShape, relativePath, "project.yml: ", errors);
    }

    private static void RefuseUnreadableEngineValue(Dictionary<string, object?> engineYaml, string key,
        object? parsed, string expected, string relativePath, List<PzError> errors)
    {
        if (engineYaml.TryGetValue(key, out var raw) && raw is not (null or "") && parsed is null)
        {
            errors.Add(new PzError(PzErrorCode.InvalidEngineConfig,
                $"{relativePath}: engine.{key} must be {expected} (got '{raw}').",
                relativePath, null, $"engine.{key} must be {expected}"));
        }
    }

    private static readonly string[] KnownEngineKeys =
        ["threads", "duckdb", "force_universal", "batch_bytes", "check_samples", "breaker", "node_timeout"];
    private static readonly string[] KnownDuckDbKeys = ["memory_limit", "threads", "temp_directory"];

    private static EngineConfig ParseEngineConfig(Dictionary<string, object?> engineYaml, string relativePath,
        List<PzError> errors)
    {
        RefuseUnknownKeys(engineYaml, KnownEngineKeys, PzErrorCode.InvalidEngineConfig, relativePath,
            "engine: ", errors);

        RefuseUnreadableEngineValue(engineYaml, "threads", TryGetInt(engineYaml, "threads"),
            "an integer", relativePath, errors);
        var threads = TryGetInt(engineYaml, "threads") ?? 4;
        if (threads <= 0)
        {
            errors.Add(new PzError(PzErrorCode.InvalidEngineConfig,
                $"{relativePath}: engine.threads must be >= 1 (got {threads}).",
                relativePath, null, "threads must be >= 1"));
        }

        DuckOptionsConfig? duckDb = null;

        if (engineYaml.TryGetValue("duckdb", out var duckDbValue) && duckDbValue is Dictionary<string, object?> duckDbYaml)
        {
            RefuseUnknownKeys(duckDbYaml, KnownDuckDbKeys, PzErrorCode.InvalidEngineConfig, relativePath,
                "engine.duckdb: ", errors);
            RefuseUnreadableEngineValue(duckDbYaml, "threads", TryGetInt(duckDbYaml, "threads"),
                "an integer", relativePath, errors);
            duckDb = new DuckOptionsConfig(
                TryGetString(duckDbYaml, "memory_limit"),
                TryGetInt(duckDbYaml, "threads"),
                TryGetString(duckDbYaml, "temp_directory"));
        }
        else if (duckDbValue is not (null or ""))
        {
            errors.Add(new PzError(PzErrorCode.InvalidEngineConfig,
                $"{relativePath}: engine.duckdb must be a mapping of DuckDB options (got '{duckDbValue}').",
                relativePath, null, "duckdb:\n    memory_limit: 1GiB"));
        }

        RefuseUnreadableEngineValue(engineYaml, "force_universal", TryGetBool(engineYaml, "force_universal"),
            "true or false", relativePath, errors);
        var forceUniversal = TryGetBool(engineYaml, "force_universal") ?? false;

        // Absent -> null, meaning every batch-producing site falls back to BatchOptions.Default (32MB).
        // Bounds (1MB..512MB) guard against pathological configs: too small starves throughput with
        // per-batch overhead, too large defeats the bounded-channel backpressure the memory budget
        // formula assumes.
        RefuseUnreadableEngineValue(engineYaml, "batch_bytes", TryGetInt(engineYaml, "batch_bytes"),
            "an integer number of bytes", relativePath, errors);
        var batchBytes = TryGetInt(engineYaml, "batch_bytes");
        if (batchBytes is { } bytes && (bytes < MinBatchBytes || bytes > MaxBatchBytes))
        {
            errors.Add(new PzError(PzErrorCode.InvalidEngineConfig,
                $"{relativePath}: engine.batch_bytes must be between {MinBatchBytes} and {MaxBatchBytes} bytes (got {bytes}).",
                relativePath, null, $"batch_bytes must be between {MinBatchBytes} and {MaxBatchBytes} bytes"));
        }

        // Project-wide default for a failing check's sample reporting; absent -> true.
        RefuseUnreadableEngineValue(engineYaml, "check_samples", TryGetBool(engineYaml, "check_samples"),
            "true or false", relativePath, errors);
        var checkSamples = TryGetBool(engineYaml, "check_samples") ?? true;

        var breaker = ParseBreakerConfig(engineYaml, relativePath, errors);

        // Absent -> null, meaning no node is ever cancelled for running long.
        TimeSpan? nodeTimeout = null;
        if (engineYaml.TryGetValue("node_timeout", out var nodeTimeoutRaw) && nodeTimeoutRaw is not null)
        {
            if (nodeTimeoutRaw is string or long &&
                DurationParser.TryParse(nodeTimeoutRaw.ToString(), out var parsed) && parsed > TimeSpan.Zero)
            {
                nodeTimeout = parsed;
            }
            else
            {
                var shown = nodeTimeoutRaw is string or long or double or bool ? nodeTimeoutRaw : "a list or mapping";
                errors.Add(new PzError(PzErrorCode.InvalidEngineConfig,
                    $"{relativePath}: engine.node_timeout must be a positive duration like 30s, 45m, 6h, or 1d (got '{shown}').",
                    relativePath, null, "node_timeout: 45m"));
            }
        }

        return new EngineConfig(threads, duckDb, forceUniversal, batchBytes, checkSamples, breaker, nodeTimeout);
    }

    private const int MinBatchBytes = 1024 * 1024;
    private const int MaxBatchBytes = 512 * 1024 * 1024;

    /// <summary>Absent -> <see cref="DefaultKeepLast"/>. A scalar
    /// in the off-set -> null (retention off). A map -> its <c>keep_last</c>, which must be >= 1.
    /// Anything else -> PZ0123.
    ///
    /// The off-set is matched against the RAW value rather than through TryGetBool on purpose:
    /// <see cref="YamlMapper.ConvertScalar"/> maps only the exact lowercase "true"/"false" to a bool, so
    /// the documented spelling `retention: off` -- and `no`, `Off`, `FALSE` -- all arrive here as
    /// strings. Trusting YAML boolean resolution would make the one spelling the docs teach fall through
    /// to "neither bool nor map" and raise a confusing PZ0123 instead of disabling anything.</summary>
    private static RetentionConfig? ParseRetentionConfig(
        Dictionary<string, object?> yaml, string relativePath, List<PzError> errors)
    {
        if (!yaml.TryGetValue("retention", out var value))
        {
            return new RetentionConfig(DefaultKeepLast);
        }

        if (value is false || (value is string text && OffSpellings.Contains(text)))
        {
            return null;
        }

        if (value is not Dictionary<string, object?> retentionYaml)
        {
            errors.Add(new PzError(PzErrorCode.RetentionConfigInvalid,
                $"{relativePath}: retention must be either `off` or a mapping with keep_last (got '{value}').",
                relativePath, null, "write `retention: off`, or `retention:` with a `keep_last:` >= 1"));
            return null;
        }

        var keepLast = TryGetInt(retentionYaml, "keep_last");
        if (keepLast is not { } keepLastValue || keepLastValue < 1)
        {
            var got = retentionYaml.TryGetValue("keep_last", out var raw) ? $"'{raw}'" : "nothing";
            errors.Add(new PzError(PzErrorCode.RetentionConfigInvalid,
                $"{relativePath}: retention.keep_last must be an integer >= 1 (got {got}). " +
                "Use `retention: off` to disable automatic cleanup.",
                relativePath, null, "set keep_last to 1 or more, or write `retention: off`"));
            return null;
        }

        return new RetentionConfig(keepLastValue);
    }

    /// <summary>The default when `retention:` is absent. Ten runs is well past what anyone
    /// retries by hand, and every run_results.json survives regardless -- only staging.duckdb is swept.</summary>
    private const int DefaultKeepLast = 10;

    /// <summary>project.yml's top-level <c>on_source_drift:</c>. Absent ->
    /// <see cref="DriftPolicy.Ignore"/>, matching every other off-by-default gate. An unrecognized value
    /// is PZ0126 (aggregate-errors convention: the loader still resolves to <see cref="DriftPolicy.Ignore"/>
    /// on this path rather than short-circuiting the rest of project.yml).</summary>
    private static DriftPolicy ParseDriftPolicy(
        Dictionary<string, object?> yaml, string relativePath, List<PzError> errors)
    {
        if (TryGetString(yaml, "on_source_drift") is not { } raw)
        {
            return DriftPolicy.Ignore;
        }

        switch (raw)
        {
            case "ignore": return DriftPolicy.Ignore;
            case "warn": return DriftPolicy.Warn;
            case "fail": return DriftPolicy.Fail;
        }

        errors.Add(new PzError(PzErrorCode.DriftPolicyInvalid,
            $"{relativePath}: on_source_drift must be ignore, warn, or fail (got '{raw}').",
            relativePath, null, "write `on_source_drift: warn` (or fail), or remove the key"));
        return DriftPolicy.Ignore;
    }

    private static readonly HashSet<string> OffSpellings =
        new(["off", "false", "no"], StringComparer.OrdinalIgnoreCase);

    /// <summary><c>engine.breaker:</c> — <see cref="BreakerConfig"/> for the engine-owned
    /// <see cref="Pz.Engine.Resilience.CircuitBreaker"/> (Pz.Core cannot reference Pz.Engine, hence the
    /// <c>&lt;see cref&gt;</c> rather than a compiled link). Absent entirely -> null
    /// (breaker off). Present -> a mapping requiring BOTH <c>failure_threshold</c> (int in
    /// <c>[1, int.MaxValue]</c>, via a direct raw-long pattern match rather than <see cref="TryGetInt"/>
    /// -- with an explicit upper bound, since YAML has no native 32-bit integer, so an unbounded raw
    /// <c>long</c>/<c>int</c> cast would silently truncate/wrap a huge value instead of rejecting it) and
    /// <c>cool_down</c> (positive <see cref="DurationParser"/> duration, same idiom as
    /// <see cref="ParseRetryDelay"/>) -- unlike <c>retry:</c>'s individually-optional fields,
    /// <see cref="BreakerConfig"/>'s constructor takes both non-nullably, so a missing field is a shape
    /// error too (reported as "is missing", not a misleading "(got '')"). All shape/bounds errors land on
    /// the existing PZ0120 (InvalidEngineConfig), mirroring threads/batch_bytes above, and aggregate (both
    /// fields are checked even once one is already invalid).</summary>
    private static BreakerConfig? ParseBreakerConfig(Dictionary<string, object?> engineYaml, string relativePath,
        List<PzError> errors)
    {
        if (!engineYaml.TryGetValue("breaker", out var value) || value is null)
        {
            return null;
        }

        const string hint = "breaker:\n  failure_threshold: 5\n  cool_down: 2m";

        if (value is not Dictionary<string, object?> breakerYaml)
        {
            errors.Add(new PzError(PzErrorCode.InvalidEngineConfig,
                $"{relativePath}: engine.breaker must be a mapping with failure_threshold/cool_down.",
                relativePath, null, hint));
            return null;
        }

        RefuseUnknownKeys(breakerYaml, ["failure_threshold", "cool_down"], PzErrorCode.InvalidEngineConfig,
            relativePath, "engine.breaker: ", errors);

        var valid = true;

        breakerYaml.TryGetValue("failure_threshold", out var thresholdRaw);
        int failureThreshold = 0;
        if (thresholdRaw is long thresholdValue && thresholdValue is >= 1 and <= int.MaxValue)
        {
            failureThreshold = (int)thresholdValue;
        }
        else if (thresholdRaw is null)
        {
            errors.Add(new PzError(PzErrorCode.InvalidEngineConfig,
                $"{relativePath}: engine.breaker.failure_threshold is missing.",
                relativePath, null, hint));
            valid = false;
        }
        else
        {
            errors.Add(new PzError(PzErrorCode.InvalidEngineConfig,
                $"{relativePath}: engine.breaker.failure_threshold must be between 1 and {int.MaxValue} (got '{thresholdRaw}').",
                relativePath, null, hint));
            valid = false;
        }

        breakerYaml.TryGetValue("cool_down", out var coolDownRaw);
        var coolDown = TimeSpan.Zero;
        if (coolDownRaw is not null && DurationParser.TryParse(coolDownRaw.ToString(), out var duration) &&
            duration > TimeSpan.Zero)
        {
            coolDown = duration;
        }
        else if (coolDownRaw is null)
        {
            errors.Add(new PzError(PzErrorCode.InvalidEngineConfig,
                $"{relativePath}: engine.breaker.cool_down is missing.",
                relativePath, null, hint));
            valid = false;
        }
        else
        {
            errors.Add(new PzError(PzErrorCode.InvalidEngineConfig,
                $"{relativePath}: engine.breaker.cool_down must be a positive duration like 500ms, 2s, 5m, 1h, or 1d (got '{coolDownRaw}').",
                relativePath, null, hint));
            valid = false;
        }

        return valid ? new BreakerConfig(failureThreshold, coolDown) : null;
    }
}
