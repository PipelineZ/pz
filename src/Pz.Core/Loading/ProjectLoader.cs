using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Loading;

public static class ProjectLoader
{
    // env: injected (never Environment.GetEnvironmentVariables directly) for testability
    public static PzProject Load(string projectDir, IReadOnlyDictionary<string, string> env,
        IReadOnlyDictionary<string, object?>? varOverrides = null)
    {
        var errors = new List<PzError>();

        var (name, version, connectors, vars, engine, retention, state, onSourceDrift) =
            LoadProjectFile(projectDir, env, errors);
        var connections = ConnectionsLoader.Load(projectDir, env, errors);
        RetiredConnectionDirectories.Refuse(projectDir, errors);
        var pipelines = LoadPipelines(projectDir, errors);

        ValidateStateConnection(state, connections, errors);

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
            onSourceDrift);
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
        const string relativePath = "project.yml";
        var path = Path.Combine(projectDir, relativePath);
        if (!File.Exists(path))
        {
            throw new PzValidationException([
                new PzError(PzErrorCode.YamlShape, "project.yml is missing.", relativePath, null,
                    "run this from a pz project directory (or pass --project <dir>); to start a new " +
                    "project, run 'pz init <name>'")
            ]);
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
        // thing in that file this path can possibly need.
        IReadOnlyList<ConnectionDef> connections = state.Connection is null
            ? []
            : ConnectionsLoader.Load(projectDir, env, errors);
        ValidateStateConnection(state, connections, errors);

        if (errors.Count > 0)
        {
            throw new PzValidationException(errors);
        }

        return (TryGetString(yaml, "name") ?? string.Empty, state, connections);
    }

    private static (string Name, string Version, IReadOnlyList<ConnectorRequirement> Connectors,
        Dictionary<string, object?> Vars, EngineConfig Engine,
        RetentionConfig? Retention, StateConfig State, DriftPolicy OnSourceDrift) LoadProjectFile(
            string projectDir, IReadOnlyDictionary<string, string> env, List<PzError> errors)
    {
        const string relativePath = "project.yml";
        var path = Path.Combine(projectDir, "project.yml");

        var name = string.Empty;
        var version = string.Empty;
        var connectors = new List<ConnectorRequirement>();
        var vars = new Dictionary<string, object?>();
        var engine = new EngineConfig();
        var retention = (RetentionConfig?)new RetentionConfig(DefaultKeepLast);
        var state = StateConfig.Default;
        var onSourceDrift = DriftPolicy.Ignore;

        if (!File.Exists(path))
        {
            errors.Add(new PzError(PzErrorCode.YamlShape, "project.yml is missing.", relativePath, null,
                "run this from a pz project directory (or pass --project <dir>); to start a new " +
                "project, run 'pz init <name>'"));
            return (name, version, connectors, vars, engine, retention, state, onSourceDrift);
        }

        Dictionary<string, object?> yaml;
        try
        {
            yaml = YamlMapper.LoadFile(path, relativePath);
        }
        catch (PzConfigException ex)
        {
            errors.Add(ex.Error);
            return (name, version, connectors, vars, engine, retention, state, onSourceDrift);
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

        var rawVars = GetDict(yaml, "vars");
        var interpolatedVars = (Dictionary<string, object?>)EnvInterpolator.InterpolateTree(rawVars, env, relativePath, errors)!;
        vars = interpolatedVars;

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

        return (name, version, connectors, vars, engine, retention, state, onSourceDrift);
    }

    /// <summary>A mistyped project.yml key is refused rather than silently ignored, matching
    /// connections.yml. <c>outputs:</c> gets the targeted retirement code (PZ0347) instead of the
    /// generic unknown-key error.</summary>
    /// <remarks>"pz" is the documented engine-version-constraint key (project-yml.md); accepted here
    /// because projects written to the docs must keep loading, though no constraint check reads it yet.</remarks>
    private static readonly string[] KnownProjectKeys =
        ["name", "version", "pz", "connectors", "vars", "engine", "retention", "state", "on_source_drift", "feeds"];

    private static void RefuseUnknownProjectKeys(Dictionary<string, object?> yaml, string relativePath,
        List<PzError> errors)
    {
        foreach (var key in yaml.Keys.Where(k => !KnownProjectKeys.Contains(k, StringComparer.Ordinal))
            .OrderBy(k => k, StringComparer.Ordinal))
        {
            if (key == "outputs")
            {
                errors.Add(new PzError(PzErrorCode.RetiredOutputsBlock,
                    "project.yml: the 'outputs' block is retired -- there are no per-target output profiles.",
                    relativePath, null,
                    "declare each place as a top-level connection in connections.yml"));
                continue;
            }

            errors.Add(new PzError(PzErrorCode.YamlShape,
                $"project.yml: unknown key '{key}'.",
                relativePath, null,
                "project.yml holds name, version, connectors, vars, engine, retention, state, and on_source_drift"));
        }
    }

    /// <summary>A value that is present but not readable as its declared shape is an error, never a
    /// silent fallback to the default -- a typo'd <c>threads: banana</c> must not quietly run on 4
    /// threads. A bodyless key (null or empty scalar) still means "the default".</summary>
    private static bool PresentButNot<T>(Dictionary<string, object?> dict, string key) =>
        dict.TryGetValue(key, out var value) && value is not (null or "") && value is not T;

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

    private static EngineConfig ParseEngineConfig(Dictionary<string, object?> engineYaml, string relativePath,
        List<PzError> errors)
    {
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

        return new EngineConfig(threads, duckDb, forceUniversal, batchBytes, checkSamples, breaker);
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
        if (keepLast is not { } value2 || value2 < 1)
        {
            var got = retentionYaml.TryGetValue("keep_last", out var raw) ? $"'{raw}'" : "nothing";
            errors.Add(new PzError(PzErrorCode.RetentionConfigInvalid,
                $"{relativePath}: retention.keep_last must be an integer >= 1 (got {got}). " +
                "Use `retention: off` to disable automatic cleanup.",
                relativePath, null, "set keep_last to 1 or more, or write `retention: off`"));
            return null;
        }

        return new RetentionConfig(value2);
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

    private static readonly string[] StateBackends =
        [StateConfig.Local, StateConfig.SqlServer, StateConfig.Http];

    /// <summary>Which project.yml keys mean anything under each backend. A key that is legal for one
    /// backend and written under another is PZ0124 rather than silently ignored -- the three backends
    /// have disjoint credential shapes.
    ///
    /// `token` is absent from every list on purpose: it is a credential, and this project's
    /// secret-hygiene rule keeps credentials out of project.yml. It comes from PZ_STATE_TOKEN only,
    /// exactly as `connection_string` does.</summary>
    private static readonly Dictionary<string, string[]> StateKeysByBackend = new(StringComparer.Ordinal)
    {
        [StateConfig.Local] = ["backend"],
        [StateConfig.SqlServer] = ["backend", "connection", "schema", "artifacts", "events"],
        [StateConfig.Http] = ["backend", "url", "artifacts", "events"],
    };

    /// <summary>Resolution is per key: an explicit
    /// project.yml value wins; otherwise the environment counterpart; otherwise the default. There is
    /// deliberately no way for the environment to OVERRIDE an explicit key — silently redirecting a
    /// project's state away from where it says it lives is the failure that precedence prevents.</summary>
    private static StateConfig ParseStateConfig(Dictionary<string, object?> yaml,
        IReadOnlyDictionary<string, string> env, string relativePath, List<PzError> errors)
    {
        var stateYaml = new Dictionary<string, object?>();
        var declared = false;

        if (yaml.TryGetValue("state", out var value) && value is not null)
        {
            if (value is not Dictionary<string, object?> dict)
            {
                // The value is NOT echoed: `state: "Server=...;Password=..."` is a plausible mistake, and
                // this project's secret-hygiene rule forbids printing a credential back out. The shape
                // is what the author needs to know anyway.
                errors.Add(new PzError(PzErrorCode.StateBackendConfigInvalid,
                    $"{relativePath}: state must be a mapping.", relativePath, null,
                    "state:\n  backend: sqlserver\n  connection: ops"));
                return StateConfig.Default;
            }

            stateYaml = dict;
            declared = true;
        }

        var (backend, backendSource) = Resolve(stateYaml, env, "backend", "PZ_STATE_BACKEND", relativePath);
        backend ??= StateConfig.Local;
        backendSource ??= "default";

        if (!StateBackends.Contains(backend, StringComparer.Ordinal))
        {
            errors.Add(new PzError(PzErrorCode.StateBackendConfigInvalid,
                $"{relativePath}: unknown state backend '{backend}'.", relativePath, null,
                $"use one of: {string.Join(", ", StateBackends)}"));
            return StateConfig.Default;
        }

        var isLocal = string.Equals(backend, StateConfig.Local, StringComparison.Ordinal);
        var isHttp = string.Equals(backend, StateConfig.Http, StringComparison.Ordinal);

        if (declared)
        {
            var allowed = StateKeysByBackend[backend];
            var stray = stateYaml.Keys.Where(k => !allowed.Contains(k, StringComparer.Ordinal)).ToList();
            if (stray.Count > 0)
            {
                var nextStep = stray.Contains("token", StringComparer.Ordinal)
                    ? "state.token does not exist -- a bearer token is a credential, so it comes from " +
                      "PZ_STATE_TOKEN only; remove any other stray key too"
                    : $"remove the key(s), or use a backend where they apply ({string.Join(", ", StateBackends)})";

                errors.Add(new PzError(PzErrorCode.StateBackendConfigInvalid,
                    $"{relativePath}: state key(s) {string.Join(", ", stray.OrderBy(s => s, StringComparer.Ordinal))} " +
                    $"have no meaning under `backend: {backend}`.", relativePath, null, nextStep));
                return StateConfig.Default;
            }
        }

        var (connection, _) = Resolve(stateYaml, env, "connection", null, relativePath);
        var (connectionString, _) = Resolve(stateYaml, env, null, "PZ_STATE_CONNECTION_STRING", relativePath);
        var (schema, _) = Resolve(stateYaml, env, "schema", "PZ_STATE_SCHEMA", relativePath);
        var (url, _) = Resolve(stateYaml, env, "url", "PZ_STATE_URL", relativePath);
        var (token, _) = Resolve(stateYaml, env, null, "PZ_STATE_TOKEN", relativePath);
        var artifacts = ResolveBool(stateYaml, env, "artifacts", "PZ_STATE_ARTIFACTS", relativePath, errors)
            ?? (!isLocal && !isHttp);
        var events = ResolveBool(stateYaml, env, "events", "PZ_STATE_EVENTS", relativePath, errors) ?? false;

        // Under `backend: http` pz owns watermarks and the server keeps run results and the event
        // stream, so this backend implements the keyed-state seam ONLY. A
        // truthy artifacts/events here would otherwise fall through to the SQL stores, which have no
        // credentials under this backend -- refused loudly rather than quietly downgraded, because a
        // silently-ignored deployment knob is what PZ0124 exists to prevent.
        if (isHttp)
        {
            foreach (var (key, requested) in new[] { ("artifacts", artifacts), ("events", events) })
            {
                if (!requested)
                {
                    continue;
                }

                errors.Add(new PzError(PzErrorCode.StateBackendConfigInvalid,
                    $"{relativePath}: state.{key}: true is not supported under `backend: http` — this " +
                    "backend stores watermarks and sync-state only; run artifacts and events stay where " +
                    "they already live.", relativePath, null,
                    $"set {key}: false (its default here; check PZ_STATE_{key.ToUpperInvariant()} too)"));
            }
        }

        // Events without artifacts is refused, not
        // half-honored. Without the `runs` header row only `artifacts` writes, a truncated event stream
        // has nowhere to report its drop count (`events_dropped` -- https://pipelinez.dev/events/ promises "never
        // silently") and the run's `run_events` rows are never retention/`pz clean` candidates, so the
        // table would grow without bound. Either value may come from its PZ_STATE_* counterpart; the
        // combination is refused wherever each side came from.
        if (!isLocal && !isHttp && events && !artifacts)
        {
            errors.Add(new PzError(PzErrorCode.StateBackendConfigInvalid,
                $"{relativePath}: state.events: true requires state.artifacts: true — without the runs " +
                "header row, a truncated event stream cannot be reported (events_dropped) and run_events " +
                "rows are never swept by retention or pz clean.", relativePath, null,
                "set artifacts: true (its default on a non-local backend; check PZ_STATE_ARTIFACTS too), " +
                "or set events: false"));
        }

        return new StateConfig(backend, connection, connectionString, schema ?? "pz", artifacts, events,
            backendSource, url, token);
    }

    /// <summary>project.yml value, else the environment counterpart, else null — with the source name so
    /// the caller can report provenance. <paramref name="key"/> null means the value has no project.yml
    /// spelling (PZ_STATE_CONNECTION_STRING); <paramref name="envName"/> null means it has no
    /// environment counterpart (`connection`, which names a connections.yml entry).</summary>
    private static (string? Value, string? Source) Resolve(Dictionary<string, object?> stateYaml,
        IReadOnlyDictionary<string, string> env, string? key, string? envName, string relativePath)
    {
        if (key is not null && TryGetString(stateYaml, key) is { } declared)
        {
            return (declared, relativePath);
        }

        if (envName is not null && env.TryGetValue(envName, out var fromEnv) && !string.IsNullOrWhiteSpace(fromEnv))
        {
            return (fromEnv, envName);
        }

        return (null, null);
    }

    /// <summary>An unparseable PZ_STATE_ARTIFACTS/PZ_STATE_EVENTS value is PZ0124, naming the variable
    /// and the value that could not be parsed — never a silent fallback to the key's default. A
    /// deployment knob that is silently ignored is worse than one that is loud, which is the failure
    /// mode PZ0124 prevents elsewhere in this parser (an unknown backend, a stray key under
    /// `backend: local`).</summary>
    private static bool? ResolveBool(Dictionary<string, object?> stateYaml, IReadOnlyDictionary<string, string> env,
        string key, string envName, string relativePath, List<PzError> errors)
    {
        if (stateYaml.TryGetValue(key, out var raw) && raw is not null)
        {
            if (raw is bool b) { return b; }

            errors.Add(new PzError(PzErrorCode.StateBackendConfigInvalid,
                $"{relativePath}: state.{key} must be true or false (got '{raw}').", relativePath, null,
                $"{key}: true"));
            return null;
        }

        if (env.TryGetValue(envName, out var fromEnv))
        {
            if (bool.TryParse(fromEnv, out var parsed))
            {
                return parsed;
            }

            errors.Add(new PzError(PzErrorCode.StateBackendConfigInvalid,
                $"{envName} must be true or false (got '{fromEnv}').", relativePath, null,
                $"set {envName}=true or {envName}=false, or unset it"));
            return null;
        }

        return null;
    }

    /// <summary>A non-local backend must resolve credentials
    /// from exactly one of two paths — a named connections.yml entry (project-scoped, wins) or
    /// PZ_STATE_CONNECTION_STRING (host-scoped default). Neither is PZ0125 at validation time rather than
    /// a runtime surprise on the first watermark write.</summary>
    private static void ValidateStateConnection(StateConfig state, IReadOnlyList<ConnectionDef> connections,
        List<PzError> errors)
    {
        if (state.IsLocal)
        {
            return;
        }

        if (state.IsHttp)
        {
            ValidateStateUrl(state, errors);
            return;
        }

        if (state.Connection is { } name)
        {
            var def = connections.FirstOrDefault(c => string.Equals(c.Name, name, StringComparison.Ordinal));
            if (def is null)
            {
                errors.Add(new PzError(PzErrorCode.StateConnectionInvalid,
                    $"state.connection '{name}' is not declared in connections.yml.", "project.yml", null,
                    $"declare a connection named '{name}' with connector: {state.Backend}"));
            }
            else if (!string.Equals(def.Connector, state.Backend, StringComparison.Ordinal))
            {
                errors.Add(new PzError(PzErrorCode.StateConnectionInvalid,
                    $"state.connection '{name}' uses connector '{def.Connector}', but state backend " +
                    $"'{state.Backend}' requires a '{state.Backend}' connection.", "project.yml", null,
                    $"point state.connection at a connector: {state.Backend} connection"));
            }

            return;
        }

        if (state.ConnectionString is null)
        {
            errors.Add(new PzError(PzErrorCode.StateConnectionInvalid,
                $"state backend '{state.Backend}' has no credentials: set state.connection, " +
                "or set PZ_STATE_CONNECTION_STRING.", "project.yml", null,
                "state:\n  backend: sqlserver\n  connection: ops"));
        }
    }

    /// <summary>`backend: http` needs exactly one thing, the run-scoped state URL, and it must be
    /// absolute http(s) -- checked here so a typo is PZ0125 at load time rather than a PZ0518 on the
    /// first watermark read, after nodes have already run.
    ///
    /// The token is deliberately NOT required: a server is free to serve these endpoints
    /// unauthenticated, so demanding a credential here would refuse a config that
    /// works. pz sends the bearer header the moment PZ_STATE_TOKEN is set, which is what makes turning
    /// authentication on a server-side change alone.</summary>
    private static void ValidateStateUrl(StateConfig state, List<PzError> errors)
    {
        if (state.Url is not { } url)
        {
            errors.Add(new PzError(PzErrorCode.StateConnectionInvalid,
                "state backend 'http' has no endpoint: set state.url, or set PZ_STATE_URL.",
                "project.yml", null,
                "state:\n  backend: http\n  url: https://state.example/api/agents/runs/<id>/state"));
            return;
        }

        if (!Uri.TryCreate(url, UriKind.Absolute, out var parsed)
            || (parsed.Scheme != Uri.UriSchemeHttp && parsed.Scheme != Uri.UriSchemeHttps))
        {
            errors.Add(new PzError(PzErrorCode.StateConnectionInvalid,
                $"state.url '{url}' is not an absolute http(s) URL.", "project.yml", null,
                "use the run-scoped state URL the server issued, e.g. " +
                "https://state.example/api/agents/runs/<id>/state"));
        }
    }

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

    private static readonly string[] IncrementalOnlyKeys = ["cursor", "max_window", "initial", "until"];

    /// <summary>The unified dataset <c>sync:</c> block. Absent
    /// entirely -> null (mode: auto, omitted form). Present but not a mapping, missing/unknown
    /// 'mode', 'mode: incremental' missing 'cursor', a non-scalar 'sync.slot', or an unknown
    /// sub-key under the resolved mode -> PZ0334 (SyncModeInvalid).
    /// 'mode: cdc' accepts only 'mode' and the optional 'slot'; whether the named
    /// cursor actually exists in -- and has an allowed type in -- a declared <c>columns:</c>
    /// contract remains a semantic question for DagCompiler (PZ0212), not the loader.</summary>
    internal static SyncModeDef? ParseSyncMode(
        Dictionary<string, object?> datasetYaml, string datasetName, string relativePath, List<PzError> errors)
    {
        if (!datasetYaml.TryGetValue("sync", out var value) || value is null)
        {
            return null; // mode: auto, omitted form
        }

        const string hint = "sync:\n  mode: incremental\n  cursor: <column>";
        if (value is not Dictionary<string, object?> syncYaml)
        {
            errors.Add(new PzError(PzErrorCode.SyncModeInvalid,
                $"{relativePath}: dataset '{datasetName}' field 'sync' must be a mapping with a 'mode' key.",
                relativePath, null, hint));
            return null;
        }

        var modeRaw = TryGetString(syncYaml, "mode");
        switch (modeRaw)
        {
            case "incremental":
                if (TryGetString(syncYaml, "cursor") is not { Length: > 0 } cursor)
                {
                    errors.Add(new PzError(PzErrorCode.SyncModeInvalid,
                        $"{relativePath}: dataset '{datasetName}': sync mode 'incremental' requires 'cursor'.",
                        relativePath, null, hint));
                    return null;
                }

                RefuseUnknownSyncKeys(syncYaml, ["mode", .. IncrementalOnlyKeys], datasetName, relativePath, errors);
                var maxWindow = ParseIncrementalScalar(syncYaml, "max_window", datasetName, relativePath, errors);
                var initial = ParseIncrementalScalar(syncYaml, "initial", datasetName, relativePath, errors);
                var until = ParseIncrementalScalar(syncYaml, "until", datasetName, relativePath, errors);
                return new SyncModeDef(SyncMode.Incremental, new IncrementalDef(cursor, maxWindow, initial, until));

            case "cdc":
                RefuseUnknownSyncKeys(syncYaml, ["mode", "slot"], datasetName, relativePath, errors);
                string? slot = null;
                if (syncYaml.TryGetValue("slot", out var slotRaw) && slotRaw is not null)
                {
                    if (slotRaw is List<object?> or Dictionary<string, object?>)
                    {
                        errors.Add(new PzError(PzErrorCode.SyncModeInvalid,
                            $"{relativePath}: dataset '{datasetName}' field 'sync.slot' must be a scalar value.",
                            relativePath, null, "sync:\n  mode: cdc\n  slot: <name>"));
                    }
                    else
                    {
                        slot = Convert.ToString(slotRaw, System.Globalization.CultureInfo.InvariantCulture);
                    }
                }

                return new SyncModeDef(SyncMode.Cdc, null, slot);

            case "auto":
                RefuseUnknownSyncKeys(syncYaml, ["mode"], datasetName, relativePath, errors);
                return new SyncModeDef(SyncMode.Auto, null);

            case null:
                errors.Add(new PzError(PzErrorCode.SyncModeInvalid,
                    $"{relativePath}: dataset '{datasetName}': 'sync' is missing required field 'mode' " +
                    "(a bare marker sync block is retired — connector-managed feeds need no sync block at all: delete it).",
                    relativePath, null, hint));
                return null;

            default:
                errors.Add(new PzError(PzErrorCode.SyncModeInvalid,
                    $"{relativePath}: dataset '{datasetName}': unknown sync mode '{modeRaw}' (accepted: incremental, cdc, auto).",
                    relativePath, null, hint));
                return null;
        }
    }

    private static void RefuseUnknownSyncKeys(Dictionary<string, object?> syncYaml, string[] allowed,
        string datasetName, string relativePath, List<PzError> errors)
    {
        foreach (var key in syncYaml.Keys.Where(k => !allowed.Contains(k, StringComparer.Ordinal))
            .OrderBy(k => k, StringComparer.Ordinal))
        {
            var detail = allowed.Length == 1
                ? "mode 'auto' accepts no other keys"
                : $"unknown 'sync' key '{key}'";
            errors.Add(new PzError(PzErrorCode.SyncModeInvalid,
                $"{relativePath}: dataset '{datasetName}': {detail}.",
                relativePath, null, "sync:\n  mode: incremental\n  cursor: <column>"));
        }
    }

    /// <summary>The window trio must be YAML scalars; anything list/mapping-shaped is
    /// PZ0334 (mirroring the rest of ParseSyncMode). Values are surfaced as raw strings (numbers
    /// included — a numeric `initial: 100` round-trips as "100", which IS its canonical digit form)
    /// for DagCompiler's semantic pass.</summary>
    private static string? ParseIncrementalScalar(Dictionary<string, object?> syncYaml, string key,
        string datasetName, string relativePath, List<PzError> errors)
    {
        if (!syncYaml.TryGetValue(key, out var value) || value is null)
        {
            return null;
        }

        if (value is List<object?> or Dictionary<string, object?>)
        {
            errors.Add(new PzError(PzErrorCode.SyncModeInvalid,
                $"{relativePath}: dataset '{datasetName}' field 'sync.{key}' must be a scalar value.",
                relativePath, null, $"sync:\n  mode: incremental\n  cursor: <column>\n  {key}: <value>"));
            return null;
        }

        return Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>The <c>retry:</c> block — one parser for BOTH levels: instance (sources/sinks file top
    /// level) and dataset/output. Absent -> null. All
    /// three keys optional; absent keys stay null and the engine cascades dataset/output -> instance ->
    /// <c>RetryPolicy.Default</c> field-wise. The max_delay >= base_delay relation is checked only when
    /// BOTH are declared in the SAME block: a single declared key that crosses a coarser level is
    /// harmless (ComputeDelay caps at MaxDelay), and Pz.Core cannot see the engine default to compare
    /// against (layering). Errors are PZ0121, aggregate, and name the file — <paramref name="context"/>
    /// additionally names the dataset/output block per the repo's error rule; instance-level call sites
    /// pass "".</summary>
    internal static RetryDef? ParseRetry(Dictionary<string, object?> yaml, string relativePath, List<PzError> errors,
        string context = "")
    {
        if (!yaml.TryGetValue("retry", out var value) || value is null)
        {
            return null;
        }

        const string hint = "retry:\n  max_attempts: 8\n  base_delay: 2s\n  max_delay: 5m";
        if (value is not Dictionary<string, object?> retryYaml)
        {
            errors.Add(new PzError(PzErrorCode.RetryConfigInvalid,
                $"{relativePath}: {context}'retry' must be a mapping with max_attempts/base_delay/max_delay.",
                relativePath, null, hint));
            return null;
        }

        var valid = true;

        int? maxAttempts = null;
        if (retryYaml.TryGetValue("max_attempts", out var attemptsRaw) && attemptsRaw is not null)
        {
            maxAttempts = TryGetInt(retryYaml, "max_attempts");
            if (maxAttempts is null or < 1)
            {
                errors.Add(new PzError(PzErrorCode.RetryConfigInvalid,
                    $"{relativePath}: {context}retry.max_attempts must be an integer >= 1 (got '{attemptsRaw}').",
                    relativePath, null, hint));
                valid = false;
            }
        }

        var baseDelay = ParseRetryDelay(retryYaml, "base_delay", relativePath, errors, hint, context, ref valid);
        var maxDelay = ParseRetryDelay(retryYaml, "max_delay", relativePath, errors, hint, context, ref valid);

        if (baseDelay is { } b && maxDelay is { } m && m < b)
        {
            errors.Add(new PzError(PzErrorCode.RetryConfigInvalid,
                $"{relativePath}: {context}retry.max_delay must be >= retry.base_delay.",
                relativePath, null, hint));
            valid = false;
        }

        return valid ? new RetryDef(maxAttempts, baseDelay, maxDelay) : null;
    }

    private static TimeSpan? ParseRetryDelay(Dictionary<string, object?> retryYaml, string key,
        string relativePath, List<PzError> errors, string hint, string context, ref bool valid)
    {
        if (!retryYaml.TryGetValue(key, out var raw) || raw is null)
        {
            return null;
        }

        if (!DurationParser.TryParse(raw.ToString(), out var duration) || duration <= TimeSpan.Zero)
        {
            errors.Add(new PzError(PzErrorCode.RetryConfigInvalid,
                $"{relativePath}: {context}retry.{key} must be a positive duration like 500ms, 2s, 5m, 1h, or 1d (got '{raw}').",
                relativePath, null, hint));
            valid = false;
            return null;
        }

        return duration;
    }

    /// <summary>The instance-level `rate_limit:` block — mirrors <see cref="ParseRetry"/>'s
    /// skeleton, but simpler (two int fields, both bounded 1..1_000_000, no cross-field relation). Absent -> null.
    /// `requests_per_minute` is required; `burst` optional (absent -> engine derives it via
    /// <see cref="RateLimitDef.EffectiveBurst"/>). Errors are PZ0318, aggregate, and name the file. `rate_limit:`
    /// is instance-level only -- callers at the dataset/output level don't invoke this parser at all; instead
    /// they refuse the key outright (see the `ContainsKey("rate_limit")` check in
    /// <see cref="ConnectionsLoader"/>'s read block).</summary>
    internal static RateLimitDef? ParseRateLimit(Dictionary<string, object?> yaml, string relativePath,
        List<PzError> errors)
    {
        if (!yaml.TryGetValue("rate_limit", out var value) || value is null)
        {
            return null;
        }

        const string hint = "rate_limit:\n  requests_per_minute: 60\n  burst: 10";
        if (value is not Dictionary<string, object?> rateLimitYaml)
        {
            errors.Add(new PzError(PzErrorCode.RateLimitConfigInvalid,
                $"{relativePath}: 'rate_limit' must be a mapping with requests_per_minute (and optional burst).",
                relativePath, null, hint));
            return null;
        }

        var valid = true;

        int? rpm = null;
        if (rateLimitYaml.TryGetValue("requests_per_minute", out var rpmRaw) && rpmRaw is not null)
        {
            rpm = TryGetInt(rateLimitYaml, "requests_per_minute");
            if (rpm is null or < 1 or > 1_000_000)
            {
                errors.Add(new PzError(PzErrorCode.RateLimitConfigInvalid,
                    $"{relativePath}: rate_limit.requests_per_minute must be an integer in 1..1000000 (got '{rpmRaw}').",
                    relativePath, null, hint));
                valid = false;
            }
        }
        else
        {
            errors.Add(new PzError(PzErrorCode.RateLimitConfigInvalid,
                $"{relativePath}: rate_limit.requests_per_minute is required.",
                relativePath, null, hint));
            valid = false;
        }

        int? burst = null;
        if (rateLimitYaml.TryGetValue("burst", out var burstRaw) && burstRaw is not null)
        {
            burst = TryGetInt(rateLimitYaml, "burst");
            if (burst is null or < 1 or > 1_000_000)
            {
                errors.Add(new PzError(PzErrorCode.RateLimitConfigInvalid,
                    $"{relativePath}: rate_limit.burst must be an integer in 1..1000000 (got '{burstRaw}').",
                    relativePath, null, hint));
                valid = false;
            }
        }

        return valid ? new RateLimitDef(rpm!.Value, burst) : null;
    }

    /// <summary>Top-level `max_concurrency:` on a source/sink file — caps how
    /// many of this instance's nodes the dispatcher runs concurrently. Loader validates shape+bound only;
    /// enforcement is RunOrchestrator's. Absent -> null (unbounded, engine.threads still governs).</summary>
    internal static int? ParseMaxConcurrency(Dictionary<string, object?> yaml, string relativePath, List<PzError> errors)
    {
        if (!yaml.TryGetValue("max_concurrency", out var raw) || raw is null)
        {
            return null;
        }

        var value = TryGetInt(yaml, "max_concurrency");
        if (value is null or < 1)
        {
            errors.Add(new PzError(PzErrorCode.ConcurrencyConfigInvalid,
                $"{relativePath}: 'max_concurrency' must be an integer >= 1 (got '{raw}').",
                relativePath, null, "max_concurrency: 2"));
            return null;
        }

        return value;
    }

    /// <summary>Connection-level `allow_unsigned_extensions:` -- opts a connection's native scans/copies
    /// out of the planner's unsigned-packaged-extension gate (PZ0359). Absent -> false (a value present
    /// but not a bool is a loud error, never a silent false, matching the engine.force_universal
    /// precedent in <see cref="RefuseUnreadableEngineValue"/>).</summary>
    internal static bool ParseAllowUnsignedExtensions(Dictionary<string, object?> yaml, string relativePath,
        string connectionName, List<PzError> errors)
    {
        if (!yaml.TryGetValue("allow_unsigned_extensions", out var raw) || raw is (null or ""))
        {
            return false;
        }

        if (raw is bool value)
        {
            return value;
        }

        errors.Add(new PzError(PzErrorCode.YamlShape,
            $"{relativePath}: connection '{connectionName}' allow_unsigned_extensions must be true or false (got '{raw}').",
            relativePath, null, "allow_unsigned_extensions: true"));
        return false;
    }

    private static List<PipelineDef> LoadPipelines(string projectDir, List<PzError> errors)
    {
        var pipelines = new List<PipelineDef>();
        var pipelinesDir = Path.Combine(projectDir, "pipelines");
        if (!Directory.Exists(pipelinesDir))
        {
            return pipelines;
        }

        var sqlFiles = Directory.EnumerateFiles(pipelinesDir, "*.sql", SearchOption.AllDirectories)
            .Where(f => !RelativePath(projectDir, f).Split('/').Contains("configs"))
            .OrderBy(f => f, StringComparer.Ordinal);

        var byName = new Dictionary<string, PipelineDef>();
        var firstFileByName = new Dictionary<string, string>();

        foreach (var filePath in sqlFiles)
        {
            var relativePath = RelativePath(projectDir, filePath);
            var pipelineName = Path.GetFileNameWithoutExtension(filePath);
            var rawSql = File.ReadAllText(filePath);

            if (firstFileByName.TryGetValue(pipelineName, out var firstFile))
            {
                errors.Add(new PzError(
                    PzErrorCode.DuplicateName,
                    $"Duplicate pipeline name '{pipelineName}' defined in {firstFile} and {relativePath}.",
                    relativePath,
                    null,
                    "rename one of the pipelines so names are unique within the project."));
                continue;
            }

            firstFileByName[pipelineName] = relativePath;
            byName[pipelineName] = new PipelineDef(pipelineName, rawSql, "table",
                Array.Empty<string>(), Array.Empty<CheckDef>(), relativePath);
        }

        ApplySidecars(projectDir, byName, errors);

        pipelines.AddRange(byName.Values);
        return pipelines;
    }

    private static void ApplySidecars(string projectDir, Dictionary<string, PipelineDef> pipelines, List<PzError> errors)
    {
        var configsDir = Path.Combine(projectDir, "pipelines", "configs");
        if (!Directory.Exists(configsDir))
        {
            return;
        }

        foreach (var filePath in Directory.EnumerateFiles(configsDir, "*.yml").OrderBy(f => f, StringComparer.Ordinal))
        {
            var relativePath = RelativePath(projectDir, filePath);
            Dictionary<string, object?> yaml;
            try
            {
                yaml = YamlMapper.LoadFile(filePath, relativePath);
            }
            catch (PzConfigException ex)
            {
                errors.Add(ex.Error);
                continue;
            }

            var pipelineName = TryGetString(yaml, "pipeline") ?? string.Empty;

            if (!pipelines.TryGetValue(pipelineName, out var pipeline))
            {
                errors.Add(new PzError(
                    PzErrorCode.SidecarUnknownPipeline,
                    $"Sidecar config {relativePath} references unknown pipeline '{pipelineName}'.",
                    relativePath,
                    null,
                    "fix the pipeline: key or remove the sidecar."));
                continue;
            }

            var materialization = TryGetString(yaml, "materialization") ?? "table";
            var tags = GetList(yaml, "tags").Select(t => t?.ToString() ?? string.Empty).ToList();

            var checks = new List<CheckDef>();
            // Indices (into `checks`) whose `column:` value was already reported as malformed below --
            // ValidateChecks skips its own "requires exactly one 'column'" rule for these so the one
            // root cause (a bad `column:` value) doesn't also surface as a second PZ0113: one error per
            // root cause.
            var columnShapeErrorIndices = new HashSet<int>();
            foreach (var checkEntry in GetList(yaml, "checks"))
            {
                if (checkEntry is not Dictionary<string, object?> checkDict || checkDict.Count == 0)
                {
                    errors.Add(new PzError(PzErrorCode.InvalidCheck,
                        $"pipeline '{pipelineName}': check entry must be a mapping like `- not_null: [id]`.",
                        relativePath, null, "each check is a single-key mapping: `- <type>: <options>`"));
                    continue;
                }

                if (checkDict.Count > 1)
                {
                    var keys = string.Join(", ", checkDict.Keys.Order(StringComparer.Ordinal));
                    errors.Add(new PzError(PzErrorCode.InvalidCheck,
                        $"pipeline '{pipelineName}': one check per list item, got keys: {keys}.",
                        relativePath, null, "split into separate list items"));
                    continue;
                }

                var (checkType, checkValue) = checkDict.First();
                var checkValueDict = checkValue as Dictionary<string, object?>;

                // A check declared in dict form (e.g. `row_count: { min: 10 }`) may also carry
                // `columns:`, so not_null/unique can combine columns with `sample_values:`, e.g.
                // `not_null: { columns: [id], sample_values: false }`. The bare-list shape
                // (`not_null: [id, email]`) carries no sample_values and inherits the project default.
                // `columns`/`sample_values` are reserved keys, stripped from Options below so
                // row_count's flat `{min, max}` shape is unaffected by the reservation.
                var columns = checkValue is List<object?> columnsList
                    ? columnsList.Select(c => c?.ToString() ?? string.Empty).ToList()
                    : checkValueDict is not null && checkValueDict.TryGetValue("columns", out var columnsRaw)
                        && columnsRaw is List<object?> dictColumnsList
                        ? dictColumnsList.Select(c => c?.ToString() ?? string.Empty).ToList()
                        : new List<string>();
                // `column:` singular (dbt parity, used by freshness/accepted_values) normalizes into
                // Columns so node naming and canonical
                // hashing treat it exactly like `columns:`. Declaring both is refused below.
                if (checkValueDict is not null && checkValueDict.TryGetValue("column", out var columnRaw))
                {
                    if (columns.Count > 0)
                    {
                        errors.Add(new PzError(PzErrorCode.InvalidCheck,
                            $"pipeline '{pipelineName}': {checkType} check declares both 'column' and 'columns'.",
                            relativePath, null, "keep exactly one of the two keys."));
                    }
                    else if (columnRaw is string { Length: > 0 } singleColumn)
                    {
                        columns = [singleColumn];
                    }
                    else
                    {
                        errors.Add(new PzError(PzErrorCode.InvalidCheck,
                            $"pipeline '{pipelineName}': {checkType} check field 'column' must be a single column name.",
                            relativePath, null, "'column' must be a single column name, e.g. `column: updated_at`"));
                        columnShapeErrorIndices.Add(checks.Count);
                    }
                }

                var sampleValues = checkValueDict is not null ? TryGetBool(checkValueDict, "sample_values") : null;
                var options = checkValueDict is not null
                    ? checkValueDict.Where(kv => kv.Key is not ("column" or "columns" or "sample_values"))
                        .ToDictionary(kv => kv.Key, kv => kv.Value)
                    : new Dictionary<string, object?>();

                checks.Add(new CheckDef(checkType, columns, options, sampleValues));
            }

            ValidateChecks(checks, pipelineName, relativePath, errors, columnShapeErrorIndices);

            pipelines[pipelineName] = pipeline with
            {
                Materialization = materialization,
                Tags = tags,
                Checks = checks,
            };
        }
    }

    /// <summary>Aggregate compile-time validation of every
    /// check definition — unknown types, per-type option shapes, and unknown option keys are all
    /// PZ0113, reported together, naming the sidecar file and pipeline. Guarantees the executor
    /// relies on (exactly-one-column, parseable max_age, scalar values, named custom_sql) are
    /// established here, never re-checked at runtime.</summary>
    private static void ValidateChecks(List<CheckDef> checks, string pipelineName, string relativePath,
        List<PzError> errors, HashSet<int> columnShapeErrorIndices)
    {
        var customSqlNames = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < checks.Count; i++)
        {
            var check = checks[i];
            void Add(string message, string hint) => errors.Add(new PzError(PzErrorCode.InvalidCheck,
                $"pipeline '{pipelineName}': {message}.", relativePath, null, hint));

            switch (check.Type)
            {
                case "not_null" or "unique":
                    if (check.Columns.Count == 0)
                    {
                        Add($"{check.Type} check declares no columns",
                            $"list at least one column, e.g. `{check.Type}: [id]`");
                    }

                    RefuseUnknownOptions(check, [], Add);
                    break;

                case "row_count":
                    var hasMin = check.Options.TryGetValue("min", out var minRaw);
                    var hasMax = check.Options.TryGetValue("max", out var maxRaw);
                    if (!hasMin && !hasMax)
                    {
                        Add("row_count check needs at least one of min/max", "e.g. `row_count: { min: 1 }`");
                    }

                    if (hasMin && minRaw is not long)
                    {
                        Add($"row_count 'min' must be an integer, got '{minRaw}'", "e.g. `min: 1`");
                    }

                    if (hasMax && maxRaw is not long)
                    {
                        Add($"row_count 'max' must be an integer, got '{maxRaw}'", "e.g. `max: 1000`");
                    }

                    if (minRaw is long minValue && maxRaw is long maxValue && minValue > maxValue)
                    {
                        Add($"row_count min ({minValue}) exceeds max ({maxValue})", "make min <= max");
                    }

                    RefuseUnknownOptions(check, ["min", "max"], Add);
                    break;

                case "freshness":
                    // Skip when a malformed `column:` value already produced a PZ0113 above -- that's
                    // the one root cause; this check having zero Columns is a downstream symptom, not
                    // a second distinct problem (see columnShapeErrorIndices comment above).
                    if (check.Columns.Count != 1 && !columnShapeErrorIndices.Contains(i))
                    {
                        Add("freshness check requires exactly one 'column'",
                            "e.g. `freshness: { column: updated_at, max_age: 24h }`");
                    }

                    var maxAgeRaw = check.Options.TryGetValue("max_age", out var ma) ? ma?.ToString() : null;
                    if (maxAgeRaw is null || !DurationParser.TryParse(maxAgeRaw, out var maxAge) ||
                        maxAge <= TimeSpan.Zero)
                    {
                        Add($"freshness 'max_age' must be a positive duration, got '{maxAgeRaw ?? "(absent)"}'",
                            "use `<integer><unit>` with unit ms|s|m|h|d, e.g. `max_age: 24h`");
                    }

                    RefuseUnknownOptions(check, ["max_age"], Add);
                    break;

                case "accepted_values":
                    // Skip when a malformed `column:` value already produced a PZ0113 above -- see
                    // columnShapeErrorIndices comment above.
                    if (check.Columns.Count != 1 && !columnShapeErrorIndices.Contains(i))
                    {
                        Add("accepted_values check requires exactly one 'column'",
                            "e.g. `accepted_values: { column: status, values: [a, b] }`");
                    }

                    if (!check.Options.TryGetValue("values", out var valuesRaw) ||
                        valuesRaw is not List<object?> { Count: > 0 } values)
                    {
                        Add("accepted_values 'values' must be a non-empty list",
                            "e.g. `values: [pending, shipped]`");
                    }
                    else
                    {
                        foreach (var value in values.Where(v => v is not (string or long or double or bool)))
                        {
                            Add($"accepted_values 'values' entries must be scalars, got '{value ?? "null"}'",
                                "use strings, integers, floats, or booleans");
                        }
                    }

                    RefuseUnknownOptions(check, ["values"], Add);
                    break;

                case "custom_sql":
                    var name = check.Options.TryGetValue("name", out var n) ? n as string : null;
                    if (name is null || !IsValidCheckName(name))
                    {
                        Add($"custom_sql requires 'name' matching [a-z][a-z0-9_]*, got '{name ?? "(absent)"}'",
                            "e.g. `name: no_negative_totals` — it becomes the node name check_<pipeline>_<name>");
                    }
                    else if (!customSqlNames.Add(name))
                    {
                        Add($"duplicate custom_sql name '{name}'",
                            "each custom_sql check on a pipeline needs a distinct name");
                    }

                    if (!(check.Options.TryGetValue("sql", out var s) && s is string { Length: > 0 }))
                    {
                        Add("custom_sql requires non-empty 'sql'",
                            "a query returning VIOLATING rows, e.g. `sql: select * from staging.p where total < 0`");
                    }

                    if (check.Columns.Count > 0)
                    {
                        Add("custom_sql does not take columns", "put the column logic in the sql itself");
                    }

                    RefuseUnknownOptions(check, ["name", "sql"], Add);
                    break;

                default:
                    Add($"unknown check type '{check.Type}'",
                        "accepted: not_null | unique | row_count | freshness | accepted_values | custom_sql");
                    break;
            }
        }
    }

    private static void RefuseUnknownOptions(CheckDef check, string[] known, Action<string, string> add)
    {
        foreach (var key in check.Options.Keys
                     .Where(k => !known.Contains(k, StringComparer.Ordinal)).Order(StringComparer.Ordinal))
        {
            add($"{check.Type} check has unknown option '{key}'", known.Length == 0
                ? $"{check.Type} takes no options"
                : $"accepted options: {string.Join(", ", known.Order(StringComparer.Ordinal))}");
        }
    }

    private static bool IsValidCheckName(string name) =>
        name.Length > 0 && char.IsAsciiLetterLower(name[0]) &&
        name.All(ch => char.IsAsciiLetterLower(ch) || char.IsAsciiDigit(ch) || ch == '_');

    internal static bool RegisterName(Dictionary<string, string> seenNames, string name, string relativePath,
        List<PzError> errors, string kind)
    {
        if (seenNames.TryGetValue(name, out var firstFile))
        {
            errors.Add(new PzError(
                PzErrorCode.DuplicateName,
                $"Duplicate {kind} name '{name}' defined in {firstFile} and {relativePath}.",
                relativePath,
                null,
                $"rename one of the {kind}s so names are unique within the project."));
            return false;
        }

        seenNames[name] = relativePath;
        return true;
    }

    internal static string RelativePath(string projectDir, string fullPath) =>
        Path.GetRelativePath(projectDir, fullPath).Replace(Path.DirectorySeparatorChar, '/');

    internal static string? TryGetString(Dictionary<string, object?> dict, string key) =>
        dict.TryGetValue(key, out var value) ? value?.ToString() : null;

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
