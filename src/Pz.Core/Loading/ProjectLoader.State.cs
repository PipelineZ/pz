using System.Globalization;
using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Core.Loading;

/// <summary>project.yml's <c>state:</c> block and the credentials it names.</summary>
public static partial class ProjectLoader
{
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
        [StateConfig.Http] = ["backend", "url", "artifacts", "events", "timeout_seconds"],
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

        var timeoutSeconds = ParseStateTimeoutSeconds(stateYaml, env, relativePath, errors);

        return new StateConfig(backend, connection, connectionString, schema ?? "pz", artifacts, events,
            backendSource, url, token, timeoutSeconds);
    }

    private const int MinStateTimeoutSeconds = 1;
    private const int MaxStateTimeoutSeconds = 3600;

    /// <summary>`backend: http`'s <c>state.timeout_seconds</c> (or <c>PZ_STATE_TIMEOUT_SECONDS</c>):
    /// null means "the HTTP client's own default" (unchanged behaviour), never a value substituted
    /// silently for an absent one. A project.yml value that is not a whole number, or the environment
    /// variable's text not parsing as one, is PZ0124 rather than a silently-ignored knob, matching every
    /// other state.* key's error discipline.</summary>
    private static int? ParseStateTimeoutSeconds(Dictionary<string, object?> stateYaml,
        IReadOnlyDictionary<string, string> env, string relativePath, List<PzError> errors)
    {
        int? seconds;
        if (stateYaml.TryGetValue("timeout_seconds", out var raw) && raw is not null)
        {
            seconds = TryGetInt(stateYaml, "timeout_seconds");
            if (seconds is null)
            {
                errors.Add(new PzError(PzErrorCode.StateBackendConfigInvalid,
                    $"{relativePath}: state.timeout_seconds must be an integer (got '{raw}').",
                    relativePath, null, "timeout_seconds: 30"));
                return null;
            }
        }
        else if (env.TryGetValue("PZ_STATE_TIMEOUT_SECONDS", out var fromEnv) && !string.IsNullOrWhiteSpace(fromEnv))
        {
            if (!int.TryParse(fromEnv, NumberStyles.None, CultureInfo.InvariantCulture, out var envSeconds))
            {
                errors.Add(new PzError(PzErrorCode.StateBackendConfigInvalid,
                    $"PZ_STATE_TIMEOUT_SECONDS must be an integer (got '{fromEnv}').", relativePath, null,
                    "set PZ_STATE_TIMEOUT_SECONDS to a positive integer number of seconds, or unset it"));
                return null;
            }

            seconds = envSeconds;
        }
        else
        {
            return null;
        }

        if (seconds is { } s && (s < MinStateTimeoutSeconds || s > MaxStateTimeoutSeconds))
        {
            errors.Add(new PzError(PzErrorCode.StateBackendConfigInvalid,
                $"{relativePath}: state.timeout_seconds must be between {MinStateTimeoutSeconds} and " +
                $"{MaxStateTimeoutSeconds} (got {s}).", relativePath, null,
                $"timeout_seconds must be between {MinStateTimeoutSeconds} and {MaxStateTimeoutSeconds}"));
            return null;
        }

        return seconds;
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
        List<PzError> errors, List<PzWarning> warnings)
    {
        if (state.IsLocal)
        {
            return;
        }

        if (state.IsHttp)
        {
            ValidateStateUrl(state, errors, warnings);
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
    private static void ValidateStateUrl(StateConfig state, List<PzError> errors, List<PzWarning> warnings)
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
            return;
        }

        // A bearer token over a plaintext connection is readable by anything on the network path --
        // worth a loud warning, not silence, but not a hard refusal either: an operator may have a
        // deliberate reason (a VPN-only endpoint) this check cannot see. Loopback is exempt: that
        // traffic never leaves the host.
        if (parsed.Scheme == Uri.UriSchemeHttp && !parsed.IsLoopback && !string.IsNullOrWhiteSpace(state.Token))
        {
            warnings.Add(new PzWarning(PzErrorCode.HttpStateTokenOverInsecureUrl,
                "state.url uses http:// with a bearer token configured (PZ_STATE_TOKEN) -- the token " +
                "travels in cleartext.", "project.yml", null,
                "use an https:// state.url, or drop the token if this endpoint genuinely needs none"));
        }
    }
}
