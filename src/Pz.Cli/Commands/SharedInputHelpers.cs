using System.Collections;
using System.Text.Json;
using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;
using Pz.DuckDb;
using Pz.Engine.Execution;
using Pz.Engine.Validation;
using Pz.PackageManagement.Hosting;

namespace Pz.Cli.Commands;

/// <summary>Shared by every verb that loads a project (compile, plan, run, test, validate, restore,
/// retry): snapshots the current process's environment variables and parses `--vars`'s raw JSON text,
/// in the exact shapes <c>ProjectLoader.Load</c> expects. Kept in one place, following
/// <see cref="SharedOptions"/>'s precedent, so the command files cannot drift apart.</summary>
internal static class SharedInputHelpers
{
    /// <summary>Applies the project-directory anchor every verb applies before compiling, with the
    /// package-manifest half looked up from this project's own <c>.pz/packages</c> — see
    /// <see cref="ProjectDirectoryAnchor"/> for which connectors get one and why it is declared rather
    /// than matched by name.</summary>
    internal static Pz.Core.Model.PzProject AnchorToProjectDir(Pz.Core.Model.PzProject project, string projectDir) =>
        ProjectDirectoryAnchor.Inject(
            project, projectDir,
            PackageManifests.AnchoredConnectorNames(Path.Combine(projectDir, ".pz", "packages")));

    /// <summary>Snapshots every environment variable visible to this process into a plain
    /// string-to-string map.</summary>
    internal static Dictionary<string, string> SnapshotEnvironment()
    {
        var env = new Dictionary<string, string>();
        foreach (DictionaryEntry entry in Environment.GetEnvironmentVariables())
        {
            if (entry.Key is string key)
            {
                env[key] = entry.Value?.ToString() ?? string.Empty;
            }
        }

        return env;
    }

    /// <summary>Parses `--vars`'s raw JSON text into the override map <c>ProjectLoader.Load</c>
    /// consumes. A non-object root (<c>--vars '[1,2]'</c>) and non-JSON text (<c>--vars 'not json'</c>)
    /// both surface as one <see cref="PzValidationException"/> (PZ0102) — that is the only exception type
    /// every calling command's `catch` clause handles, so any other shape would crash the process instead
    /// of exiting cleanly as a config error.</summary>
    internal static Dictionary<string, object?>? ParseVars(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return null;
        }

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(json);
        }
        catch (JsonException ex)
        {
            throw VarsError($"--vars is not valid JSON: {ex.Message}");
        }

        using (document)
        {
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw VarsError($"--vars must be a JSON object, but was {DescribeKind(document.RootElement.ValueKind)}");
            }

            return (Dictionary<string, object?>)ToObject(document.RootElement)!;
        }
    }

    /// <summary>Prints every non-blocking <see cref="PzWarning"/> from a successful
    /// <c>DagCompiler.Compile</c> to stderr, one line each, in the shape
    /// <c>warning: CODE message (file) — hint</c> (file/hint segments omitted when absent). Shared by
    /// `pz run`, `pz plan`, and `pz validate` — each calls this immediately after a successful compile,
    /// before proceeding. Never touches the exit code: warnings advise, they don't block.</summary>
    internal static void WriteWarnings(IReadOnlyList<PzWarning> warnings)
    {
        foreach (var w in warnings)
        {
            Console.Error.WriteLine(
                $"warning: {w.Code} {w.Message}" +
                (w.File is null ? "" : $" ({w.File})") +
                (w.Hint is null ? "" : $" — {w.Hint}"));
        }
    }

    /// <summary>Loads, anchors and compiles the project the way `pz test` and `pz retry` start: any
    /// load/compile failure (or a bad <c>--state-url</c>) is printed and reported as <c>false</c>, which the
    /// caller maps to <see cref="ExitCodes.ConfigError"/>; on success the compile notices are printed as
    /// <c>note:</c> lines.</summary>
    internal static bool TryLoadAndCompile(
        string projectDir, string? varsJson, string? stateUrlRaw,
        out PzProject project, out CompiledDag dag)
    {
        project = null!;
        dag = null!;
        var compileNotices = new List<string>();
        try
        {
            var env = SnapshotEnvironment();
            project = AnchorToProjectDir(ProjectLoader.Load(projectDir, env, ParseVars(varsJson)), projectDir);
            if (!StateUrlOverride.TryApply(project, stateUrlRaw, env, out project, out var stateUrlError))
            {
                Console.Error.WriteLine($"error: {stateUrlError}");
                return false;
            }

            var renderCtx = new RenderContext(project, Guid.NewGuid().ToString("N"), DateTimeOffset.UtcNow) { Env = env };
            using var sqlAst = new DuckDbSqlAstReader();
            dag = DagCompiler.Compile(project, renderCtx, compileNotices, sqlAst);
        }
        catch (PzValidationException ex)
        {
            WriteErrors(ex.Errors);
            return false;
        }

        foreach (var notice in compileNotices)
        {
            Console.WriteLine($"note: {notice}");
        }

        return true;
    }

    /// <summary>The `project.yml is missing` refusal shared by the verbs that read only the <c>state:</c>
    /// key (`pz state`, `pz runs`, `pz clean`): they must not be pointable at a stray `.pz`.</summary>
    internal static bool ProjectYmlExists(string verb, string projectDir)
    {
        if (File.Exists(Path.Combine(projectDir, "project.yml")))
        {
            return true;
        }

        Console.Error.WriteLine(
            $"error {PzErrorCode.YamlShape}: project.yml is missing — run pz {verb} from a project directory " +
            "or pass --project <dir>");
        return false;
    }

    /// <summary>Resolves the state backends from project.yml's <c>state:</c> key alone
    /// (<see cref="ProjectLoader.LoadStateOnly"/>), so a broken pipeline or connections file never stops
    /// a state-only verb. Prints the error and returns <c>false</c> when resolution fails.</summary>
    internal static bool TryResolveStateBackends(string projectDir, TimeProvider time, out StateBackends backends)
    {
        backends = null!;
        try
        {
            var (name, state, connections) = ProjectLoader.LoadStateOnly(projectDir, SnapshotEnvironment());
            backends = StateBackendFactory.Create(state, name, connections, projectDir, time);
            return true;
        }
        catch (PzValidationException ex)
        {
            WriteErrors(ex.Errors);
        }
        catch (PzConfigException ex)
        {
            Console.Error.WriteLine($"error {ex.Error}");
        }

        return false;
    }

    /// <summary>Loads and compiles the project without anchoring or state overrides -- the read-only
    /// verbs' (`pz compile`, `pz ls`) start -- printing the compile notices as <c>note:</c> lines. Load and
    /// compile failures surface as <see cref="PzValidationException"/>.</summary>
    internal static (PzProject Project, CompiledDag Dag) LoadAndCompile(string projectDir, string? varsJson)
    {
        var env = SnapshotEnvironment();
        var project = ProjectLoader.Load(projectDir, env, ParseVars(varsJson));
        var renderCtx = new RenderContext(project, Guid.NewGuid().ToString("N"), DateTimeOffset.UtcNow) { Env = env };
        var compileNotices = new List<string>();
        using var sqlAst = new DuckDbSqlAstReader();
        var dag = DagCompiler.Compile(project, renderCtx, compileNotices, sqlAst);
        foreach (var notice in compileNotices)
        {
            Console.WriteLine($"note: {notice}");
        }

        return (project, dag);
    }

    /// <summary>The implicit pre-run SQL dry-compile (validation tier 4): prints any errors and returns
    /// <c>false</c> so a broken pipeline is rejected before a run starts, never as a node failure inside
    /// one. Quiet on success.</summary>
    internal static async Task<bool> DryCompilePassesAsync(CompiledDag dag, PzProject project, CancellationToken ct)
    {
        var dry = await SqlDryCompiler.RunAsync(dag, project.Extensions, ct);
        if (dry.Errors.Count == 0)
        {
            return true;
        }

        WriteErrors(dry.Errors);
        return false;
    }

    /// <summary>Prints each error to stderr as <c>error CODE: message</c>, one per line -- the shape every
    /// verb uses for a failed load, compile or validation.</summary>
    internal static void WriteErrors(IEnumerable<PzError> errors)
    {
        foreach (var error in errors)
        {
            Console.Error.WriteLine($"error {error}");
        }
    }

    private static PzValidationException VarsError(string message) => new([new PzError(
        PzErrorCode.VarsInvalid,
        message,
        null,
        null,
        "--vars must be a JSON object, e.g. '{\"key\": 1}'")]);

    private static string DescribeKind(JsonValueKind kind) => kind switch
    {
        JsonValueKind.Array => "a JSON array",
        JsonValueKind.String => "a JSON string",
        JsonValueKind.Number => "a JSON number",
        JsonValueKind.True or JsonValueKind.False => "a JSON boolean",
        JsonValueKind.Null => "JSON null",
        _ => "not a JSON object",
    };

    private static object? ToObject(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Object => element.EnumerateObject().ToDictionary(p => p.Name, p => ToObject(p.Value)),
        JsonValueKind.Array => element.EnumerateArray().Select(ToObject).ToList(),
        JsonValueKind.String => element.GetString(),
        // The false branch casts to object deliberately: a conditional over a long branch and a double
        // branch widens to double (long -> double is implicit, the reverse is not), so every whole
        // number was boxed as double -- not the long ProjectLoader and every kwarg parser expect.
        JsonValueKind.Number => element.TryGetInt64(out var longValue) ? longValue : (object)element.GetDouble(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Null or JsonValueKind.Undefined => null,
        _ => throw new ArgumentOutOfRangeException(nameof(element), element.ValueKind, "unsupported JSON value kind"),
    };
}
