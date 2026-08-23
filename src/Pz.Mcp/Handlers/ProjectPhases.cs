using System.Collections;
using Pz.Core.Dag;
using Pz.Core.Loading;
using Pz.PackageManagement.Hosting;
using Pz.Core.Model;
using Pz.Core.Templating;
using Pz.Core.Validation;

namespace Pz.Mcp.Handlers;

/// <summary>The load→compile composition every CLI verb that loads a project performs
/// (CompileCommand/ValidateCommand/PlanCommand's own opening lines), stripped of every
/// <c>Console.*</c> call (handlers must be console-free — notices/warnings ride the envelope
/// result instead). Pz.Mcp cannot reference Pz.Cli (Pz.Cli references Pz.Mcp, not the reverse), so
/// this deliberately duplicates the shape of <c>Pz.Cli.Commands.SharedInputHelpers.SnapshotEnvironment</c>
/// rather than calling it — same semantics (a plain foreach over
/// <see cref="Environment.GetEnvironmentVariables()"/>, last-value-wins per key), not the same
/// assembly.</summary>
internal static class ProjectPhases
{
    /// <summary>Phases load→compile exactly as the CLI verbs compose them (ProjectLoader.Load →
    /// DagCompiler.Compile with the DuckDB AST reader). Throws PzValidationException with the
    /// aggregate error list — callers turn that into an error envelope.</summary>
    internal static (PzProject Project, CompiledDag Dag, IReadOnlyList<string> Notices) LoadAndCompile(
        string projectDir)
    {
        var env = SnapshotEnvironment();
        var project = LoadWithMcpHints(projectDir, env);
        ThrowOnPathEscapes(project, projectDir);
        var ctx = new RenderContext(project, Guid.NewGuid().ToString("N"), DateTimeOffset.UtcNow) { Env = env };
        var notices = new List<string>();
        var dag = DagCompiler.Compile(project, ctx, notices, new Pz.DuckDb.DuckDbSqlAstReader());
        return (project, dag, notices);
    }

    /// <summary>Bare load, no compile — for handlers that only need connections/pipelines metadata
    /// (pz_connector_reference, pz_state) or want project-level info even when compile itself would
    /// fail (pz_project_overview's fallback). Throws PzValidationException on a broken project.yml/
    /// connections.yml, exactly like <see cref="LoadAndCompile"/>'s first step.</summary>
    internal static PzProject Load(string projectDir)
    {
        var project = LoadWithMcpHints(projectDir, SnapshotEnvironment());
        ThrowOnPathEscapes(project, projectDir);
        return project;
    }

    /// <summary><see cref="ProjectLoader.Load"/> with the load-path hints that name a CLI verb
    /// rewritten for a caller that has no shell. An MCP client cannot act on "run 'pz init &lt;name&gt;'"
    /// — it can only call a tool, or ask its human — so the hint has to say which. The rewrite is
    /// keyed on the hint text the loader writes; if that text ever changes the agent simply gets the
    /// CLI phrasing back, which is a worse hint rather than a wrong one.</summary>
    private static PzProject LoadWithMcpHints(string projectDir, Dictionary<string, string> env)
    {
        try
        {
            return ProjectLoader.Load(projectDir, env);
        }
        catch (PzValidationException ex)
        {
            throw new PzValidationException([.. ex.Errors.Select(RewriteHint)]);
        }
    }

    private static PzError RewriteHint(PzError error)
    {
        if (error.Hint is not { } hint)
        {
            return error;
        }

        if (hint.Contains("pz init", StringComparison.Ordinal))
        {
            return error with
            {
                Hint = "this directory is not a pz project -- call pz_init_project to scaffold one " +
                    "here, or ask the user to point the server at an existing project directory",
            };
        }

        if (error.Code == PzErrorCode.UndeclaredEnvVar)
        {
            // The restart is the non-obvious half: `pz mcp` snapshots the environment per call, but
            // from ITS OWN process, which inherited the client's environment at launch. Exporting the
            // variable in some other shell changes nothing until the server is restarted.
            return error with
            {
                Hint = hint + " An MCP server reads the environment of the process it was launched " +
                    "in, so ask the user to set it and restart the pz MCP server -- setting it " +
                    "elsewhere will not reach this server.",
            };
        }

        return error;
    }

    /// <summary>Every MCP tool that touches the project refuses an escaping localfiles path
    /// uniformly, through this one seam — see
    /// <see cref="PathGuard"/> for the posture. Same aggregate-exception shape as a load failure, so
    /// every handler's existing PzValidationException catch envelopes it with no extra code.</summary>
    private static void ThrowOnPathEscapes(PzProject project, string projectDir)
    {
        var escapes = PathGuard.FindEscapes(project, projectDir);
        if (escapes.Count > 0)
        {
            throw new PzValidationException(escapes);
        }
    }

    /// <summary>The same project-directory anchor Run/Plan/Validate/Retry apply, through the same two
    /// helpers rather than a second copy of the rule: which connectors get one is declared (a builtin
    /// in <see cref="ProjectDirectoryAnchor.BuiltinAnchoredConnectors"/>, a package connector in its own
    /// manifest), never matched by name here.</summary>
    internal static PzProject InjectProjectDirectoryAnchor(PzProject project, string projectDir) =>
        ProjectDirectoryAnchor.Inject(
            project, projectDir,
            PackageManifests.AnchoredConnectorNames(Path.Combine(projectDir, ".pz", "packages")));

    /// <summary>Same semantics as Pz.Cli.Commands.SharedInputHelpers.SnapshotEnvironment: a plain
    /// foreach over every environment variable visible to this process into a string-to-string map
    /// (not a LINQ ToDictionary, which throws on a duplicate key — a real possibility on Windows,
    /// where env var names are case-insensitive but GetEnvironmentVariables can still yield two
    /// differently-cased entries for the same logical variable).</summary>
    private static Dictionary<string, string> SnapshotEnvironment()
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
}
