using System.Collections;
using Pz.Core.Dag;
using Pz.Core.Loading;
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
        var project = ProjectLoader.Load(projectDir, env);
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
        var project = ProjectLoader.Load(projectDir, SnapshotEnvironment());
        ThrowOnPathEscapes(project, projectDir);
        return project;
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

    /// <summary>Same base_dir injection Run/Plan/Validate/Retry commands perform for the connectors
    /// that resolve relative paths against the project directory (localfiles, sqlite).</summary>
    internal static PzProject InjectLocalFilesBaseDir(PzProject project, string projectDir)
    {
        var connections = project.Connections
            .Select(s => s.Connector is "localfiles" or "sqlite"
                ? s with { Connection = new Dictionary<string, object?>(s.Connection) { ["base_dir"] = projectDir } }
                : s)
            .ToList();
        return project with { Connections = connections };
    }

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
