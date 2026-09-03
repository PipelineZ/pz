using Pz.Core.Model;
using Pz.Core.Validation;

namespace Pz.Mcp.Handlers;

/// <summary>Under `pz mcp`, a localfiles `path:`/`root:`/`base_dir:` that resolves outside the project
/// directory is refused with PZ0606,
/// matching the posture <see cref="Pz.Core.Validation.PzErrorCode.McpMutationTarget"/> already takes
/// for `../` in mutation targets — the agent surface operates only on files inside the project. The
/// plain CLI stays paths-are-trusted (your config, your files); the guard runs only in this
/// assembly, at <see cref="ProjectPhases"/>'s shared load seam (so verify/execute/introspect tools
/// all refuse uniformly on an escaping EXISTING config) and in <see cref="AuthoringTools"/>' mutation
/// pipeline (so an escaping PROPOSED block is refused before anything is written). Containment is
/// lexical (<see cref="Path.GetFullPath(string)"/>), the right bar for steering an agent — it is not
/// a symlink-proof security boundary, and does not try to be one.</summary>
public static class PathGuard
{
    private static readonly string[] PathKeys = ["path", "root", "base_dir", "data_path"];

    /// <summary>The connectors whose config carries project-relative file paths — the ones this guard
    /// walks. sqlite and duckdb count too: their connection `path:` is a database file exactly like a
    /// localfiles root. ducklake carries two path-shaped keys — `path:` (the catalog file, file-backed
    /// catalogs only) and `data_path:` (the lake's data directory, every catalog) — and `data_path:`
    /// may instead name an object store (a URL), which <see cref="Check"/> skips: a URL is never a
    /// project-relative path to begin with.</summary>
    internal static bool IsPathScoped(string? connector) => connector is "localfiles" or "sqlite" or "duckdb" or "ducklake";

    /// <summary>Every path-scoped connector's path-shaped value in the loaded project that resolves
    /// outside <paramref name="projectDir"/> — connection blocks, entity reads, and entity writes.</summary>
    public static IReadOnlyList<PzError> FindEscapes(PzProject project, string projectDir)
    {
        var errors = new List<PzError>();
        foreach (var connection in project.Connections)
        {
            if (!IsPathScoped(connection.Connector))
            {
                continue;
            }

            Check(connection.Connection, connection.Connector, $"connection '{connection.Name}'",
                connection.FilePath, errors, projectDir);
            foreach (var dataset in connection.Datasets)
            {
                Check(dataset.Options, connection.Connector, $"'{connection.Name}.{dataset.Name}'",
                    connection.FilePath, errors, projectDir);
            }

            foreach (var output in connection.Outputs)
            {
                Check(output.Options, connection.Connector, $"output '{connection.Name}.{output.Name}'",
                    connection.FilePath, errors, projectDir);
            }
        }

        return errors;
    }

    /// <summary>Same guard over a PROPOSED entity block, before it is written — the authoring twin of
    /// <see cref="CredentialGuard.FindLiteralCredentials(Dictionary{string, object?})"/>.</summary>
    public static IReadOnlyList<PzError> FindEscapes(string connector, string connection, string entity,
        IReadOnlyDictionary<string, object?>? read, IReadOnlyDictionary<string, object?>? write,
        string projectDir)
    {
        var errors = new List<PzError>();
        if (read is not null)
        {
            Check(read, connector, $"proposed read for '{connection}.{entity}'", "connections.yml", errors, projectDir);
        }

        if (write is not null)
        {
            Check(write, connector, $"proposed write for '{connection}.{entity}'", "connections.yml", errors, projectDir);
        }

        return errors;
    }

    /// <summary>Same guard over a PROPOSED connection block (its `root:`/`base_dir:`/`path:`).</summary>
    public static IReadOnlyList<PzError> FindEscapes(string connector, string connection,
        IReadOnlyDictionary<string, object?> block, string projectDir)
    {
        var errors = new List<PzError>();
        Check(block, connector, $"proposed connection '{connection}'", "connections.yml", errors, projectDir);
        return errors;
    }

    private static void Check(IReadOnlyDictionary<string, object?> options, string connector, string subject,
        string file, List<PzError> errors, string projectDir)
    {
        foreach (var key in PathKeys)
        {
            if (!options.TryGetValue(key, out var raw) || raw is not string value)
            {
                continue;
            }

            if (value.Contains("://", StringComparison.Ordinal))
            {
                continue; // an object-store URL (ducklake's data_path:) is never a project-relative path
            }

            if (Escapes(projectDir, value))
            {
                errors.Add(new PzError(PzErrorCode.McpPathEscapesProject,
                    $"{subject}: {connector} {key} '{value}' resolves outside the project directory",
                    file, null,
                    "pz mcp only operates on files inside the project; use a path under the project " +
                    "root, or run the pz CLI directly if reading outside the project is intended"));
            }
        }
    }

    private static bool Escapes(string projectDir, string value)
    {
        try
        {
            var root = Path.GetFullPath(projectDir);
            var resolved = Path.GetFullPath(Path.Combine(root, value));
            var rootWithSeparator = root.EndsWith(Path.DirectorySeparatorChar)
                ? root
                : root + Path.DirectorySeparatorChar;
            return resolved != root && !resolved.StartsWith(rootWithSeparator, StringComparison.Ordinal);
        }
        catch (Exception ex) when (ex is ArgumentException or PathTooLongException or NotSupportedException)
        {
            return false; // unresolvable is not this guard's finding — the connector will refuse it itself
        }
    }
}
