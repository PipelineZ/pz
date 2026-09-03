using Pz.Core.Model;

namespace Pz.Core.Loading;

/// <summary>Injects <c>base_dir = &lt;projectDir&gt;</c> into the <c>connection:</c> config of every
/// connector that asked for a project-directory anchor, so a relative <c>root:</c>/<c>path:</c> resolves
/// against the project rather than against wherever <c>pz</c> happened to be invoked from.
///
/// <para>WHY THE HOST AND NOT THE CONNECTOR: only the caller knows the project directory. A connector
/// stays pure — it resolves relative paths against a connection option named <c>base_dir</c> and assumes
/// no project layout of its own.</para>
///
/// <para>WHO GETS ONE IS DECLARED, NOT MATCHED BY NAME. <see cref="BuiltinAnchoredConnectors"/> is the
/// builtin half; a connector loaded from a package declares <c>"projectDirectoryAnchor": true</c> in its
/// <c>pz.connector.json</c> and the host passes its name in through
/// <paramref name="declaredAnchoredConnectors"/>. Opt-in in both directions: a connector that says
/// nothing receives nothing, so the injected option can never trip a <c>ConnectionConfigSchema</c>'s
/// <c>additionalProperties: false</c>. An opted-in connector must NOT declare <c>base_dir</c> in that
/// schema: tier-3 validation runs on the connection as the user wrote it, before this injection, so the
/// key is never checked against the schema anyway, and declaring it would advertise a host-owned option
/// as authorable -- a user-written value would validate and then be silently overwritten here.</para>
///
/// <para>Safe to apply before compiling: neither <c>SourceLoad</c> nor <c>SinkWrite</c> node IDs are
/// derived from <c>Connection</c> — <c>DagCompiler</c> canonicalizes only dataset/output options and
/// columns.</para></summary>
public static class ProjectDirectoryAnchor
{
    /// <summary>The connection option a connector reads its anchor from.</summary>
    public const string OptionName = "base_dir";

    /// <summary>Builtin connector names that resolve a relative path against a project-directory anchor
    /// — the builtin half of the same opt-in a package connector declares in its manifest. A connector
    /// belongs here only if it reads <see cref="OptionName"/> when resolving a relative path, and none of
    /// them declares that key in its <c>ConnectionConfigSchema</c> (see the class remarks).</summary>
    public static readonly IReadOnlySet<string> BuiltinAnchoredConnectors =
        new HashSet<string>(StringComparer.Ordinal) { "localfiles", "sqlite", "duckdb", "ducklake" };

    /// <summary><paramref name="declaredAnchoredConnectors"/> are the names read out of materialized
    /// package manifests (see <c>Pz.PackageManagement.Hosting.PackageManifests</c>); pass none for a
    /// project with no package connectors.</summary>
    public static PzProject Inject(
        PzProject project, string projectDir, IEnumerable<string>? declaredAnchoredConnectors = null)
    {
        var anchored = new HashSet<string>(BuiltinAnchoredConnectors, StringComparer.Ordinal);
        if (declaredAnchoredConnectors is not null)
        {
            anchored.UnionWith(declaredAnchoredConnectors);
        }

        var connections = project.Connections
            .Select(c => anchored.Contains(c.Connector)
                ? c with { Connection = WithBaseDir(c.Connection, projectDir) }
                : c)
            .ToList();
        return project with { Connections = connections };
    }

    private static IReadOnlyDictionary<string, object?> WithBaseDir(
        IReadOnlyDictionary<string, object?> connection, string projectDir) =>
        new Dictionary<string, object?>(connection) { [OptionName] = projectDir };
}
