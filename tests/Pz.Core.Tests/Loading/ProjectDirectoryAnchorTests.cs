using Pz.Core.Loading;
using Pz.Core.Model;

namespace Pz.Core.Tests.Loading;

/// <summary>Which connectors get a project-directory anchor is DECLARED, not matched by name. Before
/// that, a connector loaded from a package could never receive one, so a relative <c>root:</c> in its
/// config could only resolve against the process working directory — meaning the same project wrote to
/// a different place depending on where <c>pz</c> was invoked from.</summary>
public sealed class ProjectDirectoryAnchorTests
{
    private const string ProjectDir = "/projects/demo";

    private static PzProject ProjectWith(params string[] connectorNames) =>
        new("demo", "1", new EngineConfig(), new Dictionary<string, object?>(), [],
            connectorNames
                .Select((connector, i) => new ConnectionDef(
                    $"c{i}", connector,
                    new Dictionary<string, object?> { ["root"] = "data" },
                    [], "connections.yml"))
                .ToList(),
            []);

    private static object? BaseDirOf(PzProject project, string connector) =>
        project.Connections.Single(c => c.Connector == connector)
            .Connection.TryGetValue(ProjectDirectoryAnchor.OptionName, out var v) ? v : null;

    [Fact]
    public void Builtin_anchored_connectors_receive_the_project_dir()
    {
        var anchored = ProjectDirectoryAnchor.Inject(
            ProjectWith("localfiles", "sqlite", "duckdb", "ducklake", "iceberg"), ProjectDir);

        Assert.Equal(ProjectDir, BaseDirOf(anchored, "localfiles"));
        Assert.Equal(ProjectDir, BaseDirOf(anchored, "sqlite"));
        Assert.Equal(ProjectDir, BaseDirOf(anchored, "duckdb"));
        Assert.Equal(ProjectDir, BaseDirOf(anchored, "ducklake"));
        Assert.Equal(ProjectDir, BaseDirOf(anchored, "iceberg"));
    }

    /// <summary>Opt-in in both directions: a connector that never asked receives nothing. This is what
    /// keeps the injected option from tripping a <c>ConnectionConfigSchema</c> with
    /// <c>additionalProperties: false</c>, which every connector that has not declared
    /// <c>base_dir</c> has.</summary>
    [Fact]
    public void A_connector_that_did_not_ask_is_untouched()
    {
        var anchored = ProjectDirectoryAnchor.Inject(ProjectWith("postgres", "http"), ProjectDir);

        Assert.Null(BaseDirOf(anchored, "postgres"));
        Assert.Null(BaseDirOf(anchored, "http"));
    }

    /// <summary>The gap this closes: a third-party connector, matched by nothing, declaring the anchor
    /// in its own manifest.</summary>
    [Fact]
    public void A_declared_package_connector_receives_the_project_dir()
    {
        var anchored = ProjectDirectoryAnchor.Inject(
            ProjectWith("deltalake", "postgres"), ProjectDir, ["deltalake"]);

        Assert.Equal(ProjectDir, BaseDirOf(anchored, "deltalake"));
        Assert.Null(BaseDirOf(anchored, "postgres"));
    }

    /// <summary>The anchor is added, never substituted for what the author wrote: the rest of the
    /// connection config has to survive intact.</summary>
    [Fact]
    public void Existing_connection_config_survives()
    {
        var anchored = ProjectDirectoryAnchor.Inject(ProjectWith("localfiles"), ProjectDir);

        Assert.Equal("data", anchored.Connections.Single().Connection["root"]);
    }

    /// <summary>Injection returns a new project rather than mutating the loaded one —
    /// <c>ValidateCommand</c> depends on it, deliberately validating tier 3 against the config as the
    /// user wrote it while opening connectors against the anchored copy.</summary>
    [Fact]
    public void Injection_does_not_mutate_the_input_project()
    {
        var original = ProjectWith("localfiles");

        ProjectDirectoryAnchor.Inject(original, ProjectDir);

        Assert.Null(BaseDirOf(original, "localfiles"));
    }
}
