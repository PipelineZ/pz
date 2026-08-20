namespace Pz.Core.Model;

/// <summary>project.yml's top-level <c>state:</c>, already resolved against the <c>PZ_STATE_*</c>
/// environment defaults. Never null on <see cref="PzProject.State"/> — an absent block resolves to
/// <see cref="Local"/>.
///
/// <see cref="BackendSource"/> exists so the run header and `pz state show` can print WHERE the backend
/// came from ("project.yml", an environment variable's name, or "default"). The value is ambient by
/// design when it comes from the environment, so its provenance is printed rather than hidden — that is
/// what keeps env-supplied defaults inside the no-silent-failures rule.
///
/// For <see cref="Http"/>: <see cref="Url"/> is the run-scoped state endpoint a server handed this run,
/// <see cref="Token"/> its bearer credential.</summary>
public sealed record StateConfig(
    string Backend,
    string? Connection,
    string? ConnectionString,
    string Schema,
    bool Artifacts,
    bool Events,
    string BackendSource,
    string? Url = null,
    string? Token = null)
{
    public const string Local = "local";
    public const string SqlServer = "sqlserver";
    public const string Http = "http";

    public static StateConfig Default { get; } =
        new(Local, null, null, "pz", Artifacts: false, Events: false, BackendSource: "default");

    public bool IsLocal => string.Equals(Backend, Local, StringComparison.Ordinal);

    public bool IsHttp => string.Equals(Backend, Http, StringComparison.Ordinal);
}
