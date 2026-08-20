using Pz.Core.Model;

namespace Pz.Cli.Commands;

/// <summary>A system dispatching pz never deletes or rewrites
/// the host's environment; it states its intent explicitly, and an explicit CLI argument outranks both
/// project.yml's <c>state:</c> block and every inherited <c>PZ_STATE_*</c> variable. The bearer
/// credential deliberately stays in <c>PZ_STATE_TOKEN</c>: argv is visible to every process on the host
/// (`ps`), so a secret never rides it.</summary>
internal static class StateUrlOverride
{
    /// <summary>Null/blank url: <paramref name="result"/> is <paramref name="project"/> unchanged and
    /// the existing project.yml/environment resolution stands. Otherwise the run's state becomes the
    /// http backend at the given absolute http(s) URL, token from PZ_STATE_TOKEN, artifacts/events off
    /// (the http backend refuses them anyway).</summary>
    public static bool TryApply(PzProject project, string? stateUrlRaw, IReadOnlyDictionary<string, string> env,
        out PzProject result, out string? error)
    {
        result = project;
        error = null;
        if (string.IsNullOrWhiteSpace(stateUrlRaw))
        {
            return true;
        }

        if (!Uri.TryCreate(stateUrlRaw, UriKind.Absolute, out var uri)
            || uri.Scheme is not ("http" or "https"))
        {
            error = $"invalid --state-url value '{stateUrlRaw}' (expected an absolute http(s) URL)";
            return false;
        }

        env.TryGetValue("PZ_STATE_TOKEN", out var token);
        result = project with
        {
            State = new StateConfig(StateConfig.Http, null, null, "pz",
                Artifacts: false, Events: false, BackendSource: "--state-url", Url: stateUrlRaw,
                Token: string.IsNullOrWhiteSpace(token) ? null : token),
        };
        return true;
    }
}
