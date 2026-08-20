namespace Pz.PackageManagement.Restore;

/// <summary>
/// Host-only feeds: feeds are an operator trust decision, not pipeline
/// authoring, so they never come from project.yml. Precedence: an explicit list (the CLI's
/// --feeds), else PZ_FEEDS split on ';', else nuget.org. Public so a host embedding pz in-process
/// resolves feeds identically to `pz restore` on the command line.
/// </summary>
public static class HostFeeds
{
    public const string EnvVar = "PZ_FEEDS";

    public static readonly IReadOnlyList<string> Default = ["https://api.nuget.org/v3/index.json"];

    public static IReadOnlyList<string> Resolve(
        IReadOnlyList<string>? explicitFeeds, IReadOnlyDictionary<string, string> env)
    {
        if (explicitFeeds is { Count: > 0 })
        {
            return explicitFeeds;
        }

        if (env.TryGetValue(EnvVar, out var raw))
        {
            var parsed = raw.Split(';', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            if (parsed.Length > 0)
            {
                return parsed;
            }
        }

        return Default;
    }
}
