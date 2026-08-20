using Pz.PackageManagement.Restore;

namespace Pz.PackageManagement.Tests.Restore;

public sealed class HostFeedsTests
{
    private static readonly IReadOnlyDictionary<string, string> NoEnv =
        new Dictionary<string, string>();

    [Fact]
    public void Explicit_list_wins_over_env_and_default()
    {
        var env = new Dictionary<string, string> { ["PZ_FEEDS"] = "https://ignored.example/index.json" };
        var feeds = HostFeeds.Resolve(["./local-feed"], env);
        Assert.Equal(["./local-feed"], feeds);
    }

    [Fact]
    public void Empty_explicit_list_falls_through_to_env()
    {
        var env = new Dictionary<string, string> { ["PZ_FEEDS"] = "https://a.example/v3/index.json" };
        var feeds = HostFeeds.Resolve([], env);
        Assert.Equal(["https://a.example/v3/index.json"], feeds);
    }

    [Fact]
    public void Env_splits_on_semicolon_trims_and_drops_empties()
    {
        var env = new Dictionary<string, string>
        {
            ["PZ_FEEDS"] = " https://a.example/v3/index.json ; ;./local-feed;",
        };
        var feeds = HostFeeds.Resolve(null, env);
        Assert.Equal(["https://a.example/v3/index.json", "./local-feed"], feeds);
    }

    [Fact]
    public void Blank_or_all_separator_env_falls_to_default()
    {
        var env = new Dictionary<string, string> { ["PZ_FEEDS"] = " ; ; " };
        Assert.Equal(HostFeeds.Default, HostFeeds.Resolve(null, env));
    }

    [Fact]
    public void Default_is_nuget_org()
    {
        Assert.Equal(["https://api.nuget.org/v3/index.json"], HostFeeds.Resolve(null, NoEnv));
    }
}
