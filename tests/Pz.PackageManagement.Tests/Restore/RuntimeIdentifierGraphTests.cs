using Pz.PackageManagement.Restore;

namespace Pz.PackageManagement.Tests.Restore;

/// <summary>The expansion is asserted against the portable RID graph's own import lists, RID by RID —
/// the whole point of the type is that a package shipping only <c>linux-x64</c> is reachable from the
/// linux RIDs NuGet considers its descendants, and unreachable from the ones it does not.</summary>
public sealed class RuntimeIdentifierGraphTests
{
    [Fact]
    public void Host_rid_is_always_first()
    {
        Assert.Equal("linux-x64", RuntimeIdentifierGraph.Expand("linux-x64")[0]);
        Assert.Equal("win-x64", RuntimeIdentifierGraph.Expand("win-x64")[0]);
    }

    [Fact]
    public void Musl_reaches_the_glibc_build_of_the_same_architecture()
    {
        var chain = RuntimeIdentifierGraph.Expand("linux-musl-x64");

        Assert.Equal(
            ["linux-musl-x64", "linux-musl", "linux-x64", "linux", "unix-x64", "unix", "any", "base"],
            chain);

        // Order is the contract, not just membership: the musl-specific build must win over the glibc
        // one when a package ships both.
        Assert.True(
            chain.ToList().IndexOf("linux-musl-x64") < chain.ToList().IndexOf("linux-x64"));
    }

    [Theory]
    [InlineData("linux-arm64", "unix-arm64")]
    [InlineData("osx-arm64", "unix-arm64")]
    [InlineData("android-x64", "linux-bionic-x64")]
    [InlineData("maccatalyst-arm64", "ios-arm64")]
    public void Architecture_is_carried_down_the_os_ancestry(string rid, string expectedAncestor) =>
        Assert.Contains(expectedAncestor, RuntimeIdentifierGraph.Expand(rid));

    /// <summary>A package shipping only linux assets must stay invisible to a macOS or Windows host —
    /// a fallback that reached across operating systems would install an unloadable library.</summary>
    [Theory]
    [InlineData("osx-arm64")]
    [InlineData("osx-x64")]
    [InlineData("win-x64")]
    public void Other_operating_systems_never_reach_linux(string rid) =>
        Assert.DoesNotContain("linux-x64", RuntimeIdentifierGraph.Expand(rid));

    /// <summary>Windows sits directly under the architecture-less root, so it has no
    /// architecture-carrying ancestor to fall back to — win-x64 must not "reach" win-arm64.</summary>
    [Fact]
    public void Windows_has_no_cross_architecture_fallback()
    {
        var chain = RuntimeIdentifierGraph.Expand("win-x64");

        Assert.Equal(["win-x64", "win", "any", "base"], chain);
    }

    /// <summary>A version-qualified legacy RID is not part of the portable graph. Expanding it to itself
    /// alone is the honest outcome — the alternative is inventing an ancestry — and it preserves the
    /// exact-match behavior for anything this table does not recognize.</summary>
    [Theory]
    [InlineData("ubuntu.20.04-x64")]
    [InlineData("win10-x64")]
    [InlineData("not-a-rid-at-all")]
    public void Unrecognized_rid_expands_to_itself_alone(string rid) =>
        Assert.Equal([rid], RuntimeIdentifierGraph.Expand(rid));
}
