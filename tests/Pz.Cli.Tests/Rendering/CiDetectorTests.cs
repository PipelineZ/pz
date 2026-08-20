using Pz.Cli.Rendering;

namespace Pz.Cli.Tests.Rendering;

/// <summary>Interactive (live tree) only when stdout is not redirected AND no
/// `CI` env var is set. Exercised entirely through the injectable seam so no test touches the real
/// process environment or console streams.</summary>
public class CiDetectorTests
{
    [Theory]
    [InlineData("true", false, false)]   // CI=true, TTY -> not interactive (CI wins)
    [InlineData("true", true, false)]    // CI=true, redirected -> not interactive
    [InlineData("", false, false)]       // CI="" (still set) -> not interactive
    [InlineData(null, true, false)]      // no CI, redirected -> not interactive (no TTY)
    [InlineData(null, false, true)]      // no CI, real TTY -> interactive
    public void IsInteractive_env_seam(string? ciValue, bool isOutputRedirected, bool expected)
    {
        var actual = CiDetector.IsInteractive(
            name => name == "CI" ? ciValue : null,
            () => isOutputRedirected);

        Assert.Equal(expected, actual);
    }
}
