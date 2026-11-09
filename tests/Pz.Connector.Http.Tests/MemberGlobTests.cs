using Pz.Connector.Http;

namespace Pz.Connector.Http.Tests;

public class MemberGlobTests
{
    [Theory]
    [InlineData("*.csv", "a.csv", true)]
    [InlineData("*.csv", "d/a.csv", false)]
    [InlineData("**/*.csv", "d/e/a.csv", true)]
    [InlineData("**/*.csv", "a.csv", true)]
    [InlineData("s?.csv", "s1.csv", true)]
    [InlineData("*.CSV", "a.csv", false)]
    [InlineData("s?.csv", "s12.csv", false)]
    [InlineData("d/*.csv", "d/a.csv", true)]
    [InlineData("d/*", "d/e/a.csv", false)]
    [InlineData("d/**", "d/e/a.csv", true)]
    [InlineData("sessions_*.csv", "sessions_2026-11-07.csv", true)]
    [InlineData("a*b*c", "axxbyyc", true)]
    [InlineData("a*b*c", "axxbyy", false)]
    [InlineData("**/x/*.csv", "a/b/x/y.csv", true)]
    [InlineData("**/x/*.csv", "a/b/y.csv", false)]
    [InlineData("events.jsonl", "events.jsonl", true)]
    [InlineData("[ab].csv", "a.csv", false)]
    [InlineData("[ab].csv", "[ab].csv", true)]
    public void Matches(string glob, string path, bool expected)
        => Assert.Equal(expected, MemberGlob.IsMatch(glob, path));
}
