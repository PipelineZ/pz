using Pz.Core.Loading;

namespace Pz.Core.Tests.Loading;

public class DurationParserTests
{
    [Theory]
    [InlineData("500ms", 0, 0, 0, 0, 500)]
    [InlineData("0s", 0, 0, 0, 0, 0)]
    [InlineData("2s", 0, 0, 0, 2, 0)]
    [InlineData("90s", 0, 0, 1, 30, 0)]
    [InlineData("5m", 0, 0, 5, 0, 0)]
    [InlineData("1h", 0, 1, 0, 0, 0)]
    [InlineData("1d", 1, 0, 0, 0, 0)]
    public void Parses_each_unit(string text, int d, int h, int m, int s, int ms)
    {
        Assert.True(DurationParser.TryParse(text, out var value));
        Assert.Equal(new TimeSpan(d, h, m, s, ms), value);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("5")]        // no unit
    [InlineData("s")]        // no digits
    [InlineData("5 s")]      // whitespace
    [InlineData(" 5s")]
    [InlineData("-1s")]      // sign
    [InlineData("+1s")]
    [InlineData("1.5s")]     // fraction
    [InlineData("5S")]       // uppercase unit
    [InlineData("5sec")]     // unknown unit
    [InlineData("5m2s")]     // compound
    [InlineData("99999999999999999999d")] // overflow (exceeds long AND TimeSpan)
    [InlineData("9999999999d")]           // fits long, overflows TimeSpan
    public void Rejects_invalid(string? text)
    {
        Assert.False(DurationParser.TryParse(text, out _));
    }
}
