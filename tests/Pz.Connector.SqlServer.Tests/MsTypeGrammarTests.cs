using Pz.Connector.SqlServer;

namespace Pz.Connector.SqlServer.Tests;

public sealed class MsTypeGrammarTests
{
    [Theory]
    [InlineData("nvarchar(20)", "nvarchar(20)")]
    [InlineData("NVARCHAR(MAX)", "nvarchar(max)")]
    [InlineData("nvarchar( 4000 )", "nvarchar(4000)")]
    [InlineData("varchar(8000)", "varchar(8000)")]
    [InlineData("varchar(max)", "varchar(max)")]
    [InlineData("decimal(38,9)", "decimal(38,9)")]
    [InlineData("Decimal(18, 2)", "decimal(18,2)")]
    [InlineData("datetime2(0)", "datetime2(0)")]
    [InlineData("datetime2(7)", "datetime2(7)")]
    [InlineData("int", "int")]
    [InlineData("BIGINT", "bigint")]
    [InlineData("float", "float")]
    [InlineData("bit", "bit")]
    [InlineData("date", "date")]
    public void Accepts_and_canonicalizes(string input, string expected)
    {
        Assert.True(MsTypeGrammar.TryParse(input, out var canonical, out _));
        Assert.Equal(expected, canonical);
    }

    [Theory]
    [InlineData("nvarchar")]                       // length required
    [InlineData("nvarchar(0)")]
    [InlineData("nvarchar(4001)")]
    [InlineData("varchar(8001)")]
    [InlineData("decimal(39,0)")]
    [InlineData("decimal(10,11)")]                 // scale > precision
    [InlineData("decimal(10)")]                    // both args required
    [InlineData("datetime2(8)")]
    [InlineData("text")]
    [InlineData("nvarchar(20); drop table x--")]   // injection shapes never round-trip
    [InlineData("nvarchar(20)) as (select 1")]
    [InlineData("")]
    public void Rejects_everything_outside_the_grammar(string input)
    {
        Assert.False(MsTypeGrammar.TryParse(input, out _, out var error));
        Assert.NotNull(error);
        Assert.Contains("nvarchar", error); // error names the accepted grammar
    }
}
