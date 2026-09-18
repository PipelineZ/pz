using Pz.Core.Model;

namespace Pz.Core.Tests.Model;

/// <summary>A pipeline file stem and a connection name both reach a raw, unquoted DuckDB identifier
/// (<c>staging.&lt;pipeline&gt;</c>, <c>src_&lt;connection&gt;__...</c>) -- shape only, no reserved-word
/// refusal (see <see cref="PzIdentifier"/>'s own doc comment for why).</summary>
public class PzIdentifierTests
{
    [Theory]
    [InlineData("orders")]
    [InlineData("stg_orders")]
    [InlineData("_private")]
    [InlineData("a1")]
    [InlineData("A")]
    [InlineData("order")] // a DuckDB reserved word -- fine once schema-qualified, see PzIdentifierTests probe
    public void A_well_formed_identifier_is_valid(string name) => Assert.True(PzIdentifier.IsValid(name));

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("01_load")]
    [InlineData("daily-orders")]
    [InlineData("my.pipeline")]
    [InlineData("my pipeline")]
    [InlineData("café")]
    public void A_malformed_identifier_is_invalid(string? name) => Assert.False(PzIdentifier.IsValid(name));

    [Fact]
    public void Problem_names_an_empty_name_precisely() => Assert.Equal("is empty", PzIdentifier.Problem(""));

    [Fact]
    public void Problem_is_null_for_a_valid_name() => Assert.Null(PzIdentifier.Problem("orders"));

    [Fact]
    public void Problem_names_a_malformed_shape() =>
        Assert.Contains("not a valid identifier", PzIdentifier.Problem("01_load"));

    [Theory]
    [InlineData("01_load", "load_01")]
    [InlineData("daily-orders", "daily_orders")]
    [InlineData("2024_report", "report_2024")]
    [InlineData("my.pipeline", "my_pipeline")]
    [InlineData("café", "caf")]
    public void Suggest_produces_a_valid_rename(string name, string expected)
    {
        var suggested = PzIdentifier.Suggest(name);
        Assert.Equal(expected, suggested);
        Assert.True(PzIdentifier.IsValid(suggested));
    }

    [Fact]
    public void Suggest_never_produces_an_empty_or_all_digit_result()
    {
        Assert.True(PzIdentifier.IsValid(PzIdentifier.Suggest("123")));
        Assert.True(PzIdentifier.IsValid(PzIdentifier.Suggest("---")));
    }
}
