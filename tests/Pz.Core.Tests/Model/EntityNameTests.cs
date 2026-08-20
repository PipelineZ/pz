using Pz.Core.Model;

namespace Pz.Core.Tests.Model;

/// <summary>Entity validation is two-tier, and this is the offline
/// tier -- shape only. Existence, permissions, and schema stay <c>--connect</c> work.</summary>
public class EntityNameTests
{
    [Theory]
    [InlineData("orders")]
    [InlineData("dbo.orders")]
    [InlineData("raw.orders_2024")]
    [InlineData("/v2/events")]
    [InlineData("repos/acme/pz/issues")]
    [InlineData("orders-current")]
    public void A_well_formed_entity_name_has_no_problem(string name) =>
        Assert.Null(EntityName.Problem(name));

    [Theory]
    [InlineData("", "is empty")]
    [InlineData("  ", "is empty")]
    [InlineData(".orders", "has an empty dotted segment")]
    [InlineData("dbo.", "has an empty dotted segment")]
    [InlineData("dbo..orders", "has an empty dotted segment")]
    [InlineData("dbo orders", "contains whitespace")]
    [InlineData("dbo.\torders", "contains whitespace")]
    public void A_malformed_entity_name_is_named_precisely(string name, string expected) =>
        Assert.Equal(expected, EntityName.Problem(name));

    [Fact]
    public void A_null_name_is_empty_not_a_crash() => Assert.Equal("is empty", EntityName.Problem(null));
}
