using Pz.Connectors.Abstractions;
using Pz.PackageManagement.ProcessHosting;

namespace Pz.PackageManagement.Tests.ProcessHosting;

public sealed class AmbientIdentityTests
{
    [Fact]
    public void Every_group_the_sdk_can_declare_has_variables()
    {
        foreach (var group in AmbientIdentityGroups.All)
        {
            Assert.NotEmpty(AmbientIdentity.VariablesFor([group]));
        }

        Assert.Empty(AmbientIdentity.UnknownGroups(AmbientIdentityGroups.All));
    }

    /// <summary>The groups carry where an identity is, never a static credential: those belong in
    /// connections.yml as ${VAR}.</summary>
    [Theory]
    [InlineData("AWS_ACCESS_KEY_ID")]
    [InlineData("AWS_SECRET_ACCESS_KEY")]
    [InlineData("AWS_SESSION_TOKEN")]
    [InlineData("AZURE_CLIENT_SECRET")]
    [InlineData("AZURE_CLIENT_CERTIFICATE_PATH")]
    [InlineData("AZURE_STORAGE_KEY")]
    [InlineData("AZURE_STORAGE_CONNECTION_STRING")]
    public void No_group_forwards_a_static_secret(string variable)
    {
        Assert.DoesNotContain(variable, AmbientIdentity.VariablesFor(AmbientIdentityGroups.All));
    }

    [Fact]
    public void An_unknown_group_forwards_nothing_and_is_reported_once()
    {
        Assert.Empty(AmbientIdentity.VariablesFor(["oracle"]));
        Assert.Equal(["oracle"], AmbientIdentity.UnknownGroups(["azure", "oracle", "oracle"]));
    }

    [Fact]
    public void A_repeated_group_forwards_its_variables_once()
    {
        Assert.Equal(AmbientIdentity.VariablesFor(["gcp"]), AmbientIdentity.VariablesFor(["gcp", "gcp"]));
    }
}
