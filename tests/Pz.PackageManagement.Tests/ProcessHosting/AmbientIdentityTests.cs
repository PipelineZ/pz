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

    /// <summary>Apart from an Azure service principal, the groups carry where an identity is, never a
    /// static credential: those belong in connections.yml as ${VAR}.</summary>
    [Theory]
    [InlineData("AWS_ACCESS_KEY_ID")]
    [InlineData("AWS_SECRET_ACCESS_KEY")]
    [InlineData("AWS_SESSION_TOKEN")]
    [InlineData("AZURE_STORAGE_KEY")]
    [InlineData("AZURE_STORAGE_CONNECTION_STRING")]
    [InlineData("AZURE_USERNAME")]
    [InlineData("AZURE_PASSWORD")]
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
    public void The_host_knows_exactly_the_groups_the_sdk_can_declare()
    {
        Assert.Equal(AmbientIdentityGroups.All, AmbientIdentity.Groups.Order(StringComparer.Ordinal));
    }

    /// <summary>Runs on every platform, unlike the spawning tests: on Windows a declared spawn that lost
    /// SystemRoot would fail its very first socket.</summary>
    [Fact]
    public void A_declared_spawn_keeps_the_whole_base_allowlist()
    {
        var undeclared = ConnectorProcess.EnvironmentNames(null);

        Assert.Equal(undeclared, ConnectorProcess.EnvironmentNames([]));
        foreach (var group in AmbientIdentityGroups.All)
        {
            var declared = ConnectorProcess.EnvironmentNames([group]);
            Assert.Equal(undeclared, declared.Take(undeclared.Count));
            Assert.Equal(AmbientIdentity.VariablesFor([group]), declared.Skip(undeclared.Count));
        }

        Assert.Contains("PATH", undeclared);
        if (OperatingSystem.IsWindows())
        {
            Assert.Contains("SystemRoot", undeclared);
        }
    }

    /// <summary>Azure Arc on Windows validates its token-file path against %ProgramData%; Azure ML
    /// needs DEFAULT_IDENTITY_CLIENT_ID for a system-assigned identity.</summary>
    [Theory]
    [InlineData("ProgramData")]
    [InlineData("DEFAULT_IDENTITY_CLIENT_ID")]
    public void The_azure_group_carries_what_arc_and_azure_ml_need(string variable)
    {
        Assert.Contains(variable, AmbientIdentity.VariablesFor([AmbientIdentityGroups.Azure]));
    }

    /// <summary>A host configured with a service principal through the environment must work for a
    /// declaring connector: the whole EnvironmentCredential set travels together, never the id alone.</summary>
    [Theory]
    [InlineData("AZURE_CLIENT_ID")]
    [InlineData("AZURE_TENANT_ID")]
    [InlineData("AZURE_CLIENT_SECRET")]
    [InlineData("AZURE_CLIENT_CERTIFICATE_PATH")]
    [InlineData("AZURE_CLIENT_CERTIFICATE_PASSWORD")]
    [InlineData("AZURE_CLIENT_SEND_CERTIFICATE_CHAIN")]
    public void The_azure_group_carries_a_service_principal(string variable)
    {
        Assert.Contains(variable, AmbientIdentity.VariablesFor([AmbientIdentityGroups.Azure]));
        Assert.DoesNotContain(variable, AmbientIdentity.VariablesFor([AmbientIdentityGroups.Aws, AmbientIdentityGroups.Gcp]));
    }

    [Fact]
    public void A_repeated_group_forwards_its_variables_once()
    {
        Assert.Equal(AmbientIdentity.VariablesFor(["gcp"]), AmbientIdentity.VariablesFor(["gcp", "gcp"]));
    }
}
