using Pz.Engine.Execution;
using Pz.PackageManagement.ProcessHosting;

namespace Pz.Cli.Tests;

/// <summary>The engine sets the reserved instance-id key and the out-of-process host strips it; neither
/// assembly may reference the other, so each spells the key itself. This is the one place both are
/// visible, and the place that keeps them one key.</summary>
public sealed class InstanceIdKeyTests
{
    [Fact]
    public void Engine_and_host_agree_on_the_reserved_instance_key() =>
        Assert.Equal(ConnectorRegistry.InstanceIdKey, ProcessConnectorHost.InstanceIdKey);
}
