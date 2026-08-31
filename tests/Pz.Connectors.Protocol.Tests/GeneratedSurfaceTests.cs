using Pz.Connectors.Protocol.V1;

namespace Pz.Connectors.Protocol.Tests;

public class GeneratedSurfaceTests
{
    [Fact]
    public void Service_and_key_messages_exist()
    {
        Assert.NotNull(PzConnector.Descriptor.FindMethodByName("Handshake"));
        Assert.NotNull(PzConnector.Descriptor.FindMethodByName("HostChannel"));
        _ = new DatasetSpecMsg { Source = "s", Dataset = "d" };
        _ = new Hello { Capabilities = 0 };
    }
}
