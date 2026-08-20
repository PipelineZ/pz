using Pz.Connectors.Abstractions;
using Pz.Connectors.TestKit.Reference;
using Xunit;

namespace Pz.Connectors.TestKit.Tests;

public sealed class Stage5AbiTests
{
    [Fact]
    public void New_capability_flags_have_the_ratified_values()
    {
        Assert.Equal(32768, (int)ConnectorCapabilities.ReplaceWrites);
        Assert.Equal(65536, (int)ConnectorCapabilities.CheckpointableWrites);
    }

    [Fact]
    public async Task Existing_sinks_default_to_DiscardsAll()
    {
        ISinkConnector connector = new InMemoryConnector();
        await using var sink = await connector.OpenAsync(ConnectorConfig.Empty, CancellationToken.None);
        Assert.Equal(AbortSemantics.DiscardsAll, sink.AbortSemantics);
    }
}
