using Pz.Connectors.Abstractions;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

public sealed class ManifestWriterTests
{
    [Fact]
    public void Renders_byte_stable_json_in_fixed_key_order()
    {
        var connector = new FakeSourceConnector(
            ConnectorCapabilities.NativeScan | ConnectorCapabilities.SyncState, feed: true);
        var entrypoints = new SortedDictionary<string, string>(StringComparer.Ordinal)
        {
            ["win-x64"] = "native/fake.exe",
            ["linux-x64"] = "native/fake",
        };

        var json = ManifestWriter.Render(connector, entrypoints);

        Assert.Equal(
            "{\n" +
            "  \"name\": \"fake\",\n" +
            $"  \"protocolMajorMin\": {ProtocolVersion.Major},\n" +
            $"  \"protocolMajorMax\": {ProtocolVersion.Major},\n" +
            "  \"capabilities\": [\n" +
            "    \"NativeScan\",\n" +
            "    \"SyncState\"\n" +
            "  ],\n" +
            "  \"runtime\": \"process\",\n" +
            "  \"entrypoints\": {\n" +
            "    \"linux-x64\": \"native/fake\",\n" +
            "    \"win-x64\": \"native/fake.exe\"\n" +
            "  }\n" +
            "}\n",
            json);
    }

    [Fact]
    public void No_capabilities_renders_an_empty_list_and_no_entrypoints_an_empty_map()
    {
        var connector = new FakeSourceConnector(ConnectorCapabilities.None, feed: false);
        var json = ManifestWriter.Render(connector, new SortedDictionary<string, string>(StringComparer.Ordinal));
        Assert.Contains("\"capabilities\": []", json, StringComparison.Ordinal);
        Assert.Contains("\"entrypoints\": {}", json, StringComparison.Ordinal);
    }

    [Fact]
    public void Capability_names_are_the_enum_member_names_in_ascending_flag_order()
    {
        Assert.Equal(
            ["PartitionedRead", "NativeScan", "SyncState"],
            ManifestWriter.CapabilityNames(
                ConnectorCapabilities.SyncState | ConnectorCapabilities.NativeScan | ConnectorCapabilities.PartitionedRead));
        Assert.Empty(ManifestWriter.CapabilityNames(ConnectorCapabilities.None));
    }
}
