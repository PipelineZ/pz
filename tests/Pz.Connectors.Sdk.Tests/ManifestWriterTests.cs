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
    public void ProjectDirectoryAnchor_is_omitted_when_false()
    {
        var connector = new FakeSourceConnector(ConnectorCapabilities.None, feed: false);
        var json = ManifestWriter.Render(connector, new SortedDictionary<string, string>(StringComparer.Ordinal));
        Assert.DoesNotContain("projectDirectoryAnchor", json, StringComparison.Ordinal);
    }

    [Fact]
    public void ProjectDirectoryAnchor_renders_true_in_the_slot_and_spelling_the_host_reads()
    {
        // Not a round-trip through the host's actual ManifestReader: Pz.Connectors.Sdk.Tests cannot
        // reference Pz.PackageManagement (both projects compile the same .proto file, so referencing
        // both from one test project makes every message type -- PartitionMsg, HostChannelUp, etc. --
        // ambiguous, CS0433). Asserting the JSON text against the exact key/value/placement
        // src/Pz.PackageManagement/.../ManifestReader.cs's ManifestDto and PackageManifests.cs (~L41)
        // require is the fallback the brief allows.
        var connector = new FakeSourceConnector(ConnectorCapabilities.None, feed: false);
        var json = ManifestWriter.Render(
            connector, new SortedDictionary<string, string>(StringComparer.Ordinal), projectDirectoryAnchor: true);

        Assert.Equal(
            "{\n" +
            "  \"name\": \"fake\",\n" +
            $"  \"protocolMajorMin\": {ProtocolVersion.Major},\n" +
            $"  \"protocolMajorMax\": {ProtocolVersion.Major},\n" +
            "  \"capabilities\": [],\n" +
            "  \"projectDirectoryAnchor\": true,\n" +
            "  \"runtime\": \"process\",\n" +
            "  \"entrypoints\": {}\n" +
            "}\n",
            json);
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
