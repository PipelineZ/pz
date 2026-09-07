using Pz.Connectors.Abstractions;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

public sealed class PzConnectorHostTests : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("pzsdk").FullName;

    [Fact]
    public async Task Manifest_mode_writes_the_file_and_exits_zero()
    {
        var path = Path.Combine(_dir, "nested", "pz.connector.json");
        var exit = await PzConnectorHost.RunAsync(
            ["--pz-manifest", "--out", path, "--entrypoint", "linux-x64=native/fake"],
            new FakeSourceConnector(ConnectorCapabilities.NativeScan, feed: false));

        Assert.Equal(0, exit);
        var json = await File.ReadAllTextAsync(path);
        Assert.EndsWith("}\n", json, StringComparison.Ordinal);
        Assert.Contains("\"linux-x64\": \"native/fake\"", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\r", json, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Invalid_argv_exits_two_without_touching_the_connector()
    {
        var created = false;
        var exit = await PzConnectorHost.RunAsync(["--root", "/data"], _ =>
        {
            created = true;
            return new FakeSourceConnector(ConnectorCapabilities.None, feed: false);
        });

        Assert.Equal(2, exit);
        Assert.False(created);
    }

    [Fact]
    public async Task A_connector_that_is_neither_source_nor_sink_is_refused()
    {
        var path = Path.Combine(_dir, "pz.connector.json");
        await Assert.ThrowsAsync<ArgumentException>(() =>
            PzConnectorHost.RunAsync(["--pz-manifest", "--out", path], new NeitherConnector()));
    }

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private sealed class NeitherConnector : IConnector
    {
        public ConnectorInfo Info => new("neither", "1.0.0", ProtocolVersion.Major);
        public ConnectorCapabilities Capabilities => ConnectorCapabilities.None;
        public string ConnectionConfigSchema => "{}";
        public string DatasetConfigSchema => "{}";
        public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
            ValueTask.FromResult(new ValidationResult([]));
        public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
            ValueTask.FromResult(new ConnectionCheck(true, null));
    }
}
