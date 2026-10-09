using Pz.PackageManagement.ProcessHosting;

namespace Pz.PackageManagement.Tests.ProcessHosting;

/// <summary>What the handshake's <c>HostInfo</c> tells a connector about telemetry, per protocol.</summary>
public sealed class HostInfoTelemetryTests
{
    [Fact]
    public void Http_settings_reach_the_connector_without_otel_endpoint()
    {
        var telemetry = new HostTelemetry("r1", null, "http/protobuf", new Uri("https://t/x"), new Uri("https://m/x"), "/run/h");

        var info = PcpClient.BuildHostInfo(telemetry);

        Assert.False(info.HasOtelEndpoint);
        Assert.Equal("http/protobuf", info.OtelProtocol);
        Assert.Equal("https://t/x", info.OtelTracesEndpoint);
        Assert.Equal("https://m/x", info.OtelMetricsEndpoint);
        Assert.Equal("/run/h", info.OtelHeadersFile);
        Assert.Equal("r1", info.RunId);
    }

    [Fact]
    public void Grpc_settings_are_unchanged()
    {
        var info = PcpClient.BuildHostInfo(new HostTelemetry("r1", new Uri("http://c:4317")));

        Assert.Equal("http://c:4317/", info.OtelEndpoint);
        Assert.False(info.HasOtelProtocol);
        Assert.False(info.HasOtelTracesEndpoint);
        Assert.False(info.HasOtelHeadersFile);
    }

    [Fact]
    public void No_telemetry_sends_no_otel_fields()
    {
        var info = PcpClient.BuildHostInfo(HostTelemetry.None);

        Assert.False(info.HasOtelEndpoint);
        Assert.False(info.HasOtelProtocol);
        Assert.Equal(string.Empty, info.RunId);
    }
}
