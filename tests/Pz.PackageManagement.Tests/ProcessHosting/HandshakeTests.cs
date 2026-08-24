using System.Runtime.Versioning;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;
using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.ProcessHosting;

namespace Pz.PackageManagement.Tests.ProcessHosting;

/// <summary>Drives <see cref="PcpClient"/> against the real out-of-process <c>PcpFakeConnector</c>
/// fixture (Task 5) via <see cref="ConnectorProcess"/> (Task 6) -- these tests are the wire-level proof
/// that the handshake discipline and error mapping documented on <see cref="PcpClient"/> hold against an
/// actual peer, not a mock of one.
///
/// <para>Unix-only, same reasoning as <c>ConnectorProcessTests</c>: the fixture itself refuses to serve
/// on Windows (unix domain sockets only), so every fact skips there rather than failing.</para></summary>
[SupportedOSPlatform("linux")]
public sealed class HandshakeTests : IDisposable
{
    private static readonly TimeSpan ShortHandshakeTimeout = TimeSpan.FromMilliseconds(500);

    private readonly List<string> _socketDirs = [];

    [SkippableFact]
    public async Task Handshake_and_configure_succeed()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        await using var process = ConnectorProcess.Spawn(FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp");
        var manifest = LocalFilesManifest();
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = Path.GetTempPath() });

        await using var client = await PcpClient.ConnectAndConfigureAsync(
            process, manifest, "test-instance", config, CancellationToken.None);

        Assert.Equal("localfiles-pcp", client.Hello.Info.Name);
        Assert.Equal((long)new LocalFilesConnector().Capabilities, client.Hello.Capabilities);
    }

    [SkippableFact]
    public async Task Hang_is_PZ0356_with_stderr_tail()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        await using var process = ConnectorProcess.Spawn(
            FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp", ["--hang-handshake"]);

        var ex = await Assert.ThrowsAsync<ConnectorHostException>(() => PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", ConnectorConfig.Empty,
            ShortHandshakeTimeout, CancellationToken.None));

        Assert.Equal("PZ0356", ex.Code);
        Assert.Contains("handshake", ex.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public async Task Wrong_protocol_major_is_PZ0356()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        await using var process = ConnectorProcess.Spawn(
            FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp", ["--wrong-protocol-major"]);

        var ex = await Assert.ThrowsAsync<ConnectorHostException>(() => PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", ConnectorConfig.Empty, CancellationToken.None));

        Assert.Equal("PZ0356", ex.Code);
    }

    [SkippableFact]
    public async Task Capability_mismatch_vs_manifest_is_PZ0356()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        await using var process = ConnectorProcess.Spawn(
            FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp", ["--misreport-capabilities"]);

        // The manifest declares the TRUE capability list; the fixture's Hello reports it plus Merge,
        // so the handshake-authoritative check must catch the disagreement even though the manifest
        // itself is accurate about what the connector should be.
        var ex = await Assert.ThrowsAsync<ConnectorHostException>(() => PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", ConnectorConfig.Empty, CancellationToken.None));

        Assert.Equal("PZ0356", ex.Code);
    }

    [SkippableFact]
    public async Task Error_detail_maps_to_PzConnectorException()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        await using var process = ConnectorProcess.Spawn(
            FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp", ["--fail-check-transient"]);
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = Path.GetTempPath() });

        await using var client = await PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", config, CancellationToken.None);

        var rpcEx = await Assert.ThrowsAsync<RpcException>(() => client.Grpc.CheckConnectionAsync(
            new CheckRequest { Config = new Struct() }).ResponseAsync);

        var mapped = client.MapRpcException(rpcEx);

        var connectorEx = Assert.IsType<PzConnectorException>(mapped);
        Assert.True(connectorEx.IsTransient);
        Assert.Equal(TimeSpan.FromMilliseconds(250), connectorEx.RetryAfter);
    }

    private static ConnectorManifest LocalFilesManifest() => new(
        Name: "localfiles-pcp",
        ProtocolMajorMin: ProtocolVersion.Major,
        ProtocolMajorMax: ProtocolVersion.Major,
        Capabilities: new LocalFilesConnector().Capabilities.ToString()
            .Split(", ", StringSplitOptions.RemoveEmptyEntries));

    /// <summary>The fixture builds to its own bin dir, a sibling of this test project's under
    /// <c>tests/</c>, not this project's output -- resolved relative to <see cref="AppContext.BaseDirectory"/>
    /// rather than hardcoded so it tracks whichever configuration (Debug/Release) actually ran.</summary>
    private static string FixtureExecutablePath()
    {
        var baseDir = new DirectoryInfo(AppContext.BaseDirectory);
        var tfm = baseDir.Name;                    // net10.0
        var config = baseDir.Parent!.Name;          // Debug | Release
        // .../tests/Pz.PackageManagement.Tests/bin/<config>/<tfm>/ -> .../tests/
        var testsDir = baseDir.Parent!.Parent!.Parent!.Parent!.FullName;
        var exeName = OperatingSystem.IsWindows() ? "PcpFakeConnector.exe" : "PcpFakeConnector";
        return Path.Combine(testsDir, "fixtures", "PcpFakeConnector", "bin", config, tfm, exeName);
    }

    /// <summary>Short, outside the test output tree: a unix domain socket path is capped at roughly 104
    /// bytes (<c>sun_path</c>), and the deep <c>tests/.../bin/Release/net10.0/...</c> tree this
    /// assembly lives under leaves no room for <c>control.sock</c>/<c>control.sock.data</c> on top.</summary>
    private string NewSocketDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-" + Guid.NewGuid().ToString("N")[..8]);
        _socketDirs.Add(dir);
        return dir;
    }

    public void Dispose()
    {
        foreach (var dir in _socketDirs)
        {
            try
            {
                if (Directory.Exists(dir))
                {
                    Directory.Delete(dir, recursive: true);
                }
            }
            catch
            {
                // best-effort cleanup
            }
        }
    }
}
