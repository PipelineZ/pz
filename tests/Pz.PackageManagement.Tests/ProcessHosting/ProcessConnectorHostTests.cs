using System.Globalization;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text.Json;
using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.ProcessHosting;

namespace Pz.PackageManagement.Tests.ProcessHosting;

/// <summary>Drives <see cref="ProcessConnectorHost"/> over a real materialized package layout whose
/// entrypoint is the <c>PcpFakeConnector</c> fixture: manifest gate at load, nothing spawned until an
/// open, and every spawned process reaped by dispose.
///
/// <para>Unix-only, same reasoning as its siblings: the fixture serves unix domain sockets only, and
/// the entrypoint is a shell wrapper that needs a unix exec bit.</para></summary>
[SupportedOSPlatform("linux")]
public sealed class ProcessConnectorHostTests : IDisposable
{
    private const string PackageId = "Pz.Connector.LocalFilesPcp";
    private const string PackageVersion = "1.0.0";
    private const string ConnectorName = "localfiles-pcp";

    private readonly List<string> _tempDirs = [];

    // ---- load: manifest gate, no spawn ---------------------------------------------------------

    [SkippableFact]
    public async Task Load_registers_the_connector_without_spawning_anything()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        var packagesRoot = NewPackageLayout();
        var socketRoot = NewTempDir();

        await using var host = ProcessConnectorHost.LoadFromDirectory(
            packagesRoot, [new ConnectorPackageRef(PackageId, PackageVersion)], socketRoot);

        var connector = host.Get(ConnectorName);
        Assert.Equal(ConnectorName, connector.Info.Name);
        Assert.Equal(PackageVersion, connector.Info.Version);
        Assert.Equal(ProtocolVersion.Major, connector.Info.ProtocolMajor);
        Assert.IsAssignableFrom<ISourceConnector>(connector);
        Assert.Equal([connector.Info], host.Installed);

        // Identity answered from the manifest, so nothing was started to answer it: a spawn is exactly
        // what creates a subdirectory of the run-scoped socket root.
        Assert.Empty(Directory.GetDirectories(socketRoot));
    }

    [SkippableFact]
    public async Task Unknown_connector_name_is_PZ0305()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        await using var host = ProcessConnectorHost.LoadFromDirectory(
            NewPackageLayout(), [new ConnectorPackageRef(PackageId, PackageVersion)], NewTempDir());

        var ex = Assert.Throws<ConnectorHostException>(() => host.Get("not-installed"));
        Assert.Equal("PZ0305", ex.Code);
        Assert.Contains(ConnectorName, ex.Hint ?? string.Empty, StringComparison.Ordinal);
    }

    [SkippableFact]
    public void Package_with_no_binary_for_this_rid_is_PZ0354_at_load()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        // A RID nothing can fall back to: the expansion walks OS ancestry, and this OS is not in it.
        var packagesRoot = NewPackageLayout(rid: "nosuchos-x64");

        var ex = Assert.Throws<ConnectorHostException>(() => ProcessConnectorHost.LoadFromDirectory(
            packagesRoot, [new ConnectorPackageRef(PackageId, PackageVersion)], NewTempDir()));

        Assert.Equal("PZ0354", ex.Code);
        Assert.Contains(RuntimeInformation.RuntimeIdentifier, ex.Message, StringComparison.Ordinal);
    }

    [SkippableFact]
    public void Package_declaring_the_dotnet_runtime_is_PZ0354_at_load()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        var packagesRoot = NewPackageLayout(runtime: "dotnet");

        var ex = Assert.Throws<ConnectorHostException>(() => ProcessConnectorHost.LoadFromDirectory(
            packagesRoot, [new ConnectorPackageRef(PackageId, PackageVersion)], NewTempDir()));

        Assert.Equal("PZ0354", ex.Code);
    }

    [SkippableFact]
    public void Missing_package_directory_is_PZ0304()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        var ex = Assert.Throws<ConnectorHostException>(() => ProcessConnectorHost.LoadFromDirectory(
            NewTempDir(), [new ConnectorPackageRef(PackageId, PackageVersion)], NewTempDir()));

        Assert.Equal("PZ0304", ex.Code);
    }

    // ---- open: lazy spawn, and dispose reaps ---------------------------------------------------

    [SkippableFact]
    public async Task First_open_spawns_and_dispose_reaps()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        var dataDir = NewTempDir();
        WriteCsv(Path.Combine(dataDir, "small.csv"), 20);
        var socketRoot = NewTempDir();
        var host = ProcessConnectorHost.LoadFromDirectory(
            NewPackageLayout(), [new ConnectorPackageRef(PackageId, PackageVersion)], socketRoot);

        var connector = (ISourceConnector)host.Get(ConnectorName);
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dataDir });
        var source = await connector.OpenAsync(config, CancellationToken.None);

        // A spawned instance leaves its own socket dir under the run-scoped root, and answers a real
        // RPC -- the handshake and Configure the shim never makes itself have both happened.
        var socketDir = Assert.Single(Directory.GetDirectories(socketRoot));
        Assert.True(Directory.Exists(socketDir));

        var spec = new DatasetSpec("files", "orders", new Dictionary<string, object?>
        {
            ["path"] = "small.csv",
            ["format"] = "csv",
            ["columns"] = CsvColumns,
        });
        var schema = await source.GetSchemaAsync(spec, CancellationToken.None);
        Assert.Equal(CsvColumns.Keys, schema.Schema.FieldsList.Select(field => field.Name));

        await host.DisposeAsync();

        // The host owns every process it spawned: disposing it takes the socket dir with it.
        Assert.False(Directory.Exists(socketDir));
    }

    // ---- capability masking --------------------------------------------------------------------

    [SkippableFact]
    public async Task Capabilities_the_process_shims_do_not_implement_are_masked_out()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        // The package DECLARES CheckpointableReads and the fixture reports it at handshake, so the two
        // agree -- this is not a misdeclaration the handshake could catch. There is no PCP wiring for
        // ICheckpointingPartition, so surfacing the flag would let the planner accept a checkpointed
        // dataset and silently get a plain full read instead.
        var capabilities = new LocalFilesConnector().Capabilities | ConnectorCapabilities.CheckpointableReads;
        var packagesRoot = NewPackageLayout(
            capabilities: capabilities, extraArgs: ["--declare-checkpointable-reads"]);

        var warnings = new List<string>();
        await using var host = ProcessConnectorHost.LoadFromDirectory(
            packagesRoot, [new ConnectorPackageRef(PackageId, PackageVersion)], NewTempDir(), warnings.Add);

        var connector = host.Get(ConnectorName);
        Assert.Equal(ConnectorCapabilities.None, connector.Capabilities & ConnectorCapabilities.CheckpointableReads);
        Assert.Contains(warnings, warning => warning.Contains("CheckpointableReads", StringComparison.Ordinal));

        // ... and still absent once a real handshake has happened and the shim is answering from Hello.
        var source = await ((ISourceConnector)connector).OpenAsync(
            new ConnectorConfig(new Dictionary<string, object?> { ["root"] = NewTempDir() }), CancellationToken.None);
        Assert.NotNull(source);
        Assert.Equal(ConnectorCapabilities.None, connector.Capabilities & ConnectorCapabilities.CheckpointableReads);
    }

    // ---- shared fixtures -----------------------------------------------------------------------

    private static readonly Dictionary<string, string> CsvColumns = new()
    {
        ["id"] = "bigint",
        ["name"] = "varchar",
    };

    private static void WriteCsv(string path, int rows)
    {
        using var writer = new StreamWriter(path);
        writer.NewLine = "\n";
        writer.WriteLine("id,name");
        for (var i = 0; i < rows; i++)
        {
            writer.WriteLine($"{i.ToString(CultureInfo.InvariantCulture)},row-{i}");
        }
    }

    /// <summary>Materializes <c>&lt;root&gt;/&lt;PackageId&gt;/&lt;Version&gt;/</c> with a manifest and
    /// an executable entrypoint. The entrypoint is a wrapper script rather than a copy of the fixture
    /// binary: what is under test is the manifest→RID→spawn path, and a script keeps the layout to two
    /// files instead of a whole publish tree.</summary>
    private string NewPackageLayout(
        string? rid = null, string runtime = "process",
        ConnectorCapabilities? capabilities = null, IReadOnlyList<string>? extraArgs = null)
    {
        var root = NewTempDir();
        var packageDir = Path.Combine(root, PackageId, PackageVersion);
        var binDir = Path.Combine(packageDir, "bin");
        Directory.CreateDirectory(binDir);

        var entrypoint = Path.Combine(binDir, "connector");
        var args = extraArgs is null ? string.Empty : " " + string.Join(' ', extraArgs);
        File.WriteAllText(entrypoint, $"#!/bin/sh\nexec \"{FixtureExecutablePath()}\"{args} \"$@\"\n");
        File.SetUnixFileMode(
            entrypoint,
            UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute |
            UnixFileMode.GroupRead | UnixFileMode.GroupExecute);

        var manifest = new Dictionary<string, object?>
        {
            ["name"] = ConnectorName,
            ["protocolMajorMin"] = ProtocolVersion.Major,
            ["protocolMajorMax"] = ProtocolVersion.Major,
            ["capabilities"] = (capabilities ?? new LocalFilesConnector().Capabilities)
                .ToString().Split(", ", StringSplitOptions.RemoveEmptyEntries),
            ["runtime"] = runtime,
            ["entrypoints"] = new Dictionary<string, string>
            {
                [rid ?? RuntimeInformation.RuntimeIdentifier] = "bin/connector",
            },
        };
        File.WriteAllText(Path.Combine(packageDir, "pz.connector.json"), JsonSerializer.Serialize(manifest));

        return root;
    }

    private static string FixtureExecutablePath()
    {
        var baseDir = new DirectoryInfo(AppContext.BaseDirectory);
        var tfm = baseDir.Name;
        var config = baseDir.Parent!.Name;
        var testsDir = baseDir.Parent!.Parent!.Parent!.Parent!.FullName;
        var exeName = OperatingSystem.IsWindows() ? "PcpFakeConnector.exe" : "PcpFakeConnector";
        return Path.Combine(testsDir, "fixtures", "PcpFakeConnector", "bin", config, tfm, exeName);
    }

    /// <summary>Short, outside the test output tree: a unix domain socket path is capped at roughly 104
    /// bytes, and the spawned instance's own subdirectory plus <c>control.sock.data</c> ride on top of
    /// whatever this returns.</summary>
    private string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-pch-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        _tempDirs.Add(dir);
        return dir;
    }

    public void Dispose()
    {
        foreach (var dir in _tempDirs)
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
