using System.Globalization;
using System.Runtime.Versioning;
using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;
using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.ProcessHosting;

namespace Pz.PackageManagement.Tests.ProcessHosting;

/// <summary>Drives <see cref="HostChannelPump"/> against the real out-of-process <c>PcpFakeConnector</c>
/// fixture's <c>--use-gate</c> mode: the wire-level proof that a connector-authored <c>GateAcquire</c>/
/// <c>GateComplete</c> round trip really reaches a host-side <see cref="IOperationGate"/>, and that a
/// <c>LogEvent</c> reaches the log sink with its fields intact. Unix-only, same reasoning as
/// <c>HandshakeTests</c>/<c>ShimTests</c>.</summary>
[SupportedOSPlatform("linux")]
public sealed class HostChannelTests : IDisposable
{
    // Mirrors PcpService.GateOpLabel -- there is no shared constant across the host/fixture boundary
    // (op labels are connector-authored strings, never a shared contract type), so the wire value is
    // asserted literally, same as every other fixture-reported string this test suite checks.
    private const string ExpectedOpLabel = "localfiles-pcp.read_partition";

    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(10);

    private readonly List<string> _tempDirs = [];

    [SkippableFact]
    public async Task UseGate_read_produces_one_ExecuteAsync_per_partition_with_the_static_op_label()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        var dataDir = NewTempDir();
        WriteCsv(Path.Combine(dataDir, "small.csv"), 25);

        await using var process = ConnectorProcess.Spawn(
            FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp", ["--use-gate"]);
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dataDir });
        await using var client = await PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", config, CancellationToken.None);

        var gate = new RecordingOperationGate();
        await using var pump = HostChannelPump.Start(client, process, gate);

        var connector = new ProcessSourceConnector(client, process);
        await using var source = await connector.OpenAsync(config, CancellationToken.None);

        var spec = new DatasetSpec("files", "orders", new Dictionary<string, object?>
        {
            ["path"] = "small.csv",
            ["format"] = "csv",
            ["columns"] = CsvColumns,
        });

        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        var partition = Assert.Single(partitions);

        var rows = 0;
        await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
        {
            rows += batch.Length;
            batch.Dispose();
        }

        Assert.Equal(25, rows);
        Assert.Equal([ExpectedOpLabel], gate.Labels);
    }

    [SkippableFact]
    public async Task LogEvent_from_Configure_reaches_the_sink_with_fields_intact()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        var dataDir = NewTempDir();
        await using var process = ConnectorProcess.Spawn(FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp");
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dataDir });
        await using var client = await PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", config, CancellationToken.None);

        var gate = new RecordingOperationGate();
        var logged = new TaskCompletionSource<(int Level, string Message, IReadOnlyDictionary<string, string> Fields)>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        await using var pump = HostChannelPump.Start(client, process, gate,
            (level, message, fields) => logged.TrySetResult((level, message, fields)));

        // Configure already ran (inside ConnectAndConfigureAsync, before this pump even existed) -- the
        // fixture buffers the LogEvent and flushes it the moment this pump's HostChannel call attaches,
        // so waiting on the TCS (not a sleep) is what proves that buffering/flush handoff works.
        var (level, message, fields) = await logged.Task.WaitAsync(WaitTimeout);

        Assert.Equal(2, level);
        Assert.Equal("connector configured", message);
        Assert.Equal("test-instance", fields["instance_id"]);
        Assert.Equal("localfiles-pcp", fields["connector"]);
    }

    [SkippableFact]
    public async Task Disposing_the_pump_ends_it_quietly_with_no_pending_gate_operations()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        var dataDir = NewTempDir();
        await using var process = ConnectorProcess.Spawn(FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp");
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dataDir });
        await using var client = await PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", config, CancellationToken.None);

        var gate = new RecordingOperationGate();
        var pump = HostChannelPump.Start(client, process, gate);

        // No PZ03xx / connector-failure noise -- DisposeAsync completing at all (rather than hanging or
        // throwing) is the assertion; the pump's own lifetime token cancelling is the only thing that
        // should end its background loop here.
        await pump.DisposeAsync();
    }

    private sealed class RecordingOperationGate : IOperationGate
    {
        private readonly Lock _lock = new();
        private readonly List<string> _labels = [];
        private readonly List<(int Remaining, DateTimeOffset ResetAt)> _budgets = [];

        public IReadOnlyList<string> Labels { get { lock (_lock) { return [.. _labels]; } } }
        public IReadOnlyList<(int Remaining, DateTimeOffset ResetAt)> Budgets { get { lock (_lock) { return [.. _budgets]; } } }

        public async Task<T> ExecuteAsync<T>(
            string opLabel, bool idempotent, Func<CancellationToken, Task<T>> op, CancellationToken ct)
        {
            lock (_lock) { _labels.Add(opLabel); }
            return await op(ct).ConfigureAwait(false);
        }

        public void ReportBudget(int remaining, DateTimeOffset resetAt)
        {
            lock (_lock) { _budgets.Add((remaining, resetAt)); }
        }
    }

    private static readonly Dictionary<string, string> CsvColumns = new()
    {
        ["id"] = "bigint",
        ["name"] = "varchar",
        ["amount"] = "double",
        ["flag"] = "boolean",
        ["created"] = "timestamp",
    };

    private static void WriteCsv(string path, int rows)
    {
        using var writer = new StreamWriter(path);
        writer.NewLine = "\n";
        writer.WriteLine("id,name,amount,flag,created");
        var start = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        for (var i = 0; i < rows; i++)
        {
            var ts = start.AddMinutes(i);
            writer.WriteLine(string.Join(',',
                i.ToString(CultureInfo.InvariantCulture),
                $"row-{i}",
                (i * 1.5).ToString(CultureInfo.InvariantCulture),
                (i % 2 == 0).ToString(),
                ts.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture)));
        }
    }

    private static ConnectorManifest LocalFilesManifest() => new(
        Name: "localfiles-pcp",
        ProtocolMajorMin: ProtocolVersion.Major,
        ProtocolMajorMax: ProtocolVersion.Major,
        Capabilities: new LocalFilesConnector().Capabilities.ToString()
            .Split(", ", StringSplitOptions.RemoveEmptyEntries));

    /// <summary>Mirrors ShimTests/HandshakeTests: the fixture builds to its own bin dir, a sibling of
    /// this test project's under <c>tests/</c>, resolved relative to <see cref="AppContext.BaseDirectory"/>
    /// so it tracks whichever configuration actually ran.</summary>
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
    /// bytes (<c>sun_path</c>), and the deep <c>tests/.../bin/Release/net10.0/...</c> tree this assembly
    /// lives under leaves no room for <c>control.sock</c>/<c>control.sock.data</c> on top.</summary>
    private string NewSocketDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-hostch-" + Guid.NewGuid().ToString("N")[..8]);
        _tempDirs.Add(dir);
        return dir;
    }

    private string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-hostch-data-" + Guid.NewGuid().ToString("N")[..8]);
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
