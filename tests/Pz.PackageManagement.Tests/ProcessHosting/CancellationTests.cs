using System.Globalization;
using System.Runtime.Versioning;
using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;
using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.ProcessHosting;

namespace Pz.PackageManagement.Tests.ProcessHosting;

/// <summary>The cancellation ladder, end to end against the real fixture: token fires →
/// <c>Cancel{opId}</c> → cancel grace → <c>Shutdown</c> → shutdown grace → kill the process tree.
///
/// <para>Both facts here read the same way to the CALLER — the read ends in
/// <see cref="OperationCanceledException"/>, never a PZ03xx — and differ only in what happens to the
/// connector afterwards: a connector that acknowledges the cancel keeps running, one that ignores it is
/// condemned. Graces are compressed through the internal seam so the escalation is observed in
/// milliseconds; the only waiting done here is on <see cref="ConnectorProcess.Exited"/>, never on a
/// wall clock.</para>
///
/// <para>Unix-only, same reasoning as its siblings: the fixture serves unix domain sockets only.</para></summary>
[SupportedOSPlatform("linux")]
public sealed class CancellationTests : IDisposable
{
    private readonly List<string> _tempDirs = [];

    /// <summary>Long enough that a slow CI box cannot mistake scheduling delay for an unacknowledged
    /// cancel, short enough that the whole ladder resolves well inside a test's patience.</summary>
    private static readonly TimeSpan TestCancelGrace = TimeSpan.FromMilliseconds(500);

    private static readonly TimeSpan TestShutdownGrace = TimeSpan.FromMilliseconds(500);

    [SkippableFact]
    public async Task Cancelled_read_ends_in_OperationCanceledException_and_kills_a_connector_that_ignores_Cancel()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        await using var process = ConnectorProcess.Spawn(
            FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp", ["--endless-read", "--ignore-cancel"]);
        var exited = new TaskCompletionSource();
        process.Exited += () => exited.TrySetResult();

        await using var client = await ConnectAsync(process);
        var (_, _, partition) = await OpenEndlessPartitionAsync(client, process);

        using var cts = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var batch in partition.ReadAsync(new BatchOptions(TargetBatchBytes: 2_000), cts.Token))
            {
                batch.Dispose();
                await cts.CancelAsync();
            }
        });

        // The fixture never answers Cancel, so the ladder must escalate on its own deadline: Shutdown,
        // then the kill. Waiting on the process's own exit signal rather than a sleep is what keeps
        // this deterministic.
        await exited.Task.WaitAsync(TimeSpan.FromSeconds(30));
        Assert.True(process.HasExited);
    }

    [SkippableFact]
    public async Task Cancelled_read_leaves_a_cooperating_connector_alive()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        await using var process = ConnectorProcess.Spawn(
            FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp", ["--endless-read"]);
        var exited = new TaskCompletionSource();
        process.Exited += () => exited.TrySetResult();

        await using var client = await ConnectAsync(process);
        var (source, spec, partition) = await OpenEndlessPartitionAsync(client, process);

        using var cts = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var batch in partition.ReadAsync(new BatchOptions(TargetBatchBytes: 2_000), cts.Token))
            {
                batch.Dispose();
                await cts.CancelAsync();
            }
        });

        // Acknowledging the cancel is the whole difference: nothing escalates. Asserting a negative
        // needs a bound, so the bound is the escalation window itself -- if the ladder were going to
        // condemn this process it would already have done so several times over by the time this
        // elapses.
        var escalationWindow = TestCancelGrace + TestShutdownGrace + TimeSpan.FromSeconds(2);
        var died = await Task.WhenAny(exited.Task, Task.Delay(escalationWindow)) == exited.Task;
        Assert.False(died, "a connector that acknowledged Cancel must not be killed");

        // And the instance is genuinely still usable, not merely un-reaped.
        var schema = await source.GetSchemaAsync(spec, CancellationToken.None);
        Assert.Equal(CsvColumns.Keys, schema.Schema.FieldsList.Select(field => field.Name));
    }

    [SkippableFact]
    public async Task Dispose_does_not_return_while_an_escalation_ladder_is_still_running()
    {
        Skip.If(OperatingSystem.IsWindows(), "the fixture serves unix domain sockets only");

        // --ignore-shutdown widens the ladder's last rung to the full shutdown grace (the connector
        // acknowledges Shutdown and keeps running, so only the kill ends it), which is what makes
        // "dispose arrived mid-ladder" a state a test can actually sit in.
        await using var process = ConnectorProcess.Spawn(
            FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp",
            ["--endless-read", "--ignore-cancel", "--ignore-shutdown"]);

        var client = await ConnectAsync(process, shutdownGrace: TimeSpan.FromSeconds(3));
        var (_, _, partition) = await OpenEndlessPartitionAsync(client, process);

        using var cts = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var batch in partition.ReadAsync(new BatchOptions(TargetBatchBytes: 2_000), cts.Token))
            {
                batch.Dispose();
                await cts.CancelAsync();
            }
        });

        // Gate, not a sleep: this completes exactly when the escalation has claimed the ladder, so the
        // dispose below is guaranteed to be the one that LOSES the claim -- the only case where the
        // join is load-bearing.
        await client.EscalationClaimedLadder.WaitAsync(TimeSpan.FromSeconds(30));

        await client.DisposeAsync();
        Assert.True(
            process.HasExited,
            "DisposeAsync returned while a cancellation escalation was still reaping the child");
    }

    private async Task<PcpClient> ConnectAsync(ConnectorProcess process, TimeSpan? shutdownGrace = null)
    {
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = DataDirWithCsv() });
        var client = await PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", config, CancellationToken.None);
        client.CancelGrace = TestCancelGrace;
        client.ShutdownGrace = shutdownGrace ?? TestShutdownGrace;
        return client;
    }

    /// <summary>Plans the fixture's one CSV partition, which <c>--endless-read</c> makes replay forever
    /// -- the only shape long enough for a caller to cancel mid-read.</summary>
    private static async Task<(ISource Source, DatasetSpec Spec, IDatasetPartition Partition)> OpenEndlessPartitionAsync(
        PcpClient client, ConnectorProcess process)
    {
        var connector = new ProcessSourceConnector(client, process);
        var source = await connector.OpenAsync(ConnectorConfig.Empty, CancellationToken.None);
        var spec = new DatasetSpec("files", "orders", new Dictionary<string, object?>
        {
            ["path"] = "small.csv",
            ["format"] = "csv",
            ["columns"] = CsvColumns,
        });

        var partitions = await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        return (source, spec, Assert.Single(partitions));
    }

    private static readonly Dictionary<string, string> CsvColumns = new()
    {
        ["id"] = "bigint",
        ["name"] = "varchar",
    };

    private string DataDirWithCsv()
    {
        var dir = NewTempDir();
        using var writer = new StreamWriter(Path.Combine(dir, "small.csv"));
        writer.NewLine = "\n";
        writer.WriteLine("id,name");
        for (var i = 0; i < 200; i++)
        {
            writer.WriteLine($"{i.ToString(CultureInfo.InvariantCulture)},row-{i}");
        }

        return dir;
    }

    private static ConnectorManifest LocalFilesManifest() => new(
        Name: "localfiles-pcp",
        ProtocolMajorMin: ProtocolVersion.Major,
        ProtocolMajorMax: ProtocolVersion.Major,
        Capabilities: new LocalFilesConnector().Capabilities.ToString()
            .Split(", ", StringSplitOptions.RemoveEmptyEntries));

    private static string FixtureExecutablePath()
    {
        var baseDir = new DirectoryInfo(AppContext.BaseDirectory);
        var tfm = baseDir.Name;
        var config = baseDir.Parent!.Name;
        var testsDir = baseDir.Parent!.Parent!.Parent!.Parent!.FullName;
        var exeName = OperatingSystem.IsWindows() ? "PcpFakeConnector.exe" : "PcpFakeConnector";
        return Path.Combine(testsDir, "fixtures", "PcpFakeConnector", "bin", config, tfm, exeName);
    }

    private string NewSocketDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-cancel-" + Guid.NewGuid().ToString("N")[..8]);
        _tempDirs.Add(dir);
        return dir;
    }

    private string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-cancel-data-" + Guid.NewGuid().ToString("N")[..8]);
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
