using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;
using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.ProcessHosting;

namespace Pz.PackageManagement.Tests.ProcessHosting;

/// <summary>The run context crosses PCP: the host shims carry <see cref="ConnectorRunContext"/> on every request
/// that may open the connector's source or sink and on a scoped connection check, and the SDK hands it to the
/// connector's <see cref="IRunContextAware"/>/<see cref="IScopedConnectionCheck"/>. Driven against the real
/// <c>PcpFakeConnector</c> in its <c>--report-run-context</c> mode, which answers with what it was given.</summary>
[Trait("Category", "Pcp")]
public sealed class RunContextTests : IDisposable
{
    private readonly List<string> _tempDirs = [];

    private static readonly DatasetSpec Spec = new("files", "orders", new Dictionary<string, object?>
    {
        ["path"] = "x.csv", ["format"] = "csv",
    });

    private async Task<(ConnectorProcess Process, PcpClient Client, ConnectorConfig Config)> SpawnAsync()
    {
        var process = ConnectorProcess.Spawn(
            FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp", ["--report-run-context"]);
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = NewTempDir() });
        var client = await PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", config, CancellationToken.None);
        return (process, client, config);
    }

    [Fact]
    public async Task A_source_opened_for_a_node_receives_its_run_and_node()
    {
        var (process, client, config) = await SpawnAsync();
        await using var _ = process;
        await using var __ = client;
        await using var source = await new ProcessSourceConnector(client, process).OpenAsync(config, CancellationToken.None);
        ((IRunContextAware)source).UseRunContext(new ConnectorRunContext("run-1", "node-a"));

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.GetSchemaAsync(Spec, CancellationToken.None));

        Assert.Contains("fixture: source run context run-1/node-a", ex.Message);
    }

    [Fact]
    public async Task The_run_start_crosses_with_the_run_context()
    {
        var (process, client, config) = await SpawnAsync();
        await using var _ = process;
        await using var __ = client;
        await using var source = await new ProcessSourceConnector(client, process).OpenAsync(config, CancellationToken.None);
        ((IRunContextAware)source).UseRunContext(new ConnectorRunContext("run-1", "node-a")
        {
            StartedAt = new DateTimeOffset(2026, 11, 7, 23, 59, 58, TimeSpan.Zero),
        });

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.GetSchemaAsync(Spec, CancellationToken.None));

        Assert.Contains("fixture: source run context run-1/node-a@2026-11-07T23:59:58.0000000+00:00", ex.Message);
    }

    [Fact]
    public async Task A_source_the_host_names_no_run_for_behaves_as_before()
    {
        var (process, client, config) = await SpawnAsync();
        await using var _ = process;
        await using var __ = client;
        await using var source = await new ProcessSourceConnector(client, process).OpenAsync(config, CancellationToken.None);

        var ex = await Assert.ThrowsAsync<PzConnectorException>(
            async () => await source.GetSchemaAsync(Spec, CancellationToken.None));

        Assert.Contains("fixture: source run context none", ex.Message);
    }

    [Fact]
    public async Task A_sink_opened_for_a_node_receives_its_run_and_node()
    {
        var (process, client, config) = await SpawnAsync();
        await using var _ = process;
        await using var __ = client;
        await using var sink = await new ProcessSinkConnector(client, process).OpenAsync(config, CancellationToken.None);
        ((IRunContextAware)sink).UseRunContext(new ConnectorRunContext("run-1", "node-b"));

        var schema = new Schema([new Field("id", Int64Type.Default, true)], null);
        var ex = await Assert.ThrowsAsync<PzConnectorException>(async () => await sink.BeginWriteAsync(
            new OutputSpec("files", "out", "replace", "fail_on_change", new Dictionary<string, object?> { ["format"] = "csv" }),
            schema, CancellationToken.None));

        Assert.Contains("fixture: sink run context run-1/node-b", ex.Message);
    }

    [Fact]
    public async Task A_scoped_check_carries_the_pass_and_a_plain_check_carries_none()
    {
        var (process, client, config) = await SpawnAsync();
        await using var _ = process;
        await using var __ = client;
        var connector = new ProcessSourceConnector(client, process);

        var scoped = await ((IScopedConnectionCheck)connector).CheckConnectionAsync(
            config, new ConnectorRunContext("validate-1", null), CancellationToken.None);
        var plain = await connector.CheckConnectionAsync(config, CancellationToken.None);

        Assert.Equal("run context validate-1/-", scoped.Message);
        Assert.Equal("run context none", plain.Message);
    }

    [Fact]
    public async Task An_older_host_request_without_the_field_still_checks()
    {
        // What a host built before the field existed sends: a CheckRequest with config only.
        var (process, client, config) = await SpawnAsync();
        await using var _ = process;
        await using var __ = client;

        var response = await client.Grpc.CheckConnectionAsync(
            new CheckRequest { Config = MessageMapping.ToStruct(config.Values) }).ResponseAsync;

        Assert.True(response.Ok);
        Assert.Equal("run context none", response.Message);
    }

    private async Task<(IDatasetPartition Partition, Func<Task> Read, IAsyncDisposable[] Owned)> PlanOneAsync(
        params string[] args)
    {
        var dataDir = NewTempDir();
        File.WriteAllText(Path.Combine(dataDir, "x.csv"), "id\n1\n");
        var process = ConnectorProcess.Spawn(FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp", args);
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dataDir });
        var client = await PcpClient.ConnectAndConfigureAsync(
            process, LocalFilesManifest(), "test-instance", config, CancellationToken.None);
        var source = await new ProcessSourceConnector(client, process).OpenAsync(config, CancellationToken.None);
        var spec = new DatasetSpec("files", "orders", new Dictionary<string, object?>
        {
            ["path"] = "x.csv", ["format"] = "csv", ["columns"] = new Dictionary<string, object?> { ["id"] = "bigint" },
        });
        var partition = Assert.Single(await source.PlanReadAsync(spec, ReadHints.None, CancellationToken.None));
        return (partition, async () =>
        {
            await foreach (var batch in partition.ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batch.Dispose();
            }
        }, [source, client, process]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task A_partition_reports_a_skipped_read_across_the_process_boundary(bool skip)
    {
        var (partition, read, owned) = await PlanOneAsync(skip ? ["--skip-reads"] : []);
        try
        {
            await read();

            Assert.Equal(skip ? "fixture rule (skip) matched" : null,
                Assert.IsAssignableFrom<ISkippedReadPartition>(partition).SkippedBy);
        }
        finally
        {
            foreach (var o in owned)
            {
                await o.DisposeAsync();
            }
        }
    }

    [Fact]
    public async Task A_read_whose_skip_report_cannot_be_read_fails_transiently()
    {
        var (_, read, owned) = await PlanOneAsync("--fail-read-stats");
        try
        {
            var ex = await Assert.ThrowsAsync<PzConnectorException>(read);

            Assert.True(ex.IsTransient);
            Assert.Contains("whether a rule skipped part of it could not be read", ex.Message);
        }
        finally
        {
            foreach (var o in owned)
            {
                await o.DisposeAsync();
            }
        }
    }

    [Fact]
    public async Task A_connector_without_the_read_stats_call_reads_as_not_skipped()
    {
        var (partition, read, owned) = await PlanOneAsync("--skip-reads", "--unimplemented-read-stats");
        try
        {
            await read();

            Assert.Null(Assert.IsAssignableFrom<ISkippedReadPartition>(partition).SkippedBy);
        }
        finally
        {
            foreach (var o in owned)
            {
                await o.DisposeAsync();
            }
        }
    }

    [Fact]
    public async Task A_connector_that_exits_during_the_stats_poll_reports_its_exit_not_the_poll()
    {
        var (_, read, owned) = await PlanOneAsync("--exit-on-read-stats");
        try
        {
            var ex = await Assert.ThrowsAsync<PzConnectorException>(read);

            Assert.Contains("connector process exited after completing a partition read", ex.Message);
            Assert.DoesNotContain("whether a rule skipped part of it could not be read", ex.Message);
        }
        finally
        {
            foreach (var o in owned)
            {
                await o.DisposeAsync();
            }
        }
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

    // Short, outside the test output tree: a unix domain socket path is capped at roughly 104 bytes.
    private string NewSocketDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-runctx-" + Guid.NewGuid().ToString("N")[..8]);
        _tempDirs.Add(dir);
        return dir;
    }

    private string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-runctx-data-" + Guid.NewGuid().ToString("N")[..8]);
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
