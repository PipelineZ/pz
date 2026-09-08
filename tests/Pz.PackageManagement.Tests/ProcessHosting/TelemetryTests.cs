using System.Diagnostics;
using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using Google.Protobuf;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Trace.V1;
using Pz.Connector.LocalFiles;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;
using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.ProcessHosting;
using Pz.PackageManagement.Tests.Otlp;

namespace Pz.PackageManagement.Tests.ProcessHosting;

/// <summary>Telemetry end to end, against the real fixture and a real OTLP receiver: the child's
/// <c>pcp.*</c> spans arrive at the endpoint the host named, in the HOST's trace, under the host's
/// span; the connector's own instrument arrives on the metrics signal with the same resource; and with
/// no endpoint neither signal produces anything at all.</summary>
[Trait("Category", "Pcp")]
public sealed class TelemetryTests : IDisposable
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(20);
    private readonly List<string> _tempDirs = [];

    private static string Hex(ByteString bytes) => Convert.ToHexString(bytes.ToByteArray()).ToLowerInvariant();

    private static (ActivityListener Listener, ActivitySource Source) HostTracing()
    {
        var name = "pz-host-" + Guid.NewGuid().ToString("N");
        var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);
        return (listener, new ActivitySource(name));
    }

    [SkippableFact]
    public async Task Read_and_write_spans_land_at_the_collector_under_the_host_span()
    {
        Skip.If(OperatingSystem.IsWindows(), "AF_UNIX transport unproven on the windows runner (Winsock 10106)");

        await using var receiver = await OtlpReceiver.StartAsync();
        var (listener, source) = HostTracing();
        using var _l = listener;
        using var _s = source;

        var dataDir = NewTempDir();
        WriteCsv(Path.Combine(dataDir, "small.csv"), 5);
        await using var process = ConnectorProcess.Spawn(FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp");
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dataDir });

        string traceId, hostSpanId;
        using (var root = source.StartActivity("node.SourceLoad")!)
        {
            traceId = root.TraceId.ToHexString();
            hostSpanId = root.SpanId.ToHexString();

            await using var client = await PcpClient.ConnectAndConfigureAsync(
                process, LocalFilesManifest(), "test-instance", config,
                new HostTelemetry("run-123", receiver.Endpoint), CancellationToken.None);

            var sourceConnector = new ProcessSourceConnector(client, process);
            await using var src = await sourceConnector.OpenAsync(config, CancellationToken.None);
            var spec = new DatasetSpec("files", "orders", new Dictionary<string, object?>
            {
                ["path"] = "small.csv", ["format"] = "csv", ["columns"] = CsvColumns,
            });
            var partitions = await src.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
            await foreach (var batch in Assert.Single(partitions).ReadAsync(BatchOptions.Default, CancellationToken.None))
            {
                batch.Dispose();
            }

            var sinkConnector = new ProcessSinkConnector(client, process);
            await using var sink = await sinkConnector.OpenAsync(config, CancellationToken.None);
            var schema = BuildSchema();
            var outputSpec = new OutputSpec("lake", "out", "replace", "fail_on_change",
                new Dictionary<string, object?> { ["path"] = "out", ["format"] = "parquet" });
            await using var session = await sink.BeginWriteAsync(outputSpec, schema, CancellationToken.None);
            using (var batch = BuildBatch(schema, 0, 2))
            {
                await session.WriteBatchAsync(batch, CancellationToken.None);
            }

            await session.CommitAsync(CancellationToken.None);
        }
        // `client` disposed above: Shutdown RPC -> the child flushed before exiting.

        var spans = await receiver.WaitForSpansAsync(
            s => s.Any(x => x.Name == "pcp.read_stream") && s.Any(x => x.Name == "pcp.CommitWrite"), WaitTimeout,
            () => process.StderrTail);

        var planRead = Assert.Single(spans, s => s.Name == "pcp.PlanRead");
        Assert.Equal(traceId, Hex(planRead.TraceId));
        Assert.Equal(hostSpanId, Hex(planRead.ParentSpanId));
        Assert.Contains(planRead.Attributes, a => a.Key == "pz.instance" && a.Value.StringValue == "test-instance");

        var open = Assert.Single(spans, s => s.Name == "pcp.OpenReadStream");
        var readStream = Assert.Single(spans, s => s.Name == "pcp.read_stream");
        Assert.Equal(Hex(open.SpanId), Hex(readStream.ParentSpanId));
        Assert.Equal(traceId, Hex(readStream.TraceId));

        var begin = Assert.Single(spans, s => s.Name == "pcp.BeginWrite");
        var writeStream = Assert.Single(spans, s => s.Name == "pcp.write_stream");
        Assert.Equal(Hex(begin.SpanId), Hex(writeStream.ParentSpanId));

        Assert.DoesNotContain(spans, s => s.Name == "pcp.HostChannel");

        var resource = receiver.ResourceSpans[0].Resource;
        Assert.Contains(resource.Attributes, a => a.Key == "service.name" && a.Value.StringValue == "pz-connector");
        Assert.Contains(resource.Attributes, a => a.Key == "pz.connector.name" && a.Value.StringValue == "localfiles-pcp");
        Assert.Contains(resource.Attributes, a => a.Key == "pz.run.id" && a.Value.StringValue == "run-123");

        // The metrics half of the same contract: the fixture's counter, recorded inside Configure on
        // the SDK-provided Meter, reaches the same endpoint under the same resource. The MeterProvider
        // exports on the same Shutdown, but on its own connection, so this needs its own gate rather
        // than riding on the span wait above.
        var metrics = await receiver.WaitForMetricsAsync(
            m => m.Any(r => Instruments(r).Any(i => i.Name == FixtureCounter)), WaitTimeout, () => process.StderrTail);
        var metricResource = Assert.Single(metrics, r => Instruments(r).Any(i => i.Name == FixtureCounter));
        Assert.Contains(metricResource.Resource.Attributes,
            a => a.Key == "service.name" && a.Value.StringValue == "pz-connector");
        Assert.Contains(metricResource.Resource.Attributes,
            a => a.Key == "pz.connector.name" && a.Value.StringValue == "localfiles-pcp");
        var counter = Assert.Single(Instruments(metricResource), i => i.Name == FixtureCounter);
        Assert.Equal(1, Assert.Single(counter.Sum.DataPoints).AsInt);
    }

    /// <summary>The instrument name <c>PcpFakeConnector</c>'s <c>StagedConnector</c> records one Configure
    /// on. Spelled out rather than referenced: the fixture is a separate executable this assembly spawns,
    /// not a project reference.</summary>
    private const string FixtureCounter = "pz.fixture.configure_calls";

    private static IEnumerable<Metric> Instruments(ResourceMetrics resource) =>
        resource.ScopeMetrics.SelectMany(s => s.Metrics);

    [SkippableFact]
    public async Task Without_an_endpoint_the_connector_exports_nothing()
    {
        Skip.If(OperatingSystem.IsWindows(), "AF_UNIX transport unproven on the windows runner (Winsock 10106)");

        await using var receiver = await OtlpReceiver.StartAsync();
        var (listener, source) = HostTracing();
        using var _l = listener;
        using var _s = source;
        var dataDir = NewTempDir();
        WriteCsv(Path.Combine(dataDir, "small.csv"), 2);
        await using var process = ConnectorProcess.Spawn(FixtureExecutablePath(), NewSocketDir(), "localfiles-pcp");
        var config = new ConnectorConfig(new Dictionary<string, object?> { ["root"] = dataDir });

        using (source.StartActivity("node.SourceLoad"))
        {
            await using var client = await PcpClient.ConnectAndConfigureAsync(
                process, LocalFilesManifest(), "test-instance", config, HostTelemetry.None, CancellationToken.None);
            var connector = new ProcessSourceConnector(client, process);
            await using var src = await connector.OpenAsync(config, CancellationToken.None);
            var spec = new DatasetSpec("files", "orders", new Dictionary<string, object?>
            {
                ["path"] = "small.csv", ["format"] = "csv", ["columns"] = CsvColumns,
            });
            await src.PlanReadAsync(spec, ReadHints.None, CancellationToken.None);
        }

        // The client's dispose waits for the child to exit, and the child flushes before it exits, so
        // "nothing arrived by now" is a deterministic statement, not a race.
        Assert.True(process.HasExited);
        Assert.Empty(receiver.Spans);
        Assert.Empty(receiver.ResourceMetrics);
    }

    // ---- helpers copied from HostChannelTests / ShimTests (private there) ----------------------

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

    private static Schema BuildSchema() => new Schema.Builder()
        .Field(f => f.Name("id").DataType(Int64Type.Default).Nullable(false))
        .Field(f => f.Name("name").DataType(StringType.Default).Nullable(false))
        .Build();

    private static RecordBatch BuildBatch(Schema schema, int startId, int rows)
    {
        var idBuilder = new Int64Array.Builder();
        var nameBuilder = new StringArray.Builder();
        for (var i = 0; i < rows; i++)
        {
            idBuilder.Append(startId + i);
            nameBuilder.Append($"row-{startId + i}");
        }

        return new RecordBatch(schema, [idBuilder.Build(), nameBuilder.Build()], rows);
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
        var dir = Path.Combine(Path.GetTempPath(), "pz-otel-" + Guid.NewGuid().ToString("N")[..8]);
        _tempDirs.Add(dir);
        return dir;
    }

    private string NewTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-otel-data-" + Guid.NewGuid().ToString("N")[..8]);
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
