using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Types;
using Google.Protobuf;
using OpenTelemetry.Proto.Metrics.V1;
using OpenTelemetry.Proto.Trace.V1;
using Pz.Connectors.Abstractions;
using Pz.PackageManagement.Hosting;
using Pz.PackageManagement.ProcessHosting;
using Pz.PackageManagement.Tests.Otlp;

namespace Pz.PackageManagement.Tests.ProcessHosting;

/// <summary>The Rust SDK's half of telemetry export -- spans and meters -- against the built
/// <c>memory_sink</c> example.
/// Skips when the example is not built (scripts/rust-conformance.sh builds it and then runs this
/// category), so a contributor without cargo still gets a green suite.</summary>
[Trait("Category", "RustPcp")]
public sealed class RustSinkTelemetryTests : IDisposable
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(20);
    private readonly List<string> _tempDirs = [];

    private static string Hex(ByteString bytes) => Convert.ToHexString(bytes.ToByteArray()).ToLowerInvariant();

    /// <summary>Walks up from the test binary to the directory holding <c>Pz.slnx</c>.</summary>
    private static string? MemorySinkPath()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Pz.slnx")))
        {
            dir = dir.Parent;
        }

        if (dir is null)
        {
            return null;
        }

        var path = Path.Combine(dir.FullName, "rust", "target", "debug", "examples", "memory_sink");
        return File.Exists(path) ? path : null;
    }

    /// <summary>The instrument <c>rust/pz-connector/examples/memory_sink.rs</c> records one
    /// <c>begin_write</c> on. Spelled out rather than referenced: the example is a separate executable
    /// this assembly spawns.</summary>
    private const string ExampleCounter = "pz.memory_sink.begin_write_calls";

    private static IEnumerable<Metric> Instruments(ResourceMetrics resource) =>
        resource.ScopeMetrics.SelectMany(s => s.Metrics);

    [SkippableFact]
    public async Task Write_spans_and_meters_from_the_rust_sdk_reach_the_collector()
    {
        Skip.If(OperatingSystem.IsWindows(), "AF_UNIX transport unproven on the windows runner (Winsock 10106)");
        var binary = MemorySinkPath();
        Skip.If(binary is null, "rust/target/debug/examples/memory_sink is not built (cargo build --example memory_sink)");

        await using var receiver = await OtlpReceiver.StartAsync();
        var sourceName = "pz-host-" + Guid.NewGuid().ToString("N");
        using var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name == sourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(listener);
        using var source = new ActivitySource(sourceName);

        await using var process = ConnectorProcess.Spawn(binary!, NewSocketDir(), "memory-sink");
        var config = new ConnectorConfig(new Dictionary<string, object?>());

        string traceId, hostSpanId;
        using (var root = source.StartActivity("node.SinkWrite")!)
        {
            traceId = root.TraceId.ToHexString();
            hostSpanId = root.SpanId.ToHexString();

            await using var client = await PcpClient.ConnectAndConfigureAsync(
                process, null, "mem", config, new HostTelemetry("run-rs", receiver.Endpoint), CancellationToken.None);
            var connector = new ProcessSinkConnector(client, process);
            await using var sink = await connector.OpenAsync(config, CancellationToken.None);
            var schema = BuildSchema();
            var spec = new OutputSpec("mem", "out", "replace", "match", new Dictionary<string, object?>());
            await using var session = await sink.BeginWriteAsync(spec, schema, CancellationToken.None);
            using (var batch = BuildBatch(schema, 0, 3))
            {
                await session.WriteBatchAsync(batch, CancellationToken.None);
            }

            var result = await session.CommitAsync(CancellationToken.None);
            Assert.Equal(3, result.RowsWritten);
        }

        // A batching receiver can flush pcp.CommitWrite before pcp.write_stream's own export lands, so
        // wait on all three spans this assertion block needs rather than just the last one issued.
        var spans = await receiver.WaitForSpansAsync(
            s => s.Any(x => x.Name == "pcp.BeginWrite") && s.Any(x => x.Name == "pcp.write_stream")
                && s.Any(x => x.Name == "pcp.CommitWrite"),
            WaitTimeout);

        var begin = Assert.Single(spans, s => s.Name == "pcp.BeginWrite");
        Assert.Equal(traceId, Hex(begin.TraceId));
        Assert.Equal(hostSpanId, Hex(begin.ParentSpanId));
        Assert.Contains(begin.Attributes, a => a.Key == "pz.instance" && a.Value.StringValue == "mem");

        var writeStream = Assert.Single(spans, s => s.Name == "pcp.write_stream");
        Assert.Equal(Hex(begin.SpanId), Hex(writeStream.ParentSpanId));
        Assert.DoesNotContain(spans, s => s.Name == "pcp.HostChannel");

        // Pick the resource whose scope actually carries the memory-sink spans rather than assuming a
        // single ResourceSpans entry always arrives first.
        var resourceSpans = Assert.Single(receiver.ResourceSpans,
            r => r.ScopeSpans.Any(ss => ss.Spans.Any(sp => sp.Name == "pcp.BeginWrite")));
        var resource = resourceSpans.Resource;
        Assert.Contains(resource.Attributes, a => a.Key == "service.name" && a.Value.StringValue == "pz-connector");
        Assert.Contains(resource.Attributes, a => a.Key == "pz.connector.name" && a.Value.StringValue == "memory-sink");
        Assert.Contains(resource.Attributes, a => a.Key == "pz.run.id" && a.Value.StringValue == "run-rs");

        // The meter half: the example's counter, recorded inside begin_write on the meter the SDK's
        // provider backs, arrives under the same resource. The PeriodicReader's export interval is far
        // longer than this test, so what lands here is the flush the Shutdown RPC drove -- the same
        // bounded shutdown the spans above rode on, over its own connection.
        var metrics = await receiver.WaitForMetricsAsync(
            m => m.Any(r => Instruments(r).Any(i => i.Name == ExampleCounter)), WaitTimeout);
        var metricResource = Assert.Single(metrics, r => Instruments(r).Any(i => i.Name == ExampleCounter));
        Assert.Contains(metricResource.Resource.Attributes,
            a => a.Key == "service.name" && a.Value.StringValue == "pz-connector");
        Assert.Contains(metricResource.Resource.Attributes,
            a => a.Key == "pz.connector.name" && a.Value.StringValue == "memory-sink");
        Assert.Contains(metricResource.Resource.Attributes,
            a => a.Key == "pz.run.id" && a.Value.StringValue == "run-rs");
        var counter = Assert.Single(Instruments(metricResource), i => i.Name == ExampleCounter);
        Assert.Equal(1, Assert.Single(counter.Sum.DataPoints).AsInt);
    }

    private string NewSocketDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "pz-rsotel-" + Guid.NewGuid().ToString("N")[..8]);
        _tempDirs.Add(dir);
        return dir;
    }

    public void Dispose()
    {
        foreach (var dir in _tempDirs)
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort */ }
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
}
