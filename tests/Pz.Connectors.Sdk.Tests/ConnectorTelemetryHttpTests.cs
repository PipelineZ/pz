using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Pz.Cli.Tests.Otel;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Protocol.V1;
using Pz.Connectors.Sdk;

namespace Pz.Connectors.Sdk.Tests;

/// <summary>OTLP/HTTP export from a connector process: per-signal URLs, the headers file, delta and exponential
/// metrics, and the same flush bound as gRPC. Serialized with the other telemetry classes (process-wide listeners).</summary>
[Collection("connector-telemetry-serialized")]
public sealed class ConnectorTelemetryHttpTests : IAsyncLifetime
{
    private static readonly ConnectorInfo Info = new("fake", "1.2.3", ProtocolVersion.Major);
    private readonly OtlpHttpReceiver _receiver = new();
    private readonly string _headers = Path.GetTempFileName();

    public Task InitializeAsync() => _receiver.StartAsync();

    public async Task DisposeAsync()
    {
        await _receiver.DisposeAsync();
        File.Delete(_headers);
    }

    private TelemetryTarget Http(bool traces = true, bool metrics = true) => new(
        null, traces ? _receiver.TracesUrl.AbsoluteUri : null, metrics ? _receiver.MetricsUrl.AbsoluteUri : null, _headers);

    [Fact]
    public async Task Traces_and_metrics_reach_their_urls_with_the_files_header()
    {
        await File.WriteAllTextAsync(_headers, "Authorization=Bearer abc\n");
        using (var telemetry = new ConnectorTelemetry(new PzConnectorHostOptions()))
        {
            telemetry.Start(Http(), Info, "run-1");
            using (telemetry.ActivitySource.StartActivity("connector.read")) { }
            telemetry.Meter.CreateCounter<long>("rows").Add(3);
            telemetry.FlushAndDispose();
        }

        var traces = Assert.Single(_receiver.Requests, r => r.Path == "/traces");
        Assert.Equal("Bearer abc", traces.Authorization);
        Assert.Equal("gzip", traces.Encoding);
        Assert.Contains(traces.Traces!.ResourceSpans.SelectMany(r => r.ScopeSpans).SelectMany(s => s.Spans), s => s.Name == "connector.read");
        var metrics = Assert.Single(_receiver.Requests, r => r.Path == "/metrics");
        Assert.Equal("Bearer abc", metrics.Authorization);
    }

    [Fact]
    public void Http_metrics_are_delta_with_exponential_histograms()
    {
        using (var telemetry = new ConnectorTelemetry(new PzConnectorHostOptions()))
        {
            telemetry.Start(Http(traces: false), Info, "");
            telemetry.Meter.CreateCounter<long>("rows").Add(3);
            telemetry.Meter.CreateHistogram<double>("latency").Record(4.5);
            telemetry.FlushAndDispose();
        }

        var all = _receiver.Requests.Where(r => r.Metrics is not null)
            .SelectMany(r => r.Metrics!.ResourceMetrics).SelectMany(r => r.ScopeMetrics).SelectMany(s => s.Metrics).ToList();
        Assert.Equal(OpenTelemetry.Proto.Metrics.V1.AggregationTemporality.Delta, all.Single(m => m.Name == "rows").Sum.AggregationTemporality);
        Assert.NotNull(all.Single(m => m.Name == "latency").ExponentialHistogram);
        Assert.DoesNotContain(_receiver.Requests, r => r.Path == "/traces");
    }

    [Fact]
    public void Http_span_batches_stay_at_256_for_the_1_mb_limit()
    {
        using (var telemetry = new ConnectorTelemetry(new PzConnectorHostOptions()))
        {
            telemetry.Start(Http(metrics: false), Info, "");
            for (var i = 0; i < 600; i++)
                using (telemetry.ActivitySource.StartActivity("connector.page")) { }
            telemetry.FlushAndDispose();
        }

        var counts = _receiver.Requests.Where(r => r.Traces is not null)
            .Select(r => r.Traces!.ResourceSpans.SelectMany(x => x.ScopeSpans).Sum(x => x.Spans.Count)).ToList();
        Assert.Equal(600, counts.Sum());
        Assert.All(counts, c => Assert.True(c <= 256, $"{c} spans in one request"));
    }

    [Fact]
    public void Http_with_no_url_starts_nothing()
    {
        using var telemetry = new ConnectorTelemetry(new PzConnectorHostOptions());
        telemetry.Start(new TelemetryTarget(null, null, null, _headers), Info, "");
        Assert.False(telemetry.IsExporting);
        Assert.False(telemetry.ActivitySource.HasListeners());
    }

    [Fact]
    public async Task Flush_against_an_http_receiver_that_never_answers_is_bounded()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        _ = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    using var client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                    await Task.Delay(Timeout.InfiniteTimeSpan).ConfigureAwait(false);
                }
            }
            catch (Exception)
            {
                // Listener stopped from the finally block below.
            }
        });

        try
        {
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            using var telemetry = new ConnectorTelemetry(new PzConnectorHostOptions());
            telemetry.Start(new TelemetryTarget(null, $"http://127.0.0.1:{port}/traces", $"http://127.0.0.1:{port}/metrics", null), Info, "");
            using (telemetry.ActivitySource.StartActivity("pending")) { }

            var stopwatch = Stopwatch.StartNew();
            await Task.Run(telemetry.FlushAndDispose).WaitAsync(ConnectorTelemetry.FlushBound * 2);
            Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(4), $"flush took {stopwatch.Elapsed}");
        }
        finally
        {
            listener.Stop();
        }
    }

    [Fact]
    public void HostInfo_maps_to_a_target_per_protocol()
    {
        var grpc = TelemetryTarget.FromHostInfo(new HostInfo { OtelEndpoint = "http://c:4317" });
        Assert.Equal(new TelemetryTarget("http://c:4317", null, null, null), grpc);

        var http = TelemetryTarget.FromHostInfo(new HostInfo
        {
            OtelProtocol = "http/protobuf", OtelTracesEndpoint = "https://t/x", OtelMetricsEndpoint = "https://m/x", OtelHeadersFile = "/h",
        });
        Assert.Equal(new TelemetryTarget(null, "https://t/x", "https://m/x", "/h"), http);

        Assert.Null(TelemetryTarget.FromHostInfo(new HostInfo()));
        Assert.Null(TelemetryTarget.FromHostInfo(new HostInfo { OtelProtocol = "http/protobuf" }));
    }
}
