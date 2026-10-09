using System.Diagnostics;
using System.Diagnostics.Metrics;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Sdk.Telemetry;

namespace Pz.Connectors.Sdk;

/// <summary>The connector process's own OpenTelemetry composition root. The
/// <see cref="ActivitySource"/> and <see cref="Meter"/> exist from construction so a connector can hold
/// them before the host has said anything; providers (and therefore listeners) exist only after
/// <see cref="Start"/> ran with the endpoint the host sent in <c>HostInfo</c>. Until then every
/// <c>StartActivity</c>/<c>Counter.Add</c> is the documented BCL no-op -- the same zero-cost rule the
/// engine relies on when OTel is off.</summary>
internal sealed class ConnectorTelemetry(PzConnectorHostOptions options) : IDisposable
{
    public const string SourceName = "Pz.Connector";

    /// <summary>Upper bound on flushing at shutdown. Inside the host's ten-second shutdown grace and the
    /// server's five-second host shutdown timeout, so a dead collector never turns a graceful Shutdown
    /// into a kill.</summary>
    public static readonly TimeSpan FlushBound = TimeSpan.FromSeconds(3);

    private static readonly TimeSpan ExportTimeout = TimeSpan.FromSeconds(2);

    private readonly Lock _gate = new();
    private TracerProvider? _tracer;
    private MeterProvider? _meter;

    /// <summary>Backs <see cref="InstanceId"/>. Written once, from Configure's RPC handler thread;
    /// read from every later RPC's own thread via <see cref="TraceContextServerInterceptor.Begin"/>,
    /// which has no other synchronization with Configure. Volatile so that write is visible to every
    /// reader rather than each thread being free to keep observing null past the point Configure
    /// actually returned.</summary>
    private volatile string? _instanceId;

    public ActivitySource ActivitySource { get; } = new(SourceName);

    public Meter Meter { get; } = new(SourceName);

    /// <summary>The host's id for this connector instance (the <c>instance_id</c> it sent with Configure:
    /// a connection name when the host threaded one in, else <c>&lt;connector&gt;#&lt;n&gt;</c>), known from
    /// Configure onward. A span tag, not a resource attribute, because providers are built at Handshake,
    /// before Configure runs.</summary>
    public string? InstanceId
    {
        get => _instanceId;
        set => _instanceId = value;
    }

    public bool IsExporting
    {
        get { lock (_gate) { return _tracer is not null || _meter is not null; } }
    }

    /// <summary>The pre-0.9.1 entry point: a gRPC endpoint for both signals.</summary>
    public void Start(string endpoint, ConnectorInfo info, string runId) =>
        Start(new TelemetryTarget(endpoint, null, null, null), info, runId);

    /// <summary>Builds and registers the providers once. A second call, or a URL that is not an absolute
    /// http(s) URL, changes nothing for that signal: the host validated it, and a connector must not fail
    /// its handshake over telemetry. A URL that could not be used is reported on stderr -- silence would
    /// leave an operator staring at a backend that never receives anything -- matching what the Rust SDK
    /// prints. URLs are the host's own addresses, not secrets; the headers file's content never reaches
    /// stderr. gRPC exports both signals to one endpoint (cumulative metrics, as before 0.9.1);
    /// http/protobuf exports each signal to its own URL with the file's headers, delta temporality and
    /// base-2 exponential histograms.</summary>
    public void Start(TelemetryTarget target, ConnectorInfo info, string runId)
    {
        var http = target.GrpcEndpoint is null;
        var traces = Usable(http ? target.TracesEndpoint : target.GrpcEndpoint);
        var metrics = Usable(http ? target.MetricsEndpoint : target.GrpcEndpoint);
        if (traces is null && metrics is null)
        {
            return;
        }

        lock (_gate)
        {
            if (_tracer is not null || _meter is not null)
            {
                return;
            }

            var attributes = new List<KeyValuePair<string, object>>
            {
                new("pz.connector.name", info.Name),
            };
            if (runId.Length > 0)
            {
                attributes.Add(new("pz.run.id", runId));
            }

            var resource = ResourceBuilder.CreateDefault()
                .AddService("pz-connector", serviceVersion: info.Version)
                .AddAttributes(attributes);

            // One stderr note per process, not one per signal: each exporter owns its own handler.
            var noted = 0;
            void NoteOnce(string text)
            {
                if (Interlocked.Exchange(ref noted, 1) == 0) Console.Error.WriteLine($"pz connector: telemetry: {text}");
            }

            void Exporter(OtlpExporterOptions o, Uri url)
            {
                o.Endpoint = url;
                o.TimeoutMilliseconds = (int)ExportTimeout.TotalMilliseconds;
                o.Protocol = http ? OtlpExportProtocol.HttpProtobuf : OtlpExportProtocol.Grpc;
                if (http)
                {
                    // OTel 1.16 bounds an HTTP export by the client's own Timeout, not TimeoutMilliseconds.
                    o.HttpClientFactory = () => new HttpClient(
                        new HeadersFileHandler(target.HeadersFile, NoteOnce) { InnerHandler = new HttpClientHandler() })
                    {
                        Timeout = ExportTimeout,
                    };
                }
            }

            if (traces is not null)
            {
                var tracerBuilder = OpenTelemetry.Sdk.CreateTracerProviderBuilder()
                    .SetResourceBuilder(resource)
                    .AddSource(SourceName)
                    .AddOtlpExporter(o =>
                    {
                        Exporter(o, traces);
                        // Over HTTP, at most 256 spans a request keeps a gzipped body under Azure Monitor's 1 MB limit.
                        if (http)
                        {
                            o.BatchExportProcessorOptions = new() { MaxExportBatchSize = 256 };
                        }
                    });
                foreach (var source in options.ActivitySources)
                {
                    tracerBuilder.AddSource(source);
                }

                _tracer = tracerBuilder.Build();
            }

            if (metrics is not null)
            {
                var meterBuilder = OpenTelemetry.Sdk.CreateMeterProviderBuilder()
                    .SetResourceBuilder(resource)
                    .AddMeter(SourceName)
                    .AddOtlpExporter((o, reader) =>
                    {
                        Exporter(o, metrics);
                        reader.TemporalityPreference = http
                            ? MetricReaderTemporalityPreference.Delta
                            : MetricReaderTemporalityPreference.Cumulative;
                    });
                if (http)
                {
                    meterBuilder.AddView(instrument => IsHistogram(instrument) ? new Base2ExponentialBucketHistogramConfiguration() : null);
                }

                foreach (var meter in options.Meters)
                {
                    meterBuilder.AddMeter(meter);
                }

                _meter = meterBuilder.Build();
            }
        }
    }

    private static Uri? Usable(string? url)
    {
        if (url is null)
        {
            return null;
        }

        if (Uri.TryCreate(url, UriKind.Absolute, out var uri) && (uri.Scheme == Uri.UriSchemeHttp || uri.Scheme == Uri.UriSchemeHttps))
        {
            return uri;
        }

        Console.Error.WriteLine($"pz connector: telemetry: otel endpoint is not an absolute http(s) URL: {url}");
        return null;
    }

    private static bool IsHistogram(Instrument instrument) =>
        instrument.GetType() is { IsGenericType: true } type && type.GetGenericTypeDefinition() == typeof(Histogram<>);

    /// <summary>Shutdown -- which performs a final flush itself, so no separate ForceFlush is needed --
    /// bounded by <see cref="FlushBound"/> IN AGGREGATE across both providers, then tear down. The two
    /// Shutdowns run concurrently so a slow/unreachable collector on one signal never adds its bound to
    /// the other's; disposal waits for both to return, since disposing a provider mid-Shutdown is unsafe.
    /// Anything not exported by the bound is dropped: a late span is worth less than an on-time exit.</summary>
    public void FlushAndDispose()
    {
        TracerProvider? tracer;
        MeterProvider? meter;
        lock (_gate)
        {
            tracer = _tracer;
            meter = _meter;
            _tracer = null;
            _meter = null;
        }

        var bound = (int)FlushBound.TotalMilliseconds;
        Task.WaitAll(
            Task.Run(() => tracer?.Shutdown(bound)),
            Task.Run(() => meter?.Shutdown(bound)));
        tracer?.Dispose();
        meter?.Dispose();
    }

    public void Dispose()
    {
        FlushAndDispose();
        ActivitySource.Dispose();
        Meter.Dispose();
    }
}
