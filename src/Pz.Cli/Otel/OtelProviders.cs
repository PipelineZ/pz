using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Pz.Diagnostics.Otel;

namespace Pz.Cli.Otel;

/// <summary>The ONLY place the OpenTelemetry SDK and OTLP exporter packages are referenced —
/// Pz.Engine/Pz.Diagnostics stay package-free (BCL-only), so this composition
/// root is where <see cref="PzActivitySource"/>/<see cref="PzMeters"/> actually get wired up to an
/// exporter, and ONLY when the resolved <see cref="OtelOptions"/> are on. When they are off
/// (see <see cref="Create"/>), <see cref="NoOp"/> is returned: no
/// listener is ever registered anywhere, so every <c>StartActivity</c>/<c>Counter.Add</c> call in the
/// engine stays the documented zero-cost BCL no-op <see cref="PzActivitySource"/> relies on.</summary>
public sealed class OtelProviders : IAsyncDisposable
{
    private static readonly OtelProviders NoOp = new(null, null);

    private readonly TracerProvider? _tracerProvider;
    private readonly MeterProvider? _meterProvider;

    private OtelProviders(TracerProvider? tracerProvider, MeterProvider? meterProvider)
    {
        _tracerProvider = tracerProvider;
        _meterProvider = meterProvider;
    }

    /// <summary>Spans per OTLP/HTTP request: a full batch of pz's spans gzips to far below Azure Monitor's 1 MB cap.</summary>
    public const int MaxSpanBatch = 256;

    /// <summary>Real exporting providers for <paramref name="options"/>, or <see cref="NoOp"/> when telemetry is off.
    /// gRPC keeps the pre-0.9.1 shape (one endpoint, cumulative metrics, explicit histograms). http/protobuf sends each
    /// signal to its own full URL through <see cref="HeadersFileHandler"/>, with delta temporality and a base-2
    /// exponential <c>pz.node.duration</c> (the shape Application Insights' views expect), and caps span batches so a
    /// gzipped request stays under Azure Monitor's 1 MB limit. A signal with no URL is not exported. Every option is set
    /// here, so ambient OTEL_EXPORTER_OTLP_* protocol/endpoint variables cannot change it.</summary>
    public static OtelProviders Create(OtelOptions options, Action<string> notice)
    {
        if (!options.IsOn) return NoOp;
        var resourceBuilder = ResourceBuilder.CreateDefault().AddService("pz");
        var http = options.Protocol == OtelProtocol.HttpProtobuf;
        HttpClient Client() => new(new HeadersFileHandler(options.HeadersFile, notice) { InnerHandler = new HttpClientHandler() });

        void Exporter(OtlpExporterOptions o, Uri? signalUrl)
        {
            if (http)
            {
                o.Protocol = OtlpExportProtocol.HttpProtobuf;
                o.Endpoint = signalUrl!;
                o.HttpClientFactory = Client;
            }
            else
            {
                o.Protocol = OtlpExportProtocol.Grpc;
                o.Endpoint = options.Endpoint!;
            }
        }

        TracerProvider? tracerProvider = null;
        if (!http || options.TracesEndpoint is not null)
        {
            tracerProvider = OpenTelemetry.Sdk.CreateTracerProviderBuilder()
                .SetResourceBuilder(resourceBuilder)
                .AddSource(PzActivitySource.Name)
                .AddOtlpExporter(o =>
                {
                    Exporter(o, options.TracesEndpoint);
                    o.BatchExportProcessorOptions = new() { MaxExportBatchSize = MaxSpanBatch };
                })
                .Build();
        }

        MeterProvider? meterProvider = null;
        if (!http || options.MetricsEndpoint is not null)
        {
            var meters = OpenTelemetry.Sdk.CreateMeterProviderBuilder()
                .SetResourceBuilder(resourceBuilder)
                .AddMeter(PzMeters.Name)
                .AddOtlpExporter((o, reader) =>
                {
                    Exporter(o, options.MetricsEndpoint);
                    reader.TemporalityPreference = http ? MetricReaderTemporalityPreference.Delta : MetricReaderTemporalityPreference.Cumulative;
                });
            if (http) meters.AddView("pz.node.duration", new Base2ExponentialBucketHistogramConfiguration());
            meterProvider = meters.Build();
        }

        return new OtelProviders(tracerProvider, meterProvider);
    }

    /// <summary>Flushes (via the SDK providers' own Dispose-triggered shutdown) and tears down the
    /// exporters. A no-op for <see cref="NoOp"/>. Callers dispose this AFTER printing the run summary so
    /// the export covers the whole run, including its terminal events.</summary>
    public ValueTask DisposeAsync()
    {
        _tracerProvider?.Dispose();
        _meterProvider?.Dispose();
        return ValueTask.CompletedTask;
    }
}
