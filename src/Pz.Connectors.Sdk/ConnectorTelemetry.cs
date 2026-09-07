using System.Diagnostics;
using System.Diagnostics.Metrics;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Pz.Connectors.Abstractions;

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

    public ActivitySource ActivitySource { get; } = new(SourceName);

    public Meter Meter { get; } = new(SourceName);

    /// <summary>The connection name this process serves, known from Configure onward. A span tag, not
    /// a resource attribute, because providers are built at Handshake, before Configure runs.</summary>
    public string? InstanceId { get; set; }

    public bool IsExporting
    {
        get { lock (_gate) { return _tracer is not null; } }
    }

    /// <summary>Builds and registers the providers once. A second call, or an endpoint that is not an
    /// absolute http(s) URL, changes nothing: the host validated the endpoint, and a connector must not
    /// fail its handshake over telemetry.</summary>
    public void Start(string endpoint, ConnectorInfo info, string runId)
    {
        if (!Uri.TryCreate(endpoint, UriKind.Absolute, out var uri) ||
            (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps))
        {
            return;
        }

        lock (_gate)
        {
            if (_tracer is not null)
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

            var tracerBuilder = OpenTelemetry.Sdk.CreateTracerProviderBuilder()
                .SetResourceBuilder(resource)
                .AddSource(SourceName)
                .AddOtlpExporter(o =>
                {
                    o.Endpoint = uri;
                    o.TimeoutMilliseconds = (int)ExportTimeout.TotalMilliseconds;
                });
            foreach (var source in options.ActivitySources)
            {
                tracerBuilder.AddSource(source);
            }

            var meterBuilder = OpenTelemetry.Sdk.CreateMeterProviderBuilder()
                .SetResourceBuilder(resource)
                .AddMeter(SourceName)
                .AddOtlpExporter(o =>
                {
                    o.Endpoint = uri;
                    o.TimeoutMilliseconds = (int)ExportTimeout.TotalMilliseconds;
                });
            foreach (var meter in options.Meters)
            {
                meterBuilder.AddMeter(meter);
            }

            _tracer = tracerBuilder.Build();
            _meter = meterBuilder.Build();
        }
    }

    /// <summary>Flush, bounded by <see cref="FlushBound"/>, then tear down. Anything not exported by
    /// the bound is dropped: a late span is worth less than an on-time exit.</summary>
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
        tracer?.ForceFlush(bound);
        meter?.ForceFlush(bound);
        tracer?.Shutdown(bound);
        meter?.Shutdown(bound);
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
