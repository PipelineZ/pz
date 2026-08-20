using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Pz.Diagnostics.Otel;

namespace Pz.Cli.Otel;

/// <summary>The ONLY place the OpenTelemetry SDK and OTLP exporter packages are referenced —
/// Pz.Engine/Pz.Diagnostics stay package-free (BCL-only), so this composition
/// root is where <see cref="PzActivitySource"/>/<see cref="PzMeters"/> actually get wired up to an
/// exporter, and ONLY when <c>--otel-endpoint</c>/<c>PZ_OTEL_ENDPOINT</c> resolves to something. When
/// <paramref name="endpoint"/> (see <see cref="Create"/>) is null, <see cref="NoOp"/> is returned: no
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

    /// <summary>Builds real, exporting providers for <paramref name="endpoint"/> (already validated by
    /// <see cref="Pz.Cli.Commands.RunCommand.TryResolveOtelEndpoint"/> — an absolute http/https URL), or
    /// <see cref="NoOp"/> when <paramref name="endpoint"/> is null (the common case: OTel not
    /// configured).</summary>
    public static OtelProviders Create(Uri? endpoint)
    {
        if (endpoint is null)
        {
            return NoOp;
        }

        var resourceBuilder = ResourceBuilder.CreateDefault().AddService("pz");

        var tracerProvider = OpenTelemetry.Sdk.CreateTracerProviderBuilder()
            .SetResourceBuilder(resourceBuilder)
            .AddSource(PzActivitySource.Name)
            .AddOtlpExporter(o => o.Endpoint = endpoint)
            .Build();

        var meterProvider = OpenTelemetry.Sdk.CreateMeterProviderBuilder()
            .SetResourceBuilder(resourceBuilder)
            .AddMeter(PzMeters.Name)
            .AddOtlpExporter(o => o.Endpoint = endpoint)
            .Build();

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
