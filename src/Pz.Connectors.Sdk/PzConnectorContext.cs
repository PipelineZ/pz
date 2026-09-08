using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;

namespace Pz.Connectors.Sdk;

/// <summary>What the SDK hands a connector at construction. <see cref="LoggerFactory"/> produces
/// loggers whose output reaches the pz host as connector log events; the host renders message and
/// fields verbatim, so never log configuration values or payloads. <see cref="ActivitySource"/> and
/// <see cref="Meter"/> are exported to the host's OTLP collector when the host configured one, under
/// the engine's own node span; with no collector every call on them is a no-op. Never put a
/// configuration value in a span name, tag, or metric label.</summary>
public sealed class PzConnectorContext
{
    internal PzConnectorContext(ILoggerFactory loggerFactory, ConnectorTelemetry telemetry)
    {
        LoggerFactory = loggerFactory;
        ActivitySource = telemetry.ActivitySource;
        Meter = telemetry.Meter;
    }

    public ILoggerFactory LoggerFactory { get; }

    public ActivitySource ActivitySource { get; }

    public Meter Meter { get; }
}
